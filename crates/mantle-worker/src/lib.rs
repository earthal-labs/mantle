//! Background cache warmer — listens for catalog footprint inserts and pre-warms Redis.

mod catalog;
mod prefetch;
mod storage;

pub use catalog::FOOTPRINT_INSERTED_CHANNEL;

use crate::prefetch::fetch_zmetadata_blob;
use crate::storage::{build_object_store, object_path, parse_storage_uri};
use mantle_cache::{CacheClient, RedisCacheClient};
use mantle_catalog::{CatalogClient, PostgresDuckLakeCatalog};
use mantle_config::MantleConfig;
use object_store::ObjectStore;
use serde::Deserialize;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
struct AssetRow {
    id: Uuid,
    format: String,
    storage_uri: String,
}

#[derive(Debug, Deserialize)]
struct FootprintNotifyPayload {
    scene_id: Uuid,
}

pub struct CacheWarmer {
    config: Arc<MantleConfig>,
    pool: PgPool,
    cache: Arc<RedisCacheClient>,
    store: Arc<dyn object_store::ObjectStore>,
    shutdown: CancellationToken,
}

impl CacheWarmer {
    pub async fn new(config: MantleConfig) -> anyhow::Result<Self> {
        let config = Arc::new(config);
        let pool = PgPool::connect(&config.catalog.postgres_url).await?;
        let cache = Arc::new(RedisCacheClient::connect(&config.cache).await?);
        let store = build_object_store(&config.storage)?;

        Ok(Self {
            config,
            pool,
            cache,
            store,
            shutdown: CancellationToken::new(),
        })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        info!(
            channel = FOOTPRINT_INSERTED_CHANNEL,
            redis = %self.config.cache.redis_url,
            "mantle-worker started"
        );

        let shutdown_watcher = self.shutdown.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                info!("shutdown signal received");
                shutdown_watcher.cancel();
            }
        });

        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(FOOTPRINT_INSERTED_CHANNEL).await?;
        info!(channel = FOOTPRINT_INSERTED_CHANNEL, "listening for footprint inserts");

        // Separate connection from `self.pool`: this one owns the DuckLake
        // session needed to physically reclaim a purged service's Parquet file.
        let catalog: Arc<dyn CatalogClient> = Arc::new(
            PostgresDuckLakeCatalog::connect(Arc::new(self.config.catalog.clone())).await?,
        );
        let mut purge_ticker = tokio::time::interval(Duration::from_secs(
            self.config.catalog.purge_poll_interval_seconds.max(1),
        ));
        info!(
            retention_days = self.config.catalog.purge_retention_days,
            poll_interval_seconds = self.config.catalog.purge_poll_interval_seconds,
            "scheduled service purge enabled"
        );

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    info!("mantle-worker shutting down");
                    break;
                }
                _ = purge_ticker.tick() => {
                    self.run_purge_tick(catalog.as_ref()).await;
                }
                notification = listener.recv() => {
                    match notification {
                        Ok(notification) => {
                            if let Err(err) = self.handle_notification(notification.payload()).await {
                                error!(error = %err, "failed to warm cache for notification");
                            }
                        }
                        Err(err) => {
                            error!(error = %err, "postgres listener error");
                            if self.shutdown.is_cancelled() {
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Purge any services whose soft-delete retention window has elapsed.
    /// Logs and continues past individual failures so one bad row doesn't
    /// block the rest of the batch or the next tick.
    async fn run_purge_tick(&self, catalog: &dyn CatalogClient) {
        let retention_days = self.config.catalog.purge_retention_days as i32;
        let rows: Result<Vec<(Uuid,)>, sqlx::Error> = sqlx::query_as(
            r#"
            SELECT service_id FROM service_deletions
            WHERE deleted_at < now() - make_interval(days => $1)
              AND purged_at IS NULL
            LIMIT 20
            "#,
        )
        .bind(retention_days)
        .fetch_all(&self.pool)
        .await;

        let rows = match rows {
            Ok(rows) => rows,
            Err(err) => {
                error!(error = %err, "failed to query purge-eligible services");
                return;
            }
        };

        for (service_id,) in rows {
            if let Err(err) = self.purge_one(catalog, service_id).await {
                error!(%service_id, error = %err, "purge failed, will retry next tick");
            }
        }
    }

    /// Deletes every asset object owned by the service (across every scene)
    /// before purging the catalog rows — a service can now have many band
    /// files, not just one, so this can no longer delete a single
    /// `storage_uri`. Logs and continues past individual object-delete
    /// failures so one bad asset doesn't block the rest.
    async fn purge_one(&self, catalog: &dyn CatalogClient, service_id: Uuid) -> anyhow::Result<()> {
        let scenes = catalog.list_scenes(service_id).await?;
        for scene in &scenes {
            for asset in &scene.assets {
                match parse_storage_uri(&asset.storage_uri, &self.config.storage.bucket) {
                    Ok((_bucket, key)) => {
                        let path = object_path(&key);
                        match self.store.delete(&path).await {
                            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                            Err(err) => error!(%service_id, asset_id = %asset.id, error = %err, "failed to delete asset object during purge"),
                        }
                    }
                    Err(err) => error!(%service_id, asset_id = %asset.id, error = %err, "invalid asset storage_uri during purge"),
                }
            }
        }
        catalog.purge_service(service_id).await?;
        info!(%service_id, "service purged by scheduled job");
        Ok(())
    }

    async fn handle_notification(&self, payload: &str) -> anyhow::Result<()> {
        let notify: FootprintNotifyPayload = serde_json::from_str(payload)
            .map_err(|err| anyhow::anyhow!("invalid footprint notify payload: {payload}: {err}"))?;

        let assets = self.load_scene_assets(notify.scene_id).await?;
        for asset in &assets {
            if let Err(err) = self.warm_asset(asset).await {
                error!(scene_id = %notify.scene_id, asset_id = %asset.id, error = %err, "failed to warm cache for asset");
            }
        }
        Ok(())
    }

    async fn load_scene_assets(&self, scene_id: Uuid) -> anyhow::Result<Vec<AssetRow>> {
        let rows = sqlx::query_as::<_, AssetRow>(
            r#"SELECT id, format, storage_uri FROM service_assets WHERE scene_id = $1"#,
        )
        .bind(scene_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    async fn warm_asset(&self, asset: &AssetRow) -> anyhow::Result<()> {
        let ttl = self.config.cache.ifd_ttl_seconds;
        match asset.format.as_str() {
            "cog" => {
                // Tile rendering (mantle-raster::cog) reads COGs via oxigdal,
                // which needs random byte-range access to the whole file, not
                // a cached IFD-bytes prefix — nothing reads this cache
                // anymore, so there's nothing useful to pre-warm here.
                debug!(asset_id = %asset.id, "skipping COG IFD cache warm (unused by oxigdal-based rendering)");
            }
            "icechunk" => {
                let repo_id = asset.id.to_string();
                info!(asset_id = %asset.id, repo_id, "warming Icechunk zmetadata cache");
                let blob = fetch_zmetadata_blob(
                    self.store.clone(),
                    &asset.storage_uri,
                    &self.config.storage.bucket,
                )
                .await?;
                self.cache.set_zmetadata(&repo_id, &blob, ttl).await?;
                info!(asset_id = %asset.id, bytes = blob.len(), "zmetadata cached");
            }
            other => {
                warn!(asset_id = %asset.id, format = other, "skipping unknown asset format");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notify_channel_matches_catalog_contract() {
        assert_eq!(FOOTPRINT_INSERTED_CHANNEL, "mantle_footprint_insert");
    }
}
