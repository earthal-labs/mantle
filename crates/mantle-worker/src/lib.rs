//! Background cache warmer — listens for catalog footprint inserts and pre-warms Redis.

mod catalog;
mod prefetch;
mod storage;

pub use catalog::FOOTPRINT_INSERTED_CHANNEL;

use crate::prefetch::{fetch_cog_ifd_blob, fetch_zmetadata_blob};
use crate::storage::{build_object_store, parse_storage_uri};
use mantle_cache::{CacheClient, RedisCacheClient};
use mantle_config::MantleConfig;
use serde::Deserialize;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, sqlx::FromRow)]
struct DatasetRow {
    id: Uuid,
    format: String,
    storage_uri: String,
}

#[derive(Debug, Deserialize)]
struct FootprintNotifyPayload {
    dataset_id: Uuid,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    storage_uri: Option<String>,
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

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    info!("mantle-worker shutting down");
                    break;
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

    async fn handle_notification(&self, payload: &str) -> anyhow::Result<()> {
        let notify: FootprintNotifyPayload = serde_json::from_str(payload)
            .unwrap_or(FootprintNotifyPayload {
                dataset_id: Uuid::parse_str(payload.trim()).map_err(|err| {
                    anyhow::anyhow!("invalid footprint notify payload: {payload}: {err}")
                })?,
                format: None,
                storage_uri: None,
            });

        let dataset = if notify.format.is_some() && notify.storage_uri.is_some() {
            DatasetRow {
                id: notify.dataset_id,
                format: notify.format.expect("checked some"),
                storage_uri: notify.storage_uri.expect("checked some"),
            }
        } else {
            self.load_dataset(notify.dataset_id).await?
        };

        self.warm_dataset(&dataset).await
    }

    async fn load_dataset(&self, dataset_id: Uuid) -> anyhow::Result<DatasetRow> {
        let row = sqlx::query_as::<_, DatasetRow>(
            r#"SELECT id, format, storage_uri FROM datasets WHERE id = $1"#,
        )
        .bind(dataset_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("dataset not found: {dataset_id}"))?;

        Ok(row)
    }

    async fn warm_dataset(&self, dataset: &DatasetRow) -> anyhow::Result<()> {
        let ttl = self.config.cache.ifd_ttl_seconds;
        match dataset.format.as_str() {
            "cog" => {
                let (_bucket, s3_key) =
                    parse_storage_uri(&dataset.storage_uri, &self.config.storage.bucket)?;
                info!(dataset_id = %dataset.id, s3_key, "warming COG IFD cache");
                let blob = fetch_cog_ifd_blob(self.store.clone(), &s3_key).await?;
                self.cache.set_ifd(&s3_key, &blob, ttl).await?;
                info!(dataset_id = %dataset.id, bytes = blob.len(), "COG IFD cached");
            }
            "icechunk" => {
                let repo_id = dataset.id.to_string();
                info!(dataset_id = %dataset.id, repo_id, "warming Icechunk zmetadata cache");
                let blob = fetch_zmetadata_blob(
                    self.store.clone(),
                    &dataset.storage_uri,
                    &self.config.storage.bucket,
                )
                .await?;
                self.cache.set_zmetadata(&repo_id, &blob, ttl).await?;
                info!(dataset_id = %dataset.id, bytes = blob.len(), "zmetadata cached");
            }
            other => {
                warn!(dataset_id = %dataset.id, format = other, "skipping unknown dataset format");
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
