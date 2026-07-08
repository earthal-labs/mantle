use crate::ducklake::DuckLakeSession;
use crate::error::CatalogError;
use crate::postgres::{
    fetch_scene, fetch_scene_any, fetch_scenes_for_service, fetch_service, fetch_service_any,
    fetch_service_by_slug, insert_asset, insert_footprint_row, insert_scene, insert_service,
};
use crate::services::sanitize_slug;
use crate::virtual_services::{
    attach_function_to_service, fetch_virtual_service_by_slug, fetch_virtual_services,
    insert_virtual_service, slug_exists,
};
use crate::{
    AssetRecord, DeletionRecord, FootprintRecord, SceneDeletionRecord, SceneRecord,
    SceneWithAssets, ServiceRecord, SpatialQuery, VirtualServiceKind, VirtualServiceRecord,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mantle_arrow::SceneRef;
use mantle_config::CatalogConfig;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

/// DuckLake + Postgres catalog client.
///
/// Postgres holds transactional service/scene/asset rows; DuckLake stores append-only
/// GeoParquet V2 partitions keyed by acquisition month for spatial predicate pushdown.
pub struct PostgresDuckLakeCatalog {
    pool: PgPool,
    ducklake: DuckLakeSession,
    config: Arc<CatalogConfig>,
}

impl PostgresDuckLakeCatalog {
    pub async fn connect(config: Arc<CatalogConfig>) -> Result<Self, CatalogError> {
        let pool = PgPool::connect(&config.postgres_url).await?;
        let ducklake = DuckLakeSession::open(config.clone())?;
        info!("PostgresDuckLakeCatalog connected");
        Ok(Self {
            pool,
            ducklake,
            config,
        })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub fn config(&self) -> &CatalogConfig {
        &self.config
    }

    fn validate_append_only(scene: &SceneRecord, footprint: &FootprintRecord) -> Result<(), CatalogError> {
        if footprint.geometry_wkt.trim().is_empty() {
            return Err(CatalogError::InvalidGeometry(
                "footprint geometry_wkt must not be empty".into(),
            ));
        }
        if footprint.scene_id != scene.id {
            return Err(CatalogError::AppendOnlyViolation(format!(
                "footprint.scene_id {} does not match scene.id {}",
                footprint.scene_id, scene.id
            )));
        }
        if footprint.service_id != scene.service_id {
            return Err(CatalogError::AppendOnlyViolation(format!(
                "footprint.service_id {} does not match scene.service_id {}",
                footprint.service_id, scene.service_id
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl crate::CatalogClient for PostgresDuckLakeCatalog {
    async fn create_service(&self, mut service: ServiceRecord) -> Result<ServiceRecord, CatalogError> {
        if let Ok(existing) = fetch_service_any(&self.pool, service.id).await {
            return Ok(existing);
        }

        let base_slug = if service.slug.trim().is_empty() {
            sanitize_slug(&service.name)
        } else {
            sanitize_slug(&service.slug)
        };
        let mut candidate = base_slug.clone();
        let mut attempt = 0u32;
        while slug_exists(&self.pool, &candidate).await? {
            attempt += 1;
            candidate = format!("{base_slug}-{attempt}");
            if attempt > 50 {
                return Err(CatalogError::Config(
                    "could not generate a unique service slug".into(),
                ));
            }
        }
        service.slug = candidate;

        insert_service(&self.pool, &service).await?;
        Ok(service)
    }

    async fn add_scene(
        &self,
        scene: SceneRecord,
        assets: Vec<AssetRecord>,
        footprint: FootprintRecord,
    ) -> Result<Uuid, CatalogError> {
        Self::validate_append_only(&scene, &footprint)?;
        if assets.is_empty() {
            return Err(CatalogError::InvalidGeometry(
                "a scene requires at least one asset".into(),
            ));
        }

        let partition_key = crate::ducklake::resolve_partition_key(&footprint, &scene);
        let mut footprint = footprint;
        footprint.partition_key = partition_key.clone();

        // Service name is denormalized onto the DuckLake footprint row (spatial
        // search can't join back to Postgres); fetch it up front, tombstone
        // check included so we never index a scene under a deleted service.
        let service = fetch_service(&self.pool, scene.service_id).await?;

        let mut tx = self.pool.begin().await?;
        insert_scene(&mut *tx, &scene).await?;
        for asset in &assets {
            insert_asset(&mut *tx, asset).await?;
        }

        let ducklake = self.ducklake.clone();
        let service_name = service.name.clone();
        let scene_for_duck = scene.clone();
        let assets_for_duck = assets.clone();
        let footprint_for_duck = footprint.clone();
        let partition_for_duck = partition_key.clone();

        let parquet_uri = tokio::task::spawn_blocking(move || {
            ducklake.append_scene_footprint_parquet(
                &service_name,
                &scene_for_duck,
                &assets_for_duck,
                &footprint_for_duck,
                &partition_for_duck,
            )
        })
        .await
        .map_err(|e| CatalogError::Config(format!("ducklake task join: {e}")))??;

        let footprint_id = insert_footprint_row(&mut *tx, &footprint).await?;
        tx.commit().await?;

        info!(
            scene_id = %scene.id,
            service_id = %scene.service_id,
            footprint_id,
            asset_count = assets.len(),
            partition_key = %footprint.partition_key,
            parquet_uri = %parquet_uri,
            "append-only scene inserted"
        );

        Ok(scene.id)
    }

    async fn spatial_query(&self, query: SpatialQuery) -> Result<Vec<SceneRef>, CatalogError> {
        let ducklake = self.ducklake.clone();
        tokio::task::spawn_blocking(move || ducklake.spatial_query(&query))
            .await
            .map_err(|e| CatalogError::Config(format!("ducklake task join: {e}")))?
    }

    async fn get_service(&self, id: Uuid) -> Result<ServiceRecord, CatalogError> {
        fetch_service(&self.pool, id).await
    }

    async fn get_service_by_slug(&self, slug: &str) -> Result<ServiceRecord, CatalogError> {
        fetch_service_by_slug(&self.pool, slug).await
    }

    async fn list_scenes(&self, service_id: Uuid) -> Result<Vec<SceneWithAssets>, CatalogError> {
        fetch_scenes_for_service(&self.pool, service_id).await
    }

    async fn get_scene(&self, scene_id: Uuid) -> Result<SceneWithAssets, CatalogError> {
        fetch_scene(&self.pool, scene_id).await
    }

    async fn delete_scene(
        &self,
        scene_id: Uuid,
        reason: Option<String>,
    ) -> Result<SceneDeletionRecord, CatalogError> {
        // Ensure the scene exists at all (any state) before tombstoning it.
        fetch_scene_any(&self.pool, scene_id).await?;

        let inserted: Option<DateTime<Utc>> = sqlx::query_scalar(
            r#"
            INSERT INTO scene_deletions (scene_id, reason)
            VALUES ($1, $2)
            ON CONFLICT (scene_id) DO NOTHING
            RETURNING deleted_at
            "#,
        )
        .bind(scene_id)
        .bind(&reason)
        .fetch_optional(&self.pool)
        .await?;

        let deleted_at = match inserted {
            Some(ts) => ts,
            None => {
                sqlx::query_scalar("SELECT deleted_at FROM scene_deletions WHERE scene_id = $1")
                    .bind(scene_id)
                    .fetch_one(&self.pool)
                    .await?
            }
        };

        let purge_eligible_at =
            deleted_at + chrono::Duration::days(self.config.purge_retention_days as i64);

        Ok(SceneDeletionRecord {
            scene_id,
            deleted_at,
            purge_eligible_at,
        })
    }

    async fn purge_scene(&self, scene_id: Uuid) -> Result<(), CatalogError> {
        let ducklake = self.ducklake.clone();
        tokio::task::spawn_blocking(move || ducklake.purge_scene(scene_id))
            .await
            .map_err(|e| CatalogError::Config(format!("ducklake task join: {e}")))??;

        let mut tx = self.pool.begin().await?;
        sqlx::query("SET LOCAL mantle.allow_purge = 'on'")
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM footprints WHERE scene_id = $1")
            .bind(scene_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM service_assets WHERE scene_id = $1")
            .bind(scene_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM scenes WHERE id = $1")
            .bind(scene_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE scene_deletions SET purged_at = now() WHERE scene_id = $1")
            .bind(scene_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        info!(%scene_id, "scene purged");
        Ok(())
    }

    async fn attach_function(
        &self,
        service_id: Uuid,
        function_id: String,
        params_defaults: serde_json::Value,
        endpoint_slug: Option<String>,
    ) -> Result<VirtualServiceRecord, CatalogError> {
        let parent = fetch_service(&self.pool, service_id).await?;
        attach_function_to_service(
            &self.pool,
            &parent,
            function_id,
            params_defaults,
            endpoint_slug,
        )
        .await
    }

    async fn get_virtual_service_by_slug(
        &self,
        slug: &str,
    ) -> Result<VirtualServiceRecord, CatalogError> {
        fetch_virtual_service_by_slug(&self.pool, slug).await
    }

    async fn list_virtual_services(
        &self,
        service_id: Option<Uuid>,
    ) -> Result<Vec<VirtualServiceRecord>, CatalogError> {
        fetch_virtual_services(&self.pool, service_id).await
    }

    async fn register_output_service(
        &self,
        mut output_service: ServiceRecord,
        function_id: String,
        endpoint_slug: String,
    ) -> Result<VirtualServiceRecord, CatalogError> {
        if output_service.slug.trim().is_empty() {
            output_service.slug = sanitize_slug(&output_service.name);
        }
        let mut tx = self.pool.begin().await?;
        insert_service(&mut *tx, &output_service).await?;
        let record = {
            let slug = sanitize_slug(&endpoint_slug);
            if slug_exists(&self.pool, &slug).await? {
                return Err(CatalogError::DuplicateSlug(slug));
            }
            let record = VirtualServiceRecord {
                id: Uuid::new_v4(),
                slug,
                service_kind: VirtualServiceKind::Output,
                service_id: output_service.id,
                parent_service_id: None,
                function_id,
                params_defaults: serde_json::json!({}),
                created_at: Utc::now(),
            };
            insert_virtual_service(&mut *tx, &record).await?;
            record
        };
        tx.commit().await?;
        Ok(record)
    }

    async fn soft_delete_service(
        &self,
        service_id: Uuid,
        reason: Option<String>,
    ) -> Result<DeletionRecord, CatalogError> {
        // Ensure the service exists at all (any state) before tombstoning it.
        fetch_service_any(&self.pool, service_id).await?;

        let mut tx = self.pool.begin().await?;

        let inserted: Option<DateTime<Utc>> = sqlx::query_scalar(
            r#"
            INSERT INTO service_deletions (service_id, reason)
            VALUES ($1, $2)
            ON CONFLICT (service_id) DO NOTHING
            RETURNING deleted_at
            "#,
        )
        .bind(service_id)
        .bind(&reason)
        .fetch_optional(&mut *tx)
        .await?;

        // Idempotent: if a tombstone already existed, return its original
        // deleted_at rather than erroring on retry.
        let deleted_at = match inserted {
            Some(ts) => ts,
            None => {
                sqlx::query_scalar(
                    "SELECT deleted_at FROM service_deletions WHERE service_id = $1",
                )
                .bind(service_id)
                .fetch_one(&mut *tx)
                .await?
            }
        };

        sqlx::query(
            r#"
            UPDATE virtual_services SET deleted_at = now()
            WHERE (service_id = $1 OR parent_service_id = $1) AND deleted_at IS NULL
            "#,
        )
        .bind(service_id)
        .execute(&mut *tx)
        .await?;

        // Cascade into every scene the service owns — scene_deletions has no
        // FK (same reasoning as service_deletions), so a bulk INSERT is safe
        // even across many scenes.
        sqlx::query(
            r#"
            INSERT INTO scene_deletions (scene_id, reason)
            SELECT id, $2 FROM scenes WHERE service_id = $1
            ON CONFLICT (scene_id) DO NOTHING
            "#,
        )
        .bind(service_id)
        .bind(&reason)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        let purge_eligible_at =
            deleted_at + chrono::Duration::days(self.config.purge_retention_days as i64);

        Ok(DeletionRecord {
            service_id,
            deleted_at,
            purge_eligible_at,
        })
    }

    async fn get_service_any(&self, id: Uuid) -> Result<ServiceRecord, CatalogError> {
        fetch_service_any(&self.pool, id).await
    }

    async fn purge_service(&self, service_id: Uuid) -> Result<(), CatalogError> {
        let ducklake = self.ducklake.clone();
        tokio::task::spawn_blocking(move || ducklake.purge_service_scenes(service_id))
            .await
            .map_err(|e| CatalogError::Config(format!("ducklake task join: {e}")))??;

        let mut tx = self.pool.begin().await?;
        // Narrow, session-scoped bypass of the append-only trigger — see
        // migrations/004_service_deletion.sql. Ordinary connections never set
        // this, so they remain fully blocked.
        sqlx::query("SET LOCAL mantle.allow_purge = 'on'")
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM virtual_services WHERE service_id = $1 OR parent_service_id = $1")
            .bind(service_id)
            .execute(&mut *tx)
            .await?;
        // Mark every scene tombstone purged while the scenes rows still
        // exist to join against (deleted right after).
        sqlx::query(
            r#"
            UPDATE scene_deletions SET purged_at = now()
            WHERE scene_id IN (SELECT id FROM scenes WHERE service_id = $1)
            "#,
        )
        .bind(service_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM footprints WHERE service_id = $1")
            .bind(service_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM service_assets WHERE service_id = $1")
            .bind(service_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM scenes WHERE service_id = $1")
            .bind(service_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM services WHERE id = $1")
            .bind(service_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE service_deletions SET purged_at = now() WHERE service_id = $1")
            .bind(service_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        info!(%service_id, "service purged");
        Ok(())
    }
}
