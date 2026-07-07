use crate::ducklake::DuckLakeSession;
use crate::error::CatalogError;
use crate::postgres::{fetch_service, fetch_service_any, insert_footprint_row, insert_service};
use crate::services::sanitize_slug;
use crate::virtual_services::{
    attach_function_to_service, fetch_virtual_service_by_slug, fetch_virtual_services,
    insert_virtual_service, slug_exists,
};
use crate::{
    DeletionRecord, FootprintRecord, ServiceRecord, SpatialQuery, VirtualServiceKind,
    VirtualServiceRecord,
};
use chrono::{DateTime, Utc};
use async_trait::async_trait;
use mantle_arrow::ServiceRef;
use mantle_config::CatalogConfig;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

/// DuckLake + Postgres catalog client.
///
/// Postgres holds transactional service/footprint rows; DuckLake stores append-only
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

    fn validate_append_only(service: &ServiceRecord, footprint: &FootprintRecord) -> Result<(), CatalogError> {
        if footprint.geometry_wkt.trim().is_empty() {
            return Err(CatalogError::InvalidGeometry(
                "footprint geometry_wkt must not be empty".into(),
            ));
        }
        if footprint.service_id != service.id {
            return Err(CatalogError::AppendOnlyViolation(format!(
                "footprint.service_id {} does not match service.id {}",
                footprint.service_id, service.id
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl crate::CatalogClient for PostgresDuckLakeCatalog {
    async fn insert_footprint(
        &self,
        service: ServiceRecord,
        footprint: FootprintRecord,
    ) -> Result<Uuid, CatalogError> {
        Self::validate_append_only(&service, &footprint)?;

        let partition_key = crate::ducklake::resolve_partition_key(&footprint, &service);
        let mut footprint = footprint;
        footprint.partition_key = partition_key.clone();

        let mut tx = self.pool.begin().await?;
        insert_service(&mut *tx, &service).await?;

        let ducklake = self.ducklake.clone();
        let service_for_duck = service.clone();
        let footprint_for_duck = footprint.clone();
        let partition_for_duck = partition_key.clone();

        let parquet_uri = tokio::task::spawn_blocking(move || {
            ducklake.append_footprint_parquet(
                &service_for_duck,
                &footprint_for_duck,
                &partition_for_duck,
            )
        })
        .await
        .map_err(|e| CatalogError::Config(format!("ducklake task join: {e}")))??;

        let footprint_id = insert_footprint_row(&mut *tx, &footprint).await?;
        tx.commit().await?;

        info!(
            service_id = %service.id,
            footprint_id,
            partition_key = %footprint.partition_key,
            parquet_uri = %parquet_uri,
            "append-only footprint inserted"
        );

        Ok(service.id)
    }

    async fn spatial_query(&self, query: SpatialQuery) -> Result<Vec<ServiceRef>, CatalogError> {
        let ducklake = self.ducklake.clone();
        tokio::task::spawn_blocking(move || ducklake.spatial_query(&query))
            .await
            .map_err(|e| CatalogError::Config(format!("ducklake task join: {e}")))?
    }

    async fn get_service(&self, id: Uuid) -> Result<ServiceRecord, CatalogError> {
        fetch_service(&self.pool, id).await
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
        output_service: ServiceRecord,
        function_id: String,
        endpoint_slug: String,
    ) -> Result<VirtualServiceRecord, CatalogError> {
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
        tokio::task::spawn_blocking(move || ducklake.purge_service(service_id))
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
        sqlx::query("DELETE FROM footprints WHERE service_id = $1")
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
