use crate::ducklake::DuckLakeSession;
use crate::error::CatalogError;
use crate::postgres::{fetch_dataset, fetch_dataset_any, insert_dataset, insert_footprint_row};
use crate::services::sanitize_slug;
use crate::virtual_services::{
    attach_function_to_dataset, fetch_virtual_service_by_slug, insert_virtual_service,
    slug_exists,
};
use crate::{
    DatasetRecord, DeletionRecord, FootprintRecord, SpatialQuery, VirtualServiceKind,
    VirtualServiceRecord,
};
use chrono::{DateTime, Utc};
use async_trait::async_trait;
use mantle_arrow::DatasetRef;
use mantle_config::CatalogConfig;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

/// DuckLake + Postgres catalog client.
///
/// Postgres holds transactional dataset/footprint rows; DuckLake stores append-only
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

    fn validate_append_only(dataset: &DatasetRecord, footprint: &FootprintRecord) -> Result<(), CatalogError> {
        if footprint.geometry_wkt.trim().is_empty() {
            return Err(CatalogError::InvalidGeometry(
                "footprint geometry_wkt must not be empty".into(),
            ));
        }
        if footprint.dataset_id != dataset.id {
            return Err(CatalogError::AppendOnlyViolation(format!(
                "footprint.dataset_id {} does not match dataset.id {}",
                footprint.dataset_id, dataset.id
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl crate::CatalogClient for PostgresDuckLakeCatalog {
    async fn insert_footprint(
        &self,
        dataset: DatasetRecord,
        footprint: FootprintRecord,
    ) -> Result<Uuid, CatalogError> {
        Self::validate_append_only(&dataset, &footprint)?;

        let partition_key = crate::ducklake::resolve_partition_key(&footprint, &dataset);
        let mut footprint = footprint;
        footprint.partition_key = partition_key.clone();

        let mut tx = self.pool.begin().await?;
        insert_dataset(&mut *tx, &dataset).await?;

        let ducklake = self.ducklake.clone();
        let dataset_for_duck = dataset.clone();
        let footprint_for_duck = footprint.clone();
        let partition_for_duck = partition_key.clone();

        let parquet_uri = tokio::task::spawn_blocking(move || {
            ducklake.append_footprint_parquet(
                &dataset_for_duck,
                &footprint_for_duck,
                &partition_for_duck,
            )
        })
        .await
        .map_err(|e| CatalogError::Config(format!("ducklake task join: {e}")))??;

        let footprint_id = insert_footprint_row(&mut *tx, &footprint).await?;
        tx.commit().await?;

        info!(
            dataset_id = %dataset.id,
            footprint_id,
            partition_key = %footprint.partition_key,
            parquet_uri = %parquet_uri,
            "append-only footprint inserted"
        );

        Ok(dataset.id)
    }

    async fn spatial_query(&self, query: SpatialQuery) -> Result<Vec<DatasetRef>, CatalogError> {
        let ducklake = self.ducklake.clone();
        tokio::task::spawn_blocking(move || ducklake.spatial_query(&query))
            .await
            .map_err(|e| CatalogError::Config(format!("ducklake task join: {e}")))?
    }

    async fn get_dataset(&self, id: Uuid) -> Result<DatasetRecord, CatalogError> {
        fetch_dataset(&self.pool, id).await
    }

    async fn attach_function(
        &self,
        dataset_id: Uuid,
        function_id: String,
        params_defaults: serde_json::Value,
        endpoint_slug: Option<String>,
    ) -> Result<VirtualServiceRecord, CatalogError> {
        let parent = fetch_dataset(&self.pool, dataset_id).await?;
        attach_function_to_dataset(
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

    async fn register_output_service(
        &self,
        output_dataset: DatasetRecord,
        function_id: String,
        endpoint_slug: String,
    ) -> Result<VirtualServiceRecord, CatalogError> {
        let mut tx = self.pool.begin().await?;
        insert_dataset(&mut *tx, &output_dataset).await?;
        let record = {
            let slug = sanitize_slug(&endpoint_slug);
            if slug_exists(&self.pool, &slug).await? {
                return Err(CatalogError::DuplicateSlug(slug));
            }
            let record = VirtualServiceRecord {
                id: Uuid::new_v4(),
                slug,
                service_kind: VirtualServiceKind::Output,
                dataset_id: output_dataset.id,
                parent_dataset_id: None,
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

    async fn soft_delete_dataset(
        &self,
        dataset_id: Uuid,
        reason: Option<String>,
    ) -> Result<DeletionRecord, CatalogError> {
        // Ensure the dataset exists at all (any state) before tombstoning it.
        fetch_dataset_any(&self.pool, dataset_id).await?;

        let mut tx = self.pool.begin().await?;

        let inserted: Option<DateTime<Utc>> = sqlx::query_scalar(
            r#"
            INSERT INTO dataset_deletions (dataset_id, reason)
            VALUES ($1, $2)
            ON CONFLICT (dataset_id) DO NOTHING
            RETURNING deleted_at
            "#,
        )
        .bind(dataset_id)
        .bind(&reason)
        .fetch_optional(&mut *tx)
        .await?;

        // Idempotent: if a tombstone already existed, return its original
        // deleted_at rather than erroring on retry.
        let deleted_at = match inserted {
            Some(ts) => ts,
            None => {
                sqlx::query_scalar(
                    "SELECT deleted_at FROM dataset_deletions WHERE dataset_id = $1",
                )
                .bind(dataset_id)
                .fetch_one(&mut *tx)
                .await?
            }
        };

        sqlx::query(
            r#"
            UPDATE virtual_services SET deleted_at = now()
            WHERE (dataset_id = $1 OR parent_dataset_id = $1) AND deleted_at IS NULL
            "#,
        )
        .bind(dataset_id)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        let purge_eligible_at =
            deleted_at + chrono::Duration::days(self.config.purge_retention_days as i64);

        Ok(DeletionRecord {
            dataset_id,
            deleted_at,
            purge_eligible_at,
        })
    }

    async fn get_dataset_any(&self, id: Uuid) -> Result<DatasetRecord, CatalogError> {
        fetch_dataset_any(&self.pool, id).await
    }

    async fn purge_dataset(&self, dataset_id: Uuid) -> Result<(), CatalogError> {
        let ducklake = self.ducklake.clone();
        tokio::task::spawn_blocking(move || ducklake.purge_dataset(dataset_id))
            .await
            .map_err(|e| CatalogError::Config(format!("ducklake task join: {e}")))??;

        let mut tx = self.pool.begin().await?;
        // Narrow, session-scoped bypass of the append-only trigger — see
        // migrations/004_dataset_deletion.sql. Ordinary connections never set
        // this, so they remain fully blocked.
        sqlx::query("SET LOCAL mantle.allow_purge = 'on'")
            .execute(&mut *tx)
            .await?;

        sqlx::query("DELETE FROM virtual_services WHERE dataset_id = $1 OR parent_dataset_id = $1")
            .bind(dataset_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM footprints WHERE dataset_id = $1")
            .bind(dataset_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM datasets WHERE id = $1")
            .bind(dataset_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE dataset_deletions SET purged_at = now() WHERE dataset_id = $1")
            .bind(dataset_id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        info!(%dataset_id, "dataset purged");
        Ok(())
    }
}
