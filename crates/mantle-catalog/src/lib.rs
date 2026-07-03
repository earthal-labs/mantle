//! DuckLake + Postgres catalog client.
//!
//! # Append-only catalog
//!
//! Mantle never updates catalog metadata in place. Each footprint insert writes a new
//! GeoParquet V2 object and registers it as a new DuckLake snapshot; Postgres rows are
//! insert-only (enforced by [`migrations/002_append_only_notify.sql`](../../migrations/002_append_only_notify.sql)).
//!
//! # Partition strategy
//!
//! Footprint Parquet files are partitioned by **acquisition month** (`YYYY-MM`), derived
//! from the dataset `temporal_start` (or the insert time when absent). Paths look like:
//!
//! ```text
//! {ducklake_data_path}partitions/2024-07/{uuid}.parquet
//! ```
//!
//! Monthly partitions keep DuckLake compaction predictable and avoid rewriting hot
//! partitions on every insert.

mod client;
mod ducklake;
mod error;
mod notify;
mod partition;
mod postgres;
mod services;
mod virtual_services;

pub use client::PostgresDuckLakeCatalog;
pub use error::CatalogError;
pub use notify::{parse_footprint_insert_event, subscribe_footprint_inserts, FootprintInsertEvent, FOOTPRINT_INSERT_CHANNEL};
pub use services::{
    generate_service_slug, sanitize_slug, VirtualServiceKind, VirtualServiceRecord,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use geo_types::Rect;
use mantle_arrow::{DatasetFormat, DatasetRef};
use mantle_config::CatalogConfig;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetRecord {
    pub id: Uuid,
    pub name: String,
    pub format: DatasetFormat,
    pub storage_uri: String,
    pub crs: Option<String>,
    pub temporal_start: Option<DateTime<Utc>>,
    pub temporal_end: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl DatasetRecord {
    pub fn to_dataset_ref(&self) -> DatasetRef {
        DatasetRef {
            id: self.id,
            name: self.name.clone(),
            format: self.format,
            storage_uri: self.storage_uri.clone(),
            crs: self.crs.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FootprintRecord {
    pub dataset_id: Uuid,
    pub geometry_wkt: String,
    pub cloud_cover: Option<f64>,
    pub partition_key: String,
}

#[derive(Debug, Clone, Default)]
pub struct SpatialQuery {
    pub bbox: Option<Rect<f64>>,
    pub datetime_start: Option<DateTime<Utc>>,
    pub datetime_end: Option<DateTime<Utc>>,
    pub cloud_cover_max: Option<f64>,
}

#[async_trait]
pub trait CatalogClient: Send + Sync {
    async fn insert_footprint(
        &self,
        dataset: DatasetRecord,
        footprint: FootprintRecord,
    ) -> Result<Uuid, CatalogError>;

    async fn spatial_query(&self, query: SpatialQuery) -> Result<Vec<DatasetRef>, CatalogError>;

    async fn get_dataset(&self, id: Uuid) -> Result<DatasetRecord, CatalogError>;

    /// Attach an on-the-fly raster function to an existing dataset (virtual service).
    async fn attach_function(
        &self,
        dataset_id: Uuid,
        function_id: String,
        params_defaults: serde_json::Value,
        endpoint_slug: Option<String>,
    ) -> Result<VirtualServiceRecord, CatalogError>;

    /// Resolve a virtual service by its public URL slug.
    async fn get_virtual_service_by_slug(
        &self,
        slug: &str,
    ) -> Result<VirtualServiceRecord, CatalogError>;

    /// Register a pRPM output as a new virtual service + dataset.
    async fn register_output_service(
        &self,
        output_dataset: DatasetRecord,
        function_id: String,
        endpoint_slug: String,
    ) -> Result<VirtualServiceRecord, CatalogError>;
}

/// Stub catalog client — returns empty results when Postgres/DuckLake are unavailable.
pub struct StubCatalogClient {
    _config: Arc<CatalogConfig>,
    services: std::sync::Mutex<std::collections::HashMap<String, VirtualServiceRecord>>,
    datasets: std::sync::Mutex<std::collections::HashMap<Uuid, DatasetRecord>>,
}

impl StubCatalogClient {
    pub fn new(config: Arc<CatalogConfig>) -> Self {
        Self {
            _config: config,
            services: std::sync::Mutex::new(std::collections::HashMap::new()),
            datasets: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

#[async_trait]
impl CatalogClient for StubCatalogClient {
    async fn insert_footprint(
        &self,
        dataset: DatasetRecord,
        _footprint: FootprintRecord,
    ) -> Result<Uuid, CatalogError> {
        self.datasets
            .lock()
            .expect("stub datasets lock")
            .insert(dataset.id, dataset.clone());
        Ok(dataset.id)
    }

    async fn spatial_query(&self, _query: SpatialQuery) -> Result<Vec<DatasetRef>, CatalogError> {
        Ok(Vec::new())
    }

    async fn get_dataset(&self, id: Uuid) -> Result<DatasetRecord, CatalogError> {
        self.datasets
            .lock()
            .expect("stub datasets lock")
            .get(&id)
            .cloned()
            .ok_or(CatalogError::NotFound(id))
    }

    async fn attach_function(
        &self,
        dataset_id: Uuid,
        function_id: String,
        params_defaults: serde_json::Value,
        endpoint_slug: Option<String>,
    ) -> Result<VirtualServiceRecord, CatalogError> {
        let dataset = self.get_dataset(dataset_id).await?;
        let slug = generate_service_slug(dataset_id, &function_id, endpoint_slug.as_deref());
        let mut services = self.services.lock().expect("stub services lock");
        if services.contains_key(&slug) {
            return Err(CatalogError::DuplicateSlug(slug));
        }
        let record = VirtualServiceRecord {
            id: Uuid::new_v4(),
            slug: slug.clone(),
            service_kind: VirtualServiceKind::Attached,
            dataset_id: dataset.id,
            parent_dataset_id: Some(dataset.id),
            function_id,
            params_defaults,
            created_at: Utc::now(),
        };
        services.insert(slug, record.clone());
        Ok(record)
    }

    async fn get_virtual_service_by_slug(
        &self,
        slug: &str,
    ) -> Result<VirtualServiceRecord, CatalogError> {
        let normalized = sanitize_slug(slug);
        self.services
            .lock()
            .expect("stub services lock")
            .get(&normalized)
            .cloned()
            .ok_or(CatalogError::ServiceNotFound(normalized))
    }

    async fn register_output_service(
        &self,
        output_dataset: DatasetRecord,
        function_id: String,
        endpoint_slug: String,
    ) -> Result<VirtualServiceRecord, CatalogError> {
        let slug = sanitize_slug(&endpoint_slug);
        let mut services = self.services.lock().expect("stub services lock");
        if services.contains_key(&slug) {
            return Err(CatalogError::DuplicateSlug(slug));
        }
        self.datasets
            .lock()
            .expect("stub datasets lock")
            .insert(output_dataset.id, output_dataset.clone());
        let record = VirtualServiceRecord {
            id: Uuid::new_v4(),
            slug: slug.clone(),
            service_kind: VirtualServiceKind::Output,
            dataset_id: output_dataset.id,
            parent_dataset_id: None,
            function_id,
            params_defaults: serde_json::json!({}),
            created_at: Utc::now(),
        };
        services.insert(slug, record.clone());
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo_types::coord;

    #[test]
    fn dataset_record_to_ref() {
        let record = DatasetRecord {
            id: Uuid::nil(),
            name: "test".into(),
            format: DatasetFormat::Cog,
            storage_uri: "s3://bucket/key".into(),
            crs: Some("EPSG:4326".into()),
            temporal_start: None,
            temporal_end: None,
            created_at: Utc::now(),
        };
        let reference = record.to_dataset_ref();
        assert_eq!(reference.name, "test");
        assert_eq!(reference.format, DatasetFormat::Cog);
    }

    #[tokio::test]
    #[ignore = "requires postgres, duckdb ducklake+spatial extensions"]
    async fn round_trip_insert_and_query() {
        let config = Arc::new(CatalogConfig {
            postgres_url: std::env::var("MANTLE_TEST_POSTGRES_URL")
                .unwrap_or_else(|_| "postgres://mantle:mantle@localhost:5432/mantle".into()),
            ducklake_data_path: std::env::var("MANTLE_TEST_DUCKLAKE_PATH")
                .unwrap_or_else(|_| "./target/test-ducklake/".into()),
            geometry_column: "footprint".into(),
        });

        let catalog = PostgresDuckLakeCatalog::connect(config).await.expect("connect");
        let id = Uuid::new_v4();
        let now = Utc::now();
        let dataset = DatasetRecord {
            id,
            name: "integration-test".into(),
            format: DatasetFormat::Cog,
            storage_uri: "s3://mantle-data/test.tif".into(),
            crs: Some("EPSG:4326".into()),
            temporal_start: Some(now),
            temporal_end: None,
            created_at: now,
        };
        let footprint = FootprintRecord {
            dataset_id: id,
            geometry_wkt: "POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1))".into(),
            cloud_cover: Some(10.0),
            partition_key: String::new(),
        };

        catalog
            .insert_footprint(dataset.clone(), footprint)
            .await
            .expect("insert");

        let fetched = catalog.get_dataset(id).await.expect("get");
        assert_eq!(fetched.name, "integration-test");

        let hits = catalog
            .spatial_query(SpatialQuery {
                bbox: Some(Rect::new(
                    coord! { x: -2.0, y: -2.0 },
                    coord! { x: 2.0, y: 2.0 },
                )),
                ..Default::default()
            })
            .await
            .expect("spatial");
        assert!(hits.iter().any(|hit| hit.id == id));
    }
}
