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
//! from the service `temporal_start` (or the insert time when absent). Paths look like:
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
use mantle_arrow::{ServiceFormat, ServiceRef};
use mantle_config::CatalogConfig;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceRecord {
    pub id: Uuid,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub format: ServiceFormat,
    pub storage_uri: String,
    pub crs: Option<String>,
    pub temporal_start: Option<DateTime<Utc>>,
    pub temporal_end: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl ServiceRecord {
    pub fn to_service_ref(&self) -> ServiceRef {
        ServiceRef {
            id: self.id,
            name: self.name.clone(),
            format: self.format,
            storage_uri: self.storage_uri.clone(),
            crs: self.crs.clone(),
            // ServiceRecord doesn't carry footprint geometry (that lives in
            // FootprintRecord/the DuckLake-backed table) — only spatial_query
            // populates this field.
            geometry_wkt: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FootprintRecord {
    pub service_id: Uuid,
    pub geometry_wkt: String,
    pub cloud_cover: Option<f64>,
    pub partition_key: String,
}

/// Result of a soft-delete: hidden from all reads immediately, physically
/// purged once `purge_eligible_at` passes (unless purged sooner via the
/// immediate-purge admin override).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletionRecord {
    pub service_id: Uuid,
    pub deleted_at: DateTime<Utc>,
    pub purge_eligible_at: DateTime<Utc>,
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
        service: ServiceRecord,
        footprint: FootprintRecord,
    ) -> Result<Uuid, CatalogError>;

    async fn spatial_query(&self, query: SpatialQuery) -> Result<Vec<ServiceRef>, CatalogError>;

    async fn get_service(&self, id: Uuid) -> Result<ServiceRecord, CatalogError>;

    /// Attach an on-the-fly raster function to an existing service (virtual service).
    async fn attach_function(
        &self,
        service_id: Uuid,
        function_id: String,
        params_defaults: serde_json::Value,
        endpoint_slug: Option<String>,
    ) -> Result<VirtualServiceRecord, CatalogError>;

    /// Resolve a virtual service by its public URL slug.
    async fn get_virtual_service_by_slug(
        &self,
        slug: &str,
    ) -> Result<VirtualServiceRecord, CatalogError>;

    /// List virtual services, optionally filtered to those belonging to (or
    /// attached to) one base service. `None` lists every virtual service.
    async fn list_virtual_services(
        &self,
        service_id: Option<Uuid>,
    ) -> Result<Vec<VirtualServiceRecord>, CatalogError>;

    /// Register a pRPM output as a new virtual service + service.
    async fn register_output_service(
        &self,
        output_service: ServiceRecord,
        function_id: String,
        endpoint_slug: String,
    ) -> Result<VirtualServiceRecord, CatalogError>;

    /// Hide a service (and any virtual services attached to or produced from
    /// it) from every read path immediately. The underlying rows/files are
    /// physically removed later by the purge job, or immediately via the
    /// admin purge-now override. Idempotent: calling it again on an
    /// already-deleted service returns the original `deleted_at`.
    async fn soft_delete_service(
        &self,
        service_id: Uuid,
        reason: Option<String>,
    ) -> Result<DeletionRecord, CatalogError>;

    /// Like [`CatalogClient::get_service`] but ignores the soft-delete
    /// tombstone. Used only by purge orchestration (scheduled job / immediate
    /// override), which needs `storage_uri` for an already-hidden service in
    /// order to reclaim its S3 object.
    async fn get_service_any(&self, id: Uuid) -> Result<ServiceRecord, CatalogError>;

    /// Physically remove a soft-deleted service's catalog rows (Postgres +
    /// DuckLake) and mark its tombstone `purged_at`. Does **not** delete the
    /// S3 object — the caller does that (via `get_service_any`'s
    /// `storage_uri`) before calling this, since object storage access isn't
    /// a catalog-crate concern. Idempotent: safe to call again on a service
    /// that's already fully purged.
    async fn purge_service(&self, service_id: Uuid) -> Result<(), CatalogError>;
}

/// Stub catalog client — returns empty results when Postgres/DuckLake are unavailable.
pub struct StubCatalogClient {
    _config: Arc<CatalogConfig>,
    services: std::sync::Mutex<std::collections::HashMap<String, VirtualServiceRecord>>,
    base_services: std::sync::Mutex<std::collections::HashMap<Uuid, ServiceRecord>>,
}

impl StubCatalogClient {
    pub fn new(config: Arc<CatalogConfig>) -> Self {
        Self {
            _config: config,
            services: std::sync::Mutex::new(std::collections::HashMap::new()),
            base_services: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

#[async_trait]
impl CatalogClient for StubCatalogClient {
    async fn insert_footprint(
        &self,
        service: ServiceRecord,
        _footprint: FootprintRecord,
    ) -> Result<Uuid, CatalogError> {
        self.base_services
            .lock()
            .expect("stub services lock")
            .insert(service.id, service.clone());
        Ok(service.id)
    }

    async fn spatial_query(&self, _query: SpatialQuery) -> Result<Vec<ServiceRef>, CatalogError> {
        Ok(Vec::new())
    }

    async fn get_service(&self, id: Uuid) -> Result<ServiceRecord, CatalogError> {
        self.base_services
            .lock()
            .expect("stub services lock")
            .get(&id)
            .cloned()
            .ok_or(CatalogError::NotFound(id))
    }

    async fn attach_function(
        &self,
        service_id: Uuid,
        function_id: String,
        params_defaults: serde_json::Value,
        endpoint_slug: Option<String>,
    ) -> Result<VirtualServiceRecord, CatalogError> {
        let service = self.get_service(service_id).await?;
        let slug = generate_service_slug(service_id, &function_id, endpoint_slug.as_deref());
        let mut services = self.services.lock().expect("stub services lock");
        if services.contains_key(&slug) {
            return Err(CatalogError::DuplicateSlug(slug));
        }
        let record = VirtualServiceRecord {
            id: Uuid::new_v4(),
            slug: slug.clone(),
            service_kind: VirtualServiceKind::Attached,
            service_id: service.id,
            parent_service_id: Some(service.id),
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

    async fn list_virtual_services(
        &self,
        service_id: Option<Uuid>,
    ) -> Result<Vec<VirtualServiceRecord>, CatalogError> {
        Ok(self
            .services
            .lock()
            .expect("stub services lock")
            .values()
            .filter(|record| match service_id {
                Some(id) => record.service_id == id || record.parent_service_id == Some(id),
                None => true,
            })
            .cloned()
            .collect())
    }

    async fn register_output_service(
        &self,
        output_service: ServiceRecord,
        function_id: String,
        endpoint_slug: String,
    ) -> Result<VirtualServiceRecord, CatalogError> {
        let slug = sanitize_slug(&endpoint_slug);
        let mut services = self.services.lock().expect("stub services lock");
        if services.contains_key(&slug) {
            return Err(CatalogError::DuplicateSlug(slug));
        }
        self.base_services
            .lock()
            .expect("stub services lock")
            .insert(output_service.id, output_service.clone());
        let record = VirtualServiceRecord {
            id: Uuid::new_v4(),
            slug: slug.clone(),
            service_kind: VirtualServiceKind::Output,
            service_id: output_service.id,
            parent_service_id: None,
            function_id,
            params_defaults: serde_json::json!({}),
            created_at: Utc::now(),
        };
        services.insert(slug, record.clone());
        Ok(record)
    }

    async fn soft_delete_service(
        &self,
        service_id: Uuid,
        _reason: Option<String>,
    ) -> Result<DeletionRecord, CatalogError> {
        self.base_services
            .lock()
            .expect("stub services lock")
            .get(&service_id)
            .cloned()
            .ok_or(CatalogError::NotFound(service_id))?;
        let deleted_at = Utc::now();
        let purge_eligible_at =
            deleted_at + chrono::Duration::days(self._config.purge_retention_days as i64);
        Ok(DeletionRecord {
            service_id,
            deleted_at,
            purge_eligible_at,
        })
    }

    async fn get_service_any(&self, id: Uuid) -> Result<ServiceRecord, CatalogError> {
        self.get_service(id).await
    }

    async fn purge_service(&self, service_id: Uuid) -> Result<(), CatalogError> {
        self.base_services
            .lock()
            .expect("stub services lock")
            .remove(&service_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo_types::coord;

    #[test]
    fn service_record_to_ref() {
        let record = ServiceRecord {
            id: Uuid::nil(),
            name: "test".into(),
            description: None,
            format: ServiceFormat::Cog,
            storage_uri: "s3://bucket/key".into(),
            crs: Some("EPSG:4326".into()),
            temporal_start: None,
            temporal_end: None,
            created_at: Utc::now(),
        };
        let reference = record.to_service_ref();
        assert_eq!(reference.name, "test");
        assert_eq!(reference.format, ServiceFormat::Cog);
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
            purge_retention_days: 7,
            purge_poll_interval_seconds: 3600,
        });

        let catalog = PostgresDuckLakeCatalog::connect(config).await.expect("connect");
        let id = Uuid::new_v4();
        let now = Utc::now();
        let service = ServiceRecord {
            id,
            name: "integration-test".into(),
            description: Some("integration test service".into()),
            format: ServiceFormat::Cog,
            storage_uri: "s3://mantle-data/test.tif".into(),
            crs: Some("EPSG:4326".into()),
            temporal_start: Some(now),
            temporal_end: None,
            created_at: now,
        };
        let footprint = FootprintRecord {
            service_id: id,
            geometry_wkt: "POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1))".into(),
            cloud_cover: Some(10.0),
            partition_key: String::new(),
        };

        catalog
            .insert_footprint(service.clone(), footprint)
            .await
            .expect("insert");

        let fetched = catalog.get_service(id).await.expect("get");
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
