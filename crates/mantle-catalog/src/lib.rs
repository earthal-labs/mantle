//! DuckLake + Postgres catalog client.
//!
//! # Hierarchy
//!
//! A service is a pure container (e.g. "Landsat 9 Collection 2"). Each service has
//! one or more scenes — one spatiotemporal acquisition, the STAC Item equivalent.
//! Each scene has one or more assets — the actual raster files, one band each, the
//! STAC Asset equivalent. A plain single-file upload is the degenerate case: one
//! scene, one asset.
//!
//! # Append-only catalog
//!
//! Mantle never updates catalog metadata in place. Each scene insert writes a new
//! GeoParquet V2 object and registers it as a new DuckLake snapshot; Postgres rows are
//! insert-only (enforced by [`migrations/002_append_only_notify.sql`](../../migrations/002_append_only_notify.sql)).
//!
//! # Partition strategy
//!
//! Footprint Parquet files are partitioned by **acquisition month** (`YYYY-MM`), derived
//! from the scene's `acquired_at` (or the insert time when absent). Paths look like:
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
use mantle_arrow::{AssetRef, SceneRef, ServiceFormat};
use mantle_config::CatalogConfig;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

/// A service container — name/description shell, nothing file-specific. All
/// raster content lives in its scenes/assets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceRecord {
    pub id: Uuid,
    /// URL-safe, human-readable identifier, unique across the whole
    /// `/services/{slug}` namespace (base and virtual services share one
    /// slug space). Pass an empty string to [`CatalogClient::create_service`]
    /// to have one generated from `name`.
    #[serde(default)]
    pub slug: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    pub format: ServiceFormat,
    pub created_at: DateTime<Utc>,
}

/// One spatiotemporal acquisition within a service (the STAC Item equivalent).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SceneRecord {
    pub id: Uuid,
    pub service_id: Uuid,
    pub label: Option<String>,
    pub acquired_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// One band file within a scene (the STAC Asset equivalent).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetRecord {
    pub id: Uuid,
    pub service_id: Uuid,
    pub scene_id: Uuid,
    pub band_role: String,
    pub band_index: u32,
    pub format: ServiceFormat,
    pub storage_uri: String,
    pub crs: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl AssetRecord {
    pub fn to_asset_ref(&self) -> AssetRef {
        AssetRef {
            id: self.id,
            band_role: self.band_role.clone(),
            band_index: self.band_index,
            format: self.format,
            storage_uri: self.storage_uri.clone(),
            crs: self.crs.clone(),
        }
    }
}

/// A scene with its full asset list and spatial metadata — the shape
/// returned by `list_scenes`/`get_scene`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SceneWithAssets {
    pub scene: SceneRecord,
    pub assets: Vec<AssetRecord>,
    pub geometry_wkt: Option<String>,
    pub cloud_cover: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FootprintRecord {
    pub scene_id: Uuid,
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

/// Same shape as [`DeletionRecord`], one level down — a single scene can be
/// soft-deleted/purged without touching the rest of its service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SceneDeletionRecord {
    pub scene_id: Uuid,
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
    /// Create a new service container (no scene/asset yet).
    /// Create a service container, generating a unique slug from `name` if
    /// `service.slug` is empty. Idempotent: calling it again for an id that
    /// already exists just returns the existing record unchanged. Returns
    /// the record actually stored (with its final slug).
    async fn create_service(&self, service: ServiceRecord) -> Result<ServiceRecord, CatalogError>;

    /// Add a new scene (one or more band assets + one footprint) to an
    /// existing service. One Postgres tx + one DuckLake Parquet append.
    async fn add_scene(
        &self,
        scene: SceneRecord,
        assets: Vec<AssetRecord>,
        footprint: FootprintRecord,
    ) -> Result<Uuid, CatalogError>;

    async fn spatial_query(&self, query: SpatialQuery) -> Result<Vec<SceneRef>, CatalogError>;

    async fn get_service(&self, id: Uuid) -> Result<ServiceRecord, CatalogError>;

    /// Resolve a base service by its public URL slug (the unified item
    /// lookup tries this when a `/services/{id}` path segment doesn't parse
    /// as a UUID, before falling back to a virtual service slug).
    async fn get_service_by_slug(&self, slug: &str) -> Result<ServiceRecord, CatalogError>;

    /// List every (non-deleted) scene for a service, each with its full asset list.
    async fn list_scenes(&self, service_id: Uuid) -> Result<Vec<SceneWithAssets>, CatalogError>;

    /// Fetch one scene with its full asset list.
    async fn get_scene(&self, scene_id: Uuid) -> Result<SceneWithAssets, CatalogError>;

    /// Hide one scene from every read path immediately, without affecting
    /// the rest of its service. Idempotent.
    async fn delete_scene(
        &self,
        scene_id: Uuid,
        reason: Option<String>,
    ) -> Result<SceneDeletionRecord, CatalogError>;

    /// Physically remove a soft-deleted scene's catalog rows (Postgres +
    /// DuckLake). Does **not** delete its assets' S3 objects — the caller
    /// does that first. Idempotent.
    async fn purge_scene(&self, scene_id: Uuid) -> Result<(), CatalogError>;

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

    /// Register a pRPM output as a new virtual service + service container.
    /// Note: does not currently register a scene/asset for the output's own
    /// raster data — nothing calls this yet (see `crates/mantle-api` job
    /// completion handling, not yet wired), so that's deferred until a real
    /// caller exists rather than speculatively built now.
    async fn register_output_service(
        &self,
        output_service: ServiceRecord,
        function_id: String,
        endpoint_slug: String,
    ) -> Result<VirtualServiceRecord, CatalogError>;

    /// Hide a service (and every scene/virtual service belonging to or
    /// attached to it) from every read path immediately. The underlying
    /// rows/files are physically removed later by the purge job, or
    /// immediately via the admin purge-now override. Idempotent: calling it
    /// again on an already-deleted service returns the original `deleted_at`.
    async fn soft_delete_service(
        &self,
        service_id: Uuid,
        reason: Option<String>,
    ) -> Result<DeletionRecord, CatalogError>;

    /// Like [`CatalogClient::get_service`] but ignores the soft-delete
    /// tombstone. Used only by purge orchestration (scheduled job / immediate
    /// override).
    async fn get_service_any(&self, id: Uuid) -> Result<ServiceRecord, CatalogError>;

    /// Physically remove a soft-deleted service's catalog rows (Postgres +
    /// DuckLake), including every scene/asset it owns, and mark its
    /// tombstone `purged_at`. Does **not** delete S3 objects — the caller
    /// does that (looping over every scene/asset via `list_scenes`) before
    /// calling this, since object storage access isn't a catalog-crate
    /// concern. Idempotent: safe to call again on a service that's already
    /// fully purged.
    async fn purge_service(&self, service_id: Uuid) -> Result<(), CatalogError>;

    /// Convenience: resolve a service to one representative `ServiceRef` —
    /// its most-recently-created scene's default asset (`band_role ==
    /// "data"` if present, else the first asset by creation order). For the
    /// common single-asset case this *is* the file; for a multi-asset scene
    /// it's a reasonable single-file stand-in for code paths that only ever
    /// needed one raster reference per service (debug metadata, the legacy
    /// `/tiles?service_id=` route, EDR/process job dispatch) and haven't
    /// been generalized to real multi-asset rendering.
    async fn default_service_ref(
        &self,
        service_id: Uuid,
    ) -> Result<mantle_arrow::ServiceRef, CatalogError> {
        let service = self.get_service(service_id).await?;
        let scenes = self.list_scenes(service_id).await?;
        let scene = scenes.into_iter().next().ok_or(CatalogError::NotFound(service_id))?;
        let asset = scene
            .assets
            .iter()
            .find(|a| a.band_role == "data")
            .or_else(|| scene.assets.first())
            .ok_or(CatalogError::NotFound(service_id))?;
        Ok(mantle_arrow::ServiceRef {
            id: asset.id,
            name: service.name,
            format: asset.format,
            storage_uri: asset.storage_uri.clone(),
            crs: asset.crs.clone(),
            geometry_wkt: scene.geometry_wkt.clone(),
        })
    }
}

/// Stub catalog client — returns empty results when Postgres/DuckLake are unavailable.
pub struct StubCatalogClient {
    _config: Arc<CatalogConfig>,
    virtual_services: std::sync::Mutex<std::collections::HashMap<String, VirtualServiceRecord>>,
    base_services: std::sync::Mutex<std::collections::HashMap<Uuid, ServiceRecord>>,
    scenes: std::sync::Mutex<std::collections::HashMap<Uuid, SceneWithAssets>>,
}

impl StubCatalogClient {
    pub fn new(config: Arc<CatalogConfig>) -> Self {
        Self {
            _config: config,
            virtual_services: std::sync::Mutex::new(std::collections::HashMap::new()),
            base_services: std::sync::Mutex::new(std::collections::HashMap::new()),
            scenes: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

#[async_trait]
impl CatalogClient for StubCatalogClient {
    async fn create_service(&self, mut service: ServiceRecord) -> Result<ServiceRecord, CatalogError> {
        let mut services = self.base_services.lock().expect("stub services lock");
        if let Some(existing) = services.get(&service.id) {
            return Ok(existing.clone());
        }
        let base_slug = if service.slug.trim().is_empty() {
            sanitize_slug(&service.name)
        } else {
            sanitize_slug(&service.slug)
        };
        let taken: std::collections::HashSet<&str> =
            services.values().map(|s| s.slug.as_str()).collect();
        let mut candidate = base_slug.clone();
        let mut attempt = 0u32;
        while taken.contains(candidate.as_str()) {
            attempt += 1;
            candidate = format!("{base_slug}-{attempt}");
        }
        service.slug = candidate;
        services.insert(service.id, service.clone());
        Ok(service)
    }

    async fn add_scene(
        &self,
        scene: SceneRecord,
        assets: Vec<AssetRecord>,
        footprint: FootprintRecord,
    ) -> Result<Uuid, CatalogError> {
        let id = scene.id;
        self.scenes.lock().expect("stub scenes lock").insert(
            id,
            SceneWithAssets {
                scene,
                assets,
                geometry_wkt: Some(footprint.geometry_wkt),
                cloud_cover: footprint.cloud_cover,
            },
        );
        Ok(id)
    }

    async fn spatial_query(&self, _query: SpatialQuery) -> Result<Vec<SceneRef>, CatalogError> {
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

    async fn get_service_by_slug(&self, slug: &str) -> Result<ServiceRecord, CatalogError> {
        let normalized = sanitize_slug(slug);
        self.base_services
            .lock()
            .expect("stub services lock")
            .values()
            .find(|s| s.slug == normalized)
            .cloned()
            .ok_or_else(|| CatalogError::ServiceNotFound(normalized))
    }

    async fn list_scenes(&self, service_id: Uuid) -> Result<Vec<SceneWithAssets>, CatalogError> {
        Ok(self
            .scenes
            .lock()
            .expect("stub scenes lock")
            .values()
            .filter(|s| s.scene.service_id == service_id)
            .cloned()
            .collect())
    }

    async fn get_scene(&self, scene_id: Uuid) -> Result<SceneWithAssets, CatalogError> {
        self.scenes
            .lock()
            .expect("stub scenes lock")
            .get(&scene_id)
            .cloned()
            .ok_or(CatalogError::NotFound(scene_id))
    }

    async fn delete_scene(
        &self,
        scene_id: Uuid,
        _reason: Option<String>,
    ) -> Result<SceneDeletionRecord, CatalogError> {
        let deleted_at = Utc::now();
        let purge_eligible_at =
            deleted_at + chrono::Duration::days(self._config.purge_retention_days as i64);
        Ok(SceneDeletionRecord {
            scene_id,
            deleted_at,
            purge_eligible_at,
        })
    }

    async fn purge_scene(&self, scene_id: Uuid) -> Result<(), CatalogError> {
        self.scenes.lock().expect("stub scenes lock").remove(&scene_id);
        Ok(())
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
        let mut virtual_services = self.virtual_services.lock().expect("stub services lock");
        if virtual_services.contains_key(&slug) {
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
        virtual_services.insert(slug, record.clone());
        Ok(record)
    }

    async fn get_virtual_service_by_slug(
        &self,
        slug: &str,
    ) -> Result<VirtualServiceRecord, CatalogError> {
        let normalized = sanitize_slug(slug);
        self.virtual_services
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
            .virtual_services
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
        mut output_service: ServiceRecord,
        function_id: String,
        endpoint_slug: String,
    ) -> Result<VirtualServiceRecord, CatalogError> {
        let slug = sanitize_slug(&endpoint_slug);
        let mut virtual_services = self.virtual_services.lock().expect("stub services lock");
        if virtual_services.contains_key(&slug) {
            return Err(CatalogError::DuplicateSlug(slug));
        }
        if output_service.slug.trim().is_empty() {
            output_service.slug = sanitize_slug(&output_service.name);
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
        virtual_services.insert(slug, record.clone());
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
        self.scenes
            .lock()
            .expect("stub scenes lock")
            .retain(|_, s| s.scene.service_id != service_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo_types::coord;

    #[tokio::test]
    async fn stub_add_scene_and_list_scenes_round_trip() {
        let config = Arc::new(CatalogConfig {
            postgres_url: "postgres://localhost/mantle".into(),
            ducklake_data_path: "s3://mantle-data/catalog/".into(),
            geometry_column: "footprint".into(),
            purge_retention_days: 7,
            purge_poll_interval_seconds: 3600,
        });
        let catalog = StubCatalogClient::new(config);

        let service_id = Uuid::new_v4();
        catalog
            .create_service(ServiceRecord {
                id: service_id,
                slug: String::new(),
                name: "landsat".into(),
                description: None,
                format: ServiceFormat::Cog,
                created_at: Utc::now(),
            })
            .await
            .expect("create_service");

        let scene_id = Uuid::new_v4();
        let asset_id = Uuid::new_v4();
        catalog
            .add_scene(
                SceneRecord {
                    id: scene_id,
                    service_id,
                    label: Some("2026-06-05".into()),
                    acquired_at: Some(Utc::now()),
                    created_at: Utc::now(),
                },
                vec![AssetRecord {
                    id: asset_id,
                    service_id,
                    scene_id,
                    band_role: "red".into(),
                    band_index: 1,
                    format: ServiceFormat::Cog,
                    storage_uri: "s3://bucket/B4.tif".into(),
                    crs: Some("EPSG:32611".into()),
                    created_at: Utc::now(),
                }],
                FootprintRecord {
                    scene_id,
                    service_id,
                    geometry_wkt: "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))".into(),
                    cloud_cover: Some(5.0),
                    partition_key: String::new(),
                },
            )
            .await
            .expect("add_scene");

        let scenes = catalog.list_scenes(service_id).await.expect("list_scenes");
        assert_eq!(scenes.len(), 1);
        assert_eq!(scenes[0].assets.len(), 1);
        assert_eq!(scenes[0].assets[0].band_role, "red");

        let fetched = catalog.get_scene(scene_id).await.expect("get_scene");
        assert_eq!(fetched.scene.id, scene_id);
    }

    #[tokio::test]
    async fn stub_create_service_generates_unique_slugs() {
        let config = Arc::new(CatalogConfig {
            postgres_url: "postgres://localhost/mantle".into(),
            ducklake_data_path: "s3://mantle-data/catalog/".into(),
            geometry_column: "footprint".into(),
            purge_retention_days: 7,
            purge_poll_interval_seconds: 3600,
        });
        let catalog = StubCatalogClient::new(config);

        let first = catalog
            .create_service(ServiceRecord {
                id: Uuid::new_v4(),
                slug: String::new(),
                name: "Landsat 9".into(),
                description: None,
                format: ServiceFormat::Cog,
                created_at: Utc::now(),
            })
            .await
            .expect("create first");
        assert_eq!(first.slug, "landsat-9");

        let second = catalog
            .create_service(ServiceRecord {
                id: Uuid::new_v4(),
                slug: String::new(),
                name: "Landsat 9".into(),
                description: None,
                format: ServiceFormat::Cog,
                created_at: Utc::now(),
            })
            .await
            .expect("create second");
        assert_ne!(second.slug, first.slug, "colliding names must get distinct slugs");

        let by_slug = catalog
            .get_service_by_slug(&first.slug)
            .await
            .expect("get_service_by_slug");
        assert_eq!(by_slug.id, first.id);

        // Idempotent: creating again with the same id returns the original
        // record (and its original slug), no wasted slug churn.
        let recreated = catalog
            .create_service(ServiceRecord {
                id: first.id,
                slug: String::new(),
                name: "Landsat 9".into(),
                description: None,
                format: ServiceFormat::Cog,
                created_at: Utc::now(),
            })
            .await
            .expect("recreate");
        assert_eq!(recreated.slug, first.slug);
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
        let service_id = Uuid::new_v4();
        let now = Utc::now();
        catalog
            .create_service(ServiceRecord {
                id: service_id,
                slug: String::new(),
                name: "integration-test".into(),
                description: Some("integration test service".into()),
                format: ServiceFormat::Cog,
                created_at: now,
            })
            .await
            .expect("create_service");

        let scene_id = Uuid::new_v4();
        let asset_id = Uuid::new_v4();
        catalog
            .add_scene(
                SceneRecord {
                    id: scene_id,
                    service_id,
                    label: None,
                    acquired_at: Some(now),
                    created_at: now,
                },
                vec![AssetRecord {
                    id: asset_id,
                    service_id,
                    scene_id,
                    band_role: "data".into(),
                    band_index: 1,
                    format: ServiceFormat::Cog,
                    storage_uri: "s3://mantle-data/test.tif".into(),
                    crs: Some("EPSG:4326".into()),
                    created_at: now,
                }],
                FootprintRecord {
                    scene_id,
                    service_id,
                    geometry_wkt: "POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1))".into(),
                    cloud_cover: Some(10.0),
                    partition_key: String::new(),
                },
            )
            .await
            .expect("add_scene");

        let fetched = catalog.get_service(service_id).await.expect("get");
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
        assert!(hits.iter().any(|hit| hit.scene_id == scene_id));
    }
}
