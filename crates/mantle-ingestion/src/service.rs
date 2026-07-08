//! Pathway A upload orchestration (stream → S3 → catalog).

use crate::metadata::harvest_from_bytes;
use crate::storage::build_object_store;
use crate::{AddSceneRequest, AddSceneResponse, IngestionError, UploadedAsset};
use async_trait::async_trait;
use mantle_arrow::ServiceFormat;
use mantle_catalog::{AssetRecord, CatalogClient, FootprintRecord, SceneRecord, ServiceRecord};
use mantle_config::{AnalyticsConfig, StorageConfig};
use object_store::ObjectStore;
use std::sync::Arc;

/// Production ingestion service backed by S3 + catalog.
pub struct MantleIngestionService {
    pub(crate) storage: Arc<StorageConfig>,
    pub(crate) _analytics: Arc<AnalyticsConfig>,
    pub(crate) catalog: Arc<dyn CatalogClient>,
    pub(crate) store: Arc<dyn ObjectStore>,
}

impl MantleIngestionService {
    pub fn new(
        storage: Arc<StorageConfig>,
        analytics: Arc<AnalyticsConfig>,
        catalog: Arc<dyn CatalogClient>,
    ) -> Result<Self, IngestionError> {
        let store = build_object_store(&storage)?;
        Ok(Self {
            storage,
            _analytics: analytics,
            catalog,
            store,
        })
    }
}

#[async_trait]
impl crate::IngestionService for MantleIngestionService {
    async fn register_scene(
        &self,
        request: AddSceneRequest,
        assets: Vec<UploadedAsset>,
    ) -> Result<AddSceneResponse, IngestionError> {
        if assets.is_empty() {
            return Err(IngestionError::Storage(
                "scene requires at least one asset".into(),
            ));
        }

        let now = chrono::Utc::now();
        let service = self
            .catalog
            .create_service(ServiceRecord {
                id: request.service_id,
                slug: String::new(),
                name: request.service_name.clone().unwrap_or_else(|| "untitled".into()),
                description: request.description.clone(),
                format: ServiceFormat::Cog,
                created_at: now,
            })
            .await?;

        // Landsat-style bands from the same scene share one footprint; the
        // first asset's harvested geometry stands in for the whole scene
        // rather than unioning N nearly-identical polygons.
        let mut scene_geometry: Option<String> = None;
        let mut asset_records = Vec::with_capacity(assets.len());
        let asset_ids = assets.iter().map(|a| a.id).collect();

        for asset in assets {
            let spatial = harvest_from_bytes(&asset.header_peek, &asset.content_type)?;
            if scene_geometry.is_none() {
                scene_geometry = Some(spatial.geometry_wkt.clone());
            }
            asset_records.push(AssetRecord {
                id: asset.id,
                service_id: request.service_id,
                scene_id: request.scene_id,
                band_role: asset.band_role,
                band_index: 1,
                format: ServiceFormat::Cog,
                storage_uri: asset.storage_uri,
                crs: spatial.crs,
                created_at: now,
            });
        }

        let scene = SceneRecord {
            id: request.scene_id,
            service_id: request.service_id,
            label: request.label,
            acquired_at: request.acquired_at,
            created_at: now,
        };
        let footprint = FootprintRecord {
            scene_id: request.scene_id,
            service_id: request.service_id,
            geometry_wkt: scene_geometry.expect("checked assets non-empty above"),
            cloud_cover: None,
            partition_key: String::new(),
        };

        self.catalog.add_scene(scene, asset_records, footprint).await?;

        Ok(AddSceneResponse {
            service_id: request.service_id,
            service_slug: service.slug,
            scene_id: request.scene_id,
            asset_ids,
        })
    }

    async fn register_cloud_reference(
        &self,
        request: crate::CloudReferenceRequest,
    ) -> Result<uuid::Uuid, IngestionError> {
        crate::cloud_ref::register_cloud_reference(self, request).await
    }
}
