//! Service ingestion pathways: Pathway A (multipart upload → COG on S3,
//! one or more band assets per scene) and Pathway B (cloud reference +
//! VirtualiZarr→Icechunk virtual refs).

mod cloud_ref;
mod metadata;
mod service;
mod storage;
mod uri;
mod virtualize;

pub use service::MantleIngestionService;
pub use storage::{
    build_object_store, delete_by_storage_uri, scene_asset_object_key, storage_uri,
    upload_stream_with_header_peek,
};
pub use uri::{validate_storage_uri, ReferenceFormat, ReferenceScheme, ValidatedUri};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use mantle_catalog::{CatalogClient, CatalogError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudReferenceRequest {
    pub name: String,
    pub storage_uri: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionResponse {
    pub service_id: Uuid,
}

/// One already-uploaded band file, ready for catalog registration.
/// `id`/`storage_uri` are pre-minted by the caller (the admin HTTP handler)
/// since the S3 key has to be built — and the object uploaded — before the
/// catalog row can be inserted.
#[derive(Debug, Clone)]
pub struct UploadedAsset {
    pub id: Uuid,
    pub band_role: String,
    pub content_type: String,
    pub storage_uri: String,
    pub header_peek: Vec<u8>,
}

/// Request to add a scene (one or more band assets) to a service, creating
/// the service container first if it doesn't already exist yet.
#[derive(Debug, Clone)]
pub struct AddSceneRequest {
    pub service_id: Uuid,
    pub scene_id: Uuid,
    pub service_name: Option<String>,
    pub description: Option<String>,
    pub label: Option<String>,
    pub acquired_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddSceneResponse {
    pub service_id: Uuid,
    pub scene_id: Uuid,
    pub asset_ids: Vec<Uuid>,
}

#[derive(Debug, Error)]
pub enum IngestionError {
    #[error("invalid storage uri: {0}")]
    InvalidUri(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("virtualize error: {0}")]
    Virtualize(String),
    #[error("not a valid Cloud-Optimized GeoTIFF: {0}")]
    NotCog(String),
    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),
}

#[async_trait]
pub trait IngestionService: Send + Sync {
    /// Pathway A — register a scene (already-uploaded band assets) under a
    /// service, creating the service container if it doesn't exist yet.
    async fn register_scene(
        &self,
        request: AddSceneRequest,
        assets: Vec<UploadedAsset>,
    ) -> Result<AddSceneResponse, IngestionError>;

    /// Pathway B — register external URI (COG header read or VirtualiZarr→Icechunk).
    async fn register_cloud_reference(
        &self,
        request: CloudReferenceRequest,
    ) -> Result<Uuid, IngestionError>;
}

/// Stub ingestion service for tests and offline development.
pub struct StubIngestionService {
    catalog: std::sync::Arc<dyn CatalogClient>,
}

impl StubIngestionService {
    pub fn new(catalog: std::sync::Arc<dyn CatalogClient>) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl IngestionService for StubIngestionService {
    async fn register_scene(
        &self,
        request: AddSceneRequest,
        assets: Vec<UploadedAsset>,
    ) -> Result<AddSceneResponse, IngestionError> {
        if assets.is_empty() {
            return Err(IngestionError::Storage("scene requires at least one asset".into()));
        }
        let now = chrono::Utc::now();
        self.catalog
            .create_service(mantle_catalog::ServiceRecord {
                id: request.service_id,
                name: request.service_name.clone().unwrap_or_else(|| "untitled".into()),
                description: request.description.clone(),
                format: mantle_arrow::ServiceFormat::Cog,
                created_at: now,
            })
            .await?;

        let asset_ids = assets.iter().map(|a| a.id).collect();
        let asset_records = assets
            .into_iter()
            .map(|a| mantle_catalog::AssetRecord {
                id: a.id,
                service_id: request.service_id,
                scene_id: request.scene_id,
                band_role: a.band_role,
                band_index: 1,
                format: mantle_arrow::ServiceFormat::Cog,
                storage_uri: a.storage_uri,
                crs: Some("EPSG:4326".into()),
                created_at: now,
            })
            .collect();

        let scene = mantle_catalog::SceneRecord {
            id: request.scene_id,
            service_id: request.service_id,
            label: request.label,
            acquired_at: request.acquired_at,
            created_at: now,
        };
        let footprint = mantle_catalog::FootprintRecord {
            scene_id: request.scene_id,
            service_id: request.service_id,
            geometry_wkt: "POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1))".into(),
            cloud_cover: None,
            partition_key: "stub".into(),
        };
        self.catalog.add_scene(scene, asset_records, footprint).await?;

        Ok(AddSceneResponse {
            service_id: request.service_id,
            scene_id: request.scene_id,
            asset_ids,
        })
    }

    async fn register_cloud_reference(
        &self,
        request: CloudReferenceRequest,
    ) -> Result<Uuid, IngestionError> {
        validate_storage_uri(&request.storage_uri)?;
        let service_id = Uuid::new_v4();
        let scene_id = Uuid::new_v4();
        let asset_id = Uuid::new_v4();
        let now = chrono::Utc::now();
        let format = match validate_storage_uri(&request.storage_uri)?.format {
            ReferenceFormat::NetCdf | ReferenceFormat::Hdf5 => mantle_arrow::ServiceFormat::Icechunk,
            _ => mantle_arrow::ServiceFormat::Cog,
        };
        self.catalog
            .create_service(mantle_catalog::ServiceRecord {
                id: service_id,
                name: request.name,
                description: request.description,
                format,
                created_at: now,
            })
            .await?;

        let asset = mantle_catalog::AssetRecord {
            id: asset_id,
            service_id,
            scene_id,
            band_role: "data".into(),
            band_index: 1,
            format,
            storage_uri: request.storage_uri,
            crs: Some("EPSG:4326".into()),
            created_at: now,
        };
        let scene = mantle_catalog::SceneRecord {
            id: scene_id,
            service_id,
            label: None,
            acquired_at: None,
            created_at: now,
        };
        let footprint = mantle_catalog::FootprintRecord {
            scene_id,
            service_id,
            geometry_wkt: "POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1))".into(),
            cloud_cover: None,
            partition_key: "stub".into(),
        };
        self.catalog.add_scene(scene, vec![asset], footprint).await?;
        Ok(service_id)
    }
}
