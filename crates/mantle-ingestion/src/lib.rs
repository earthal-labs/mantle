//! Dataset ingestion pathways: Pathway A (multipart upload → COG on S3) and
//! Pathway B (cloud reference + VirtualiZarr→Icechunk virtual refs).

mod cloud_ref;
mod metadata;
mod service;
mod storage;
mod uri;
mod virtualize;

pub use service::MantleIngestionService;
pub use storage::{
    build_object_store, dataset_object_key, storage_uri, upload_stream_with_header_peek,
};
pub use uri::{validate_storage_uri, ReferenceFormat, ReferenceScheme, ValidatedUri};

use async_trait::async_trait;
use mantle_catalog::{CatalogClient, CatalogError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadRequest {
    pub name: String,
    pub content_type: String,
    #[serde(default)]
    pub filename: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudReferenceRequest {
    pub name: String,
    pub storage_uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionResponse {
    pub dataset_id: Uuid,
}

#[derive(Debug, Error)]
pub enum IngestionError {
    #[error("invalid storage uri: {0}")]
    InvalidUri(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("virtualize error: {0}")]
    Virtualize(String),
    #[error("catalog error: {0}")]
    Catalog(#[from] CatalogError),
}

#[async_trait]
pub trait IngestionService: Send + Sync {
    /// Pathway A — register dataset after bytes are stored (admin handler streams to S3).
    async fn register_uploaded_dataset(
        &self,
        request: UploadRequest,
        dataset_id: Uuid,
        storage_uri: String,
        header_peek: Vec<u8>,
    ) -> Result<Uuid, IngestionError>;

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
    async fn register_uploaded_dataset(
        &self,
        request: UploadRequest,
        dataset_id: Uuid,
        storage_uri: String,
        _header_peek: Vec<u8>,
    ) -> Result<Uuid, IngestionError> {
        let id = dataset_id;
        let now = chrono::Utc::now();
        let dataset = mantle_catalog::DatasetRecord {
            id,
            name: request.name,
            format: mantle_arrow::DatasetFormat::Cog,
            storage_uri,
            crs: Some("EPSG:4326".into()),
            temporal_start: None,
            temporal_end: None,
            created_at: now,
        };
        let footprint = mantle_catalog::FootprintRecord {
            dataset_id: id,
            geometry_wkt: "POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1))".into(),
            cloud_cover: None,
            partition_key: "stub".into(),
        };
        self.catalog.insert_footprint(dataset, footprint).await?;
        Ok(id)
    }

    async fn register_cloud_reference(
        &self,
        request: CloudReferenceRequest,
    ) -> Result<Uuid, IngestionError> {
        validate_storage_uri(&request.storage_uri)?;
        let id = Uuid::new_v4();
        let now = chrono::Utc::now();
        let format = match validate_storage_uri(&request.storage_uri)?.format {
            ReferenceFormat::NetCdf | ReferenceFormat::Hdf5 => mantle_arrow::DatasetFormat::Icechunk,
            _ => mantle_arrow::DatasetFormat::Cog,
        };
        let dataset = mantle_catalog::DatasetRecord {
            id,
            name: request.name,
            format,
            storage_uri: request.storage_uri,
            crs: Some("EPSG:4326".into()),
            temporal_start: None,
            temporal_end: None,
            created_at: now,
        };
        let footprint = mantle_catalog::FootprintRecord {
            dataset_id: id,
            geometry_wkt: "POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1))".into(),
            cloud_cover: None,
            partition_key: "stub".into(),
        };
        self.catalog.insert_footprint(dataset, footprint).await?;
        Ok(id)
    }
}
