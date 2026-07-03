//! Pathway A upload orchestration (stream → S3 → catalog).

use crate::metadata::harvest_from_bytes;
use crate::storage::{
    build_object_store, dataset_object_key, storage_uri, upload_stream_with_header_peek,
};
use crate::{IngestionError, UploadRequest};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;
use mantle_arrow::DatasetFormat;
use mantle_catalog::{CatalogClient, DatasetRecord, FootprintRecord};
use mantle_config::{AnalyticsConfig, StorageConfig};
use object_store::ObjectStore;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

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

    /// Stream body chunks to S3, harvest metadata, register footprint in catalog.
    pub async fn upload_from_stream<S>(
        &self,
        request: UploadRequest,
        stream: S,
    ) -> Result<Uuid, IngestionError>
    where
        S: Stream<Item = Result<Bytes, IngestionError>> + Send + Unpin,
    {
        let id = Uuid::new_v4();
        let filename = request
            .filename
            .clone()
            .unwrap_or_else(|| format!("{id}.tif"));
        let key = dataset_object_key(id, &filename);
        let uri = storage_uri(&self.storage.bucket, &key);

        let (uploaded_bytes, header_peek) =
            upload_stream_with_header_peek(self.store.clone(), &key, stream).await?;
        info!(dataset_id = %id, key = %key, bytes = uploaded_bytes, "uploaded dataset to S3");

        self.register_uploaded_dataset_with_id(request, uri, header_peek, id)
            .await
    }

    /// Register a dataset whose bytes are already stored at `storage_uri`.
    pub async fn register_uploaded_dataset_with_id(
        &self,
        request: UploadRequest,
        storage_uri: String,
        header_peek: Vec<u8>,
        dataset_id: Uuid,
    ) -> Result<Uuid, IngestionError> {
        let spatial = harvest_from_bytes(&header_peek, &request.content_type)?;
        insert_catalog_record(
            self.catalog.as_ref(),
            dataset_id,
            request.name,
            DatasetFormat::Cog,
            storage_uri,
            spatial,
        )
        .await
    }
}

pub(crate) async fn insert_catalog_record(
    catalog: &dyn CatalogClient,
    id: Uuid,
    name: String,
    format: DatasetFormat,
    storage_uri: String,
    spatial: crate::metadata::SpatialMetadata,
) -> Result<Uuid, IngestionError> {
    let now = chrono::Utc::now();
    let dataset = DatasetRecord {
        id,
        name,
        format,
        storage_uri,
        crs: spatial.crs,
        temporal_start: None,
        temporal_end: None,
        created_at: now,
    };
    let footprint = FootprintRecord {
        dataset_id: id,
        geometry_wkt: spatial.geometry_wkt,
        cloud_cover: None,
        partition_key: String::new(),
    };

    catalog
        .insert_footprint(dataset, footprint)
        .await
        .map_err(IngestionError::from)
}

#[async_trait]
impl crate::IngestionService for MantleIngestionService {
    async fn register_uploaded_dataset(
        &self,
        request: UploadRequest,
        dataset_id: Uuid,
        storage_uri: String,
        header_peek: Vec<u8>,
    ) -> Result<Uuid, IngestionError> {
        self.register_uploaded_dataset_with_id(request, storage_uri, header_peek, dataset_id)
            .await
    }

    async fn register_cloud_reference(
        &self,
        request: crate::CloudReferenceRequest,
    ) -> Result<Uuid, IngestionError> {
        crate::cloud_ref::register_cloud_reference(self, request).await
    }
}
