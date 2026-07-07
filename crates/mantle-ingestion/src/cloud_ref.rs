//! Pathway B cloud reference registration.

use crate::metadata::harvest_from_header_sample;
use crate::storage::icechunk_repo_uri;
use crate::uri::{ReferenceFormat, ValidatedUri};
use crate::virtualize::{virtualize_to_icechunk, VirtualizeRequest};
use crate::{validate_storage_uri, CloudReferenceRequest, IngestionError};
use mantle_arrow::ServiceFormat;
use tracing::info;
use uuid::Uuid;

use super::MantleIngestionService;

impl MantleIngestionService {
    pub(crate) async fn fetch_header_sample(
        &self,
        uri: &ValidatedUri,
    ) -> Result<Vec<u8>, IngestionError> {
        use object_store::path::Path;
        use object_store::GetRange;

        const HEADER_SAMPLE_BYTES: usize = 256 * 1024;

        match uri.scheme {
            crate::uri::ReferenceScheme::S3 => {
                let rest = uri.raw.strip_prefix("s3://").expect("validated s3");
                let (_bucket, key) = rest
                    .split_once('/')
                    .ok_or_else(|| IngestionError::InvalidUri("invalid s3 uri".into()))?;
                let path = Path::from(key);
                let meta = self
                    .store
                    .head(&path)
                    .await
                    .map_err(|e| IngestionError::Storage(format!("head failed: {e}")))?;
                let len = meta.size as usize;
                let end = len.min(HEADER_SAMPLE_BYTES);
                let range = GetRange::Bounded(0..end);
                let result = self
                    .store
                    .get_opts(
                        &path,
                        object_store::GetOptions {
                            range: Some(range),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|e| IngestionError::Storage(format!("range read failed: {e}")))?;
                Ok(result
                    .bytes()
                    .await
                    .map_err(|e| IngestionError::Storage(format!("read header bytes failed: {e}")))?
                    .to_vec())
            }
            crate::uri::ReferenceScheme::Https => {
                let client = reqwest::Client::new();
                let response = client
                    .get(&uri.raw)
                    .header("Range", format!("bytes=0-{HEADER_SAMPLE_BYTES}"))
                    .send()
                    .await
                    .map_err(|e| IngestionError::Storage(format!("https header read failed: {e}")))?;
                if !response.status().is_success() {
                    return Err(IngestionError::Storage(format!(
                        "https header read status {}",
                        response.status()
                    )));
                }
                Ok(response
                    .bytes()
                    .await
                    .map_err(|e| IngestionError::Storage(format!("https body read failed: {e}")))?
                    .to_vec())
            }
        }
    }
}

pub(crate) async fn register_cloud_reference(
    service: &MantleIngestionService,
    request: CloudReferenceRequest,
) -> Result<Uuid, IngestionError> {
    let validated = validate_storage_uri(&request.storage_uri)?;
    let id = Uuid::new_v4();
    let now = chrono::Utc::now();

    let (format, storage_uri, spatial) = match validated.format {
        ReferenceFormat::NetCdf | ReferenceFormat::Hdf5 => {
            let target = icechunk_repo_uri(&service.storage.bucket, id);
            let response = virtualize_to_icechunk(VirtualizeRequest {
                name: request.name.clone(),
                source_uri: validated.raw.clone(),
                target_uri: target,
            })
            .await?;
            let format = if response.format == "cog" {
                ServiceFormat::Cog
            } else {
                ServiceFormat::Icechunk
            };
            (
                format,
                response.storage_uri,
                harvest_from_header_sample(&[], validated.format),
            )
        }
        _ => {
            let header = service.fetch_header_sample(&validated).await.unwrap_or_default();
            let spatial = harvest_from_header_sample(&header, validated.format);
            (ServiceFormat::Cog, validated.raw.clone(), spatial)
        }
    };

    let record = mantle_catalog::ServiceRecord {
        id,
        name: request.name,
        description: request.description,
        format,
        storage_uri,
        crs: spatial.crs.clone(),
        temporal_start: None,
        temporal_end: None,
        created_at: now,
    };
    let footprint = mantle_catalog::FootprintRecord {
        service_id: id,
        geometry_wkt: spatial.geometry_wkt,
        cloud_cover: None,
        partition_key: String::new(),
    };

    service
        .catalog
        .insert_footprint(record, footprint)
        .await
        .map_err(IngestionError::from)?;

    info!(service_id = %id, format = ?format, "registered cloud reference");
    Ok(id)
}
