//! S3/MinIO object store helpers for ingestion uploads.

//! Stream chunks to S3 and capture a header prefix for metadata harvest.

use crate::IngestionError;
use bytes::Bytes;
use futures_util::StreamExt;
use mantle_config::StorageConfig;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

pub const HEADER_PEEK_BYTES: usize = 256 * 1024;

pub fn build_object_store(config: &StorageConfig) -> Result<Arc<dyn ObjectStore>, IngestionError> {
    use object_store::aws::AmazonS3Builder;

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_region(&config.region);

    if let Some(endpoint) = &config.endpoint {
        builder = builder
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false);
    }

    builder
        .build()
        .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
        .map_err(|e| IngestionError::Storage(format!("failed to build object store: {e}")))
}

pub fn dataset_object_key(dataset_id: uuid::Uuid, filename: &str) -> String {
    let safe_name = sanitize_filename(filename);
    format!("datasets/{dataset_id}/{safe_name}")
}

pub fn storage_uri(bucket: &str, key: &str) -> String {
    format!("s3://{bucket}/{key}")
}

pub fn icechunk_repo_uri(bucket: &str, dataset_id: uuid::Uuid) -> String {
    format!("s3://{bucket}/icechunk/{dataset_id}")
}

fn sanitize_filename(name: &str) -> String {
    let base = name
        .rsplit('/')
        .next()
        .unwrap_or(name)
        .rsplit('\\')
        .next()
        .unwrap_or(name);
    let cleaned: String = base
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "upload.tif".into()
    } else {
        cleaned
    }
}

/// Stream upload while retaining the first [`HEADER_PEEK_BYTES`] for GeoTIFF metadata harvest.
pub async fn upload_stream_with_header_peek<S>(
    store: Arc<dyn ObjectStore>,
    key: &str,
    mut stream: S,
) -> Result<(u64, Vec<u8>), IngestionError>
where
    S: futures_util::Stream<Item = Result<Bytes, IngestionError>> + Unpin,
{
    let path = Path::from(key.trim_start_matches('/'));
    let mut writer = BufWriter::new(store, path);
    let mut total_bytes = 0u64;
    let mut header_peek = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if header_peek.len() < HEADER_PEEK_BYTES {
            let remaining = HEADER_PEEK_BYTES - header_peek.len();
            header_peek.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        }
        total_bytes += chunk.len() as u64;
        writer
            .put(chunk)
            .await
            .map_err(|e| IngestionError::Storage(format!("upload chunk failed: {e}")))?;
    }

    writer
        .shutdown()
        .await
        .map_err(|e| IngestionError::Storage(format!("upload shutdown failed: {e}")))?;

    Ok((total_bytes, header_peek))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_filename_strips_path() {
        assert_eq!(sanitize_filename("../../evil/name.tif"), "name.tif");
    }

    #[test]
    fn dataset_key_includes_uuid() {
        let id = uuid::Uuid::nil();
        assert_eq!(
            dataset_object_key(id, "scene.tif"),
            "datasets/00000000-0000-0000-0000-000000000000/scene.tif"
        );
    }
}
