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

    // Credentials come from the environment (compose sets AWS_* for MinIO).
    // AmazonS3Builder::new() does not load them automatically.
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&config.bucket)
        .with_region(&config.region);

    if let Ok(key) = std::env::var("AWS_ACCESS_KEY_ID") {
        if !key.is_empty() {
            builder = builder.with_access_key_id(key);
        }
    }
    if let Ok(secret) = std::env::var("AWS_SECRET_ACCESS_KEY") {
        if !secret.is_empty() {
            builder = builder.with_secret_access_key(secret);
        }
    }

    let endpoint = config
        .endpoint
        .clone()
        .or_else(|| std::env::var("AWS_ENDPOINT_URL").ok().filter(|s| !s.is_empty()));

    if let Some(endpoint) = endpoint {
        builder = builder
            .with_endpoint(endpoint)
            .with_allow_http(true)
            // Path-style required for MinIO (bucket in URL path, not hostname).
            .with_virtual_hosted_style_request(false);
    }

    builder
        .build()
        .map(|store| Arc::new(store) as Arc<dyn ObjectStore>)
        .map_err(|e| IngestionError::Storage(format!("failed to build object store: {e}")))
}

/// Delete an object by its `s3://bucket/key` storage URI. Tolerates the
/// object already being gone (treats `NotFound` as success), so purge
/// retries after a partial failure are safe.
pub async fn delete_by_storage_uri(
    store: Arc<dyn ObjectStore>,
    bucket: &str,
    storage_uri: &str,
) -> Result<(), IngestionError> {
    let key = storage_uri
        .strip_prefix(&format!("s3://{bucket}/"))
        .unwrap_or(storage_uri);
    let path = Path::from(key.trim_start_matches('/'));
    match store.delete(&path).await {
        Ok(()) => Ok(()),
        Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(IngestionError::Storage(format!("delete {key} failed: {e}"))),
    }
}

/// Object key for one band asset within a scene.
pub fn scene_asset_object_key(
    service_id: uuid::Uuid,
    scene_id: uuid::Uuid,
    band_role: &str,
    filename: &str,
) -> String {
    let safe_name = sanitize_filename(&format!("{band_role}_{filename}"));
    format!("services/{service_id}/scenes/{scene_id}/{safe_name}")
}

pub fn storage_uri(bucket: &str, key: &str) -> String {
    format!("s3://{bucket}/{key}")
}

pub fn icechunk_repo_uri(bucket: &str, service_id: uuid::Uuid) -> String {
    format!("s3://{bucket}/icechunk/{service_id}")
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
    fn scene_asset_key_includes_service_and_scene_uuid() {
        let service_id = uuid::Uuid::nil();
        let scene_id = uuid::Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        assert_eq!(
            scene_asset_object_key(service_id, scene_id, "red", "B4.tif"),
            "services/00000000-0000-0000-0000-000000000000/scenes/11111111-1111-1111-1111-111111111111/red_B4.tif"
        );
    }
}
