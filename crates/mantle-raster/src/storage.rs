//! S3 object store helpers (mirrors mantle-worker conventions).

use mantle_config::StorageConfig;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("storage config error: {0}")]
    Config(String),
}

pub fn build_object_store(config: &StorageConfig) -> Result<Arc<dyn ObjectStore>, StorageError> {
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
        .map_err(|e| StorageError::Config(e.to_string()))
}

/// Parse `s3://bucket/key` or treat value as a key in the configured bucket.
pub fn parse_storage_uri(uri: &str, default_bucket: &str) -> Result<(String, String), StorageError> {
    if let Some(rest) = uri.strip_prefix("s3://") {
        let (bucket, key) = rest.split_once('/').ok_or_else(|| {
            StorageError::Config(format!("invalid s3 uri (missing key): {uri}"))
        })?;
        return Ok((bucket.to_string(), key.to_string()));
    }
    Ok((default_bucket.to_string(), uri.to_string()))
}

pub fn object_path(key: &str) -> Path {
    Path::from(key.trim_start_matches('/'))
}
