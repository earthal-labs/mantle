//! Build `object_store` clients from Mantle storage config.

use anyhow::{Context, Result};
use mantle_config::StorageConfig;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use std::sync::Arc;

pub fn build_object_store(config: &StorageConfig) -> Result<Arc<dyn ObjectStore>> {
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
            .with_virtual_hosted_style_request(false);
    }

    let store = builder
        .build()
        .with_context(|| format!("failed to build S3 object store for bucket {}", config.bucket))?;
    Ok(Arc::new(store))
}

/// Parse `s3://bucket/key` or treat value as a key in the configured bucket.
pub fn parse_storage_uri(uri: &str, default_bucket: &str) -> Result<(String, String)> {
    if let Some(rest) = uri.strip_prefix("s3://") {
        let (bucket, key) = rest
            .split_once('/')
            .with_context(|| format!("invalid s3 uri (missing key): {uri}"))?;
        return Ok((bucket.to_string(), key.to_string()));
    }

    Ok((default_bucket.to_string(), uri.to_string()))
}

/// Normalize a storage prefix to an object-store path (no leading slash).
pub fn object_path(key: &str) -> object_store::path::Path {
    object_store::path::Path::from(key.trim_start_matches('/'))
}

/// Join a storage prefix with a relative object name.
pub fn join_object_key(prefix: &str, name: &str) -> object_store::path::Path {
    let trimmed = prefix.trim_end_matches('/');
    object_path(&format!("{trimmed}/{name}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_s3_uri_extracts_bucket_and_key() {
        let (bucket, key) =
            parse_storage_uri("s3://mantle-data/services/a.tif", "fallback").expect("parse");
        assert_eq!(bucket, "mantle-data");
        assert_eq!(key, "services/a.tif");
    }

    #[test]
    fn parse_relative_uri_uses_default_bucket() {
        let (bucket, key) = parse_storage_uri("services/a.tif", "mantle-data").expect("parse");
        assert_eq!(bucket, "mantle-data");
        assert_eq!(key, "services/a.tif");
    }
}
