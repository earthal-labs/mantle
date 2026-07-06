//! Redis IFD/zmetadata offset cache.

mod jobs;
mod redis;

use async_trait::async_trait;
use mantle_config::CacheConfig;
use std::sync::Arc;
use thiserror::Error;

pub use jobs::{
    job_key, JobQueueClient, JobState, JobStatus, RedisJobQueueClient, StubJobQueueClient,
    JOB_KEY_PREFIX, JOB_STATUS_TTL_SECONDS,
};
pub use redis::RedisCacheClient;

/// Redis key prefix for COG IFD byte offsets.
pub const IFD_KEY_PREFIX: &str = "mantle:ifd:";
/// Redis key prefix for Icechunk `.zmetadata` offsets.
pub const ZMETA_KEY_PREFIX: &str = "mantle:zmeta:";
/// Redis stream key for analytics jobs (mirrors config `analytics.stream_key`).
pub const JOBS_STREAM_KEY: &str = "mantle:jobs";
/// Redis key prefix for encoded output tile bytes (the render_tile result cache).
pub const TILE_KEY_PREFIX: &str = "mantle:tile:";

pub fn ifd_key(s3_key: &str) -> String {
    format!("{IFD_KEY_PREFIX}{s3_key}")
}

pub fn zmeta_key(repo_id: &str) -> String {
    format!("{ZMETA_KEY_PREFIX}{repo_id}")
}

pub fn tile_key(cache_key: &str) -> String {
    format!("{TILE_KEY_PREFIX}{cache_key}")
}

/// Resolve TTL: explicit non-zero value wins; otherwise use config default.
pub fn resolve_ttl(requested_ttl_seconds: u64, default_ttl_seconds: u64) -> u64 {
    if requested_ttl_seconds == 0 {
        default_ttl_seconds
    } else {
        requested_ttl_seconds
    }
}

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("cache not implemented: {0}")]
    NotImplemented(String),
    #[error("redis error: {0}")]
    Redis(String),
}

impl From<::redis::RedisError> for CacheError {
    fn from(err: ::redis::RedisError) -> Self {
        Self::Redis(err.to_string())
    }
}

#[async_trait]
pub trait CacheClient: Send + Sync {
    async fn get_ifd(&self, s3_key: &str) -> Result<Option<Vec<u8>>, CacheError>;
    async fn set_ifd(&self, s3_key: &str, data: &[u8], ttl_seconds: u64) -> Result<(), CacheError>;
    async fn get_zmetadata(&self, repo_id: &str) -> Result<Option<Vec<u8>>, CacheError>;
    async fn set_zmetadata(
        &self,
        repo_id: &str,
        data: &[u8],
        ttl_seconds: u64,
    ) -> Result<(), CacheError>;
    /// Encoded output tile bytes, keyed by dataset(s)/z/x/y/band/render_rule/format.
    async fn get_tile(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError>;
    async fn set_tile(&self, key: &str, data: &[u8], ttl_seconds: u64) -> Result<(), CacheError>;
}

/// No-op cache client for tests and offline stubs.
pub struct StubCacheClient {
    _config: Arc<CacheConfig>,
}

impl StubCacheClient {
    pub fn new(config: Arc<CacheConfig>) -> Self {
        Self { _config: config }
    }
}

#[async_trait]
impl CacheClient for StubCacheClient {
    async fn get_ifd(&self, _s3_key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        Ok(None)
    }

    async fn set_ifd(
        &self,
        _s3_key: &str,
        _data: &[u8],
        _ttl_seconds: u64,
    ) -> Result<(), CacheError> {
        Ok(())
    }

    async fn get_zmetadata(&self, _repo_id: &str) -> Result<Option<Vec<u8>>, CacheError> {
        Ok(None)
    }

    async fn set_zmetadata(
        &self,
        _repo_id: &str,
        _data: &[u8],
        _ttl_seconds: u64,
    ) -> Result<(), CacheError> {
        Ok(())
    }

    async fn get_tile(&self, _key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        Ok(None)
    }

    async fn set_tile(&self, _key: &str, _data: &[u8], _ttl_seconds: u64) -> Result<(), CacheError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ifd_key_uses_agents_md_prefix() {
        assert_eq!(ifd_key("datasets/foo.tif"), "mantle:ifd:datasets/foo.tif");
        assert!(ifd_key("x").starts_with(IFD_KEY_PREFIX));
    }

    #[test]
    fn zmeta_key_uses_agents_md_prefix() {
        assert_eq!(
            zmeta_key("550e8400-e29b-41d4-a716-446655440000"),
            "mantle:zmeta:550e8400-e29b-41d4-a716-446655440000"
        );
        assert!(zmeta_key("repo").starts_with(ZMETA_KEY_PREFIX));
    }

    #[test]
    fn resolve_ttl_prefers_explicit_non_zero() {
        assert_eq!(resolve_ttl(3600, 86400), 3600);
    }

    #[test]
    fn resolve_ttl_falls_back_to_config_default() {
        assert_eq!(resolve_ttl(0, 86400), 86400);
    }

    #[test]
    fn jobs_stream_key_matches_config_contract() {
        assert_eq!(JOBS_STREAM_KEY, "mantle:jobs");
    }
}
