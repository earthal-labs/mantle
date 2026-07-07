//! TOML configuration schema for all Mantle services.

use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct MantleConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
    pub catalog: CatalogConfig,
    pub cache: CacheConfig,
    pub analytics: AnalyticsConfig,
    pub auth: AuthConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub bind: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub backend: String,
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CatalogConfig {
    pub postgres_url: String,
    pub ducklake_data_path: String,
    pub geometry_column: String,
    #[serde(default = "default_purge_retention_days")]
    pub purge_retention_days: u64,
    #[serde(default = "default_purge_poll_interval_seconds")]
    pub purge_poll_interval_seconds: u64,
}

fn default_purge_retention_days() -> u64 {
    7
}

fn default_purge_poll_interval_seconds() -> u64 {
    3600
}

#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    pub redis_url: String,
    pub ifd_ttl_seconds: u64,
    /// TTL for cached *encoded output tile* bytes (the render_tile result
    /// cache in Redis, keyed by service(s)/z/x/y/band/render_rule/format).
    #[serde(default = "default_tile_ttl_seconds")]
    pub tile_ttl_seconds: u64,
    /// Max size of the in-process cache of raw byte ranges read from object
    /// storage (TIFF header/IFD/tile-offset arrays/tile data) — see
    /// `mantle-raster`'s `ByteRangeCache`.
    #[serde(default = "default_byte_cache_capacity_bytes")]
    pub byte_cache_capacity_bytes: u64,
}

fn default_tile_ttl_seconds() -> u64 {
    3600
}

fn default_byte_cache_capacity_bytes() -> u64 {
    256 * 1024 * 1024
}

#[derive(Debug, Clone, Deserialize)]
pub struct AnalyticsConfig {
    pub broker: String,
    pub stream_key: String,
    pub ray_address: String,
    #[serde(default = "default_vrpm_sidecar_url")]
    pub vrpm_sidecar_url: String,
    #[serde(default)]
    pub plugin_allowlist: Vec<String>,
}

fn default_vrpm_sidecar_url() -> String {
    "http://127.0.0.1:8090".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    pub admin_token_env: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
}

impl MantleConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)?;
        Self::from_str(&contents)
    }

    pub fn from_str(contents: &str) -> Result<Self, ConfigError> {
        Ok(toml::from_str(contents)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_example_config() {
        let config = MantleConfig::from_file("../../config.toml").expect("parse config");
        assert_eq!(config.server.bind, "0.0.0.0:8080");
        assert_eq!(config.storage.bucket, "mantle-data");
        assert_eq!(config.cache.ifd_ttl_seconds, 86400);
    }
}
