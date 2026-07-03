//! Shared helpers for Mantle cross-boundary integration tests.
//!
//! - [`contracts`] — offline assertions against frozen `AGENTS.md` interfaces (no Docker).
//! - [`stack`] — HTTP/Redis helpers for live stack tests (`#[ignore]`; run with compose up).
//!   Requires the `integration` feature.

pub mod contracts;
#[cfg(feature = "integration")]
pub mod stack;

/// Environment variable names used by live integration and load tests.
pub mod env {
    /// API base URL (default `http://localhost:8080`).
    pub const API_URL: &str = "MANTLE_TEST_API_URL";
    /// Bearer token for `/admin/*` (required for live ingestion tests).
    pub const ADMIN_TOKEN: &str = "MANTLE_ADMIN_TOKEN";
    /// When set to `1`, enables the tile latency benchmark (`--ignored`).
    pub const LOAD_TEST: &str = "MANTLE_LOAD_TEST";
    /// Path to a local COG GeoTIFF for upload flow tests.
    pub const COG_PATH: &str = "MANTLE_TEST_COG_PATH";
    /// S3 URI of a warm-cache COG for latency benchmarks.
    pub const COG_URI: &str = "MANTLE_TEST_COG_URI";
    pub const REDIS_URL: &str = "MANTLE_TEST_REDIS_URL";
    pub const POSTGRES_URL: &str = "MANTLE_TEST_POSTGRES_URL";
    pub const S3_ENDPOINT: &str = "MANTLE_TEST_S3_ENDPOINT";
    pub const BUCKET: &str = "MANTLE_TEST_BUCKET";
    /// External NetCDF/HDF5 URI for cloud-reference EDR tests.
    pub const NETCDF_URI: &str = "MANTLE_TEST_NETCDF_URI";

    pub fn api_base_url() -> String {
        std::env::var(API_URL).unwrap_or_else(|_| "http://localhost:8080".into())
    }

    pub fn load_test_enabled() -> bool {
        std::env::var(LOAD_TEST).ok().as_deref() == Some("1")
    }
}
