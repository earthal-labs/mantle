//! Tile latency benchmark (p99 under 10ms warm cache). Set MANTLE_LOAD_TEST=1; see docs/operations.md.

use mantle_arrow::{ServiceFormat, ServiceRef, TileRequest};
use mantle_cache::StubCacheClient;
use mantle_catalog::StubCatalogClient;
use mantle_config::{CacheConfig, CatalogConfig, StorageConfig};
use mantle_integration::env;
use mantle_raster::{OxigdalRasterEngine, RasterEngine, TileFormat, TILE_SIZE};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const CONCURRENCY: usize = 100;
const P99_BUDGET_MS: u128 = 10;

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[tokio::test]
#[ignore = "requires raster engine + warm cache + live S3; set MANTLE_LOAD_TEST=1"]
async fn tile_latency_p99_under_10ms() {
    if !env::load_test_enabled() {
        panic!(
            "set {}=1 to run the load benchmark (see docs/operations.md)",
            env::LOAD_TEST
        );
    }

    let storage = Arc::new(StorageConfig {
        backend: "s3".into(),
        bucket: std::env::var(env::BUCKET).unwrap_or_else(|_| "mantle-data".into()),
        region: "us-east-1".into(),
        endpoint: std::env::var(env::S3_ENDPOINT).ok(),
    });
    let cache_config = CacheConfig {
        redis_url: std::env::var(env::REDIS_URL)
            .unwrap_or_else(|_| "redis://localhost:6379".into()),
        ifd_ttl_seconds: 86400,
        tile_ttl_seconds: 3600,
        byte_cache_capacity_bytes: 256 * 1024 * 1024,
    };
    let catalog_config = Arc::new(CatalogConfig {
        postgres_url: std::env::var(env::POSTGRES_URL)
            .unwrap_or_else(|_| "postgres://mantle:mantle@localhost:5432/mantle".into()),
        ducklake_data_path: "./target/test-ducklake/".into(),
        geometry_column: "footprint".into(),
        purge_retention_days: 7,
        purge_poll_interval_seconds: 3600,
    });

    let cache: Arc<dyn mantle_cache::CacheClient> =
        Arc::new(StubCacheClient::new(Arc::new(cache_config.clone())));
    let catalog: Arc<dyn mantle_catalog::CatalogClient> =
        Arc::new(StubCatalogClient::new(catalog_config));

    let engine = OxigdalRasterEngine::new(
        storage.clone(),
        cache,
        catalog,
        &cache_config,
    )
    .expect("engine");

    let fixture_id = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let service = ServiceRef {
        id: fixture_id,
        name: "latency-fixture".into(),
        format: ServiceFormat::Cog,
        storage_uri: std::env::var(env::COG_URI)
            .unwrap_or_else(|_| "s3://mantle-data/fixtures/sample.tif".into()),
        crs: Some("EPSG:4326".into()),
        geometry_wkt: None,
    };
    let warm_request = TileRequest {
        service_id: fixture_id,
        z: 10,
        x: 512,
        y: 384,
        band: Some(1),
        render_rule: None,
    };
    let _ = engine
        .render_tile(&[service.clone()], &warm_request, TileFormat::WebP)
        .await
        .expect("warm tile");

    let mut latencies = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        let start = Instant::now();
        engine
            .render_tile(&[service.clone()], &warm_request, TileFormat::WebP)
            .await
            .expect("tile");
        latencies.push(start.elapsed());
    }

    latencies.sort();
    let p50 = percentile(&latencies, 0.50);
    let p99 = percentile(&latencies, 0.99);
    eprintln!(
        "tile_latency: n={CONCURRENCY} p50={}ms p99={}ms budget={P99_BUDGET_MS}ms tile={TILE_SIZE}px",
        p50.as_millis(),
        p99.as_millis()
    );
    assert!(
        p99.as_millis() < P99_BUDGET_MS,
        "p99 {}ms exceeds {}ms budget",
        p99.as_millis(),
        P99_BUDGET_MS
    );
}

#[test]
fn percentile_helper_returns_max_for_p99_of_small_set() {
    let durs = vec![
        Duration::from_millis(1),
        Duration::from_millis(5),
        Duration::from_millis(9),
    ];
    assert_eq!(percentile(&durs, 0.99).as_millis(), 9);
}

#[test]
fn percentile_helper_empty_returns_zero() {
    assert_eq!(percentile(&[], 0.99), Duration::ZERO);
}

#[test]
fn load_test_env_contract_documented() {
    assert_eq!(env::LOAD_TEST, "MANTLE_LOAD_TEST");
}
