//! Cross-boundary flow: cache warmer pre-fills Redis IFD before first tile. See docs/operations.md.

#[cfg(feature = "integration")]
use mantle_integration::stack;
#[cfg(feature = "integration")]
use mantle_ingestion::service_object_key;
#[cfg(feature = "integration")]
use std::path::PathBuf;

#[tokio::test]
#[ignore = "requires full stack (api, worker, redis, minio); see docs/operations.md"]
#[cfg(feature = "integration")]
async fn cache_warm_before_tile_flow() {
    stack::require_api_healthy().await;

    let cog_path = std::env::var(mantle_integration::env::COG_PATH)
        .map(PathBuf::from)
        .expect("set MANTLE_TEST_COG_PATH to a local COG GeoTIFF");

    let service_id = stack::upload_cog_fixture("cache-warm-fixture", &cog_path).await;
    let filename = cog_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("fixture.tif");
    let s3_key = service_object_key(service_id, filename);

    tokio::time::sleep(std::time::Duration::from_secs(15)).await;

    assert!(
        stack::redis_has_ifd_key(&s3_key).await,
        "expected Redis IFD key mantle:ifd:{s3_key} before first tile request"
    );

    let tile = stack::fetch_tile(service_id, 10, 512, 384).await;
    assert_eq!(tile.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires full stack (api, worker, redis, minio); see docs/operations.md"]
#[cfg(not(feature = "integration"))]
async fn cache_warm_before_tile_flow() {
    panic!("enable --features integration to compile the live cache-warm flow");
}
