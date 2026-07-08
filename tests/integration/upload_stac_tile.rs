//! Upload COG, STAC search, tile 200. Run with `--features integration -- --ignored`. See docs/operations.md.

#[cfg(feature = "integration")]
use mantle_integration::stack;
#[cfg(feature = "integration")]
use std::path::PathBuf;

#[tokio::test]
#[ignore = "requires full stack (docker compose up); see docs/operations.md"]
#[cfg(feature = "integration")]
async fn upload_stac_search_tile_flow() {
    stack::require_api_healthy().await;

    let cog_path = std::env::var(mantle_integration::env::COG_PATH)
        .map(PathBuf::from)
        .expect("set MANTLE_TEST_COG_PATH to a local COG GeoTIFF");

    let (service_id, _scene_id, asset_id) =
        stack::upload_cog_fixture("integration-upload", &cog_path).await;

    let search = stack::stac_search_bbox("-180,-90,180,90").await;
    let features = search["features"]
        .as_array()
        .expect("STAC FeatureCollection.features");
    assert!(
        features.iter().any(|f| {
            // Phase 1: STAC items are still keyed by the scene's default
            // asset id (flattened via SceneRef::primary_service_ref), not
            // the scene id itself — Phase 2 (scene_to_stac_item) will make
            // this the scene id with real per-band assets.
            f["id"]
                .as_str()
                .map(|id| id == asset_id.to_string())
                .unwrap_or(false)
        }),
        "STAC search did not return uploaded service {service_id}"
    );

    let tile = stack::fetch_tile(service_id, 10, 512, 384).await;
    let status = tile.status();
    let content_type = tile
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = tile.bytes().await.expect("tile bytes");
    assert_eq!(status, reqwest::StatusCode::OK, "tile response status");
    assert!(!body.is_empty(), "tile body empty");
    assert!(
        content_type.contains("image/webp") || content_type.contains("image/png"),
        "unexpected content-type: {content_type}"
    );
}

#[tokio::test]
#[ignore = "requires full stack (docker compose up); see docs/operations.md"]
#[cfg(not(feature = "integration"))]
async fn upload_stac_search_tile_flow() {
    panic!("enable --features integration to compile the live upload→STAC→tile flow");
}
