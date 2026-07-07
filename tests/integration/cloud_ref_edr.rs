//! Cross-boundary flow: Cloud reference NetCDF, EDR point query. Run with `--features integration -- --ignored`. See docs/operations.md.

#[cfg(feature = "integration")]
use mantle_integration::stack;

#[tokio::test]
#[ignore = "requires full stack + external NetCDF URI; see docs/KNOWN_LIMITATIONS.md"]
#[cfg(feature = "integration")]
async fn cloud_ref_edr_point_query_flow() {
    stack::require_api_healthy().await;

    let netcdf_uri = std::env::var(mantle_integration::env::NETCDF_URI)
        .expect("set MANTLE_TEST_NETCDF_URI to a reachable NetCDF/HDF5 object");

    let service_id =
        stack::register_cloud_reference("integration-netcdf", &netcdf_uri).await;

    let response = stack::edr_position(&service_id.to_string(), "-122.4,37.8").await;
    assert!(
        response.status().is_success(),
        "EDR position: {} {:?}",
        response.status(),
        response.text().await
    );
}

#[tokio::test]
#[ignore = "requires full stack + external NetCDF URI; see docs/KNOWN_LIMITATIONS.md"]
#[cfg(not(feature = "integration"))]
async fn cloud_ref_edr_point_query_flow() {
    panic!("enable --features integration to compile the live cloud-ref→EDR flow");
}
