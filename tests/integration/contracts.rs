//! Offline contract tests — no Docker, Postgres, Redis, or S3 required.

use mantle_integration::contracts::{
    assert_arrow_round_trip, assert_compose_manifest_includes_core_services,
    assert_config_contract, assert_redis_key_contract, assert_route_table_covers_agents_md,
    API_ROUTES,
};

#[test]
fn redis_key_schema_matches_agents_md() {
    assert_redis_key_contract();
}

#[test]
fn arrow_ipc_round_trip_dataset_tile_job() {
    assert_arrow_round_trip();
}

#[test]
fn root_config_toml_matches_schema() {
    assert_config_contract();
}

#[test]
fn api_route_table_matches_agents_md() {
    assert_route_table_covers_agents_md();
    assert!(
        API_ROUTES.len() >= 17,
        "expected full route table, got {}",
        API_ROUTES.len()
    );
}

#[test]
fn docker_compose_declares_vrpm_sidecar_and_core_services() {
    assert_compose_manifest_includes_core_services();
}
