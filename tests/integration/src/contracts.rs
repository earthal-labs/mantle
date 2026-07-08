//! Frozen contract assertions from `AGENTS.md` (no live services required).

use mantle_arrow::{
    decode_service_refs, encode_job_spec, encode_service_ref, encode_tile_request, JobSpec,
    ServiceFormat, ServiceRef, TileRequest,
};
use mantle_cache::{
    ifd_key, tile_key, zmeta_key, IFD_KEY_PREFIX, JOBS_STREAM_KEY, TILE_KEY_PREFIX,
    ZMETA_KEY_PREFIX,
};
use mantle_config::MantleConfig;
use chrono::Utc;
use uuid::Uuid;

/// API route prefixes registered by `mantle-api`, `mantle-stac`, and `mantle-ogc`.
pub const API_ROUTES: &[(&str, &str)] = &[
    ("GET", "/health"),
    ("GET", "/status/{job_id}"),
    ("GET", "/tiles/{z}/{x}/{y}"),
    ("POST", "/admin/services/upload"),
    ("POST", "/admin/services/reference"),
    ("POST", "/admin/services/{service_id}/attach"),
    ("POST", "/admin/services/{id}/scenes"),
    ("GET", "/admin/services/{id}/scenes"),
    ("GET", "/admin/services/{id}/scenes/{scene_id}"),
    ("POST", "/admin/services/{id}/scenes/{scene_id}/delete"),
    ("POST", "/admin/services/{id}/scenes/{scene_id}/purge"),
    ("GET", "/services/{id}"),
    ("GET", "/services/{slug}/tiles/{z}/{x}/{y}"),
    ("GET", "/services/{id}/scenes/{scene_id}/composite/{z}/{x}/{y}"),
    ("GET", "/plugins"),
    ("GET", "/plugins/{plugin_id}"),
    ("GET", "/stac"),
    ("GET", "/stac/"),
    ("GET", "/stac/collections"),
    ("GET", "/stac/collections/{id}"),
    ("GET", "/stac/collections/{id}/items"),
    ("GET", "/stac/search"),
    ("POST", "/stac/search"),
    (
        "GET",
        "/ogc/tiles/{tile_matrix_set}/{tile_matrix}/{tile_row}/{tile_col}",
    ),
    ("GET", "/ogc/maps/{collection_id}"),
    ("GET", "/ogc/maps/{collection_id}/plan"),
    (
        "GET",
        "/ogc/maps/{collection_id}/tiles/{tile_matrix_set}/{tile_matrix}/{tile_row}/{tile_col}",
    ),
    ("GET", "/ogc/edr/collections/{collection_id}/position"),
    ("GET", "/ogc/processes"),
    ("GET", "/ogc/processes/{process_id}"),
    ("POST", "/ogc/processes/{process_id}/execution"),
];

pub fn sample_service_ref() -> ServiceRef {
    ServiceRef {
        id: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap(),
        name: "contract-fixture".into(),
        format: ServiceFormat::Cog,
        storage_uri: "s3://mantle-data/services/contract.tif".into(),
        crs: Some("EPSG:4326".into()),
        geometry_wkt: None,
    }
}

pub fn assert_redis_key_contract() {
    assert_eq!(IFD_KEY_PREFIX, "mantle:ifd:");
    assert_eq!(ZMETA_KEY_PREFIX, "mantle:zmeta:");
    assert_eq!(TILE_KEY_PREFIX, "mantle:tile:");
    assert_eq!(JOBS_STREAM_KEY, "mantle:jobs");
    assert_eq!(ifd_key("services/foo.tif"), "mantle:ifd:services/foo.tif");
    assert_eq!(
        zmeta_key("repo-abc"),
        "mantle:zmeta:repo-abc"
    );
    assert_eq!(
        tile_key("id1:10:512:384:1::webp"),
        "mantle:tile:id1:10:512:384:1::webp"
    );
}

pub fn assert_arrow_round_trip() {
    let service = sample_service_ref();
    let encoded = encode_service_ref(&service).expect("encode service");
    let decoded = decode_service_refs(&encoded).expect("decode service");
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0], service);

    let tile = TileRequest {
        service_id: service.id,
        z: 10,
        x: 512,
        y: 384,
        band: Some(1),
        render_rule: None,
    };
    let tile_bytes = encode_tile_request(&tile).expect("encode tile");
    assert!(!tile_bytes.is_empty());

    let job = JobSpec {
        job_id: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        process_id: "ndvi".into(),
        service_refs: vec![service],
        params: serde_json::json!({"red_band": 1, "nir_band": 2}),
        submitted_at: Utc::now(),
    };
    let job_bytes = encode_job_spec(&job).expect("encode job");
    assert!(!job_bytes.is_empty());
}

pub fn assert_config_contract() {
    let config = MantleConfig::from_file("../../config.toml").expect("parse root config.toml");
    assert_eq!(config.server.bind, "0.0.0.0:8080");
    assert_eq!(config.storage.bucket, "mantle-data");
    assert_eq!(config.cache.ifd_ttl_seconds, 86400);
    assert_eq!(config.cache.tile_ttl_seconds, 3600);
    assert_eq!(config.cache.byte_cache_capacity_bytes, 268_435_456);
    assert_eq!(config.analytics.stream_key, "mantle:jobs");
    assert_eq!(config.auth.admin_token_env, "MANTLE_ADMIN_TOKEN");
}

pub fn assert_route_table_covers_agents_md() {
    let required = [
        "/health",
        "/status/",
        "/admin/services/upload",
        "/stac/search",
        "/ogc/tiles/",
        "/ogc/processes/",
    ];
    for needle in required {
        assert!(
            API_ROUTES.iter().any(|(_, path)| path.contains(needle)),
            "route table missing prefix {needle}"
        );
    }
}

/// ``docker-compose.yml`` must declare core services for local beta stacks.
pub fn assert_compose_manifest_includes_core_services() {
    let compose = std::fs::read_to_string("../../docker-compose.yml")
        .expect("read docker-compose.yml from repo root");
    for service in [
        "api:",
        "vrpm-sidecar:",
        "analytics-worker:",
        "redis:",
        "minio:",
    ] {
        assert!(
            compose.contains(service),
            "docker-compose.yml missing service {service}"
        );
    }
    assert!(
        compose.contains("vrpm-sidecar:"),
        "docker-compose.yml must declare vrpm-sidecar service"
    );
    assert!(
        compose.contains("depends_on:") && compose.contains("vrpm-sidecar"),
        "api should depend on vrpm-sidecar"
    );
}
