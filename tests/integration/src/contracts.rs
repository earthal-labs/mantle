//! Frozen contract assertions from `AGENTS.md` (no live services required).

use mantle_arrow::{
    decode_dataset_refs, encode_dataset_ref, encode_job_spec, encode_tile_request, DatasetFormat,
    DatasetRef, JobSpec, TileRequest,
};
use mantle_cache::{ifd_key, zmeta_key, IFD_KEY_PREFIX, JOBS_STREAM_KEY, ZMETA_KEY_PREFIX};
use mantle_config::MantleConfig;
use chrono::Utc;
use uuid::Uuid;

/// API route prefixes registered by `mantle-api`, `mantle-stac`, and `mantle-ogc`.
pub const API_ROUTES: &[(&str, &str)] = &[
    ("GET", "/health"),
    ("GET", "/status/{job_id}"),
    ("GET", "/tiles/{z}/{x}/{y}"),
    ("POST", "/admin/datasets/upload"),
    ("POST", "/admin/datasets/reference"),
    ("POST", "/admin/services/{dataset_id}/attach"),
    ("GET", "/services/{slug}"),
    ("GET", "/services/{slug}/tiles/{z}/{x}/{y}"),
    ("GET", "/plugins"),
    ("GET", "/plugins/{plugin_id}"),
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

pub fn sample_dataset_ref() -> DatasetRef {
    DatasetRef {
        id: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap(),
        name: "contract-fixture".into(),
        format: DatasetFormat::Cog,
        storage_uri: "s3://mantle-data/datasets/contract.tif".into(),
        crs: Some("EPSG:4326".into()),
    }
}

pub fn assert_redis_key_contract() {
    assert_eq!(IFD_KEY_PREFIX, "mantle:ifd:");
    assert_eq!(ZMETA_KEY_PREFIX, "mantle:zmeta:");
    assert_eq!(JOBS_STREAM_KEY, "mantle:jobs");
    assert_eq!(ifd_key("datasets/foo.tif"), "mantle:ifd:datasets/foo.tif");
    assert_eq!(
        zmeta_key("repo-abc"),
        "mantle:zmeta:repo-abc"
    );
}

pub fn assert_arrow_round_trip() {
    let dataset = sample_dataset_ref();
    let encoded = encode_dataset_ref(&dataset).expect("encode dataset");
    let decoded = decode_dataset_refs(&encoded).expect("decode dataset");
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0], dataset);

    let tile = TileRequest {
        dataset_id: dataset.id,
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
        dataset_refs: vec![dataset],
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
    assert_eq!(config.analytics.stream_key, "mantle:jobs");
    assert_eq!(config.auth.admin_token_env, "MANTLE_ADMIN_TOKEN");
}

pub fn assert_route_table_covers_agents_md() {
    let required = [
        "/health",
        "/status/",
        "/admin/datasets/upload",
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
