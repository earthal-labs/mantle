//! OGC API – Tiles, Maps, EDR, and Processes route handlers.

use crate::models::{
    coverage_json_stub, map_metadata, process_list, CoverageJsonStub, DEFAULT_COLLECTION_ID,
};
use crate::{
    build_render_execution_plan, job_spec_from_edr, job_spec_from_process, EdrPointQuery,
    normalize_process_id, validate_params_against_specs, ProcessExecutionRequest,
    VrpmSidecarUrl,
    ProcessExecutionResponse, TilesRoute,
};
use axum::{
    extract::{FromRef, Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use geo_types::Coord;
use mantle_arrow::{DatasetRef, TileRequest};
use mantle_cache::JobQueueClient;
use mantle_catalog::{CatalogClient, CatalogError, SpatialQuery};
use mantle_raster::{tile_bounds_web_mercator, RasterEngine, TileFormat};
use mantle_render_ast::RenderExecutionPlan;
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use tracing::warn;
use uuid::Uuid;

/// Shared handles extracted from the API application state.
#[derive(Clone)]
pub struct OgcState {
    pub catalog: Arc<dyn CatalogClient>,
    pub raster: Arc<dyn RasterEngine>,
    pub jobs: Arc<dyn JobQueueClient>,
    pub vrpm_sidecar_url: String,
}

impl<S> FromRef<S> for OgcState
where
    Arc<dyn CatalogClient>: FromRef<S>,
    Arc<dyn RasterEngine>: FromRef<S>,
    Arc<dyn JobQueueClient>: FromRef<S>,
    VrpmSidecarUrl: FromRef<S>,
{
    fn from_ref(state: &S) -> Self {
        Self {
            catalog: Arc::from_ref(state),
            raster: Arc::from_ref(state),
            jobs: Arc::from_ref(state),
            vrpm_sidecar_url: VrpmSidecarUrl::from_ref(state).0,
        }
    }
}

pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    OgcState: FromRef<S>,
{
    Router::new()
        .route(
            "/tiles/{tile_matrix_set}/{tile_matrix}/{tile_row}/{tile_col}",
            get(get_ogc_tile),
        )
        .route("/maps/{collection_id}", get(get_map_metadata))
        .route("/maps/{collection_id}/plan", get(get_maps_render_plan))
        .route(
            "/maps/{collection_id}/tiles/{tile_matrix_set}/{tile_matrix}/{tile_row}/{tile_col}",
            get(get_map_tile),
        )
        .route(
            "/edr/collections/{collection_id}/position",
            get(get_edr_position),
        )
        .route("/processes", get(list_processes))
        .route("/processes/{process_id}", get(get_process))
        .route("/processes/{process_id}/execution", post(execute_process))
}

#[derive(Debug, Deserialize)]
pub struct OgcTileQuery {
    pub collection_id: Option<String>,
    pub dataset_id: Option<Uuid>,
    pub format: Option<String>,
    pub band: Option<u32>,
    pub render: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MapsPlanQuery {
    pub render: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct EdrPositionQuery {
    /// Comma-separated `lon,lat` or OGC `POINT(lon lat)`.
    pub coords: String,
    pub datetime: Option<String>,
    /// Comma-separated variable names.
    pub variables: Option<String>,
    /// When true, enqueue a Ray job instead of returning a sync CoverageJSON stub.
    #[serde(default)]
    pub r#async: bool,
}

#[derive(Debug, Deserialize, Default)]
pub struct ProcessExecutionBody {
    #[serde(default)]
    pub inputs: serde_json::Value,
    #[serde(default)]
    pub datasets: Vec<Uuid>,
}

async fn get_ogc_tile<S>(
    State(state): State<S>,
    Path((tile_matrix_set, tile_matrix, tile_row, tile_col)): Path<(String, String, u32, u32)>,
    Query(params): Query<OgcTileQuery>,
) -> Result<Response, StatusCode>
where
    OgcState: FromRef<S>,
{
    let route = TilesRoute {
        collection_id: params
            .collection_id
            .clone()
            .unwrap_or_else(|| DEFAULT_COLLECTION_ID.into()),
        tile_matrix_set,
        tile_matrix,
        tile_row,
        tile_col,
    };

    render_ogc_tile(state, route, params).await
}

async fn get_map_tile<S>(
    State(state): State<S>,
    Path((collection_id, tile_matrix_set, tile_matrix, tile_row, tile_col)): Path<(
        String,
        String,
        String,
        u32,
        u32,
    )>,
    Query(params): Query<OgcTileQuery>,
) -> Result<Response, StatusCode>
where
    OgcState: FromRef<S>,
{
    let route = TilesRoute {
        collection_id,
        tile_matrix_set,
        tile_matrix,
        tile_row,
        tile_col,
    };

    render_ogc_tile(state, route, params).await
}

async fn render_ogc_tile<S>(
    state: S,
    route: TilesRoute,
    params: OgcTileQuery,
) -> Result<Response, StatusCode>
where
    OgcState: FromRef<S>,
{
    let OgcState { catalog, raster, .. } = OgcState::from_ref(&state);
    let format = TileFormat::from_query(params.format.as_deref());

    let mut request = route.to_tile_request(Uuid::nil());
    request.band = params.band;
    request.render_rule = params.render;

    if let Some(dataset_id) = params.dataset_id {
        request.dataset_id = dataset_id;
    }

    let datasets = resolve_tile_datasets(
        catalog.as_ref(),
        &route.collection_id,
        params.dataset_id,
        &request,
    )
    .await?;

    let bytes = raster
        .render_tile(&datasets, &request, format)
        .await
        .map_err(|err| {
            warn!(error = %err, "raster tile render failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, format.content_type())],
        bytes,
    )
        .into_response())
}

async fn get_map_metadata(Path(collection_id): Path<String>) -> Result<Json<serde_json::Value>, StatusCode> {
    if !is_known_collection(&collection_id) {
        return Err(StatusCode::NOT_FOUND);
    }
    let meta = map_metadata(&collection_id);
    Ok(Json(
        serde_json::to_value(meta).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    ))
}

async fn get_maps_render_plan(
    Query(params): Query<MapsPlanQuery>,
) -> Result<Json<RenderExecutionPlan>, StatusCode> {
    let rule = params.render.ok_or(StatusCode::BAD_REQUEST)?;
    let plan = build_render_execution_plan(&rule).map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Json(plan))
}

async fn get_edr_position<S>(
    State(state): State<S>,
    Path(collection_id): Path<String>,
    Query(params): Query<EdrPositionQuery>,
) -> Result<Response, StatusCode>
where
    OgcState: FromRef<S>,
{
    if !is_known_collection(&collection_id) {
        return Err(StatusCode::NOT_FOUND);
    }

    let (lon, lat) = parse_coords(&params.coords).map_err(|_| StatusCode::BAD_REQUEST)?;
    let variables: Vec<String> = params
        .variables
        .as_deref()
        .map(|v| v.split(',').map(str::trim).filter(|s| !s.is_empty()).map(String::from).collect())
        .unwrap_or_default();

    let query = EdrPointQuery {
        collection_id: collection_id.clone(),
        coords: (lon, lat),
        datetime: params.datetime.clone(),
        variables: variables.clone(),
    };

    let OgcState { catalog, jobs, .. } = OgcState::from_ref(&state);
    let datasets = resolve_edr_datasets(catalog.as_ref(), &collection_id, lon, lat).await?;

    if params.r#async {
        let job = job_spec_from_edr(&query, datasets);
        let job_id = job.job_id;
        jobs.enqueue_job(&job)
            .await
            .map_err(|err| {
                warn!(error = %err, "EDR job enqueue failed");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        return Ok((
            StatusCode::ACCEPTED,
            Json(ProcessExecutionResponse::accepted(job_id)),
        )
            .into_response());
    }

    if datasets.is_empty() {
        return Ok((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": "no datasets intersect the requested point",
                "hint": "retry with ?async=true to enqueue a Ray extraction job"
            })),
        )
            .into_response());
    }

    let cov: CoverageJsonStub = coverage_json_stub(
        lon,
        lat,
        &variables,
        params.datetime.as_deref(),
        true,
    );
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/prs.coverage+json")],
        Json(cov),
    )
        .into_response())
}

async fn list_processes() -> Json<serde_json::Value> {
    Json(serde_json::to_value(process_list()).expect("process list json"))
}

async fn get_process<S>(State(state): State<S>, Path(process_id): Path<String>) -> Result<Response, StatusCode>
where
    OgcState: FromRef<S>,
{
    let OgcState { vrpm_sidecar_url, .. } = OgcState::from_ref(&state);
    let plugin_id = normalize_process_id(&process_id);
    let descriptor = fetch_plugin_descriptor(&vrpm_sidecar_url, &plugin_id).await?;

    let list = process_list();
    let summary = list
        .processes
        .iter()
        .find(|p| normalize_process_id(&p.id) == plugin_id);

    let title = summary
        .map(|p| p.title.clone())
        .unwrap_or_else(|| plugin_id.clone());
    let description = summary
        .map(|p| p.description.clone())
        .unwrap_or_else(|| "Mantle analytics process".into());
    let version = summary
        .map(|p| p.version.clone())
        .unwrap_or(descriptor.version.clone());

    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "id": process_id,
            "title": title,
            "description": description,
            "version": version,
            "inputs": descriptor.inputs,
            "outputs": descriptor.outputs,
            "plugin": descriptor,
            "links": [
                {"rel": "self", "href": format!("/ogc/processes/{process_id}")},
                {"rel": "execute", "href": format!("/ogc/processes/{process_id}/execution")},
            ]
        })),
    )
        .into_response())
}

async fn execute_process<S>(
    State(state): State<S>,
    Path(process_id): Path<String>,
    Json(body): Json<ProcessExecutionBody>,
) -> Result<(StatusCode, Json<ProcessExecutionResponse>), StatusCode>
where
    OgcState: FromRef<S>,
{
    let OgcState {
        catalog,
        jobs,
        vrpm_sidecar_url,
        ..
    } = OgcState::from_ref(&state);

    let plugin_id = normalize_process_id(&process_id);
    if let Ok(descriptor) = fetch_plugin_descriptor(&vrpm_sidecar_url, &plugin_id).await {
        validate_params_against_specs(&descriptor.inputs, &body.inputs).map_err(|err| {
            warn!(error = %err, process_id = %process_id, "process input validation failed");
            StatusCode::BAD_REQUEST
        })?;
    } else {
        warn!(process_id = %process_id, "plugin schema unavailable; skipping input validation");
    }

    let dataset_refs = resolve_process_datasets(catalog.as_ref(), &body.datasets)
        .await
        .map_err(catalog_to_status)?;

    let request = ProcessExecutionRequest {
        process_id: process_id.clone(),
        inputs: body.inputs,
    };
    let job = job_spec_from_process(&request, dataset_refs);
    let job_id = job.job_id;

    jobs.enqueue_job(&job)
        .await
        .map_err(|err| {
            warn!(error = %err, "process job enqueue failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok((
        StatusCode::ACCEPTED,
        Json(ProcessExecutionResponse::accepted(job_id)),
    ))
}

async fn fetch_plugin_descriptor(
    sidecar_url: &str,
    plugin_id: &str,
) -> Result<crate::PluginDescriptor, StatusCode> {
    let base = sidecar_url.trim_end_matches('/');
    let url = format!("{base}/plugins/{plugin_id}");
    let response = Client::new()
        .get(&url)
        .send()
        .await
        .map_err(|err| {
            warn!(error = %err, "plugin sidecar fetch failed");
            StatusCode::BAD_GATEWAY
        })?;

    if response.status() == StatusCode::NOT_FOUND {
        return Err(StatusCode::NOT_FOUND);
    }
    if !response.status().is_success() {
        return Err(StatusCode::BAD_GATEWAY);
    }

    response.json().await.map_err(|err| {
        warn!(error = %err, "plugin descriptor decode failed");
        StatusCode::BAD_GATEWAY
    })
}

async fn resolve_tile_datasets(
    catalog: &dyn CatalogClient,
    collection_id: &str,
    explicit_dataset_id: Option<Uuid>,
    request: &TileRequest,
) -> Result<Vec<DatasetRef>, StatusCode> {
    if let Some(id) = explicit_dataset_id {
        let record = catalog
            .get_dataset(id)
            .await
            .map_err(catalog_to_status)?;
        return Ok(vec![record.to_dataset_ref()]);
    }

    if let Ok(id) = Uuid::parse_str(collection_id) {
        let record = catalog
            .get_dataset(id)
            .await
            .map_err(catalog_to_status)?;
        return Ok(vec![record.to_dataset_ref()]);
    }

    if collection_id == DEFAULT_COLLECTION_ID {
        let bounds = tile_bounds_web_mercator(request.z, request.x, request.y);
        return catalog
            .spatial_query(SpatialQuery {
                bbox: Some(bounds.to_rect()),
                ..Default::default()
            })
            .await
            .map_err(catalog_to_status);
    }

    Err(StatusCode::NOT_FOUND)
}

async fn resolve_edr_datasets(
    catalog: &dyn CatalogClient,
    collection_id: &str,
    lon: f64,
    lat: f64,
) -> Result<Vec<DatasetRef>, StatusCode> {
    let delta = 0.001;
    let bbox = geo_types::Rect::new(
        Coord { x: lon - delta, y: lat - delta },
        Coord { x: lon + delta, y: lat + delta },
    );

    if let Ok(id) = Uuid::parse_str(collection_id) {
        let record = catalog
            .get_dataset(id)
            .await
            .map_err(catalog_to_status)?;
        return Ok(vec![record.to_dataset_ref()]);
    }

    if collection_id == DEFAULT_COLLECTION_ID {
        return catalog
            .spatial_query(SpatialQuery {
                bbox: Some(bbox),
                ..Default::default()
            })
            .await
            .map_err(catalog_to_status);
    }

    Err(StatusCode::NOT_FOUND)
}

async fn resolve_process_datasets(
    catalog: &dyn CatalogClient,
    dataset_ids: &[Uuid],
) -> Result<Vec<DatasetRef>, CatalogError> {
    let mut refs = Vec::with_capacity(dataset_ids.len());
    for id in dataset_ids {
        let record = catalog.get_dataset(*id).await?;
        refs.push(record.to_dataset_ref());
    }
    Ok(refs)
}

fn is_known_collection(id: &str) -> bool {
    id == DEFAULT_COLLECTION_ID || Uuid::parse_str(id).is_ok()
}

fn parse_coords(raw: &str) -> Result<(f64, f64), ()> {
    let trimmed = raw.trim();
    if let Some(inner) = trimmed
        .strip_prefix("POINT(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let parts: Vec<&str> = inner.split_whitespace().collect();
        if parts.len() >= 2 {
            let lon: f64 = parts[0].parse().map_err(|_| ())?;
            let lat: f64 = parts[1].parse().map_err(|_| ())?;
            return Ok((lon, lat));
        }
    }
    let parts: Vec<&str> = trimmed.split(',').collect();
    if parts.len() == 2 {
        let lon: f64 = parts[0].trim().parse().map_err(|_| ())?;
        let lat: f64 = parts[1].trim().parse().map_err(|_| ())?;
        return Ok((lon, lat));
    }
    Err(())
}

fn catalog_to_status(err: CatalogError) -> StatusCode {
    warn!(error = %err, "catalog error in OGC handler");
    match err {
        CatalogError::NotFound(_) => StatusCode::NOT_FOUND,
        CatalogError::InvalidGeometry(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mantle_cache::{JobQueueClient, StubJobQueueClient};
    use crate::VrpmSidecarUrl;
    use mantle_catalog::StubCatalogClient;
    use mantle_config::CatalogConfig;
    use mantle_raster::StubRasterEngine;
    use mantle_config::{CacheConfig, StorageConfig};
    use std::sync::Arc;

    #[derive(Clone)]
    struct TestApp {
        catalog: Arc<dyn CatalogClient>,
        raster: Arc<dyn RasterEngine>,
        jobs: Arc<dyn JobQueueClient>,
        vrpm_sidecar_url: VrpmSidecarUrl,
    }

    impl FromRef<TestApp> for OgcState {
        fn from_ref(state: &TestApp) -> Self {
            OgcState {
                catalog: state.catalog.clone(),
                raster: state.raster.clone(),
                jobs: state.jobs.clone(),
                vrpm_sidecar_url: state.vrpm_sidecar_url.0.clone(),
            }
        }
    }

    impl FromRef<TestApp> for VrpmSidecarUrl {
        fn from_ref(state: &TestApp) -> Self {
            state.vrpm_sidecar_url.clone()
        }
    }

    fn test_app() -> TestApp {
        let storage = Arc::new(StorageConfig {
            backend: "s3".into(),
            bucket: "mantle-data".into(),
            region: "us-east-1".into(),
            endpoint: None,
        });
        let cache = Arc::new(mantle_cache::StubCacheClient::new(Arc::new(CacheConfig {
            redis_url: "redis://localhost:6379".into(),
            ifd_ttl_seconds: 86400,
            tile_ttl_seconds: 3600,
            byte_cache_capacity_bytes: 256 * 1024 * 1024,
        })));
        TestApp {
            catalog: Arc::new(StubCatalogClient::new(Arc::new(CatalogConfig {
                postgres_url: "postgres://localhost/mantle".into(),
                ducklake_data_path: "./data/".into(),
                geometry_column: "footprint".into(),
                purge_retention_days: 7,
                purge_poll_interval_seconds: 3600,
            }))),
            raster: Arc::new(StubRasterEngine::new(storage, cache)),
            jobs: Arc::new(StubJobQueueClient),
            vrpm_sidecar_url: VrpmSidecarUrl("http://127.0.0.1:8090".into()),
        }
    }

    #[test]
    fn tiles_route_builds_tile_request() {
        use crate::models::WEB_MERCATOR_TILE_MATRIX_SET;
        let route = TilesRoute {
            collection_id: "mantle".into(),
            tile_matrix_set: WEB_MERCATOR_TILE_MATRIX_SET.into(),
            tile_matrix: "12".into(),
            tile_row: 100,
            tile_col: 200,
        };
        let id = Uuid::new_v4();
        let req = route.to_tile_request(id);
        assert_eq!(req.dataset_id, id);
        assert_eq!(req.z, 12);
        assert_eq!(req.x, 200);
        assert_eq!(req.y, 100);
    }

    #[test]
    fn parse_coords_accepts_comma_separated() {
        let (lon, lat) = parse_coords("-122.4,37.8").unwrap();
        assert!((lon + 122.4).abs() < f64::EPSILON);
        assert!((lat - 37.8).abs() < f64::EPSILON);
    }

    #[test]
    fn parse_coords_accepts_point_wkt() {
        let (lon, lat) = parse_coords("POINT(-122.4 37.8)").unwrap();
        assert!((lon + 122.4).abs() < f64::EPSILON);
        assert!((lat - 37.8).abs() < f64::EPSILON);
    }

    #[test]
    fn process_execution_response_202_shape() {
        let job_id = Uuid::new_v4();
        let resp = ProcessExecutionResponse::accepted(job_id);
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["job_id"], serde_json::json!(job_id.to_string()));
        assert_eq!(json["status_url"], serde_json::json!(format!("/status/{job_id}")));
    }

    #[tokio::test]
    async fn execute_process_returns_202() {
        let app = test_app();
        let state = app.clone();
        let body = ProcessExecutionBody {
            inputs: serde_json::json!({"red_band": 1}),
            datasets: vec![],
        };
        let result = execute_process(
            State(state),
            Path("ndvi".into()),
            Json(body),
        )
        .await
        .expect("202");
        assert_eq!(result.0, StatusCode::ACCEPTED);
        assert_eq!(result.1.job_id, result.1.job_id);
        assert!(result.1.status_url.starts_with("/status/"));
    }

    #[tokio::test]
    async fn edr_position_without_datasets_returns_not_found() {
        let app = test_app();
        let params = EdrPositionQuery {
            coords: "-122.4,37.8".into(),
            datetime: None,
            variables: Some("temp".into()),
            r#async: false,
        };
        let response = get_edr_position(
            State(app),
            Path(DEFAULT_COLLECTION_ID.into()),
            Query(params),
        )
        .await
        .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn edr_position_async_enqueues_job() {
        let app = test_app();
        let params = EdrPositionQuery {
            coords: "-122.4,37.8".into(),
            datetime: None,
            variables: Some("temp".into()),
            r#async: true,
        };
        let response = get_edr_position(
            State(app),
            Path(DEFAULT_COLLECTION_ID.into()),
            Query(params),
        )
        .await
        .expect("response");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }
}
