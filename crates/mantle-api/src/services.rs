//! Service routes — unified `GET /services/{id}` resource (base services by
//! UUID, attached/output virtual services by slug), plus virtual-service
//! tile/OGC-tile/STAC routes.

use crate::error::ApiError;
use crate::vrpm_client::VrpmSidecarClient;
use crate::AppState;
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use mantle_catalog::{CatalogError, SpatialQuery, VirtualServiceKind};
use mantle_ogc::validate_params_against_specs;
use mantle_raster::{
    apply_colormap, colormap_from_lut_id, encode_tile, normalize_band, parse_colormap,
    TileFormat, TILE_SIZE,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct AttachFunctionRequest {
    pub function_id: String,
    #[serde(default)]
    pub params_defaults: Value,
    pub endpoint_slug: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AttachFunctionResponse {
    pub service_id: Uuid,
    pub slug: String,
    pub function_id: String,
    pub parent_service_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct ServiceTileQuery {
    pub format: Option<String>,
    pub colormap: Option<String>,
    #[serde(flatten)]
    pub overrides: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct CompositeTileQuery {
    /// Comma-separated asset `band_role`s, positionally mapped to
    /// red/green/blue (a 4th+ role is accepted but currently ignored by the
    /// RGB compositor) — e.g. `bands=B4,B3,B2` for a Landsat true-color view.
    pub bands: String,
    pub format: Option<String>,
}

/// Absolute origin (`scheme://host`) for the incoming request, used to
/// build fully-qualified `links[].href` values — matching ArcGIS's own REST
/// directory convention of publishing complete service URLs rather than
/// paths a client has to resolve itself. Reads `Host` (required on every
/// HTTP/1.1+ request) and `X-Forwarded-Proto` (set by any reverse proxy;
/// defaults to `http` for direct/dev access).
fn request_base_url(headers: &HeaderMap) -> String {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost");
    format!("{scheme}://{host}")
}

pub fn services_router() -> Router<AppState> {
    Router::new()
        .route("/{id}", get(get_service_resource))
        .route("/{slug}/tiles/{z}/{x}/{y}", get(get_service_tile))
        .route(
            "/{slug}/ogc/tiles/{tile_matrix_set}/{tile_matrix}/{tile_row}/{tile_col}",
            get(get_service_ogc_tile),
        )
        .route(
            "/{slug}/stac/collections/{collection_id}/items",
            get(get_service_stac_items),
        )
        .route(
            "/{id}/scenes/{scene_id}/composite/{z}/{x}/{y}",
            get(get_composite_tile),
        )
}

/// `POST /admin/services/{service_id}/attach` — attach a vRPM to a service.
pub async fn attach_function(
    State(state): State<AppState>,
    Path(service_id): Path<Uuid>,
    Json(body): Json<AttachFunctionRequest>,
) -> Result<Json<AttachFunctionResponse>, ApiError> {
    if body.function_id.trim().is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "function_id must not be empty",
        ));
    }

    let client = VrpmSidecarClient::new(&state.config.analytics.vrpm_sidecar_url);
    let descriptor = client
        .get_plugin(&body.function_id)
        .await
        .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, format!("unknown function: {err}")))?;
    validate_params_against_specs(&descriptor.inputs, &body.params_defaults).map_err(|err| {
        ApiError::new(
            StatusCode::BAD_REQUEST,
            format!("invalid params_defaults: {err}"),
        )
    })?;

    let record = state
        .catalog
        .attach_function(
            service_id,
            body.function_id.clone(),
            body.params_defaults,
            body.endpoint_slug,
        )
        .await
        .map_err(catalog_err)?;

    Ok(Json(AttachFunctionResponse {
        service_id: record.id,
        slug: record.slug,
        function_id: record.function_id,
        parent_service_id: record.service_id,
    }))
}

/// `GET /services` — unified catalog list: base services (from the spatial
/// index) plus every attached/output virtual service, each tagged with
/// `kind` so the console can render one flat card grid.
pub async fn list_services(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let scenes = state
        .catalog
        .spatial_query(SpatialQuery::default())
        .await
        .map_err(catalog_err)?;
    let virtual_services = state
        .catalog
        .list_virtual_services(None)
        .await
        .map_err(catalog_err)?;

    // A service can now span multiple scenes; the catalog list shows one
    // card per service, not per scene, so dedupe on service_id.
    let mut seen = std::collections::HashSet::new();
    let mut items: Vec<Value> = Vec::new();
    for scene in &scenes {
        if !seen.insert(scene.service_id) {
            continue;
        }
        let format = scene.assets.first().map(|a| a.format);
        items.push(json!({
            "id": scene.service_id,
            "name": scene.service_name,
            "format": format,
            "kind": "service",
        }));
    }
    items.extend(virtual_services.iter().map(|service| {
        json!({
            "id": service.id,
            "name": service.slug,
            "slug": service.slug,
            "function_id": service.function_id,
            "kind": service.service_kind,
        })
    }));

    Ok(Json(json!({ "services": items })))
}

/// `GET /services/{id}` — unified item lookup. Tries `id` as a base service
/// UUID first; if that fails to parse, tries it as a base service slug, then
/// falls back to an attached/output virtual service slug. One flat
/// namespace spanning both slug kinds, matching ArcGIS's own REST directory
/// convention.
async fn get_service_resource(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let base = request_base_url(&headers);
    if let Ok(service_id) = Uuid::parse_str(&id) {
        return get_base_service_resource(state, service_id, &base).await;
    }
    match state.catalog.get_service_by_slug(&id).await {
        Ok(service) => get_base_service_resource(state, service.id, &base).await,
        Err(_) => get_virtual_service_resource(state, &id, &base).await,
    }
}

/// Base service (formerly "dataset") resource — name, description, extent,
/// available operations, and any virtual services attached to it.
async fn get_base_service_resource(
    state: AppState,
    service_id: Uuid,
    base: &str,
) -> Result<Json<Value>, ApiError> {
    let service = state
        .catalog
        .get_service(service_id)
        .await
        .map_err(catalog_err)?;

    // Best-effort: a service can exist in the catalog with no readable scene
    // yet (e.g. between upload and first scene) or an unreadable/
    // not-yet-georeferenced source file. Extent/storage_uri/crs are a bonus,
    // not a requirement, for this resource to be useful.
    let default_asset = state.catalog.default_service_ref(service_id).await.ok();
    let extent = match &default_asset {
        Some(service_ref) => state.raster.debug_metadata(service_ref).await.ok(),
        None => None,
    };

    let scenes = state
        .catalog
        .list_scenes(service_id)
        .await
        .unwrap_or_default();
    let attached_services = state
        .catalog
        .list_virtual_services(Some(service.id))
        .await
        .unwrap_or_default();

    Ok(Json(json!({
        "type": "Service",
        "id": service.id,
        "slug": service.slug,
        "name": service.name,
        "description": service.description,
        "format": service.format,
        "storage_uri": default_asset.as_ref().map(|r| &r.storage_uri),
        "crs": default_asset.as_ref().and_then(|r| r.crs.clone()),
        "created_at": service.created_at,
        "extent": extent,
        "scenes": scenes,
        "attached_services": attached_services,
        "links": [
            {"rel": "self", "href": format!("{base}/services/{}", service.slug), "method": "GET"},
            {"rel": "tiles", "href": format!("{base}/tiles/{{z}}/{{x}}/{{y}}?service_id={}", service.id), "method": "GET"},
            {"rel": "stac-items", "href": format!("{base}/stac/collections/mantle/items"), "method": "GET"},
            {"rel": "debug", "href": format!("{base}/admin/services/{}/debug", service.id), "method": "GET", "auth": "admin"},
            {"rel": "delete", "href": format!("{base}/admin/services/{}/delete", service.id), "method": "POST", "auth": "admin"},
            {"rel": "purge", "href": format!("{base}/admin/services/{}/purge", service.id), "method": "POST", "auth": "admin"},
            {"rel": "attach-service", "href": format!("{base}/admin/services/{}/attach", service.id), "method": "POST", "auth": "admin"},
        ],
    })))
}

/// Attached/output virtual service resource, resolved by public URL slug.
async fn get_virtual_service_resource(
    state: AppState,
    slug: &str,
    base: &str,
) -> Result<Json<Value>, ApiError> {
    let service = state
        .catalog
        .get_virtual_service_by_slug(slug)
        .await
        .map_err(catalog_err)?;

    let parent_id = service.parent_service_id.unwrap_or(service.service_id);
    let parent = state
        .catalog
        .get_service(parent_id)
        .await
        .map_err(catalog_err)?;
    let parent_asset = state.catalog.default_service_ref(parent_id).await.ok();

    let client = VrpmSidecarClient::new(&state.config.analytics.vrpm_sidecar_url);
    let plugin = client
        .get_plugin(&service.function_id)
        .await
        .ok();

    let mut parameters = plugin
        .as_ref()
        .map(|descriptor| descriptor.inputs.clone())
        .unwrap_or_default();
    for spec in &mut parameters {
        if let Some(value) = service.params_defaults.get(&spec.name) {
            spec.default = Some(value.clone());
        }
    }

    Ok(Json(json!({
        "type": "VirtualService",
        "id": service.id,
        "slug": service.slug,
        "service_kind": service.service_kind,
        "function_id": service.function_id,
        "params_defaults": service.params_defaults,
        "parameters": parameters,
        "plugin": plugin,
        "parent_service": {
            "id": parent.id,
            "name": parent.name,
            "storage_uri": parent_asset.as_ref().map(|r| &r.storage_uri),
            "format": parent_asset.as_ref().map(|r| r.format),
        },
        "links": [
            {"rel": "self", "href": format!("{base}/services/{}", service.slug)},
            {"rel": "tiles", "href": format!("{base}/services/{}/tiles/{{z}}/{{x}}/{{y}}", service.slug)},
            {"rel": "ogc-tiles", "href": format!("{base}/services/{}/ogc/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}", service.slug)},
        ]
    })))
}

async fn get_service_tile(
    State(state): State<AppState>,
    Path((slug, z, x, y)): Path<(String, u32, u32, u32)>,
    Query(params): Query<ServiceTileQuery>,
) -> Result<Response, ApiError> {
    render_virtual_tile(state, &slug, z, x, y, params).await
}

async fn get_service_ogc_tile(
    State(state): State<AppState>,
    Path((slug, _tms, tile_matrix, tile_row, tile_col)): Path<(
        String,
        String,
        String,
        u32,
        u32,
    )>,
    Query(params): Query<ServiceTileQuery>,
) -> Result<Response, ApiError> {
    let z: u32 = tile_matrix.parse().unwrap_or(0);
    render_virtual_tile(state, &slug, z, tile_col, tile_row, params).await
}

async fn get_service_stac_items(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((slug, collection_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let base = request_base_url(&headers);
    let service = state
        .catalog
        .get_virtual_service_by_slug(&slug)
        .await
        .map_err(catalog_err)?;

    let parent_id = service.parent_service_id.unwrap_or(service.service_id);
    let parent = state
        .catalog
        .get_service(parent_id)
        .await
        .map_err(catalog_err)?;
    let parent_asset = state.catalog.default_service_ref(parent_id).await.ok();

    let item_id = format!("{}-{}", parent.id, service.function_id);
    if collection_id != slug && collection_id != item_id {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "collection not found"));
    }

    Ok(Json(json!({
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": item_id,
            "collection": slug,
            "properties": {
                "mantle:function_id": service.function_id,
                "mantle:params_defaults": service.params_defaults,
            },
            "assets": {
                "source": {
                    "href": parent_asset.as_ref().map(|r| r.storage_uri.clone()),
                    "roles": ["data"],
                    "title": parent.name,
                }
            },
            "links": [
                {"rel": "parent", "href": format!("{base}/stac/collections/{}", parent.id)},
                {"rel": "self", "href": format!("{base}/services/{}/stac/collections/{}/items", slug, slug)},
            ]
        }]
    })))
}

async fn render_virtual_tile(
    state: AppState,
    slug: &str,
    z: u32,
    x: u32,
    y: u32,
    params: ServiceTileQuery,
) -> Result<Response, ApiError> {
    let service = state
        .catalog
        .get_virtual_service_by_slug(slug)
        .await
        .map_err(catalog_err)?;

    let format = TileFormat::from_query(params.format.as_deref());

    match service.service_kind {
        VirtualServiceKind::Output => {
            let output_ref = state
                .catalog
                .default_service_ref(service.service_id)
                .await
                .map_err(catalog_err)?;
            let request = mantle_arrow::TileRequest {
                service_id: output_ref.id,
                z,
                x,
                y,
                band: None,
                render_rule: params.colormap.clone(),
            };
            let bytes = state
                .raster
                .render_tile(&[output_ref], &request, format)
                .await
                .map_err(|e| {
                    warn!(error = %e, z, x, y, "tile render failed");
                    ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "tile render failed")
                })?;
            return Ok(tile_response(bytes, format));
        }
        VirtualServiceKind::Attached => {}
    }

    let parent_id = service.parent_service_id.unwrap_or(service.service_id);
    let parent_ref = state
        .catalog
        .default_service_ref(parent_id)
        .await
        .map_err(catalog_err)?;

    let mut effective_params = service.params_defaults.clone();
    if !params.overrides.is_empty() {
        let mut override_obj = serde_json::Map::new();
        for (key, value) in &params.overrides {
            if key == "format" || key == "colormap" {
                continue;
            }
            if let Ok(number) = value.parse::<i64>() {
                override_obj.insert(key.clone(), serde_json::json!(number));
            } else if let Ok(number) = value.parse::<f64>() {
                override_obj.insert(key.clone(), serde_json::json!(number));
            } else {
                override_obj.insert(key.clone(), serde_json::json!(value));
            }
        }
        if let Some(obj) = effective_params.as_object_mut() {
            for (key, value) in override_obj {
                obj.insert(key, value);
            }
        }
    }

    let band_map = band_indices_for_function(&service.function_id, &effective_params);
    let indices: Vec<u32> = band_map.values().copied().collect();
    let request = mantle_arrow::TileRequest {
        service_id: parent_ref.id,
        z,
        x,
        y,
        band: None,
        render_rule: None,
    };

    let layers = state
        .raster
        .read_tile_bands(&parent_ref, &request, &indices)
        .await
        .map_err(|e| {
            warn!(error = %e, z, x, y, "band read failed");
            ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "band read failed")
        })?;

    let mut named_layers = HashMap::new();
    for (name, &band_idx) in &band_map {
        let pos = indices.iter().position(|&b| b == band_idx).unwrap_or(0);
        if let Some(layer) = layers.get(pos) {
            named_layers.insert(name.clone(), layer.clone());
        }
    }

    let vrpm = VrpmSidecarClient::new(&state.config.analytics.vrpm_sidecar_url);
    let values = vrpm
        .compute_tile(
            &service.function_id,
            &effective_params,
            z,
            x,
            y,
            &named_layers,
        )
        .await
        .map_err(|err| {
            warn!(error = %err, "vRPM sidecar compute failed");
            ApiError::new(StatusCode::BAD_GATEWAY, format!("vRPM compute failed: {err}"))
        })?;

    let colormap = match params.colormap.as_deref() {
        Some(lut) => colormap_from_lut_id(lut),
        None => parse_colormap(None),
    };
    let normalized = normalize_band(&values);
    let rgba = apply_colormap(&normalized, &colormap);
    let bytes = encode_tile(&rgba, TILE_SIZE, TILE_SIZE, format).map_err(|e| {
        warn!(error = %e, "tile encode failed");
        ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "encode failed")
    })?;

    Ok(tile_response(bytes, format))
}

/// `GET /services/{id}/scenes/{scene_id}/composite/{z}/{x}/{y}?bands=...` —
/// composite a scene's single-band assets into an RGB tile directly, no
/// vRPM attach required. `id` isn't otherwise used (the scene id alone is
/// enough to resolve it) but keeps the URL self-describing.
async fn get_composite_tile(
    State(state): State<AppState>,
    Path((_service_id, scene_id, z, x, y)): Path<(Uuid, Uuid, u32, u32, u32)>,
    Query(params): Query<CompositeTileQuery>,
) -> Result<Response, ApiError> {
    let scene = state.catalog.get_scene(scene_id).await.map_err(catalog_err)?;
    let format = TileFormat::from_query(params.format.as_deref());

    let roles: Vec<&str> = params
        .bands
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if roles.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "bands query param must list at least one band role",
        ));
    }

    const CHANNEL_NAMES: [&str; 3] = ["r", "g", "b"];
    let mut assets = Vec::with_capacity(roles.len());
    for (i, role) in roles.iter().enumerate() {
        let asset = scene
            .assets
            .iter()
            .find(|a| a.band_role == *role)
            .ok_or_else(|| {
                ApiError::new(
                    StatusCode::BAD_REQUEST,
                    format!("scene has no asset with band_role '{role}'"),
                )
            })?;
        let channel = CHANNEL_NAMES.get(i).copied().unwrap_or("r").to_string();
        assets.push((
            channel,
            mantle_arrow::ServiceRef {
                id: asset.id,
                name: scene.scene.label.clone().unwrap_or_default(),
                format: asset.format,
                storage_uri: asset.storage_uri.clone(),
                crs: asset.crs.clone(),
                geometry_wkt: scene.geometry_wkt.clone(),
            },
        ));
    }

    let request = mantle_arrow::TileRequest {
        service_id: scene.scene.service_id,
        z,
        x,
        y,
        band: None,
        render_rule: None,
    };
    let bytes = state
        .raster
        .render_composite_tile(&assets, &request, format)
        .await
        .map_err(|e| {
            warn!(error = %e, z, x, y, "composite tile render failed");
            ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "composite tile render failed")
        })?;

    Ok(tile_response(bytes, format))
}

fn tile_response(bytes: Vec<u8>, format: TileFormat) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, format.content_type())],
        bytes,
    )
        .into_response()
}

/// Map logical band names to COG band indices from params_defaults.
fn band_indices_for_function(function_id: &str, params: &Value) -> HashMap<String, u32> {
    let mut map = HashMap::new();
    match function_id {
        "ndvi" => {
            let red = params.get("red_band").and_then(|v| v.as_u64()).unwrap_or(1) as u32;
            let nir = params.get("nir_band").and_then(|v| v.as_u64()).unwrap_or(2) as u32;
            map.insert("red".into(), red);
            map.insert("nir".into(), nir);
        }
        _ => {
            if let Some(obj) = params.get("band_map").and_then(|v| v.as_object()) {
                for (k, v) in obj {
                    if let Some(idx) = v.as_u64() {
                        map.insert(k.clone(), idx as u32);
                    }
                }
            }
        }
    }
    map
}

pub(crate) fn catalog_err(err: CatalogError) -> ApiError {
    match err {
        CatalogError::NotFound(id) => ApiError::new(StatusCode::NOT_FOUND, format!("not found: {id}")),
        CatalogError::ServiceNotFound(slug) => {
            ApiError::new(StatusCode::NOT_FOUND, format!("service not found: {slug}"))
        }
        CatalogError::DuplicateSlug(slug) => ApiError::new(
            StatusCode::CONFLICT,
            format!("duplicate service slug: {slug}"),
        ),
        CatalogError::InvalidGeometry(msg) => ApiError::new(StatusCode::BAD_REQUEST, msg),
        other => ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mantle_ogc::TilesRoute;

    #[test]
    fn band_indices_ndvi_defaults() {
        let params = json!({});
        let map = band_indices_for_function("ndvi", &params);
        assert_eq!(map.get("red"), Some(&1));
        assert_eq!(map.get("nir"), Some(&2));
    }

    #[test]
    fn band_indices_custom_map() {
        let params = json!({"band_map": {"a": 3, "b": 4}});
        let map = band_indices_for_function("custom_fn", &params);
        assert_eq!(map.get("a"), Some(&3));
        assert_eq!(map.get("b"), Some(&4));
    }

    #[test]
    fn tiles_route_slug_prefix() {
        let route = TilesRoute {
            collection_id: "my-slug".into(),
            tile_matrix_set: "WebMercatorQuad".into(),
            tile_matrix: "10".into(),
            tile_row: 384,
            tile_col: 512,
        };
        let req = route.to_tile_request(Uuid::nil());
        assert_eq!(req.z, 10);
    }

    #[test]
    fn attach_validation_rejects_invalid_band_index() {
        use mantle_ogc::{ParamDirection, ParamType, ParameterSpec, validate_params_against_specs};

        let specs = vec![ParameterSpec {
            name: "red_band".into(),
            param_type: ParamType::Band,
            description: "red".into(),
            direction: ParamDirection::Input,
            required: true,
            default: None,
            minimum: None,
            maximum: None,
            role: None,
            filename_template: None,
            subpath: None,
        }];
        let err = validate_params_against_specs(&specs, &json!({"red_band": 0}))
            .expect_err("invalid band");
        assert!(err.to_string().contains("red_band"));
    }
}
