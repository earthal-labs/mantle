//! Virtual service routes — vRPM attached models and output datasets.

use crate::error::ApiError;
use crate::vrpm_client::VrpmSidecarClient;
use crate::AppState;
use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use mantle_catalog::{CatalogError, VirtualServiceKind};
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
    pub dataset_id: Uuid,
}

#[derive(Debug, Deserialize)]
pub struct ServiceTileQuery {
    pub format: Option<String>,
    pub colormap: Option<String>,
    #[serde(flatten)]
    pub overrides: HashMap<String, String>,
}

pub fn services_router() -> Router<AppState> {
    Router::new()
        .route("/{slug}", get(get_service_metadata))
        .route("/{slug}/tiles/{z}/{x}/{y}", get(get_service_tile))
        .route(
            "/{slug}/ogc/tiles/{tile_matrix_set}/{tile_matrix}/{tile_row}/{tile_col}",
            get(get_service_ogc_tile),
        )
        .route(
            "/{slug}/stac/collections/{collection_id}/items",
            get(get_service_stac_items),
        )
}

/// `POST /admin/services/{dataset_id}/attach` — attach a vRPM to a dataset.
pub async fn attach_function(
    State(state): State<AppState>,
    Path(dataset_id): Path<Uuid>,
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
            dataset_id,
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
        dataset_id: record.dataset_id,
    }))
}

async fn get_service_metadata(
    State(state): State<AppState>,
    Path(slug): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let service = state
        .catalog
        .get_virtual_service_by_slug(&slug)
        .await
        .map_err(catalog_err)?;

    let parent_id = service.parent_dataset_id.unwrap_or(service.dataset_id);
    let parent = state
        .catalog
        .get_dataset(parent_id)
        .await
        .map_err(catalog_err)?;

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
        "parent_dataset": {
            "id": parent.id,
            "name": parent.name,
            "storage_uri": parent.storage_uri,
            "format": parent.format,
        },
        "links": [
            {"rel": "self", "href": format!("/services/{}/", service.slug)},
            {"rel": "tiles", "href": format!("/services/{}/tiles/{{z}}/{{x}}/{{y}}", service.slug)},
            {"rel": "ogc-tiles", "href": format!("/services/{}/ogc/tiles/WebMercatorQuad/{{z}}/{{x}}/{{y}}", service.slug)},
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
    Path((slug, collection_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let service = state
        .catalog
        .get_virtual_service_by_slug(&slug)
        .await
        .map_err(catalog_err)?;

    let parent_id = service.parent_dataset_id.unwrap_or(service.dataset_id);
    let parent = state
        .catalog
        .get_dataset(parent_id)
        .await
        .map_err(catalog_err)?;

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
                    "href": parent.storage_uri,
                    "roles": ["data"],
                    "title": parent.name,
                }
            },
            "links": [
                {"rel": "parent", "href": format!("/stac/collections/{}", parent.id)},
                {"rel": "self", "href": format!("/services/{}/stac/collections/{}/items", slug, slug)},
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
            let dataset = state
                .catalog
                .get_dataset(service.dataset_id)
                .await
                .map_err(catalog_err)?;
            let request = mantle_arrow::TileRequest {
                dataset_id: dataset.id,
                z,
                x,
                y,
                band: None,
                render_rule: params.colormap.clone(),
            };
            let bytes = state
                .raster
                .render_tile(&[dataset.to_dataset_ref()], &request, format)
                .await
                .map_err(|e| {
                    warn!(error = %e, z, x, y, "tile render failed");
                    ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "tile render failed")
                })?;
            return Ok(tile_response(bytes, format));
        }
        VirtualServiceKind::Attached => {}
    }

    let parent_id = service.parent_dataset_id.unwrap_or(service.dataset_id);
    let parent = state
        .catalog
        .get_dataset(parent_id)
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
        dataset_id: parent.id,
        z,
        x,
        y,
        band: None,
        render_rule: None,
    };

    let layers = state
        .raster
        .read_tile_bands(&parent.to_dataset_ref(), &request, &indices)
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
