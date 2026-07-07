//! Public per-dataset REST resource — a self-describing "image service"
//! style view (name, description, extent, available operations), distinct
//! from the admin-gated `/admin/datasets/{id}/debug` diagnostic endpoint.
//! Read-only and unauthenticated, same tier as STAC/tile routes: it only
//! ever surfaces catalog metadata + oxigdal-derived extent, never anything
//! requiring the admin token.

use crate::services::catalog_err;
use crate::{error::ApiError, AppState};
use axum::extract::{Path, State};
use axum::Json;
use serde_json::{json, Value};
use uuid::Uuid;

/// `GET /datasets/{id}` — name, description, format, CRS, real extent
/// (reprojected footprint + band/type info via the raster engine), and a
/// `links` list describing available operations (some admin-gated).
pub async fn get_dataset_resource(
    State(state): State<AppState>,
    Path(dataset_id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    let dataset = state
        .catalog
        .get_dataset(dataset_id)
        .await
        .map_err(catalog_err)?;

    // Best-effort: a dataset can exist in the catalog with an unreadable or
    // not-yet-georeferenced source file. Extent is a bonus, not a
    // requirement, for this resource to be useful.
    let extent = state
        .raster
        .debug_metadata(&dataset.to_dataset_ref())
        .await
        .ok();

    Ok(Json(json!({
        "id": dataset.id,
        "name": dataset.name,
        "description": dataset.description,
        "format": dataset.format,
        "storage_uri": dataset.storage_uri,
        "crs": dataset.crs,
        "created_at": dataset.created_at,
        "extent": extent,
        "links": [
            {"rel": "self", "href": format!("/datasets/{}", dataset.id), "method": "GET"},
            {"rel": "tiles", "href": format!("/tiles/{{z}}/{{x}}/{{y}}?dataset_id={}", dataset.id), "method": "GET"},
            {"rel": "stac-items", "href": "/stac/collections/mantle/items", "method": "GET"},
            {"rel": "debug", "href": format!("/admin/datasets/{}/debug", dataset.id), "method": "GET", "auth": "admin"},
            {"rel": "delete", "href": format!("/admin/datasets/{}/delete", dataset.id), "method": "POST", "auth": "admin"},
            {"rel": "purge", "href": format!("/admin/datasets/{}/purge", dataset.id), "method": "POST", "auth": "admin"},
            {"rel": "attach-service", "href": format!("/admin/services/{}/attach", dataset.id), "method": "POST", "auth": "admin"},
        ],
    })))
}
