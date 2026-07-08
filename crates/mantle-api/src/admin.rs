//! Admin ingestion route handlers.

use crate::error::ApiError;
use crate::services;
use crate::AppState;
use axum::{
    extract::{Multipart, Path, State},
    http::StatusCode,
    Json,
};
use futures_util::{StreamExt, TryStreamExt};
use mantle_ingestion::{
    build_object_store, delete_by_storage_uri, scene_asset_object_key, storage_uri,
    upload_stream_with_header_peek, AddSceneRequest, AddSceneResponse, CloudReferenceRequest,
    IngestionError, IngestionResponse, UploadedAsset,
};
use serde::Deserialize;
use tracing::info;
use uuid::Uuid;

/// Shared body for `POST /admin/services/upload` (new service) and
/// `POST /admin/services/{id}/scenes` (add a scene to an existing service)
/// — the only difference is whether `existing_service_id` is `Some`.
///
/// Multipart contract: `name`/`description`/`label` text fields, plus one
/// or more file fields. A plain field named `file` is the single-file case
/// (tagged `band_role: "data"`); fields named `band:<role>` (e.g.
/// `band:red`, `band:B4`) each contribute one band asset tagged with the
/// role after the colon. No ordinal pairing between separate "role" and
/// "file" fields — the role lives in the field name itself, so field order
/// doesn't matter.
async fn handle_scene_upload(
    state: AppState,
    existing_service_id: Option<Uuid>,
    mut multipart: Multipart,
) -> Result<Json<AddSceneResponse>, ApiError> {
    let service_id = existing_service_id.unwrap_or_else(Uuid::new_v4);
    let scene_id = Uuid::new_v4();

    let mut service_name = None::<String>;
    let mut description = None::<String>;
    let mut label = None::<String>;
    let mut assets = Vec::<UploadedAsset>::new();

    let store = build_object_store(&state.config.storage).map_err(ApiError::from)?;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, format!("multipart error: {e}")))?
    {
        let field_name = field.name().unwrap_or("").to_string();

        if field_name == "name" {
            let value = field.text().await.map_err(|e| {
                ApiError::new(StatusCode::BAD_REQUEST, format!("read name field: {e}"))
            })?;
            if !value.trim().is_empty() {
                service_name = Some(value);
            }
            continue;
        }
        if field_name == "description" {
            let value = field.text().await.map_err(|e| {
                ApiError::new(StatusCode::BAD_REQUEST, format!("read description field: {e}"))
            })?;
            if !value.trim().is_empty() {
                description = Some(value);
            }
            continue;
        }
        if field_name == "label" {
            let value = field.text().await.map_err(|e| {
                ApiError::new(StatusCode::BAD_REQUEST, format!("read label field: {e}"))
            })?;
            if !value.trim().is_empty() {
                label = Some(value);
            }
            continue;
        }

        let band_role = if field_name == "file" {
            "data".to_string()
        } else if let Some(role) = field_name.strip_prefix("band:") {
            role.to_string()
        } else {
            continue;
        };

        let filename = field.file_name().map(str::to_string).ok_or_else(|| {
            ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("multipart field '{field_name}' is missing a filename"),
            )
        })?;
        let content_type = field
            .content_type()
            .map(str::to_string)
            .unwrap_or_else(|| "application/octet-stream".into());

        let asset_id = Uuid::new_v4();
        let key = scene_asset_object_key(service_id, scene_id, &band_role, &filename);
        let uri = storage_uri(&state.config.storage.bucket, &key);

        let stream = field
            .into_stream()
            .map(|result| result.map_err(|e| IngestionError::Storage(e.to_string())));
        let (_bytes, header_peek) =
            upload_stream_with_header_peek(store.clone(), &key, stream).await?;

        assets.push(UploadedAsset {
            id: asset_id,
            band_role,
            content_type,
            storage_uri: uri,
            header_peek,
        });
    }

    if assets.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "at least one file is required (field 'file', or 'band:<role>' for multiple bands)",
        ));
    }

    let request = AddSceneRequest {
        service_id,
        scene_id,
        service_name,
        description,
        label,
        acquired_at: None,
    };
    let response = state
        .ingestion
        .register_scene(request, assets)
        .await
        .map_err(ApiError::from)?;

    info!(service_id = %response.service_id, scene_id = %response.scene_id, asset_count = response.asset_ids.len(), "scene registered via admin API");
    Ok(Json(response))
}

/// `POST /admin/services/upload` — multipart upload, creates a new service.
pub async fn upload_service(
    State(state): State<AppState>,
    multipart: Multipart,
) -> Result<Json<AddSceneResponse>, ApiError> {
    handle_scene_upload(state, None, multipart).await
}

/// `POST /admin/services/{id}/scenes` — multipart upload, adds a scene to an
/// existing service without touching its other scenes.
pub async fn add_scene(
    State(state): State<AppState>,
    Path(service_id): Path<Uuid>,
    multipart: Multipart,
) -> Result<Json<AddSceneResponse>, ApiError> {
    handle_scene_upload(state, Some(service_id), multipart).await
}

/// `GET /admin/services/{id}/scenes` — list every non-deleted scene for a service.
pub async fn list_scenes(
    State(state): State<AppState>,
    Path(service_id): Path<Uuid>,
) -> Result<Json<Vec<mantle_catalog::SceneWithAssets>>, ApiError> {
    let scenes = state
        .catalog
        .list_scenes(service_id)
        .await
        .map_err(services::catalog_err)?;
    Ok(Json(scenes))
}

/// `GET /admin/services/{id}/scenes/{scene_id}` — one scene's detail.
pub async fn get_scene(
    State(state): State<AppState>,
    Path((_service_id, scene_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<mantle_catalog::SceneWithAssets>, ApiError> {
    let scene = state
        .catalog
        .get_scene(scene_id)
        .await
        .map_err(services::catalog_err)?;
    Ok(Json(scene))
}

/// `POST /admin/services/{id}/scenes/{scene_id}/delete` — soft-delete one
/// scene, without affecting the rest of its service.
pub async fn delete_scene(
    State(state): State<AppState>,
    Path((_service_id, scene_id)): Path<(Uuid, Uuid)>,
    body: Option<Json<DeleteServiceRequest>>,
) -> Result<Json<mantle_catalog::SceneDeletionRecord>, ApiError> {
    let reason = body.and_then(|Json(b)| b.reason);
    let record = state
        .catalog
        .delete_scene(scene_id, reason)
        .await
        .map_err(services::catalog_err)?;

    info!(%scene_id, "scene soft-deleted via admin API");
    Ok(Json(record))
}

/// `POST /admin/services/{id}/scenes/{scene_id}/purge` — admin-only
/// immediate hard purge of one scene, bypassing the retention window.
pub async fn purge_scene(
    State(state): State<AppState>,
    Path((_service_id, scene_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, ApiError> {
    let scene = state
        .catalog
        .get_scene(scene_id)
        .await
        .map_err(services::catalog_err)?;

    let store = build_object_store(&state.config.storage).map_err(ApiError::from)?;
    for asset in &scene.assets {
        delete_by_storage_uri(store.clone(), &state.config.storage.bucket, &asset.storage_uri)
            .await
            .map_err(ApiError::from)?;
    }

    state
        .catalog
        .purge_scene(scene_id)
        .await
        .map_err(services::catalog_err)?;

    info!(%scene_id, "scene purged via admin API (immediate override)");
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /admin/services/reference` — register external cloud reference URI.
pub async fn register_cloud_reference(
    State(state): State<AppState>,
    Json(body): Json<CloudReferenceRequest>,
) -> Result<Json<IngestionResponse>, ApiError> {
    if body.name.trim().is_empty() {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "name must not be empty"));
    }
    if body.storage_uri.trim().is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "storage_uri must not be empty",
        ));
    }

    let service_id = state
        .ingestion
        .register_cloud_reference(body)
        .await
        .map_err(ApiError::from)?;
    let slug = state
        .catalog
        .get_service(service_id)
        .await
        .map(|service| service.slug)
        .unwrap_or_default();

    info!(%service_id, "cloud reference registered via admin API");
    Ok(Json(IngestionResponse { service_id, slug }))
}

#[derive(Debug, Deserialize, Default)]
pub struct DeleteServiceRequest {
    pub reason: Option<String>,
}

/// `POST /admin/services/{id}/delete` — soft-delete: hidden from every read
/// path immediately, physically purged later (scheduled job or `/purge`).
pub async fn delete_service(
    State(state): State<AppState>,
    Path(service_id): Path<Uuid>,
    body: Option<Json<DeleteServiceRequest>>,
) -> Result<Json<mantle_catalog::DeletionRecord>, ApiError> {
    let reason = body.and_then(|Json(b)| b.reason);
    let record = state
        .catalog
        .soft_delete_service(service_id, reason)
        .await
        .map_err(services::catalog_err)?;

    info!(%service_id, "service soft-deleted via admin API");
    Ok(Json(record))
}

/// `POST /admin/services/{id}/purge` — admin-only immediate hard purge,
/// bypassing the retention window. Requires the service to have been
/// soft-deleted first. Deletes every asset object across every scene the
/// service owns — a service can have many band files now, not just one.
pub async fn purge_service(
    State(state): State<AppState>,
    Path(service_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    // `get_service_any` just confirms the (possibly tombstoned) service
    // exists; the actual asset list for S3 cleanup comes from list_scenes.
    state
        .catalog
        .get_service_any(service_id)
        .await
        .map_err(services::catalog_err)?;
    let scenes = state
        .catalog
        .list_scenes(service_id)
        .await
        .map_err(services::catalog_err)?;

    let store = build_object_store(&state.config.storage).map_err(ApiError::from)?;
    for scene in &scenes {
        for asset in &scene.assets {
            delete_by_storage_uri(store.clone(), &state.config.storage.bucket, &asset.storage_uri)
                .await
                .map_err(ApiError::from)?;
        }
    }

    state
        .catalog
        .purge_service(service_id)
        .await
        .map_err(services::catalog_err)?;

    info!(%service_id, "service purged via admin API (immediate override)");
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /admin/services/{id}/debug` — reports what the raster engine
/// (oxigdal) actually detects for a service's default asset: CRS,
/// geotransform, dimensions, tiling. Direct diagnostic for "why is this
/// tile blank" without log-grepping or guessing tile coordinates.
pub async fn debug_service(
    State(state): State<AppState>,
    Path(service_id): Path<Uuid>,
) -> Result<Json<mantle_raster::CogDebugInfo>, ApiError> {
    let service_ref = state
        .catalog
        .default_service_ref(service_id)
        .await
        .map_err(services::catalog_err)?;

    let info = state
        .raster
        .debug_metadata(&service_ref)
        .await
        .map_err(|e| match e {
            mantle_raster::RasterError::NotImplemented(msg) => {
                ApiError::new(StatusCode::BAD_REQUEST, msg)
            }
            other => ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
        })?;

    Ok(Json(info))
}

pub use services::attach_function;
