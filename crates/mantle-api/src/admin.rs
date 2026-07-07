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
    build_object_store, delete_by_storage_uri, service_object_key, storage_uri,
    upload_stream_with_header_peek, CloudReferenceRequest, IngestionError, IngestionResponse,
    UploadRequest,
};
use serde::Deserialize;
use tracing::info;
use uuid::Uuid;

/// `POST /admin/services/upload` — multipart upload (field `file`, optional `name`).
pub async fn upload_service(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<IngestionResponse>, ApiError> {
    let mut name = None::<String>;
    let mut description = None::<String>;
    let mut content_type = "application/octet-stream".to_string();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, format!("multipart error: {e}")))?
    {
        match field.name() {
            Some("name") => {
                let value = field.text().await.map_err(|e| {
                    ApiError::new(StatusCode::BAD_REQUEST, format!("read name field: {e}"))
                })?;
                if !value.trim().is_empty() {
                    name = Some(value);
                }
            }
            Some("description") => {
                let value = field.text().await.map_err(|e| {
                    ApiError::new(StatusCode::BAD_REQUEST, format!("read description field: {e}"))
                })?;
                if !value.trim().is_empty() {
                    description = Some(value);
                }
            }
            Some("file") => {
                let filename = field.file_name().map(str::to_string).ok_or_else(|| {
                    ApiError::new(StatusCode::BAD_REQUEST, "multipart field 'file' is required")
                })?;
                if let Some(ct) = field.content_type().map(str::to_string) {
                    content_type = ct;
                }

                let name = name.unwrap_or_else(|| filename.clone());
                let service_id = uuid::Uuid::new_v4();
                let request = UploadRequest {
                    name,
                    content_type,
                    filename: Some(filename.clone()),
                    description,
                };

                let store = build_object_store(&state.config.storage).map_err(ApiError::from)?;
                let key = service_object_key(service_id, &filename);
                let uri = storage_uri(&state.config.storage.bucket, &key);

                let stream = field.into_stream().map(|result| {
                    result.map_err(|e| IngestionError::Storage(e.to_string()))
                });
                let (_bytes, header_peek) =
                    upload_stream_with_header_peek(store, &key, stream).await?;

                let service_id = state
                    .ingestion
                    .register_uploaded_service(request, service_id, uri, header_peek)
                    .await
                    .map_err(ApiError::from)?;

                info!(%service_id, "service uploaded via admin API");
                return Ok(Json(IngestionResponse { service_id }));
            }
            _ => {}
        }
    }

    Err(ApiError::new(
        StatusCode::BAD_REQUEST,
        "multipart field 'file' is required",
    ))
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

    info!(%service_id, "cloud reference registered via admin API");
    Ok(Json(IngestionResponse { service_id }))
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
/// soft-deleted first (`get_service_any` still finds it, `get_service` would
/// 404 on it since it's tombstoned).
pub async fn purge_service(
    State(state): State<AppState>,
    Path(service_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    let service = state
        .catalog
        .get_service_any(service_id)
        .await
        .map_err(services::catalog_err)?;

    let store = build_object_store(&state.config.storage).map_err(ApiError::from)?;
    delete_by_storage_uri(store, &state.config.storage.bucket, &service.storage_uri)
        .await
        .map_err(ApiError::from)?;

    state
        .catalog
        .purge_service(service_id)
        .await
        .map_err(services::catalog_err)?;

    info!(%service_id, "service purged via admin API (immediate override)");
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /admin/services/{id}/debug` — reports what the raster engine
/// (oxigdal) actually detects for a service: CRS, geotransform, dimensions,
/// tiling. Direct diagnostic for "why is this tile blank" without
/// log-grepping or guessing tile coordinates.
pub async fn debug_service(
    State(state): State<AppState>,
    Path(service_id): Path<Uuid>,
) -> Result<Json<mantle_raster::CogDebugInfo>, ApiError> {
    let service = state
        .catalog
        .get_service(service_id)
        .await
        .map_err(services::catalog_err)?;

    let info = state
        .raster
        .debug_metadata(&service.to_service_ref())
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
