//! Admin ingestion route handlers.

use crate::error::ApiError;
use crate::services;
use crate::AppState;
use axum::{
    extract::{Multipart, State},
    http::StatusCode,
    Json,
};
use futures_util::{StreamExt, TryStreamExt};
use mantle_ingestion::{
    build_object_store, dataset_object_key, storage_uri, upload_stream_with_header_peek,
    CloudReferenceRequest, IngestionError, IngestionResponse, UploadRequest,
};
use tracing::info;

/// `POST /admin/datasets/upload` — multipart upload (field `file`, optional `name`).
pub async fn upload_dataset(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<IngestionResponse>, ApiError> {
    let mut name = None::<String>;
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
            Some("file") => {
                let filename = field.file_name().map(str::to_string).ok_or_else(|| {
                    ApiError::new(StatusCode::BAD_REQUEST, "multipart field 'file' is required")
                })?;
                if let Some(ct) = field.content_type().map(str::to_string) {
                    content_type = ct;
                }

                let name = name.unwrap_or_else(|| filename.clone());
                let dataset_id = uuid::Uuid::new_v4();
                let request = UploadRequest {
                    name,
                    content_type,
                    filename: Some(filename.clone()),
                };

                let store = build_object_store(&state.config.storage).map_err(ApiError::from)?;
                let key = dataset_object_key(dataset_id, &filename);
                let uri = storage_uri(&state.config.storage.bucket, &key);

                let stream = field.into_stream().map(|result| {
                    result.map_err(|e| IngestionError::Storage(e.to_string()))
                });
                let (_bytes, header_peek) =
                    upload_stream_with_header_peek(store, &key, stream).await?;

                let dataset_id = state
                    .ingestion
                    .register_uploaded_dataset(request, dataset_id, uri, header_peek)
                    .await
                    .map_err(ApiError::from)?;

                info!(%dataset_id, "dataset uploaded via admin API");
                return Ok(Json(IngestionResponse { dataset_id }));
            }
            _ => {}
        }
    }

    Err(ApiError::new(
        StatusCode::BAD_REQUEST,
        "multipart field 'file' is required",
    ))
}

/// `POST /admin/datasets/reference` — register external cloud reference URI.
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

    let dataset_id = state
        .ingestion
        .register_cloud_reference(body)
        .await
        .map_err(ApiError::from)?;

    info!(%dataset_id, "cloud reference registered via admin API");
    Ok(Json(IngestionResponse { dataset_id }))
}

pub use services::attach_function;
