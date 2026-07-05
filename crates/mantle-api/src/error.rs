//! HTTP API error responses.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use mantle_ingestion::IngestionError;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub error: String,
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorBody {
                error: self.message,
            }),
        )
            .into_response()
    }
}

impl From<IngestionError> for ApiError {
    fn from(err: IngestionError) -> Self {
        match err {
            IngestionError::InvalidUri(msg) => ApiError::new(StatusCode::BAD_REQUEST, msg),
            IngestionError::NotCog(msg) => ApiError::new(StatusCode::BAD_REQUEST, msg),
            IngestionError::Catalog(catalog_err) => {
                ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, catalog_err.to_string())
            }
            IngestionError::Storage(msg) | IngestionError::Virtualize(msg) => {
                ApiError::new(StatusCode::BAD_GATEWAY, msg)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::IntoResponse;

    #[test]
    fn maps_invalid_uri_to_400() {
        let api_err = ApiError::from(IngestionError::InvalidUri("bad".into()));
        assert_eq!(api_err.into_response().status(), StatusCode::BAD_REQUEST);
    }
}
