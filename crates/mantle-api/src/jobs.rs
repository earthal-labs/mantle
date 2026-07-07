//! Job enqueue helper and `/status/{job_id}` polling route.

use crate::error::ApiError;
use crate::AppState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use mantle_arrow::JobSpec;
use mantle_cache::JobStatus;
use serde::Serialize;
use uuid::Uuid;

#[derive(Debug, Serialize)]
pub struct JobStatusResponse {
    pub state: String,
    pub progress: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl From<JobStatus> for JobStatusResponse {
    fn from(status: JobStatus) -> Self {
        Self {
            state: format!("{:?}", status.state).to_lowercase(),
            progress: status.progress,
            result_url: status.result_url,
            error: status.error,
        }
    }
}

/// Enqueue an analytics job on the Redis stream and seed pending status.
pub async fn enqueue_job(state: &AppState, job: JobSpec) -> Result<Uuid, ApiError> {
    let job_id = job.job_id;
    state
        .jobs
        .enqueue_job(&job)
        .await
        .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;
    Ok(job_id)
}

pub async fn get_job_status(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let status = state
        .jobs
        .get_job_status(job_id)
        .await
        .map_err(|err| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))?;

    match status {
        Some(s) => Ok((StatusCode::OK, Json(JobStatusResponse::from(s)))),
        None => Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("job {job_id} not found"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use mantle_cache::{JobQueueClient, StubJobQueueClient};
    use mantle_config::{
        AnalyticsConfig, AuthConfig, CacheConfig, CatalogConfig, MantleConfig, ServerConfig,
        StorageConfig,
    };
    use mantle_ingestion::StubIngestionService;
    use std::sync::Arc;

    fn test_state(jobs: Arc<dyn JobQueueClient>) -> AppState {
        let config = Arc::new(MantleConfig {
            server: ServerConfig {
                bind: "127.0.0.1:8080".into(),
            },
            storage: StorageConfig {
                backend: "s3".into(),
                bucket: "mantle-data".into(),
                region: "us-east-1".into(),
                endpoint: None,
            },
            catalog: CatalogConfig {
                postgres_url: "postgres://localhost/mantle".into(),
                ducklake_data_path: "s3://mantle-data/catalog/".into(),
                geometry_column: "footprint".into(),
                purge_retention_days: 7,
                purge_poll_interval_seconds: 3600,
            },
            cache: CacheConfig {
                redis_url: "redis://localhost:6379".into(),
                ifd_ttl_seconds: 86400,
                tile_ttl_seconds: 3600,
                byte_cache_capacity_bytes: 256 * 1024 * 1024,
            },
            analytics: AnalyticsConfig {
                broker: "redis-streams".into(),
                stream_key: "mantle:jobs".into(),
                ray_address: "ray://localhost:10001".into(),
                vrpm_sidecar_url: "http://127.0.0.1:8090".into(),
                plugin_allowlist: vec![],
            },
            auth: AuthConfig {
                admin_token_env: "MANTLE_ADMIN_TOKEN".into(),
            },
        });

        AppState {
            config,
            catalog: Arc::new(mantle_catalog::StubCatalogClient::new(Arc::new(
                CatalogConfig {
                    postgres_url: "postgres://localhost/mantle".into(),
                    ducklake_data_path: "s3://mantle-data/catalog/".into(),
                    geometry_column: "footprint".into(),
                    purge_retention_days: 7,
                    purge_poll_interval_seconds: 3600,
                },
            ))),
            cache: Arc::new(mantle_cache::StubCacheClient::new(Arc::new(CacheConfig {
                redis_url: "redis://localhost:6379".into(),
                ifd_ttl_seconds: 86400,
                tile_ttl_seconds: 3600,
                byte_cache_capacity_bytes: 256 * 1024 * 1024,
            }))),
            raster: Arc::new(mantle_raster::StubRasterEngine::new(
                Arc::new(StorageConfig {
                    backend: "s3".into(),
                    bucket: "mantle-data".into(),
                    region: "us-east-1".into(),
                    endpoint: None,
                }),
                Arc::new(mantle_cache::StubCacheClient::new(Arc::new(CacheConfig {
                    redis_url: "redis://localhost:6379".into(),
                    ifd_ttl_seconds: 86400,
                    tile_ttl_seconds: 3600,
                    byte_cache_capacity_bytes: 256 * 1024 * 1024,
                }))),
            )),
            ingestion: Arc::new(StubIngestionService::new(Arc::new(
                mantle_catalog::StubCatalogClient::new(Arc::new(CatalogConfig {
                    postgres_url: "postgres://localhost/mantle".into(),
                    ducklake_data_path: "s3://mantle-data/catalog/".into(),
                    geometry_column: "footprint".into(),
                    purge_retention_days: 7,
                    purge_poll_interval_seconds: 3600,
                })),
            ))),
            jobs,
            admin_token: None,
        }
    }

    #[tokio::test]
    async fn enqueue_job_returns_spec_id() {
        let state = test_state(Arc::new(StubJobQueueClient));
        let job_id = Uuid::new_v4();
        let job = JobSpec {
            job_id,
            process_id: "ndvi".into(),
            service_refs: vec![],
            params: serde_json::json!({}),
            submitted_at: Utc::now(),
        };
        let returned = enqueue_job(&state, job).await.unwrap();
        assert_eq!(returned, job_id);
    }
}
