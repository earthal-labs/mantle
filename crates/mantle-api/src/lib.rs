//! Mantle HTTP API (Axum) — health, tiles, admin ingestion.

mod admin;
mod auth;
mod error;
mod jobs;
mod vrpm_client;
mod plugins;
mod services;

use admin::{
    attach_function, debug_service, delete_service, purge_service, register_cloud_reference,
    upload_service,
};
use auth::{load_admin_token, require_admin_auth};
use jobs::get_job_status;
use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{header, StatusCode},
    middleware,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use mantle_arrow::{ServiceRef, TileRequest};
use mantle_cache::{CacheClient, JobQueueClient, RedisCacheClient, RedisJobQueueClient};
use mantle_catalog::{CatalogClient, PostgresDuckLakeCatalog, StubCatalogClient};
use mantle_config::MantleConfig;
use mantle_ingestion::{IngestionService, MantleIngestionService, StubIngestionService};
use mantle_ogc::{router as ogc_router, VrpmSidecarUrl};
use mantle_raster::{OxigdalRasterEngine, RasterEngine, TileFormat};
use mantle_stac::{landing as stac_landing, router as stac_router};
use serde::Deserialize;
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

impl axum::extract::FromRef<AppState> for Arc<dyn CatalogClient> {
    fn from_ref(state: &AppState) -> Self {
        state.catalog.clone()
    }
}

impl axum::extract::FromRef<AppState> for Arc<dyn RasterEngine> {
    fn from_ref(state: &AppState) -> Self {
        state.raster.clone()
    }
}

impl axum::extract::FromRef<AppState> for Arc<dyn JobQueueClient> {
    fn from_ref(state: &AppState) -> Self {
        state.jobs.clone()
    }
}

impl axum::extract::FromRef<AppState> for VrpmSidecarUrl {
    fn from_ref(state: &AppState) -> Self {
        VrpmSidecarUrl(state.config.analytics.vrpm_sidecar_url.clone())
    }
}

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<MantleConfig>,
    pub catalog: Arc<dyn CatalogClient>,
    pub cache: Arc<dyn CacheClient>,
    pub raster: Arc<dyn RasterEngine>,
    pub ingestion: Arc<dyn IngestionService>,
    pub jobs: Arc<dyn JobQueueClient>,
    pub admin_token: Option<String>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
}

#[derive(Debug, Deserialize)]
pub struct TileQuery {
    pub format: Option<String>,
    pub band: Option<u32>,
    pub service_id: Option<Uuid>,
    pub render: Option<String>,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// `GET /console` — barebones dev console (STAC search, service admin
/// actions, a native Leaflet tile viewer, plugin listing, job submit/poll).
/// Embedded at compile time so it ships with the binary; no separate static
/// file to remember to copy into the Docker image.
async fn console() -> Html<&'static str> {
    Html(include_str!("../static/console.html"))
}

async fn get_tile(
    State(state): State<AppState>,
    Path((z, x, y)): Path<(u32, u32, u32)>,
    Query(params): Query<TileQuery>,
) -> Result<Response, StatusCode> {
    let format = TileFormat::from_query(params.format.as_deref());
    let service_id = params.service_id.unwrap_or_else(Uuid::nil);

    let request = TileRequest {
        service_id,
        z,
        x,
        y,
        band: params.band,
        render_rule: params.render,
    };

    let services: Vec<ServiceRef> = if params.service_id.is_some() {
        match state.catalog.get_service(service_id).await {
            Ok(record) => vec![record.to_service_ref()],
            Err(_) => Vec::new(),
        }
    } else {
        Vec::new()
    };

    let bytes = state
        .raster
        .render_tile(&services, &request, format)
        .await
        .map_err(|e| {
            error!(error = %e, z, x, y, %service_id, "tile render failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, format.content_type())],
        bytes,
    )
        .into_response())
}

async fn build_ingestion(
    config: &MantleConfig,
    catalog: Arc<dyn CatalogClient>,
) -> Arc<dyn IngestionService> {
    match MantleIngestionService::new(
        Arc::new(config.storage.clone()),
        Arc::new(config.analytics.clone()),
        catalog.clone(),
    ) {
        Ok(service) => Arc::new(service),
        Err(err) => {
            tracing::warn!(error = %err, "S3 ingestion unavailable; using stub ingestion");
            Arc::new(StubIngestionService::new(catalog))
        }
    }
}

pub async fn build_router(config: Arc<MantleConfig>) -> anyhow::Result<Router> {
    let cache: Arc<dyn CacheClient> =
        Arc::new(RedisCacheClient::connect(&config.cache).await?);
    let jobs: Arc<dyn JobQueueClient> =
        Arc::new(RedisJobQueueClient::connect(&config.cache, &config.analytics).await?);
    let catalog: Arc<dyn CatalogClient> =
        match PostgresDuckLakeCatalog::connect(Arc::new(config.catalog.clone())).await {
            Ok(client) => Arc::new(client),
            Err(err) => {
                tracing::warn!(error = %err, "Postgres/DuckLake unavailable; using stub catalog");
                Arc::new(StubCatalogClient::new(Arc::new(config.catalog.clone())))
            }
        };
    let raster: Arc<dyn RasterEngine> = Arc::new(OxigdalRasterEngine::new(
        Arc::new(config.storage.clone()),
        cache.clone(),
        catalog.clone(),
        &config.cache,
    )?);
    let ingestion = build_ingestion(&config, catalog.clone()).await;
    let admin_token = load_admin_token(&config.auth.admin_token_env);

    let state = AppState {
        config,
        catalog,
        cache,
        raster,
        ingestion,
        jobs,
        admin_token,
    };

    // COG uploads exceed Axum's default 2 MiB body limit.
    const ADMIN_BODY_LIMIT: usize = 512 * 1024 * 1024;

    let admin_routes = Router::new()
        .route("/services/upload", post(upload_service))
        .route("/services/reference", post(register_cloud_reference))
        .route("/services/{id}/delete", post(delete_service))
        .route("/services/{id}/purge", post(purge_service))
        .route("/services/{id}/debug", get(debug_service))
        .route("/services/{service_id}/attach", post(attach_function))
        .layer(DefaultBodyLimit::max(ADMIN_BODY_LIMIT))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_admin_auth,
        ))
        .with_state(state.clone());

    let service_routes = services::services_router();
    let plugin_routes = plugins::plugins_router();

    let stac_routes = stac_router::<AppState>();
    let ogc_routes = ogc_router::<AppState>();

    Ok(Router::new()
        .route("/health", get(health))
        .route("/console", get(console))
        .route("/status/{job_id}", get(get_job_status))
        .route("/tiles/{z}/{x}/{y}", get(get_tile))
        // Register landing on the parent — nest("/stac")+route("/") does not
        // reliably match both `/stac` and `/stac/` in Axum 0.8.
        .route("/stac", get(stac_landing))
        .route("/stac/", get(stac_landing))
        .nest("/services", service_routes)
        .nest("/plugins", plugin_routes)
        .nest("/stac", stac_routes)
        .nest("/ogc", ogc_routes)
        .nest("/admin", admin_routes)
        .with_state(state))
}

pub use jobs::enqueue_job;

pub async fn serve(config: MantleConfig) -> anyhow::Result<()> {
    let config = Arc::new(config);
    let addr: SocketAddr = config.server.bind.parse()?;
    let app = build_router(config.clone()).await?;

    info!("mantle-api listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod console_tests {
    use super::console;

    #[tokio::test]
    async fn console_serves_embedded_html() {
        let response = console().await;
        assert!(response.0.contains("Mantle Console"));
        assert!(response.0.contains("id=\"map\""));
    }
}

pub use auth::constant_time_eq;
pub use error::ApiError as MantleApiError;
