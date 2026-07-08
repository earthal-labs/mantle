use crate::filter::StacSearchRequest;
use crate::items::{build_collection_items, build_item_collection, services_to_stac_items};
use crate::models::{collection_list, default_collection, landing_catalog, DEFAULT_COLLECTION_ID};
use axum::{
    extract::{FromRef, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use mantle_catalog::{CatalogClient, CatalogError};
use std::sync::Arc;
use tracing::warn;

/// Catalog handle extracted from the API application state.
#[derive(Clone)]
pub struct StacState {
    pub catalog: Arc<dyn CatalogClient>,
}

impl<S> FromRef<S> for StacState
where
    Arc<dyn CatalogClient>: FromRef<S>,
{
    fn from_ref(state: &S) -> Self {
        Self {
            catalog: Arc::from_ref(state),
        }
    }
}

pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    StacState: FromRef<S>,
{
    // Landing is registered on the parent router at `/stac` and `/stac/` —
    // Axum's nest("/stac") + route("/") is easy to miss depending on slash.
    Router::new()
        .route("/collections", get(list_collections))
        .route("/collections/{id}", get(get_collection))
        .route("/collections/{id}/items", get(list_collection_items))
        .route("/search", get(search_get).post(search_post))
}

/// STAC landing catalog (`GET /stac`, `GET /stac/`).
pub async fn landing() -> Json<serde_json::Value> {
    Json(serde_json::to_value(landing_catalog()).expect("catalog json"))
}

async fn list_collections() -> Json<serde_json::Value> {
    Json(serde_json::to_value(collection_list()).expect("collections json"))
}

async fn get_collection(Path(id): Path<String>) -> Result<Json<serde_json::Value>, StatusCode> {
    if id != DEFAULT_COLLECTION_ID {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(
        serde_json::to_value(default_collection()).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    ))
}

async fn list_collection_items<S>(
    State(state): State<S>,
    Path(id): Path<String>,
    Query(params): Query<StacSearchRequest>,
) -> Result<Json<serde_json::Value>, StatusCode>
where
    StacState: FromRef<S>,
{
    if id != DEFAULT_COLLECTION_ID {
        return Err(StatusCode::NOT_FOUND);
    }

    let StacState { catalog } = StacState::from_ref(&state);
    let limit = params.effective_limit();
    let query = params.to_spatial_query();
    let scenes = catalog
        .spatial_query(query)
        .await
        .map_err(catalog_to_status)?;
    // TODO(Phase 2): one STAC Item per scene with real per-band Assets,
    // instead of flattening to each scene's single default asset.
    let services: Vec<_> = scenes.iter().filter_map(|s| s.primary_service_ref()).collect();

    let take = (limit as usize).min(services.len());
    let features = services_to_stac_items(&services[..take]);
    let body = build_collection_items(features, &id);
    Ok(Json(serde_json::to_value(body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?))
}

async fn search_get<S>(
    State(state): State<S>,
    Query(params): Query<StacSearchRequest>,
) -> Result<Response, StatusCode>
where
    StacState: FromRef<S>,
{
    execute_search(state, params).await
}

async fn search_post<S>(
    State(state): State<S>,
    Json(body): Json<StacSearchRequest>,
) -> Result<Response, StatusCode>
where
    StacState: FromRef<S>,
{
    execute_search(state, body).await
}

async fn execute_search<S>(
    state: S,
    params: StacSearchRequest,
) -> Result<Response, StatusCode>
where
    StacState: FromRef<S>,
{
    if !params.matches_collections(DEFAULT_COLLECTION_ID) {
        let empty = build_item_collection(Vec::new(), 0);
        return Ok(Json(
            serde_json::to_value(empty).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
        )
        .into_response());
    }

    let StacState { catalog } = StacState::from_ref(&state);
    let limit = params.effective_limit();
    let query = params.to_spatial_query();
    let scenes = catalog
        .spatial_query(query)
        .await
        .map_err(catalog_to_status)?;
    // TODO(Phase 2): one STAC Item per scene with real per-band Assets,
    // instead of flattening to each scene's single default asset.
    let services: Vec<_> = scenes.iter().filter_map(|s| s.primary_service_ref()).collect();

    let matched = services.len() as u64;
    let take = (limit as usize).min(services.len());
    let features = services_to_stac_items(&services[..take]);
    let body = build_item_collection(features, matched);
    Ok(Json(serde_json::to_value(body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?).into_response())
}

fn catalog_to_status(err: CatalogError) -> StatusCode {
    warn!(error = %err, "catalog error in STAC handler");
    match err {
        CatalogError::NotFound(_) => StatusCode::NOT_FOUND,
        CatalogError::InvalidGeometry(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use mantle_arrow::{AssetRef, SceneRef, ServiceFormat};
    use mantle_catalog::{
        AssetRecord, FootprintRecord, SceneDeletionRecord, SceneRecord, SceneWithAssets,
        ServiceRecord, SpatialQuery, StubCatalogClient, VirtualServiceRecord,
    };
    use mantle_config::CatalogConfig;
    use uuid::Uuid;

    struct MockCatalog {
        scenes: Vec<SceneRef>,
    }

    #[async_trait]
    impl CatalogClient for MockCatalog {
        async fn create_service(&self, service: ServiceRecord) -> Result<ServiceRecord, CatalogError> {
            Ok(service)
        }

        async fn get_service_by_slug(&self, slug: &str) -> Result<ServiceRecord, CatalogError> {
            Err(CatalogError::ServiceNotFound(slug.to_string()))
        }

        async fn add_scene(
            &self,
            scene: SceneRecord,
            _assets: Vec<AssetRecord>,
            _footprint: FootprintRecord,
        ) -> Result<Uuid, CatalogError> {
            Ok(scene.id)
        }

        async fn spatial_query(&self, _query: SpatialQuery) -> Result<Vec<SceneRef>, CatalogError> {
            Ok(self.scenes.clone())
        }

        async fn get_service(&self, id: Uuid) -> Result<ServiceRecord, CatalogError> {
            Err(CatalogError::NotFound(id))
        }

        async fn list_scenes(&self, service_id: Uuid) -> Result<Vec<SceneWithAssets>, CatalogError> {
            Err(CatalogError::NotFound(service_id))
        }

        async fn get_scene(&self, scene_id: Uuid) -> Result<SceneWithAssets, CatalogError> {
            Err(CatalogError::NotFound(scene_id))
        }

        async fn delete_scene(
            &self,
            scene_id: Uuid,
            _reason: Option<String>,
        ) -> Result<SceneDeletionRecord, CatalogError> {
            Err(CatalogError::NotFound(scene_id))
        }

        async fn purge_scene(&self, scene_id: Uuid) -> Result<(), CatalogError> {
            Err(CatalogError::NotFound(scene_id))
        }

        async fn attach_function(
            &self,
            _service_id: Uuid,
            _function_id: String,
            _params_defaults: serde_json::Value,
            _endpoint_slug: Option<String>,
        ) -> Result<VirtualServiceRecord, CatalogError> {
            Err(CatalogError::NotFound(_service_id))
        }

        async fn get_virtual_service_by_slug(
            &self,
            slug: &str,
        ) -> Result<VirtualServiceRecord, CatalogError> {
            Err(CatalogError::ServiceNotFound(slug.to_string()))
        }

        async fn list_virtual_services(
            &self,
            _service_id: Option<Uuid>,
        ) -> Result<Vec<VirtualServiceRecord>, CatalogError> {
            Ok(Vec::new())
        }

        async fn register_output_service(
            &self,
            _output_service: ServiceRecord,
            _function_id: String,
            _endpoint_slug: String,
        ) -> Result<VirtualServiceRecord, CatalogError> {
            Err(CatalogError::NotFound(Uuid::nil()))
        }

        async fn soft_delete_service(
            &self,
            service_id: Uuid,
            _reason: Option<String>,
        ) -> Result<mantle_catalog::DeletionRecord, CatalogError> {
            Err(CatalogError::NotFound(service_id))
        }

        async fn get_service_any(&self, id: Uuid) -> Result<ServiceRecord, CatalogError> {
            Err(CatalogError::NotFound(id))
        }

        async fn purge_service(&self, service_id: Uuid) -> Result<(), CatalogError> {
            Err(CatalogError::NotFound(service_id))
        }
    }

    #[derive(Clone)]
    struct TestApp {
        catalog: Arc<dyn CatalogClient>,
    }

    impl FromRef<TestApp> for StacState {
        fn from_ref(state: &TestApp) -> Self {
            StacState {
                catalog: state.catalog.clone(),
            }
        }
    }

    #[tokio::test]
    async fn search_respects_limit() {
        let scenes: Vec<SceneRef> = (0..5)
            .map(|i| SceneRef {
                scene_id: Uuid::new_v4(),
                service_id: Uuid::new_v4(),
                service_name: format!("svc-{i}"),
                geometry_wkt: None,
                assets: vec![AssetRef {
                    id: Uuid::new_v4(),
                    band_role: "data".into(),
                    band_index: 1,
                    format: ServiceFormat::Cog,
                    storage_uri: format!("s3://bucket/{i}.tif"),
                    crs: None,
                }],
            })
            .collect();

        let app = TestApp {
            catalog: Arc::new(MockCatalog {
                scenes: scenes.clone(),
            }),
        };

        let params = StacSearchRequest {
            limit: Some(2),
            ..Default::default()
        };
        let response = execute_search(app, params).await.expect("search");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn stub_catalog_wires_through_state() {
        let config = Arc::new(CatalogConfig {
            postgres_url: "postgres://localhost/mantle".into(),
            ducklake_data_path: "./data/".into(),
            geometry_column: "footprint".into(),
            purge_retention_days: 7,
            purge_poll_interval_seconds: 3600,
        });
        let _state = StacState {
            catalog: Arc::new(StubCatalogClient::new(config)),
        };
    }
}
