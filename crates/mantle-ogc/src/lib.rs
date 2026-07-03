//! OGC API route types and handlers (Tiles, Maps, EDR, Processes).

mod models;
mod plugins;
mod routes;

pub use models::{
    coverage_json_stub, map_metadata, process_list, CoverageJsonStub, DEFAULT_COLLECTION_ID,
    WEB_MERCATOR_TILE_MATRIX_SET,
};
pub use plugins::{
    normalize_process_id, resolve_parameters_with_defaults, validate_params_against_specs,
    ModelKind, ParamDirection, ParamType, ParameterSpec, PluginDescriptor, PluginListResponse,
    PluginValidationError, VrpmSidecarUrl,
};
pub use routes::{router, OgcState};

use mantle_arrow::{DatasetRef, JobSpec, TileRequest};
use mantle_render_ast::{parse_render_rule, RenderExecutionPlan};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Build a render execution plan from a JSON render rule (shared by Maps and tile routes).
pub fn build_render_execution_plan(
    render_rule: &str,
) -> Result<RenderExecutionPlan, mantle_render_ast::ParseError> {
    let expr = parse_render_rule(render_rule)?;
    Ok(RenderExecutionPlan::from_expr(expr))
}

/// OGC API – Tiles route parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TilesRoute {
    pub collection_id: String,
    pub tile_matrix_set: String,
    pub tile_matrix: String,
    pub tile_row: u32,
    pub tile_col: u32,
}

impl TilesRoute {
    pub const PREFIX: &'static str = "/ogc/tiles";

    pub fn to_tile_request(&self, dataset_id: Uuid) -> TileRequest {
        let z = self.tile_matrix.parse().unwrap_or(0);
        TileRequest {
            dataset_id,
            z,
            x: self.tile_col,
            y: self.tile_row,
            band: None,
            render_rule: None,
        }
    }
}

/// OGC API – Maps route parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapsRoute {
    pub collection_id: String,
    pub style_id: Option<String>,
    pub render_rule: Option<String>,
}

impl MapsRoute {
    pub const PREFIX: &'static str = "/ogc/maps";
}

/// OGC API – EDR (Environmental Data Retrieval) point query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdrPointQuery {
    pub collection_id: String,
    pub coords: (f64, f64),
    pub datetime: Option<String>,
    pub variables: Vec<String>,
}

impl EdrPointQuery {
    pub const PREFIX: &'static str = "/ogc/edr";
}

/// OGC API – Processes async execution request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessExecutionRequest {
    pub process_id: String,
    pub inputs: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessExecutionResponse {
    pub job_id: Uuid,
    pub status_url: String,
}

impl ProcessExecutionResponse {
    pub fn accepted(job_id: Uuid) -> Self {
        Self {
            job_id,
            status_url: format!("/status/{job_id}"),
        }
    }
}

/// Build a `JobSpec` for EDR point delegation to Ray.
pub fn job_spec_from_edr(query: &EdrPointQuery, datasets: Vec<DatasetRef>) -> JobSpec {
    JobSpec {
        job_id: Uuid::new_v4(),
        process_id: "edr-point".into(),
        dataset_refs: datasets,
        params: serde_json::json!({
            "coords": [query.coords.0, query.coords.1],
            "datetime": query.datetime,
            "variables": query.variables,
            "collection_id": query.collection_id,
        }),
        submitted_at: chrono::Utc::now(),
    }
}

/// Build a `JobSpec` for OGC Processes async execution.
pub fn job_spec_from_process(request: &ProcessExecutionRequest, datasets: Vec<DatasetRef>) -> JobSpec {
    JobSpec {
        job_id: Uuid::new_v4(),
        process_id: request.process_id.clone(),
        dataset_refs: datasets,
        params: request.inputs.clone(),
        submitted_at: chrono::Utc::now(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_prefixes_match_agents_contract() {
        assert_eq!(TilesRoute::PREFIX, "/ogc/tiles");
        assert_eq!(MapsRoute::PREFIX, "/ogc/maps");
        assert_eq!(EdrPointQuery::PREFIX, "/ogc/edr");
    }

    #[test]
    fn job_spec_from_process_carries_process_id() {
        let req = ProcessExecutionRequest {
            process_id: "ndvi".into(),
            inputs: serde_json::json!({"nir": 2}),
        };
        let job = job_spec_from_process(&req, vec![]);
        assert_eq!(job.process_id, "ndvi");
        assert_eq!(job.params["nir"], 2);
    }
}
