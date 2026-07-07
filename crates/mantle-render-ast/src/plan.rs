//! Execution target classification and Ray job planning.

use crate::ast::{BandRefKey, Expr};
use crate::parse::{collect_band_refs, contains_delegate_to_ray, contains_mosaic};
use chrono::Utc;
use mantle_arrow::{encode_job_spec, ArrowError, JobSpec, ServiceRef};
use serde::Serialize;
use thiserror::Error;
use uuid::Uuid;

/// Where an AST node (or full tree) is executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionTarget {
    /// Per-pixel SIMD ops via oxigdal / local tile path.
    SimdLocal,
    /// Multi-service mosaic via parallel byte-range reads.
    MosaicParallel,
    /// Heavy ops delegated to Ray analytics workers.
    RayAsync,
}

/// Classify a single AST node (each node maps to exactly one path).
pub fn node_execution_target(expr: &Expr) -> ExecutionTarget {
    match expr {
        Expr::DelegateToRay { .. } => ExecutionTarget::RayAsync,
        Expr::Mosaic { .. } => ExecutionTarget::MosaicParallel,
        Expr::BandRef { .. }
        | Expr::Literal { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::Colormap { .. } => ExecutionTarget::SimdLocal,
    }
}

/// Classify the full expression tree for pipeline dispatch.
///
/// Precedence: any `DelegateToRay` â†’ `RayAsync`; else any `Mosaic` â†’ `MosaicParallel`; else `SimdLocal`.
pub fn execution_target(expr: &Expr) -> ExecutionTarget {
    if contains_delegate_to_ray(expr) {
        ExecutionTarget::RayAsync
    } else if contains_mosaic(expr) {
        ExecutionTarget::MosaicParallel
    } else {
        ExecutionTarget::SimdLocal
    }
}

/// Serializable execution plan for OGC Maps and tile pipelines.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RenderExecutionPlan {
    pub target: ExecutionTarget,
    pub band_refs: Vec<BandRefKey>,
    pub expr: Expr,
}

impl RenderExecutionPlan {
    pub fn from_expr(expr: Expr) -> Self {
        let band_refs = collect_band_refs(&expr);
        let target = execution_target(&expr);
        Self {
            target,
            band_refs,
            expr,
        }
    }
}

#[derive(Debug, Error)]
pub enum PlanError {
    #[error("expression is not a DelegateToRay node")]
    NotDelegateToRay,
    #[error("arrow IPC error: {0}")]
    Arrow(#[from] ArrowError),
}

/// Build a [`JobSpec`] and Arrow IPC bytes for Ray delegation.
pub fn plan_ray_job(
    expr: &Expr,
    services: Vec<ServiceRef>,
) -> Result<(JobSpec, Vec<u8>), PlanError> {
    let Expr::DelegateToRay {
        process_id,
        params,
    } = expr
    else {
        return Err(PlanError::NotDelegateToRay);
    };

    let job = JobSpec {
        job_id: Uuid::new_v4(),
        process_id: process_id.clone(),
        service_refs: services,
        params: params.clone(),
        submitted_at: Utc::now(),
    };
    let ipc = encode_job_spec(&job)?;
    Ok((job, ipc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_render_rule;
    use uuid::Uuid;

    #[test]
    fn simd_local_for_band_math() {
        let id = Uuid::new_v4();
        let json = format!(
            r#"{{"type":"binary_op","op":"add","left":{{"type":"band_ref","service_id":"{id}","band":1}},"right":{{"type":"literal","value":1.0}}}}"#,
        );
        let expr = parse_render_rule(&json).unwrap();
        assert_eq!(execution_target(&expr), ExecutionTarget::SimdLocal);
        assert_eq!(node_execution_target(&expr), ExecutionTarget::SimdLocal);
    }

    #[test]
    fn mosaic_parallel_for_mosaic_node() {
        let json = r#"{"type":"mosaic","service_filter":{"format":"cog"},"reducer":"mean"}"#;
        let expr = parse_render_rule(json).unwrap();
        assert_eq!(execution_target(&expr), ExecutionTarget::MosaicParallel);
        assert_eq!(
            node_execution_target(&expr),
            ExecutionTarget::MosaicParallel
        );
    }

    #[test]
    fn ray_async_for_delegate() {
        let json =
            r#"{"type":"delegate_to_ray","process_id":"zonal-stats","params":{"zones":"admin"}}"#;
        let expr = parse_render_rule(json).unwrap();
        assert_eq!(execution_target(&expr), ExecutionTarget::RayAsync);
    }

    #[test]
    fn nested_mosaic_promotes_tree_to_mosaic_parallel() {
        let json = r#"{"type":"colormap","expr":{"type":"mosaic","service_filter":{},"reducer":"max"},"lut_id":"viridis"}"#;
        let expr = parse_render_rule(json).unwrap();
        assert_eq!(execution_target(&expr), ExecutionTarget::MosaicParallel);
        assert_eq!(node_execution_target(&expr), ExecutionTarget::SimdLocal);
    }

    #[test]
    fn plan_ray_job_produces_ipc() {
        let json = r#"{"type":"delegate_to_ray","process_id":"ndvi","params":{}}"#;
        let expr = parse_render_rule(json).unwrap();
        let (job, ipc) = plan_ray_job(&expr, vec![]).unwrap();
        assert_eq!(job.process_id, "ndvi");
        assert!(!ipc.is_empty());
    }
}
