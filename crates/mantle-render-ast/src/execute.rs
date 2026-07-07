//! SimdLocal expression evaluation over per-band tile buffers.

use crate::ast::{BinaryOp, Expr, UnaryOp};
use crate::plan::ExecutionTarget;
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

/// Per-band tile pixel buffers keyed by `(service_id, band)`.
#[derive(Debug, Clone, Default)]
pub struct BandContext {
    pub pixel_len: usize,
    pub bands: HashMap<(Uuid, u32), Vec<f32>>,
}

impl BandContext {
    pub fn new(pixel_len: usize, bands: HashMap<(Uuid, u32), Vec<f32>>) -> Self {
        Self { pixel_len, bands }
    }

    pub fn band(&self, service_id: Uuid, band: u32) -> Result<&[f32], ExecuteError> {
        self.bands
            .get(&(service_id, band))
            .map(|v| v.as_slice())
            .ok_or_else(|| ExecuteError::MissingBand { service_id, band })
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum ExecuteError {
    #[error("expression requires {0:?} execution, not SimdLocal")]
    WrongTarget(ExecutionTarget),
    #[error("missing band data for service {service_id} band {band}")]
    MissingBand { service_id: Uuid, band: u32 },
    #[error("band buffer length mismatch: expected {expected}, got {got}")]
    LengthMismatch { expected: usize, got: usize },
    #[error("division by zero at pixel index {0}")]
    DivByZero(usize),
}

/// Evaluate a SimdLocal expression to a single-band `f32` tile buffer.
///
/// `Mosaic` and `DelegateToRay` nodes return [`ExecuteError::WrongTarget`].
pub fn execute_expr(expr: &Expr, ctx: &BandContext) -> Result<Vec<f32>, ExecuteError> {
    match expr {
        Expr::DelegateToRay { .. } => Err(ExecuteError::WrongTarget(ExecutionTarget::RayAsync)),
        Expr::Mosaic { .. } => Err(ExecuteError::WrongTarget(ExecutionTarget::MosaicParallel)),
        Expr::Colormap { expr, .. } => execute_expr(expr, ctx),
        other => eval_scalar(other, ctx),
    }
}

fn eval_scalar(expr: &Expr, ctx: &BandContext) -> Result<Vec<f32>, ExecuteError> {
    match expr {
        Expr::BandRef { service_id, band } => {
            let data = ctx.band(*service_id, *band)?;
            check_len(ctx.pixel_len, data.len())?;
            Ok(data.to_vec())
        }
        Expr::Literal { value } => Ok(vec![*value as f32; ctx.pixel_len]),
        Expr::BinaryOp { op, left, right } => {
            let l = eval_scalar(left, ctx)?;
            let r = eval_scalar(right, ctx)?;
            apply_binary_op(*op, &l, &r)
        }
        Expr::UnaryOp { op, expr } => {
            let v = eval_scalar(expr, ctx)?;
            apply_unary_op(*op, &v)
        }
        Expr::Colormap { .. } | Expr::Mosaic { .. } | Expr::DelegateToRay { .. } => {
            unreachable!("handled by execute_expr")
        }
    }
}

fn check_len(expected: usize, got: usize) -> Result<(), ExecuteError> {
    if expected != 0 && got != expected {
        return Err(ExecuteError::LengthMismatch { expected, got });
    }
    Ok(())
}

/// Pixel-wise binary operation (mantle-raster-compatible semantics).
pub fn apply_binary_op(op: BinaryOp, left: &[f32], right: &[f32]) -> Result<Vec<f32>, ExecuteError> {
    if left.len() != right.len() {
        return Err(ExecuteError::LengthMismatch {
            expected: left.len(),
            got: right.len(),
        });
    }
    let mut out = Vec::with_capacity(left.len());
    for (i, (&l, &r)) in left.iter().zip(right.iter()).enumerate() {
        let v = match op {
            BinaryOp::Add => l + r,
            BinaryOp::Sub => l - r,
            BinaryOp::Mul => l * r,
            BinaryOp::Div => {
                if r.abs() < f32::EPSILON {
                    return Err(ExecuteError::DivByZero(i));
                }
                l / r
            }
        };
        out.push(if l.is_finite() && r.is_finite() {
            v
        } else {
            f32::NAN
        });
    }
    Ok(out)
}

/// Pixel-wise unary operation.
pub fn apply_unary_op(op: UnaryOp, values: &[f32]) -> Result<Vec<f32>, ExecuteError> {
    let mut out = Vec::with_capacity(values.len());
    for &v in values {
        let r = if !v.is_finite() {
            f32::NAN
        } else {
            match op {
                UnaryOp::Sqrt => v.sqrt(),
                UnaryOp::Log => v.ln(),
                UnaryOp::Abs => v.abs(),
            }
        };
        out.push(r);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::parse_render_rule;
    use uuid::Uuid;

    fn ctx_with_bands(id: Uuid, b1: f32, b2: f32, len: usize) -> BandContext {
        BandContext::new(
            len,
            HashMap::from([
                ((id, 1), vec![b1; len]),
                ((id, 2), vec![b2; len]),
            ]),
        )
    }

    #[test]
    fn ndvi_expression() {
        let id = Uuid::new_v4();
        let json = format!(
            r#"{{"type":"binary_op","op":"div","left":{{"type":"binary_op","op":"sub","left":{{"type":"band_ref","service_id":"{id}","band":2}},"right":{{"type":"band_ref","service_id":"{id}","band":1}}}},"right":{{"type":"binary_op","op":"add","left":{{"type":"band_ref","service_id":"{id}","band":2}},"right":{{"type":"band_ref","service_id":"{id}","band":1}}}}}}"#,
        );
        let expr = parse_render_rule(&json).unwrap();
        let ctx = ctx_with_bands(id, 0.2, 0.6, 4);
        let out = execute_expr(&expr, &ctx).unwrap();
        // NDVI = (0.6 - 0.2) / (0.6 + 0.2) = 0.5
        assert!((out[0] - 0.5).abs() < 1e-5);
    }

    #[test]
    fn rejects_mosaic_in_simd_executor() {
        let json = r#"{"type":"mosaic","service_filter":{},"reducer":"mean"}"#;
        let expr = parse_render_rule(json).unwrap();
        let ctx = BandContext::default();
        let err = execute_expr(&expr, &ctx).unwrap_err();
        assert_eq!(
            err,
            ExecuteError::WrongTarget(ExecutionTarget::MosaicParallel)
        );
    }

    #[test]
    fn rejects_div_by_zero() {
        let left = vec![1.0, 2.0];
        let right = vec![0.0, 1.0];
        let err = apply_binary_op(BinaryOp::Div, &left, &right).unwrap_err();
        assert!(matches!(err, ExecuteError::DivByZero(0)));
    }
}
