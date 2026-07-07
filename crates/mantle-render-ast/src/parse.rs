//! JSON render rule parsing and validation.



use crate::ast::Expr;

use std::collections::{HashMap, HashSet};

use thiserror::Error;

use uuid::Uuid;



/// Maximum AST nesting depth (guards against pathological or cyclic-by-reference payloads).

pub const MAX_AST_DEPTH: usize = 64;



#[derive(Debug, Error)]

pub enum ParseError {

    #[error("invalid render rule JSON: {0}")]

    InvalidJson(#[from] serde_json::Error),

    #[error("validation error: {0}")]

    Validation(String),

}



/// Optional context for band upper-bound checks during validation.

#[derive(Debug, Clone, Default)]

pub struct ValidateContext {

    pub max_band_by_service: HashMap<Uuid, u32>,

}



/// Parse a JSON render rule string into an AST [`Expr`].

pub fn parse_render_rule(json: &str) -> Result<Expr, ParseError> {

    let expr: Expr = serde_json::from_str(json)?;

    validate_expr(&expr, &ValidateContext::default(), 0)?;

    Ok(expr)

}



/// Parse and validate with optional per-service band upper bounds.

pub fn parse_render_rule_with_context(

    json: &str,

    ctx: &ValidateContext,

) -> Result<Expr, ParseError> {

    let expr: Expr = serde_json::from_str(json)?;

    validate_expr(&expr, ctx, 0)?;

    Ok(expr)

}



fn validate_expr(expr: &Expr, ctx: &ValidateContext, depth: usize) -> Result<(), ParseError> {

    if depth > MAX_AST_DEPTH {

        return Err(ParseError::Validation(format!(

            "AST depth exceeds maximum of {MAX_AST_DEPTH}"

        )));

    }



    match expr {

        Expr::BandRef { service_id, band } => {

            if *band == 0 {

                return Err(ParseError::Validation(

                    "band index must be >= 1".into(),

                ));

            }

            if let Some(max) = ctx.max_band_by_service.get(service_id) {

                if *band > *max {

                    return Err(ParseError::Validation(format!(

                        "band {band} exceeds service {service_id} maximum of {max}"

                    )));

                }

            }

            Ok(())

        }

        Expr::Literal { value } => {

            if !value.is_finite() {

                return Err(ParseError::Validation(

                    "literal values must be finite".into(),

                ));

            }

            Ok(())

        }

        Expr::BinaryOp { left, right, .. } => {

            validate_expr(left, ctx, depth + 1)?;

            validate_expr(right, ctx, depth + 1)

        }

        Expr::UnaryOp { expr, .. } => validate_expr(expr, ctx, depth + 1),

        Expr::Colormap { expr, lut_id } => {

            if lut_id.is_empty() {

                return Err(ParseError::Validation("lut_id must not be empty".into()));

            }

            validate_expr(expr, ctx, depth + 1)

        }

        Expr::Mosaic {

            service_filter,

            reducer: _,

        } => {

            if service_filter.is_null() {

                return Err(ParseError::Validation(

                    "mosaic service_filter must be a JSON object".into(),

                ));

            }

            Ok(())

        }

        Expr::DelegateToRay {

            process_id,

            params: _,

        } => {

            if process_id.is_empty() {

                return Err(ParseError::Validation(

                    "process_id must not be empty".into(),

                ));

            }

            Ok(())

        }

    }

}



/// Collect all unique band references in the AST (pre-order).

pub fn collect_band_refs(expr: &Expr) -> Vec<crate::ast::BandRefKey> {

    let mut seen = HashSet::new();

    let mut out = Vec::new();

    walk_band_refs(expr, &mut seen, &mut out);

    out

}



fn walk_band_refs(

    expr: &Expr,

    seen: &mut HashSet<(Uuid, u32)>,

    out: &mut Vec<crate::ast::BandRefKey>,

) {

    match expr {

        Expr::BandRef { service_id, band } => {

            if seen.insert((*service_id, *band)) {

                out.push(crate::ast::BandRefKey {

                    service_id: *service_id,

                    band: *band,

                });

            }

        }

        Expr::BinaryOp { left, right, .. } => {

            walk_band_refs(left, seen, out);

            walk_band_refs(right, seen, out);

        }

        Expr::UnaryOp { expr, .. } => walk_band_refs(expr, seen, out),

        Expr::Colormap { expr, .. } => walk_band_refs(expr, seen, out),

        Expr::Mosaic { .. } | Expr::Literal { .. } | Expr::DelegateToRay { .. } => {}

    }

}



/// Returns the outermost colormap LUT id, if the expression is (nested) colormap-wrapped.

pub fn colormap_lut_id(expr: &Expr) -> Option<&str> {

    match expr {

        Expr::Colormap { lut_id, .. } => Some(lut_id.as_str()),

        _ => None,

    }

}



/// Peel one colormap wrapper, returning the inner expression.

pub fn peel_colormap(expr: &Expr) -> &Expr {

    match expr {

        Expr::Colormap { expr, .. } => expr,

        other => other,

    }

}



/// Whether the expression tree contains a `Mosaic` node.

pub fn contains_mosaic(expr: &Expr) -> bool {

    match expr {

        Expr::Mosaic { .. } => true,

        Expr::BinaryOp { left, right, .. } => contains_mosaic(left) || contains_mosaic(right),

        Expr::UnaryOp { expr, .. } => contains_mosaic(expr),

        Expr::Colormap { expr, .. } => contains_mosaic(expr),

        _ => false,

    }

}



/// Whether the expression tree contains a `DelegateToRay` node.

pub fn contains_delegate_to_ray(expr: &Expr) -> bool {

    match expr {

        Expr::DelegateToRay { .. } => true,

        Expr::BinaryOp { left, right, .. } => {

            contains_delegate_to_ray(left) || contains_delegate_to_ray(right)

        }

        Expr::UnaryOp { expr, .. } => contains_delegate_to_ray(expr),

        Expr::Colormap { expr, .. } => contains_delegate_to_ray(expr),

        _ => false,

    }

}



#[cfg(test)]

mod tests {

    use super::*;

    use uuid::Uuid;



    #[test]

    fn rejects_zero_band_index() {

        let id = Uuid::new_v4();

        let json = format!(

            r#"{{"type":"band_ref","service_id":"{id}","band":0}}"#,

        );

        let err = parse_render_rule(&json).unwrap_err();

        assert!(err.to_string().contains("band index"));

    }



    #[test]

    fn rejects_empty_process_id() {

        let json = r#"{"type":"delegate_to_ray","process_id":"","params":{}}"#;

        let err = parse_render_rule(json).unwrap_err();

        assert!(err.to_string().contains("process_id"));

    }



    #[test]

    fn validates_band_upper_bound_with_context() {

        let id = Uuid::new_v4();

        let json = format!(

            r#"{{"type":"band_ref","service_id":"{id}","band":5}}"#,

        );

        let ctx = ValidateContext {

            max_band_by_service: HashMap::from([(id, 4)]),

        };

        let err = parse_render_rule_with_context(&json, &ctx).unwrap_err();

        assert!(err.to_string().contains("exceeds"));

    }



    #[test]

    fn collects_unique_band_refs() {

        let id = Uuid::new_v4();

        let json = format!(

            r#"{{"type":"binary_op","op":"add","left":{{"type":"band_ref","service_id":"{id}","band":1}},"right":{{"type":"band_ref","service_id":"{id}","band":1}}}}"#,

        );

        let expr = parse_render_rule(&json).unwrap();

        let refs = collect_band_refs(&expr);

        assert_eq!(refs.len(), 1);

    }

}


