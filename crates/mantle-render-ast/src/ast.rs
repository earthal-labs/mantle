//! Render rule AST nodes.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Expr {
    BandRef { service_id: Uuid, band: u32 },
    Literal { value: f64 },
    BinaryOp {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Colormap {
        expr: Box<Expr>,
        lut_id: String,
    },
    Mosaic {
        service_filter: Value,
        reducer: MosaicReducer,
    },
    DelegateToRay {
        process_id: String,
        params: Value,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnaryOp {
    Sqrt,
    Log,
    Abs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MosaicReducer {
    Mean,
    Max,
    Min,
    Sum,
}

/// Unique band reference extracted from an AST walk.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BandRefKey {
    pub service_id: Uuid,
    pub band: u32,
}

impl From<&Expr> for BandRefKey {
    fn from(value: &Expr) -> Self {
        match value {
            Expr::BandRef { service_id, band } => BandRefKey {
                service_id: *service_id,
                band: *band,
            },
            _ => unreachable!("BandRefKey from non-BandRef"),
        }
    }
}
