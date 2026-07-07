use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("catalog not implemented: {0}")]
    NotImplemented(String),
    #[error("service not found: {0}")]
    NotFound(Uuid),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("postgres error: {0}")]
    Postgres(#[from] sqlx::Error),
    #[error("duckdb error: {0}")]
    Duckdb(#[from] duckdb::Error),
    #[error("invalid geometry: {0}")]
    InvalidGeometry(String),
    #[error("append-only violation: {0}")]
    AppendOnlyViolation(String),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("virtual service not found: {0}")]
    ServiceNotFound(String),
    #[error("duplicate virtual service slug: {0}")]
    DuplicateSlug(String),
}
