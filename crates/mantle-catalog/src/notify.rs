//! Postgres `LISTEN` helper for footprint insert events (cache warmer).

use crate::error::CatalogError;
use serde::Deserialize;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use uuid::Uuid;

pub const FOOTPRINT_INSERT_CHANNEL: &str = "mantle_footprint_insert";

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct FootprintInsertEvent {
    pub footprint_id: i64,
    pub scene_id: Uuid,
    pub service_id: Uuid,
    pub partition_key: String,
}

/// Subscribe to `mantle_footprint_insert` notifications emitted by the migration trigger.
pub async fn subscribe_footprint_inserts(
    pool: &PgPool,
) -> Result<PgListener, CatalogError> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen(FOOTPRINT_INSERT_CHANNEL).await?;
    Ok(listener)
}

pub fn parse_footprint_insert_event(payload: &str) -> Result<FootprintInsertEvent, CatalogError> {
    serde_json::from_str(payload).map_err(CatalogError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_notify_payload() {
        let payload = r#"{"footprint_id":42,"scene_id":"660e8400-e29b-41d4-a716-446655440000","service_id":"550e8400-e29b-41d4-a716-446655440000","partition_key":"2024-07"}"#;
        let event = parse_footprint_insert_event(payload).expect("parse");
        assert_eq!(event.footprint_id, 42);
        assert_eq!(event.partition_key, "2024-07");
    }
}
