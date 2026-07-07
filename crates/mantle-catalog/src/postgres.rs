use crate::error::CatalogError;
use crate::{FootprintRecord, ServiceRecord};
use chrono::{DateTime, Utc};
use mantle_arrow::ServiceFormat;
use sqlx::PgPool;
use uuid::Uuid;

pub(crate) fn format_to_db(format: ServiceFormat) -> &'static str {
    match format {
        ServiceFormat::Cog => "cog",
        ServiceFormat::Icechunk => "icechunk",
    }
}

pub(crate) fn format_from_db(value: &str) -> ServiceFormat {
    match value {
        "cog" => ServiceFormat::Cog,
        _ => ServiceFormat::Icechunk,
    }
}

pub(crate) async fn insert_service<'e, E>(
    executor: E,
    service: &ServiceRecord,
) -> Result<(), CatalogError>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query(
        r#"
        INSERT INTO services (id, name, description, format, storage_uri, crs, temporal_start, temporal_end, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(service.id)
    .bind(&service.name)
    .bind(&service.description)
    .bind(format_to_db(service.format))
    .bind(&service.storage_uri)
    .bind(&service.crs)
    .bind(service.temporal_start)
    .bind(service.temporal_end)
    .bind(service.created_at)
    .execute(executor)
    .await?;
    Ok(())
}

pub(crate) async fn insert_footprint_row<'e, E>(
    executor: E,
    footprint: &FootprintRecord,
) -> Result<i64, CatalogError>
where
    E: sqlx::PgExecutor<'e>,
{
    let row: (i64,) = sqlx::query_as(
        r#"
        INSERT INTO footprints (service_id, geometry, cloud_cover, partition_key)
        VALUES ($1, ST_GeomFromText($2), $3, $4)
        RETURNING id
        "#,
    )
    .bind(footprint.service_id)
    .bind(&footprint.geometry_wkt)
    .bind(footprint.cloud_cover)
    .bind(&footprint.partition_key)
    .fetch_one(executor)
    .await?;

    Ok(row.0)
}

pub(crate) async fn fetch_service(
    pool: &PgPool,
    id: Uuid,
) -> Result<ServiceRecord, CatalogError> {
    let row = sqlx::query_as::<_, ServiceRow>(
        r#"
        SELECT id, name, description, format, storage_uri, crs, temporal_start, temporal_end, created_at
        FROM services
        WHERE id = $1
          AND NOT EXISTS (
              SELECT 1 FROM service_deletions sd WHERE sd.service_id = services.id
          )
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    row.map(Into::into)
        .ok_or(CatalogError::NotFound(id))
}

/// Like [`fetch_service`] but ignores the soft-delete tombstone — used only by
/// the purge routine, which legitimately needs to see a soft-deleted service's
/// `storage_uri` in order to reclaim it.
pub(crate) async fn fetch_service_any(
    pool: &PgPool,
    id: Uuid,
) -> Result<ServiceRecord, CatalogError> {
    let row = sqlx::query_as::<_, ServiceRow>(
        r#"
        SELECT id, name, description, format, storage_uri, crs, temporal_start, temporal_end, created_at
        FROM services
        WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    row.map(Into::into)
        .ok_or(CatalogError::NotFound(id))
}

#[derive(sqlx::FromRow)]
struct ServiceRow {
    id: Uuid,
    name: String,
    description: Option<String>,
    format: String,
    storage_uri: String,
    crs: Option<String>,
    temporal_start: Option<DateTime<Utc>>,
    temporal_end: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

impl From<ServiceRow> for ServiceRecord {
    fn from(row: ServiceRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            description: row.description,
            format: format_from_db(&row.format),
            storage_uri: row.storage_uri,
            crs: row.crs,
            temporal_start: row.temporal_start,
            temporal_end: row.temporal_end,
            created_at: row.created_at,
        }
    }
}
