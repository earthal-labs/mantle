use crate::error::CatalogError;
use crate::{DatasetRecord, FootprintRecord};
use chrono::{DateTime, Utc};
use mantle_arrow::DatasetFormat;
use sqlx::PgPool;
use uuid::Uuid;

pub(crate) fn format_to_db(format: DatasetFormat) -> &'static str {
    match format {
        DatasetFormat::Cog => "cog",
        DatasetFormat::Icechunk => "icechunk",
    }
}

pub(crate) fn format_from_db(value: &str) -> DatasetFormat {
    match value {
        "cog" => DatasetFormat::Cog,
        _ => DatasetFormat::Icechunk,
    }
}

pub(crate) async fn insert_dataset<'e, E>(
    executor: E,
    dataset: &DatasetRecord,
) -> Result<(), CatalogError>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query(
        r#"
        INSERT INTO datasets (id, name, format, storage_uri, crs, temporal_start, temporal_end, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(dataset.id)
    .bind(&dataset.name)
    .bind(format_to_db(dataset.format))
    .bind(&dataset.storage_uri)
    .bind(&dataset.crs)
    .bind(dataset.temporal_start)
    .bind(dataset.temporal_end)
    .bind(dataset.created_at)
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
        INSERT INTO footprints (dataset_id, geometry, cloud_cover, partition_key)
        VALUES ($1, ST_GeomFromText($2), $3, $4)
        RETURNING id
        "#,
    )
    .bind(footprint.dataset_id)
    .bind(&footprint.geometry_wkt)
    .bind(footprint.cloud_cover)
    .bind(&footprint.partition_key)
    .fetch_one(executor)
    .await?;

    Ok(row.0)
}

pub(crate) async fn fetch_dataset(
    pool: &PgPool,
    id: Uuid,
) -> Result<DatasetRecord, CatalogError> {
    let row = sqlx::query_as::<_, DatasetRow>(
        r#"
        SELECT id, name, format, storage_uri, crs, temporal_start, temporal_end, created_at
        FROM datasets
        WHERE id = $1
          AND NOT EXISTS (
              SELECT 1 FROM dataset_deletions dd WHERE dd.dataset_id = datasets.id
          )
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    row.map(Into::into)
        .ok_or(CatalogError::NotFound(id))
}

/// Like [`fetch_dataset`] but ignores the soft-delete tombstone — used only by
/// the purge routine, which legitimately needs to see a soft-deleted dataset's
/// `storage_uri` in order to reclaim it.
pub(crate) async fn fetch_dataset_any(
    pool: &PgPool,
    id: Uuid,
) -> Result<DatasetRecord, CatalogError> {
    let row = sqlx::query_as::<_, DatasetRow>(
        r#"
        SELECT id, name, format, storage_uri, crs, temporal_start, temporal_end, created_at
        FROM datasets
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
struct DatasetRow {
    id: Uuid,
    name: String,
    format: String,
    storage_uri: String,
    crs: Option<String>,
    temporal_start: Option<DateTime<Utc>>,
    temporal_end: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

impl From<DatasetRow> for DatasetRecord {
    fn from(row: DatasetRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            format: format_from_db(&row.format),
            storage_uri: row.storage_uri,
            crs: row.crs,
            temporal_start: row.temporal_start,
            temporal_end: row.temporal_end,
            created_at: row.created_at,
        }
    }
}
