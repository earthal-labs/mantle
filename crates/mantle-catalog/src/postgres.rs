use crate::error::CatalogError;
use crate::{AssetRecord, FootprintRecord, SceneRecord, SceneWithAssets, ServiceRecord};
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
        INSERT INTO services (id, slug, name, description, format, created_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(service.id)
    .bind(&service.slug)
    .bind(&service.name)
    .bind(&service.description)
    .bind(format_to_db(service.format))
    .bind(service.created_at)
    .execute(executor)
    .await?;
    Ok(())
}

pub(crate) async fn insert_scene<'e, E>(
    executor: E,
    scene: &SceneRecord,
) -> Result<(), CatalogError>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query(
        r#"
        INSERT INTO scenes (id, service_id, label, acquired_at, created_at)
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(scene.id)
    .bind(scene.service_id)
    .bind(&scene.label)
    .bind(scene.acquired_at)
    .bind(scene.created_at)
    .execute(executor)
    .await?;
    Ok(())
}

pub(crate) async fn insert_asset<'e, E>(
    executor: E,
    asset: &AssetRecord,
) -> Result<(), CatalogError>
where
    E: sqlx::PgExecutor<'e>,
{
    sqlx::query(
        r#"
        INSERT INTO service_assets (id, service_id, scene_id, band_role, band_index, format, storage_uri, crs, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        "#,
    )
    .bind(asset.id)
    .bind(asset.service_id)
    .bind(asset.scene_id)
    .bind(&asset.band_role)
    .bind(asset.band_index as i32)
    .bind(format_to_db(asset.format))
    .bind(&asset.storage_uri)
    .bind(&asset.crs)
    .bind(asset.created_at)
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
        INSERT INTO footprints (scene_id, service_id, geometry, cloud_cover, partition_key)
        VALUES ($1, $2, ST_GeomFromText($3), $4, $5)
        RETURNING id
        "#,
    )
    .bind(footprint.scene_id)
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
        SELECT id, slug, name, description, format, created_at
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

/// Resolve a base service by its public URL slug. Respects the soft-delete
/// tombstone, same as [`fetch_service`].
pub(crate) async fn fetch_service_by_slug(
    pool: &PgPool,
    slug: &str,
) -> Result<ServiceRecord, CatalogError> {
    let row = sqlx::query_as::<_, ServiceRow>(
        r#"
        SELECT id, slug, name, description, format, created_at
        FROM services
        WHERE slug = $1
          AND NOT EXISTS (
              SELECT 1 FROM service_deletions sd WHERE sd.service_id = services.id
          )
        "#,
    )
    .bind(slug)
    .fetch_optional(pool)
    .await?;

    row.map(Into::into)
        .ok_or_else(|| CatalogError::ServiceNotFound(slug.to_string()))
}

/// Like [`fetch_service`] but ignores the soft-delete tombstone — used only by
/// the purge routine, which legitimately needs to see a soft-deleted service's
/// scenes/assets in order to reclaim them.
pub(crate) async fn fetch_service_any(
    pool: &PgPool,
    id: Uuid,
) -> Result<ServiceRecord, CatalogError> {
    let row = sqlx::query_as::<_, ServiceRow>(
        r#"
        SELECT id, slug, name, description, format, created_at
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

/// Fetch every non-deleted scene for a service, each with its full asset list.
pub(crate) async fn fetch_scenes_for_service(
    pool: &PgPool,
    service_id: Uuid,
) -> Result<Vec<SceneWithAssets>, CatalogError> {
    let scene_rows = sqlx::query_as::<_, SceneRow>(
        r#"
        SELECT id, service_id, label, acquired_at, created_at
        FROM scenes
        WHERE service_id = $1
          AND NOT EXISTS (SELECT 1 FROM scene_deletions sd WHERE sd.scene_id = scenes.id)
        ORDER BY created_at DESC
        "#,
    )
    .bind(service_id)
    .fetch_all(pool)
    .await?;

    let mut scenes = Vec::with_capacity(scene_rows.len());
    for row in scene_rows {
        scenes.push(fetch_scene_with_assets(pool, row.into()).await?);
    }
    Ok(scenes)
}

/// Fetch one scene with its full asset list.
pub(crate) async fn fetch_scene(
    pool: &PgPool,
    scene_id: Uuid,
) -> Result<SceneWithAssets, CatalogError> {
    let row = sqlx::query_as::<_, SceneRow>(
        r#"
        SELECT id, service_id, label, acquired_at, created_at
        FROM scenes
        WHERE id = $1
          AND NOT EXISTS (SELECT 1 FROM scene_deletions sd WHERE sd.scene_id = scenes.id)
        "#,
    )
    .bind(scene_id)
    .fetch_optional(pool)
    .await?;

    let scene: SceneRecord = row.ok_or(CatalogError::NotFound(scene_id))?.into();
    fetch_scene_with_assets(pool, scene).await
}

/// Like [`fetch_scene`] but ignores the soft-delete tombstone — used only by
/// the purge routine.
pub(crate) async fn fetch_scene_any(
    pool: &PgPool,
    scene_id: Uuid,
) -> Result<SceneWithAssets, CatalogError> {
    let row = sqlx::query_as::<_, SceneRow>(
        r#"
        SELECT id, service_id, label, acquired_at, created_at
        FROM scenes
        WHERE id = $1
        "#,
    )
    .bind(scene_id)
    .fetch_optional(pool)
    .await?;

    let scene: SceneRecord = row.ok_or(CatalogError::NotFound(scene_id))?.into();
    fetch_scene_with_assets(pool, scene).await
}

async fn fetch_scene_with_assets(
    pool: &PgPool,
    scene: SceneRecord,
) -> Result<SceneWithAssets, CatalogError> {
    let asset_rows = sqlx::query_as::<_, AssetRow>(
        r#"
        SELECT id, service_id, scene_id, band_role, band_index, format, storage_uri, crs, created_at
        FROM service_assets
        WHERE scene_id = $1
        ORDER BY created_at ASC
        "#,
    )
    .bind(scene.id)
    .fetch_all(pool)
    .await?;

    let footprint: Option<(String, Option<f64>)> = sqlx::query_as(
        r#"
        SELECT ST_AsText(geometry), cloud_cover FROM footprints WHERE scene_id = $1 LIMIT 1
        "#,
    )
    .bind(scene.id)
    .fetch_optional(pool)
    .await?;

    let (geometry_wkt, cloud_cover) = match footprint {
        Some((wkt, cover)) => (Some(wkt), cover),
        None => (None, None),
    };

    Ok(SceneWithAssets {
        scene,
        assets: asset_rows.into_iter().map(Into::into).collect(),
        geometry_wkt,
        cloud_cover,
    })
}

#[derive(sqlx::FromRow)]
struct ServiceRow {
    id: Uuid,
    slug: String,
    name: String,
    description: Option<String>,
    format: String,
    created_at: DateTime<Utc>,
}

impl From<ServiceRow> for ServiceRecord {
    fn from(row: ServiceRow) -> Self {
        Self {
            id: row.id,
            slug: row.slug,
            name: row.name,
            description: row.description,
            format: format_from_db(&row.format),
            created_at: row.created_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct SceneRow {
    id: Uuid,
    service_id: Uuid,
    label: Option<String>,
    acquired_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

impl From<SceneRow> for SceneRecord {
    fn from(row: SceneRow) -> Self {
        Self {
            id: row.id,
            service_id: row.service_id,
            label: row.label,
            acquired_at: row.acquired_at,
            created_at: row.created_at,
        }
    }
}

#[derive(sqlx::FromRow)]
struct AssetRow {
    id: Uuid,
    service_id: Uuid,
    scene_id: Uuid,
    band_role: String,
    band_index: i32,
    format: String,
    storage_uri: String,
    crs: Option<String>,
    created_at: DateTime<Utc>,
}

impl From<AssetRow> for AssetRecord {
    fn from(row: AssetRow) -> Self {
        Self {
            id: row.id,
            service_id: row.service_id,
            scene_id: row.scene_id,
            band_role: row.band_role,
            band_index: row.band_index as u32,
            format: format_from_db(&row.format),
            storage_uri: row.storage_uri,
            crs: row.crs,
            created_at: row.created_at,
        }
    }
}
