use crate::error::CatalogError;
use crate::services::{sanitize_slug, VirtualServiceKind, VirtualServiceRecord};
use crate::ServiceRecord;
use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

pub(crate) async fn insert_virtual_service<'e, E>(
    executor: E,
    record: &VirtualServiceRecord,
) -> Result<(), CatalogError>
where
    E: sqlx::PgExecutor<'e>,
{
    let kind = match record.service_kind {
        VirtualServiceKind::Attached => "attached",
        VirtualServiceKind::Output => "output",
    };

    sqlx::query(
        r#"
        INSERT INTO virtual_services
            (id, slug, service_kind, service_id, parent_service_id, function_id, params_defaults, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(record.id)
    .bind(&record.slug)
    .bind(kind)
    .bind(record.service_id)
    .bind(record.parent_service_id)
    .bind(&record.function_id)
    .bind(&record.params_defaults)
    .bind(record.created_at)
    .execute(executor)
    .await?;

    Ok(())
}

pub(crate) async fn fetch_virtual_service_by_slug(
    pool: &PgPool,
    slug: &str,
) -> Result<VirtualServiceRecord, CatalogError> {
    let normalized = sanitize_slug(slug);
    let row = sqlx::query_as::<_, VirtualServiceRow>(
        r#"
        SELECT id, slug, service_kind, service_id, parent_service_id, function_id, params_defaults, created_at
        FROM virtual_services
        WHERE slug = $1 AND deleted_at IS NULL
        "#,
    )
    .bind(&normalized)
    .fetch_optional(pool)
    .await?;

    row.map(Into::into)
        .ok_or_else(|| CatalogError::ServiceNotFound(normalized))
}

pub(crate) async fn fetch_virtual_services(
    pool: &PgPool,
    service_id: Option<Uuid>,
) -> Result<Vec<VirtualServiceRecord>, CatalogError> {
    let rows = sqlx::query_as::<_, VirtualServiceRow>(
        r#"
        SELECT id, slug, service_kind, service_id, parent_service_id, function_id, params_defaults, created_at
        FROM virtual_services
        WHERE deleted_at IS NULL
          AND ($1::uuid IS NULL OR service_id = $1 OR parent_service_id = $1)
        "#,
    )
    .bind(service_id)
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(Into::into).collect())
}

#[derive(sqlx::FromRow)]
struct VirtualServiceRow {
    id: Uuid,
    slug: String,
    service_kind: String,
    service_id: Uuid,
    parent_service_id: Option<Uuid>,
    function_id: String,
    params_defaults: serde_json::Value,
    created_at: chrono::DateTime<Utc>,
}

impl From<VirtualServiceRow> for VirtualServiceRecord {
    fn from(row: VirtualServiceRow) -> Self {
        let service_kind = match row.service_kind.as_str() {
            "output" => VirtualServiceKind::Output,
            _ => VirtualServiceKind::Attached,
        };
        Self {
            id: row.id,
            slug: row.slug,
            service_kind,
            service_id: row.service_id,
            parent_service_id: row.parent_service_id,
            function_id: row.function_id,
            params_defaults: row.params_defaults,
            created_at: row.created_at,
        }
    }
}

pub(crate) async fn slug_exists(pool: &PgPool, slug: &str) -> Result<bool, CatalogError> {
    let row: (bool,) = sqlx::query_as(
        r#"
        SELECT EXISTS(SELECT 1 FROM virtual_services WHERE slug = $1 AND deleted_at IS NULL)
        "#,
    )
    .bind(slug)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

pub(crate) async fn attach_function_to_service(
    pool: &PgPool,
    parent_service: &ServiceRecord,
    function_id: String,
    params_defaults: serde_json::Value,
    endpoint_slug: Option<String>,
) -> Result<VirtualServiceRecord, CatalogError> {
    let slug = crate::services::generate_service_slug(
        parent_service.id,
        &function_id,
        endpoint_slug.as_deref(),
    );

    if slug_exists(pool, &slug).await? {
        return Err(CatalogError::DuplicateSlug(slug));
    }

    let record = VirtualServiceRecord {
        id: Uuid::new_v4(),
        slug,
        service_kind: VirtualServiceKind::Attached,
        service_id: parent_service.id,
        parent_service_id: Some(parent_service.id),
        function_id,
        params_defaults,
        created_at: Utc::now(),
    };

    insert_virtual_service(pool, &record).await?;
    Ok(record)
}
