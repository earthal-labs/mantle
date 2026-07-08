use crate::error::CatalogError;
use crate::partition;
use crate::{AssetRecord, FootprintRecord, SceneRecord};
use duckdb::Connection;
use mantle_config::CatalogConfig;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tracing::{debug, warn};
use uuid::Uuid;

pub(crate) const CATALOG_ALIAS: &str = "mantle_catalog";
pub(crate) const FOOTPRINTS_TABLE: &str = "footprints";
pub(crate) const GEOPARQUET_VERSION: &str = "V2";
/// Plain (non-DuckLake) read-only attach to the same Postgres database, used
/// only to check `service_deletions` from within DuckDB queries — Postgres is
/// the single source of truth for the soft-delete tombstone, not DuckLake.
pub(crate) const APP_POSTGRES_ALIAS: &str = "app_postgres";

#[derive(Clone)]
pub(crate) struct DuckLakeSession {
    conn: Arc<Mutex<Connection>>,
    config: Arc<CatalogConfig>,
}

impl DuckLakeSession {
    pub fn open(config: Arc<CatalogConfig>) -> Result<Self, CatalogError> {
        let conn = Connection::open_in_memory()?;
        let session = Self {
            conn: Arc::new(Mutex::new(conn)),
            config,
        };
        session.bootstrap()?;
        Ok(session)
    }

    fn with_conn<F, T>(&self, f: F) -> Result<T, CatalogError>
    where
        F: FnOnce(&Connection) -> Result<T, CatalogError>,
    {
        let guard = self
            .conn
            .lock()
            .map_err(|_| CatalogError::Config("duckdb mutex poisoned".into()))?;
        f(&guard)
    }

    fn bootstrap(&self) -> Result<(), CatalogError> {
        self.with_conn(|conn| {
            // Containers often run as `nobody` with HOME=/nonexistent; DuckDB
            // needs a writable home to install/load extensions.
            ensure_duckdb_home(conn)?;
            load_extension(conn, "ducklake")?;
            load_extension(conn, "spatial")?;
            load_extension(conn, "postgres")?;

            let data_path = normalize_data_path(&self.config.ducklake_data_path);
            if data_path.starts_with("s3://") {
                load_extension(conn, "httpfs")?;
                // DuckDB httpfs does not inherit object_store env; configure MinIO/S3 explicitly.
                configure_s3_httpfs(conn)?;
            } else {
                std::fs::create_dir_all(Path::new(&data_path).join("partitions"))
                    .map_err(|e| CatalogError::Config(format!("create data path: {e}")))?;
            }

            let attach = format!(
                "ATTACH 'ducklake:postgres:{}' AS {CATALOG_ALIAS} (DATA_PATH '{data_path}');",
                postgres_attach_params(&self.config.postgres_url)?
            );
            debug!("attaching ducklake catalog");
            conn.execute_batch(&attach)?;
            conn.execute_batch(&format!("USE {CATALOG_ALIAS};"))?;
            self.ensure_footprints_table(conn)?;

            let app_postgres_attach = format!(
                "ATTACH '{}' AS {APP_POSTGRES_ALIAS} (TYPE POSTGRES, READ_ONLY);",
                postgres_attach_params(&self.config.postgres_url)?
            );
            debug!("attaching plain postgres for soft-delete tombstone checks");
            conn.execute_batch(&app_postgres_attach)?;
            Ok(())
        })
    }

    fn ensure_footprints_table(&self, conn: &Connection) -> Result<(), CatalogError> {
        let geom_col = &self.config.geometry_column;
        let ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {CATALOG_ALIAS}.{FOOTPRINTS_TABLE} (
                scene_id UUID,
                service_id UUID,
                service_name VARCHAR,
                {geom_col} GEOMETRY,
                cloud_cover DOUBLE,
                partition_key VARCHAR,
                temporal_start TIMESTAMPTZ,
                temporal_end TIMESTAMPTZ,
                assets_json VARCHAR,
                inserted_at TIMESTAMPTZ
            );
            "#
        );
        conn.execute_batch(&ddl)?;
        Ok(())
    }

    pub fn append_scene_footprint_parquet(
        &self,
        service_name: &str,
        scene: &SceneRecord,
        assets: &[AssetRecord],
        footprint: &FootprintRecord,
        partition_key: &str,
    ) -> Result<String, CatalogError> {
        let file_id = Uuid::new_v4();
        let data_path = normalize_data_path(&self.config.ducklake_data_path);
        let parquet_rel = format!("partitions/{partition_key}/{file_id}.parquet");
        let parquet_uri = format!("{data_path}{parquet_rel}");
        let geom_col = self.config.geometry_column.clone();

        let assets_json = serde_json::to_string(
            &assets
                .iter()
                .map(|a| {
                    serde_json::json!({
                        "id": a.id,
                        "band_role": a.band_role,
                        "band_index": a.band_index,
                        "format": crate::postgres::format_to_db(a.format),
                        "storage_uri": a.storage_uri,
                        "crs": a.crs,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .map_err(|e| CatalogError::Config(format!("serialize assets_json: {e}")))?;

        self.with_conn(|conn| {
            conn.execute_batch("BEGIN TRANSACTION;")?;

            let acquired_at = scene
                .acquired_at
                .map(|t| t.to_rfc3339())
                .unwrap_or_default();

            let staging = format!(
                r#"
                CREATE OR REPLACE TEMP TABLE mantle_footprint_stage AS
                SELECT
                    ?::UUID AS scene_id,
                    ?::UUID AS service_id,
                    ? AS service_name,
                    ST_GeomFromText(?) AS {geom_col},
                    ?::DOUBLE AS cloud_cover,
                    ? AS partition_key,
                    NULLIF(?, '')::TIMESTAMPTZ AS temporal_start,
                    NULLIF(?, '')::TIMESTAMPTZ AS temporal_end,
                    ? AS assets_json,
                    now() AS inserted_at;
                "#
            );

            conn.execute(
                &staging,
                duckdb::params![
                    scene.id.to_string(),
                    scene.service_id.to_string(),
                    service_name,
                    footprint.geometry_wkt.as_str(),
                    footprint.cloud_cover,
                    partition_key,
                    acquired_at.as_str(),
                    acquired_at.as_str(),
                    assets_json.as_str(),
                ],
            )?;

            let copy_sql = format!(
                r#"
                COPY mantle_footprint_stage
                TO '{parquet_uri}'
                (FORMAT PARQUET, GEOPARQUET_VERSION '{GEOPARQUET_VERSION}');
                "#
            );
            conn.execute_batch(&copy_sql)?;

            let register_sql = format!(
                "CALL ducklake_add_data_files('{CATALOG_ALIAS}', '{FOOTPRINTS_TABLE}', '{parquet_uri}');"
            );
            if let Err(err) = conn.execute_batch(&register_sql) {
                warn!(
                    "ducklake_add_data_files failed ({err}); falling back to INSERT for local dev"
                );
                let insert_sql = format!(
                    r#"
                    INSERT INTO {CATALOG_ALIAS}.{FOOTPRINTS_TABLE}
                    SELECT * FROM read_parquet('{parquet_uri}');
                    "#
                );
                conn.execute_batch(&insert_sql)?;
            }

            conn.execute_batch("COMMIT;")?;
            Ok(parquet_uri)
        })
    }

    pub fn spatial_query(
        &self,
        query: &crate::SpatialQuery,
    ) -> Result<Vec<mantle_arrow::SceneRef>, CatalogError> {
        let geom_col = &self.config.geometry_column;
        let mut sql = format!(
            r#"
            SELECT DISTINCT
                scene_id::VARCHAR AS scene_id,
                service_id::VARCHAR AS service_id,
                service_name,
                ST_AsText({geom_col}) AS geometry_wkt,
                assets_json
            FROM {CATALOG_ALIAS}.{FOOTPRINTS_TABLE} f
            WHERE 1=1
              AND NOT EXISTS (
                  SELECT 1 FROM {APP_POSTGRES_ALIAS}.service_deletions d
                  WHERE d.service_id = f.service_id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {APP_POSTGRES_ALIAS}.scene_deletions sd
                  WHERE sd.scene_id = f.scene_id
              )
            "#
        );

        if let Some(bbox) = &query.bbox {
            sql.push_str(&format!(
                " AND ST_Intersects({geom_col}, ST_MakeEnvelope({}, {}, {}, {}))",
                bbox.min().x, bbox.min().y, bbox.max().x, bbox.max().y
            ));
        }
        if let Some(start) = query.datetime_start {
            sql.push_str(&format!(
                " AND (temporal_end IS NULL OR temporal_end >= '{start}')"
            ));
        }
        if let Some(end) = query.datetime_end {
            sql.push_str(&format!(
                " AND (temporal_start IS NULL OR temporal_start <= '{end}')"
            ));
        }
        if let Some(max_cover) = query.cloud_cover_max {
            sql.push_str(&format!(" AND (cloud_cover IS NULL OR cloud_cover <= {max_cover})"));
        }

        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([], |row| {
                let scene_id: String = row.get(0)?;
                let service_id: String = row.get(1)?;
                let service_name: String = row.get(2)?;
                let geometry_wkt: Option<String> = row.get(3)?;
                let assets_json: String = row.get(4)?;
                Ok((scene_id, service_id, service_name, geometry_wkt, assets_json))
            })?;

            let mut scenes = Vec::new();
            for row in rows {
                let (scene_id, service_id, service_name, geometry_wkt, assets_json) = row?;
                let assets: Vec<AssetRow> = serde_json::from_str(&assets_json)
                    .map_err(CatalogError::from)?;
                scenes.push(mantle_arrow::SceneRef {
                    scene_id: Uuid::parse_str(&scene_id).unwrap_or_default(),
                    service_id: Uuid::parse_str(&service_id).unwrap_or_default(),
                    service_name,
                    geometry_wkt,
                    assets: assets.into_iter().map(AssetRow::into_asset_ref).collect(),
                });
            }
            Ok(scenes)
        })
    }

    /// Physically remove a scene's footprint row from the DuckLake-backed
    /// table and reclaim its Parquet file. A real `DELETE` here supersedes the
    /// row's current snapshot; expiring snapshots + cleaning up old files then
    /// makes the now-orphaned file eligible for physical removal. This is
    /// native DuckLake DML — not subject to the Postgres append-only trigger,
    /// which only covers the plain `services`/`footprints` tables.
    ///
    /// Note: `ducklake_cleanup_old_files` is known to no-op against an
    /// external Postgres catalog on some DuckLake builds
    /// (https://github.com/duckdb/ducklake/issues/586) — this call can
    /// silently fail to reclaim storage even though it reports success. The
    /// delete from the logical table (and hence from search/read results)
    /// still takes effect regardless.
    pub fn purge_scene(&self, scene_id: Uuid) -> Result<(), CatalogError> {
        self.with_conn(|conn| {
            conn.execute(
                &format!("DELETE FROM {CATALOG_ALIAS}.{FOOTPRINTS_TABLE} WHERE scene_id = ?;"),
                duckdb::params![scene_id.to_string()],
            )?;
            conn.execute_batch(&format!(
                "CALL ducklake_expire_snapshots('{CATALOG_ALIAS}', older_than => now());"
            ))?;
            if let Err(err) = conn.execute_batch(&format!(
                "CALL ducklake_cleanup_old_files('{CATALOG_ALIAS}', cleanup_all => true);"
            )) {
                warn!("ducklake_cleanup_old_files failed for scene {scene_id}: {err}");
            }
            Ok(())
        })
    }

    /// Purge every scene belonging to a service (used when the whole service
    /// is purged, not just one scene).
    pub fn purge_service_scenes(&self, service_id: Uuid) -> Result<(), CatalogError> {
        self.with_conn(|conn| {
            conn.execute(
                &format!("DELETE FROM {CATALOG_ALIAS}.{FOOTPRINTS_TABLE} WHERE service_id = ?;"),
                duckdb::params![service_id.to_string()],
            )?;
            conn.execute_batch(&format!(
                "CALL ducklake_expire_snapshots('{CATALOG_ALIAS}', older_than => now());"
            ))?;
            if let Err(err) = conn.execute_batch(&format!(
                "CALL ducklake_cleanup_old_files('{CATALOG_ALIAS}', cleanup_all => true);"
            )) {
                warn!("ducklake_cleanup_old_files failed for service {service_id}: {err}");
            }
            Ok(())
        })
    }
}

/// Deserialization shape for the DuckLake footprints table's inline
/// `assets_json` column — mirrors `mantle_arrow::AssetRef` but with a plain
/// `Uuid`/string format field for straightforward `serde_json` round-tripping.
#[derive(serde::Deserialize)]
struct AssetRow {
    id: Uuid,
    band_role: String,
    band_index: u32,
    format: String,
    storage_uri: String,
    crs: Option<String>,
}

impl AssetRow {
    fn into_asset_ref(self) -> mantle_arrow::AssetRef {
        mantle_arrow::AssetRef {
            id: self.id,
            band_role: self.band_role,
            band_index: self.band_index,
            format: crate::postgres::format_from_db(&self.format),
            storage_uri: self.storage_uri,
            crs: self.crs,
        }
    }
}

/// Configure DuckDB S3 access for MinIO / S3 using the same AWS_* env vars as compose.
///
/// Uses `CREATE SECRET` rather than the legacy `SET s3_*` session variables:
/// DuckLake's internal data-file registration (`ducklake_add_data_files`) resolves
/// S3 credentials/endpoint through the secrets manager, not those global variables,
/// so `SET`-only config left it falling back to default AWS endpoint resolution.
fn configure_s3_httpfs(conn: &Connection) -> Result<(), CatalogError> {
    let key = std::env::var("AWS_ACCESS_KEY_ID").unwrap_or_default();
    let secret = std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_default();
    if key.is_empty() || secret.is_empty() {
        warn!("AWS_ACCESS_KEY_ID/SECRET not set; DuckDB S3 catalog writes will fail");
        return Ok(());
    }

    let key_sql = key.replace('\'', "''");
    let secret_sql = secret.replace('\'', "''");
    let mut options = vec![
        "TYPE S3".to_string(),
        format!("KEY_ID '{key_sql}'"),
        format!("SECRET '{secret_sql}'"),
        "URL_STYLE 'path'".to_string(),
    ];

    if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
        let host = endpoint
            .trim()
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .trim_end_matches('/');
        if !host.is_empty() {
            let host_sql = host.replace('\'', "''");
            let use_ssl = endpoint.starts_with("https://");
            options.push(format!("ENDPOINT '{host_sql}'"));
            options.push(format!("USE_SSL {use_ssl}"));
        }
    }

    if let Ok(region) = std::env::var("AWS_REGION") {
        if !region.is_empty() {
            let region_sql = region.replace('\'', "''");
            options.push(format!("REGION '{region_sql}'"));
        }
    }

    let sql = format!("CREATE OR REPLACE SECRET mantle_s3 ({});", options.join(", "));
    conn.execute_batch(&sql).map_err(|e| {
        CatalogError::Config(format!("configure DuckDB S3 secret: {e}"))
    })?;
    Ok(())
}

fn ensure_duckdb_home(conn: &Connection) -> Result<(), CatalogError> {
    let home = std::env::var("MANTLE_DUCKDB_HOME")
        .or_else(|_| std::env::var("HOME"))
        .unwrap_or_else(|_| "/tmp/mantle-duckdb".into());
    let home = if home.is_empty() || home == "/nonexistent" {
        "/tmp/mantle-duckdb".to_string()
    } else {
        home
    };
    std::fs::create_dir_all(&home).map_err(|e| {
        CatalogError::Config(format!("create duckdb home '{home}': {e}"))
    })?;
    let home_sql = home.replace('\'', "''");
    conn.execute_batch(&format!("SET home_directory='{home_sql}';"))
        .map_err(|e| CatalogError::Config(format!("set duckdb home_directory: {e}")))?;
    Ok(())
}

fn load_extension(conn: &Connection, name: &str) -> Result<(), CatalogError> {
    let install = format!("INSTALL {name}; LOAD {name};");
    conn.execute_batch(&install).map_err(|err| {
        CatalogError::Config(format!(
            "failed to load duckdb extension '{name}': {err}"
        ))
    })
}

pub(crate) fn postgres_attach_params(postgres_url: &str) -> Result<String, CatalogError> {
    let parsed = url::Url::parse(postgres_url)
        .map_err(|e| CatalogError::Config(format!("invalid postgres_url: {e}")))?;
    let host = parsed.host_str().unwrap_or("localhost");
    let port = parsed.port().unwrap_or(5432);
    let dbname = parsed.path().trim_start_matches('/');
    if dbname.is_empty() {
        return Err(CatalogError::Config(
            "postgres_url missing database name".into(),
        ));
    }
    let user = parsed.username();
    let password = parsed.password().unwrap_or_default();
    Ok(format!(
        "host={host} port={port} dbname={dbname} user={user} password={password}"
    ))
}

fn normalize_data_path(path: &str) -> String {
    if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

pub(crate) fn resolve_partition_key(footprint: &FootprintRecord, scene: &SceneRecord) -> String {
    partition::resolve_partition_key(&footprint.partition_key, scene.acquired_at)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_attach_params_from_url() {
        let params = postgres_attach_params("postgres://mantle:secret@postgres:5432/mantle")
            .expect("parse");
        assert!(params.contains("host=postgres"));
        assert!(params.contains("port=5432"));
        assert!(params.contains("dbname=mantle"));
        assert!(params.contains("user=mantle"));
        assert!(params.contains("password=secret"));
    }

    #[test]
    fn normalize_data_path_adds_trailing_slash() {
        assert_eq!(
            normalize_data_path("s3://mantle-data/catalog"),
            "s3://mantle-data/catalog/"
        );
    }
}
