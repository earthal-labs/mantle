//! Arrow IPC interchange types for Rust ↔ Python handoff.

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceRef {
    pub id: Uuid,
    pub name: String,
    pub format: ServiceFormat,
    pub storage_uri: String,
    pub crs: Option<String>,
    /// Footprint geometry as WKT (e.g. `POLYGON((...))`), when known.
    #[serde(default)]
    pub geometry_wkt: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceFormat {
    Cog,
    Icechunk,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TileRequest {
    pub service_id: Uuid,
    pub z: u32,
    pub x: u32,
    pub y: u32,
    pub band: Option<u32>,
    pub render_rule: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobSpec {
    pub job_id: Uuid,
    pub process_id: String,
    pub service_refs: Vec<ServiceRef>,
    pub params: serde_json::Value,
    pub submitted_at: DateTime<Utc>,
}

#[derive(Debug, thiserror::Error)]
pub enum ArrowError {
    #[error("arrow IPC error: {0}")]
    Ipc(#[from] arrow::error::ArrowError),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// IPC schema for `ServiceRef` batches (Rust → Python).
pub fn service_ref_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("format", DataType::Utf8, false),
        Field::new("storage_uri", DataType::Utf8, false),
        Field::new("crs", DataType::Utf8, true),
    ])
}

/// IPC schema for `TileRequest` batches.
pub fn tile_request_schema() -> Schema {
    Schema::new(vec![
        Field::new("service_id", DataType::Utf8, false),
        Field::new("z", DataType::UInt32, false),
        Field::new("x", DataType::UInt32, false),
        Field::new("y", DataType::UInt32, false),
        Field::new("band", DataType::UInt32, true),
        Field::new("render_rule", DataType::Utf8, true),
    ])
}

/// IPC schema for `JobSpec` batches.
pub fn job_spec_schema() -> Schema {
    Schema::new(vec![
        Field::new("job_id", DataType::Utf8, false),
        Field::new("process_id", DataType::Utf8, false),
        Field::new("params_json", DataType::Utf8, false),
        Field::new("submitted_at", DataType::Utf8, false),
    ])
}

/// Encode a single `ServiceRef` as an Arrow IPC stream (one-record batch).
pub fn encode_service_ref(service: &ServiceRef) -> Result<Vec<u8>, ArrowError> {
    let schema = Arc::new(service_ref_schema());
    let id = Arc::new(StringArray::from(vec![service.id.to_string()]));
    let name = Arc::new(StringArray::from(vec![service.name.as_str()]));
    let format = Arc::new(StringArray::from(vec![format!(
        "{:?}",
        service.format
    )
    .to_lowercase()]));
    let storage_uri = Arc::new(StringArray::from(vec![service.storage_uri.as_str()]));
    let crs = Arc::new(StringArray::from(vec![service.crs.as_deref()]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![id, name, format, storage_uri, crs],
    )?;

    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}

/// Decode `ServiceRef` records from an Arrow IPC stream.
pub fn decode_service_refs(bytes: &[u8]) -> Result<Vec<ServiceRef>, ArrowError> {
    let cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None)?;
    let mut refs = Vec::new();

    for batch in reader {
        let batch = batch?;
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("id column");
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        let formats = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("format column");
        let uris = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("storage_uri column");
        let crs_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("crs column");

        for i in 0..batch.num_rows() {
            let format = match formats.value(i) {
                "cog" => ServiceFormat::Cog,
                _ => ServiceFormat::Icechunk,
            };
            refs.push(ServiceRef {
                id: Uuid::parse_str(ids.value(i)).unwrap_or_default(),
                name: names.value(i).to_string(),
                format,
                storage_uri: uris.value(i).to_string(),
                crs: if crs_col.is_null(i) {
                    None
                } else {
                    Some(crs_col.value(i).to_string())
                },
                // Not part of the Arrow IPC schema (Rust<->Python handoff
                // doesn't need footprint geometry, only raster access).
                geometry_wkt: None,
            });
        }
    }
    Ok(refs)
}

/// Encode a `TileRequest` as Arrow IPC.
pub fn encode_tile_request(request: &TileRequest) -> Result<Vec<u8>, ArrowError> {
    let schema = Arc::new(tile_request_schema());
    let service_id = Arc::new(StringArray::from(vec![request.service_id.to_string()]));
    let z = Arc::new(UInt32Array::from(vec![request.z]));
    let x = Arc::new(UInt32Array::from(vec![request.x]));
    let y = Arc::new(UInt32Array::from(vec![request.y]));
    let band: ArrayRef = match request.band {
        Some(b) => Arc::new(UInt32Array::from(vec![Some(b)])),
        None => Arc::new(UInt32Array::from(vec![None::<u32>])),
    };
    let render_rule = Arc::new(StringArray::from(vec![request.render_rule.as_deref()]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![service_id, z, x, y, band, render_rule],
    )?;

    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}

/// Encode a `JobSpec` as Arrow IPC.
pub fn encode_job_spec(job: &JobSpec) -> Result<Vec<u8>, ArrowError> {
    let schema = Arc::new(job_spec_schema());
    let job_id = Arc::new(StringArray::from(vec![job.job_id.to_string()]));
    let process_id = Arc::new(StringArray::from(vec![job.process_id.as_str()]));
    let params_json = Arc::new(StringArray::from(vec![serde_json::to_string(&job.params)?]));
    let submitted_at = Arc::new(StringArray::from(vec![job.submitted_at.to_rfc3339()]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![job_id, process_id, params_json, submitted_at],
    )?;

    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }
    Ok(buffer)
}
