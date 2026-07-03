"""Arrow IPC JobSpec decode — matches ``mantle-arrow`` schema."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pyarrow as pa
import pyarrow.ipc as ipc

# Mirrors ``mantle_arrow::job_spec_schema()``.
JOB_SPEC_SCHEMA = pa.schema(
    [
        ("job_id", pa.string()),
        ("process_id", pa.string()),
        ("params_json", pa.string()),
        ("submitted_at", pa.string()),
    ]
)


@dataclass
class DatasetRef:
    id: str
    name: str
    format: str
    storage_uri: str
    crs: str | None = None

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> DatasetRef:
        return cls(
            id=str(raw["id"]),
            name=str(raw["name"]),
            format=str(raw.get("format", "cog")).lower(),
            storage_uri=str(raw["storage_uri"]),
            crs=raw.get("crs"),
        )


@dataclass
class JobSpec:
    job_id: str
    process_id: str
    params: dict[str, Any]
    submitted_at: datetime
    dataset_refs: list[DatasetRef] = field(default_factory=list)


def decode_job_spec(payload: bytes) -> JobSpec:
    """Deserialize a single-row Arrow IPC stream into a :class:`JobSpec`."""
    reader = ipc.open_stream(payload)
    batch = reader.read_next_batch()
    if batch is None or batch.num_rows == 0:
        raise ValueError("JobSpec IPC stream contains no rows")

    job_id = batch.column("job_id")[0].as_py()
    process_id = batch.column("process_id")[0].as_py()
    params_raw = batch.column("params_json")[0].as_py()
    submitted_raw = batch.column("submitted_at")[0].as_py()

    params: dict[str, Any] = json.loads(params_raw) if params_raw else {}
    dataset_refs: list[DatasetRef] = []
    if "dataset_refs" in params:
        refs = params.pop("dataset_refs")
        if isinstance(refs, list):
            dataset_refs = [DatasetRef.from_dict(item) for item in refs]

    submitted_at = datetime.fromisoformat(submitted_raw.replace("Z", "+00:00"))

    return JobSpec(
        job_id=job_id,
        process_id=process_id,
        params=params,
        submitted_at=submitted_at,
        dataset_refs=dataset_refs,
    )


def encode_job_spec(job: JobSpec) -> bytes:
    """Encode a JobSpec as Arrow IPC (for tests and round-trip validation)."""
    params = dict(job.params)
    if job.dataset_refs:
        params["dataset_refs"] = [
            {
                "id": ref.id,
                "name": ref.name,
                "format": ref.format,
                "storage_uri": ref.storage_uri,
                "crs": ref.crs,
            }
            for ref in job.dataset_refs
        ]

    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([job.job_id], type=pa.string()),
            pa.array([job.process_id], type=pa.string()),
            pa.array([json.dumps(params)], type=pa.string()),
            pa.array([job.submitted_at.isoformat()], type=pa.string()),
        ],
        schema=JOB_SPEC_SCHEMA,
    )

    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, JOB_SPEC_SCHEMA) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()
