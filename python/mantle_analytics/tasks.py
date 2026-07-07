"""Ray remote tasks for Mantle analytics processes."""

from __future__ import annotations

import json
from typing import Any

import ray

from mantle_analytics.icechunk_io import read_cube_slice
from mantle_analytics.job_spec import JobSpec
from mantle_analytics.output import create_context, emit_result
from mantle_analytics.plugins.base import JobInputs
from mantle_analytics.registry import get_prpm_model, initialize_registry


def _legacy_result_payload(
    job: JobSpec,
    bucket: str,
    body: dict[str, Any],
    *,
    suffix: str = "result.json",
) -> dict[str, Any]:
    """Write a legacy non-plugin process result via the framework."""
    ctx = create_context(job_id=job.job_id, storage_bucket=bucket)
    content = json.dumps(body, default=str, sort_keys=True).encode("utf-8")
    key = f"jobs/{job.job_id}/{suffix}"
    artifact = ctx.write_bytes(key, content, "application/json")
    payload = dict(body)
    payload["result_url"] = artifact.storage_uri
    return payload


@ray.remote
def run_ndvi(job: JobSpec, bucket: str) -> dict[str, Any]:
    """Compute NDVI from red/NIR band values supplied in params or service refs."""
    red = job.params.get("red")
    nir = job.params.get("nir")
    if red is None:
        red = job.params.get("red_band", 0.2)
    if nir is None:
        nir = job.params.get("nir_band", 0.8)
    if job.service_refs and (red is None or nir is None):
        # Beta: pass service context through for future raster reads.
        first = job.service_refs[0]
        red = red if red is not None else 0.2
        nir = nir if nir is not None else 0.8
        service_note = {
            "id": first.id,
            "name": first.name,
            "storage_uri": first.storage_uri,
        }
    else:
        service_note = None

    red_value = float(red)
    nir_value = float(nir)
    denominator = nir_value + red_value
    ndvi = (nir_value - red_value) / denominator if denominator else 0.0

    body: dict[str, Any] = {
        "process_id": "ndvi",
        "ndvi": ndvi,
        "inputs": {"red": red_value, "nir": nir_value},
    }
    if job.service_refs:
        body["service_refs"] = [
            {
                "id": ref.id,
                "name": ref.name,
                "format": ref.format,
                "storage_uri": ref.storage_uri,
            }
            for ref in job.service_refs
        ]
    if service_note is not None:
        body["primary_service"] = service_note
    return _legacy_result_payload(job, bucket, body, suffix="ndvi.json")


@ray.remote
def run_zonal_stats(job: JobSpec, bucket: str) -> dict[str, Any]:
    """Aggregate zonal statistics via the plugin registry."""
    initialize_registry()
    plugin = get_prpm_model("zonal_stats")
    inputs = JobInputs(
        params=dict(job.params),
        service_refs=[
            {
                "id": ref.id,
                "name": ref.name,
                "format": ref.format,
                "storage_uri": ref.storage_uri,
            }
            for ref in job.service_refs
        ],
    )
    plugin.validate_inputs(inputs)
    ctx = create_context(job_id=job.job_id, storage_bucket=bucket)
    result = plugin.run(inputs, ctx)
    payload, _artifact = emit_result(plugin, result, ctx)
    return payload


@ray.remote
def run_plugin_job(job: JobSpec, bucket: str) -> dict[str, Any]:
    """Dispatch any registered PersistentRasterProcessingModel by process_id."""
    initialize_registry()
    plugin = get_prpm_model(job.process_id.replace("-", "_"))
    inputs = JobInputs(
        params=dict(job.params),
        service_refs=[
            {
                "id": ref.id,
                "name": ref.name,
                "format": ref.format,
                "storage_uri": ref.storage_uri,
            }
            for ref in job.service_refs
        ],
    )
    plugin.validate_inputs(inputs)
    ctx = create_context(job_id=job.job_id, storage_bucket=bucket)
    result = plugin.run(inputs, ctx)
    payload, _artifact = emit_result(plugin, result, ctx)
    return payload


@ray.remote
def run_cube_slice(job: JobSpec, bucket: str) -> dict[str, Any]:
    """Extract a multidimensional slice from an Icechunk service."""
    variable = str(job.params.get("variable", "temperature"))
    indices = job.params.get("indices")

    storage_uri = job.params.get("storage_uri")
    if not storage_uri and job.service_refs:
        storage_uri = job.service_refs[0].storage_uri

    if not storage_uri:
        raise ValueError("cube_slice requires storage_uri or service_refs")

    slice_payload = read_cube_slice(
        str(storage_uri),
        variable=variable,
        indices=indices if isinstance(indices, dict) else None,
    )

    body = {
        "process_id": "cube_slice",
        "slice": slice_payload,
    }
    return _legacy_result_payload(job, bucket, body, suffix="cube_slice.json")


@ray.remote
def run_process(job: JobSpec, bucket: str) -> dict[str, Any]:
    """Generic process runner — echoes params and writes a JSON result URI."""
    body = {
        "process_id": job.process_id,
        "params": job.params,
        "service_refs": [
            {
                "id": ref.id,
                "name": ref.name,
                "format": ref.format,
                "storage_uri": ref.storage_uri,
            }
            for ref in job.service_refs
        ],
        "summary": json.dumps(job.params, sort_keys=True),
    }
    return _legacy_result_payload(job, bucket, body)


PROCESS_DISPATCH = {
    "ndvi": run_ndvi,
    "zonal_stats": run_zonal_stats,
    "zonal-stats": run_zonal_stats,
    "cube_slice": run_cube_slice,
    "cube-slice": run_cube_slice,
    "custom_python": run_process,
    "custom-python": run_process,
}


def dispatch_process(job: JobSpec, bucket: str) -> Any:
    """Select a Ray task ref for the job's process_id."""
    initialize_registry()
    key = job.process_id.replace("-", "_")
    try:
        get_prpm_model(key)
        return run_plugin_job.remote(job, bucket)
    except KeyError:
        pass
    task = (
        PROCESS_DISPATCH.get(job.process_id) or PROCESS_DISPATCH.get(key) or run_process
    )
    return task.remote(job, bucket)
