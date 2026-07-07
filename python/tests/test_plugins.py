"""Plugin framework and output abstraction tests."""

from __future__ import annotations

import json

import numpy as np
import pytest

from mantle_analytics.output import InMemoryArtifactWriter, create_context, emit_result
from mantle_analytics.plugins.base import (
    AnalyticsContext,
    JobInputs,
    JobResult,
    TileMeta,
)
from mantle_analytics.plugins.builtin.vrpm.ndvi import NDVI
from mantle_analytics.registry import (
    get_prpm_model,
    get_vrpm_model,
    initialize_registry,
    list_prpm_models,
    list_vrpm_models,
    reset_registry,
)


@pytest.fixture(autouse=True)
def _fresh_registry() -> None:
    reset_registry()
    initialize_registry()


def test_ndvi_compute_tile() -> None:
    plugin = NDVI()
    bands = {
        "red": np.array([[0.1, 0.2], [0.1, 0.2]], dtype=np.float32),
        "nir": np.array([[0.5, 0.6], [0.5, 0.6]], dtype=np.float32),
    }
    meta = TileMeta(z=10, x=1, y=2, width=2, height=2)
    result = plugin.compute_tile(bands, {}, meta)
    assert result.shape == (2, 2)
    assert result[0, 0] > 0.5


def test_registry_lists_builtin_plugins() -> None:
    vrpm = list_vrpm_models()
    prpm = list_prpm_models()
    assert any(entry["id"] == "ndvi" for entry in vrpm)
    assert any(entry["id"] == "minimal_vrpm" for entry in vrpm)
    assert any(entry["id"] == "zonal_stats" for entry in prpm)
    assert any(entry["id"] == "minimal_prpm" for entry in prpm)


def test_registry_rejects_unknown_plugin_id() -> None:
    with pytest.raises(KeyError, match="unknown vRPM"):
        get_vrpm_model("does_not_exist")
    with pytest.raises(KeyError, match="unknown pRPM"):
        get_prpm_model("does_not_exist")


def test_zonal_stats_plugin_returns_data_without_storage_paths() -> None:
    job = get_prpm_model("zonal_stats")
    inputs = JobInputs(params={"values": [1.0, 2.0, 3.0]})
    job.validate_inputs(inputs)
    result = job.run(
        inputs, AnalyticsContext(job_id="test-job", storage_bucket="mantle-data")
    )
    assert result.data["mean"] == 2.0
    serialized = json.dumps(result.data)
    assert "s3://" not in serialized


def test_zonal_stats_reads_band_from_service_refs_stub() -> None:
    job = get_prpm_model("zonal_stats")
    inputs = JobInputs(
        params={"band": 1},
        service_refs=[
            {
                "id": "550e8400-e29b-41d4-a716-446655440001",
                "name": "fixture",
                "format": "cog",
                "storage_uri": "s3://mantle-data/services/fixture.tif",
            }
        ],
    )
    job.validate_inputs(inputs)
    result = job.run(
        inputs, AnalyticsContext(job_id="test-job", storage_bucket="mantle-data")
    )
    assert result.data["count"] > 0
    assert result.data["raster_read"]["source"] == "stub"


def test_emit_result_writes_artifact_and_sets_result_url() -> None:
    job = get_prpm_model("zonal_stats")
    writer = InMemoryArtifactWriter()
    ctx = create_context(job_id="test-job", storage_bucket="mantle-data", writer=writer)
    inputs = JobInputs(params={"values": [1.0, 2.0, 3.0]})
    result = job.run(inputs, ctx)
    payload, artifact = emit_result(job, result, ctx)

    assert artifact.storage_uri == "s3://mantle-data/jobs/test-job/zonal_stats.json"
    assert payload["result_url"] == artifact.storage_uri
    assert payload["mean"] == 2.0
    assert artifact.storage_uri in writer.objects


def test_minimal_vrpm_custom_template_loads_and_scales() -> None:
    """Custom vRPM template passes security scan and scales a single band."""
    from pathlib import Path

    from mantle_analytics.security import validate_source_ast

    source_path = (
        Path(__file__).resolve().parents[1]
        / "mantle_analytics"
        / "plugins"
        / "custom"
        / "vrpm"
        / "minimal_vrpm.py"
    )
    validate_source_ast(source_path.read_text(encoding="utf-8"), path=str(source_path))

    plugin = get_vrpm_model("minimal_vrpm")
    assert plugin.id == "minimal_vrpm"

    bands = {"source": np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)}
    meta = TileMeta(z=10, x=1, y=2, width=2, height=2)
    result = plugin.compute_tile(bands, {"scale": 2.0}, meta)
    assert result.dtype == np.float32
    assert result[0, 0] == 2.0


def test_minimal_prpm_custom_template_json_output() -> None:
    """Custom pRPM template returns JSON data only; framework handles storage."""
    from pathlib import Path

    from mantle_analytics.security import validate_source_ast

    source_path = (
        Path(__file__).resolve().parents[1]
        / "mantle_analytics"
        / "plugins"
        / "custom"
        / "prpm"
        / "minimal_prpm.py"
    )
    validate_source_ast(source_path.read_text(encoding="utf-8"), path=str(source_path))

    job = get_prpm_model("minimal_prpm")
    inputs = JobInputs(params={"values": [2.0, 4.0, 6.0]})
    job.validate_inputs(inputs)
    result = job.run(
        inputs,
        AnalyticsContext(job_id="example-job", storage_bucket="mantle-data"),
    )
    assert result.data["mean"] == 4.0
    assert "s3://" not in json.dumps(result.data)

    writer = InMemoryArtifactWriter()
    ctx = create_context(
        job_id="example-job", storage_bucket="mantle-data", writer=writer
    )
    payload, artifact = emit_result(job, result, ctx)
    assert artifact.storage_uri == "s3://mantle-data/jobs/example-job/mean.json"
    assert payload["result_url"] == artifact.storage_uri
