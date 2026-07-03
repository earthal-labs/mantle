"""Output framework unit tests."""

from __future__ import annotations

import json

import pytest

from mantle_analytics.output import (
    InMemoryArtifactWriter,
    create_context,
    emit_result,
    serialize_result,
)
from mantle_analytics.plugins.base import JobResult
from mantle_analytics.plugins.builtin.prpm.zonal_stats import ZonalStatsJob

from mantle_analytics.plugins.context import AnalyticsContext

from mantle_analytics.plugins.custom.prpm.minimal_prpm import MeanValuespRPM

from mantle_analytics.plugins.parameters import (
    ParamDirection,
    ParamType,
    ParameterSpec,
    output_parameters,
    output_spec_from_parameters,
)


def test_serialize_json_result() -> None:
    result = JobResult(data={"value": 1}, output_kind="json")
    content, content_type = serialize_result(result, output_kind="json")
    assert content_type == "application/json"
    assert json.loads(content.decode()) == {"value": 1}


def test_context_write_json_uses_writer() -> None:
    writer = InMemoryArtifactWriter()
    ctx = AnalyticsContext(job_id="j1", storage_bucket="bucket", _writer=writer)
    artifact = ctx.write_json({"ok": True}, path_suffix="jobs/j1/custom.json")
    assert artifact.storage_uri == "s3://bucket/jobs/j1/custom.json"
    stored, content_type = writer.objects[artifact.storage_uri]
    assert content_type == "application/json"
    assert json.loads(stored.decode()) == {"ok": True}


def test_write_cog_stub_raises() -> None:
    ctx = AnalyticsContext(
        job_id="j1", storage_bucket="bucket", _writer=InMemoryArtifactWriter()
    )
    with pytest.raises(NotImplementedError, match="write_cog"):
        ctx.write_cog()


def test_output_spec_from_parameters_uses_filename_template() -> None:
    job = MeanValuespRPM()
    spec = output_spec_from_parameters(job.parameters(), output_name="summary")
    assert spec.filename_template == "mean.json"
    assert spec.subpath == "jobs"
    assert spec.kind == "json"


def test_zonal_stats_has_one_output_parameter() -> None:
    job = ZonalStatsJob()
    outputs = output_parameters(job.parameters())
    assert len(outputs) == 1
    assert outputs[0].name == "statistics"
    assert outputs[0].param_type == ParamType.OUTPUT_JSON
    assert outputs[0].direction == ParamDirection.OUTPUT


def test_emit_result_enriches_payload() -> None:
    plugin = ZonalStatsJob()
    writer = InMemoryArtifactWriter()
    ctx = create_context(job_id="abc", storage_bucket="mantle-data", writer=writer)
    result = JobResult(data={"mean": 3.0}, output_name="statistics")
    payload, artifact = emit_result(plugin, result, ctx)
    assert payload["process_id"] == "zonal_stats"
    assert payload["job_id"] == "abc"
    assert payload["result_url"] == artifact.storage_uri
    assert artifact.storage_uri == "s3://mantle-data/jobs/abc/zonal_stats.json"
