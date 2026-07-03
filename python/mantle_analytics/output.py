"""Framework output handling — serialization, object-store writes, result URLs."""

from __future__ import annotations

import json

import os
from typing import Any, Literal

from mantle_analytics.plugins.base import JobResult, OutputSpec, PersistentRasterProcessingModel

from mantle_analytics.plugins.context import (
    AnalyticsContext,
    ArtifactWriter,
    WrittenArtifact,
)
from mantle_analytics.plugins.parameters import (
    OUTPUT_KIND_BY_PARAM_TYPE,
    output_parameters,
    output_spec_from_parameters,
    primary_output,
)

OutputKind = Literal["json", "geojson", "cog", "zarr", "text"]


def resolve_object_key(ctx: AnalyticsContext, spec: OutputSpec) -> str:
    """Build the object key from ``OutputSpec`` and job context."""
    filename = spec.filename_template.format(
        job_id=ctx.job_id,
        bucket=ctx.storage_bucket,
    )
    return f"{spec.subpath.strip('/')}/{ctx.job_id}/{filename}"


def _resolve_output_kind(plugin: PersistentRasterProcessingModel, result: JobResult) -> str:
    specs = plugin.parameters()
    if result.output_name is not None:
        match = next(
            (
                spec
                for spec in output_parameters(specs)
                if spec.name == result.output_name
            ),
            None,
        )
        if match is None:
            raise ValueError(f"unknown output parameter: {result.output_name}")
        return OUTPUT_KIND_BY_PARAM_TYPE[match.param_type]
    if result.output_kind is not None:
        return result.output_kind
    primary = primary_output(specs)
    if primary is None:
        raise ValueError("plugin declares no output parameters")
    return OUTPUT_KIND_BY_PARAM_TYPE[primary.param_type]


def serialize_result(result: JobResult, *, output_kind: str) -> tuple[bytes, str]:
    """Encode ``JobResult.data`` for the declared output kind."""
    if output_kind == "json":
        if not isinstance(result.data, dict):
            raise TypeError("json JobResult.data must be a dict")
        return (
            json.dumps(result.data, default=str, sort_keys=True).encode("utf-8"),
            "application/json",
        )
    if output_kind == "geojson":
        if not isinstance(result.data, dict):
            raise TypeError("geojson JobResult.data must be a dict")
        return json.dumps(result.data).encode("utf-8"), "application/geo+json"
    if output_kind == "text":
        if isinstance(result.data, bytes):
            return result.data, "text/plain; charset=utf-8"
        return str(result.data).encode("utf-8"), "text/plain; charset=utf-8"

    raise ValueError(
        f"output kind {output_kind!r} is not supported for automatic serialization"
    )


def emit_result(
    plugin: PersistentRasterProcessingModel,
    result: JobResult,
    ctx: AnalyticsContext,
) -> tuple[dict[str, Any], WrittenArtifact]:
    """Write plugin output and build API-facing payload with ``result_url``."""

    specs = plugin.parameters()
    outputs = output_parameters(specs)
    if not outputs:
        raise ValueError(f"plugin {plugin.id!r} declares no output parameters")
    if len(outputs) > 1 and result.output_name is None:
        raise ValueError(
            f"plugin {plugin.id!r} has multiple output parameters; "
            "set JobResult.output_name or declare exactly one output"
        )
    spec = output_spec_from_parameters(specs, output_name=result.output_name)
    output_kind = _resolve_output_kind(plugin, result)
    key = resolve_object_key(ctx, spec)
    content, content_type = serialize_result(result, output_kind=output_kind)
    artifact = ctx.write_bytes(key, content, content_type)
    if isinstance(result.data, dict):
        payload: dict[str, Any] = dict(result.data)

    else:
        payload = {"data": result.data}
    payload.setdefault("process_id", plugin.id)
    payload.setdefault("job_id", ctx.job_id)
    payload["output_kind"] = output_kind
    payload["result_url"] = artifact.storage_uri
    return payload, artifact


class InMemoryArtifactWriter:
    """Test double that records writes without touching object storage."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}

    def write(
        self,
        bucket: str,
        key: str,
        content: bytes,
        content_type: str,
    ) -> WrittenArtifact:
        uri = f"s3://{bucket}/{key}"
        self.objects[uri] = (content, content_type)
        return WrittenArtifact(
            storage_uri=uri,
            content_type=content_type,
            byte_size=len(content),
        )


class S3ArtifactWriter:
    """Production writer using boto3 (framework-only; not imported by plugins)."""

    def __init__(self) -> None:
        try:
            import boto3  # noqa: PLC0415
        except ImportError as exc:  # pragma: no cover - optional in unit tests
            raise RuntimeError(
                "boto3 is required for S3 artifact writes; install mantle-analytics[analytics]"
            ) from exc
        self._client = boto3.client(
            "s3",
            region_name=os.environ.get("AWS_REGION"),
            endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None,
        )

    def write(
        self,
        bucket: str,
        key: str,
        content: bytes,
        content_type: str,
    ) -> WrittenArtifact:

        self._client.put_object(
            Bucket=bucket,
            Key=key,
            Body=content,
            ContentType=content_type,
        )

        return WrittenArtifact(
            storage_uri=f"s3://{bucket}/{key}",
            content_type=content_type,
            byte_size=len(content),
        )


def create_context(
    *,
    job_id: str,
    storage_bucket: str,
    writer: ArtifactWriter | None = None,
) -> AnalyticsContext:
    """Build an ``AnalyticsContext`` with a framework artifact writer."""

    if writer is None:
        writer = S3ArtifactWriter()
    return AnalyticsContext(
        job_id=job_id,
        storage_bucket=storage_bucket,
        _writer=writer,
    )
