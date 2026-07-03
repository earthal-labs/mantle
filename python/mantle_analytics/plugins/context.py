"""Runtime context and artifact types for pRPM jobs."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class WrittenArtifact:
    """Metadata for an object written by the analytics framework."""

    storage_uri: str
    content_type: str
    byte_size: int


class ArtifactWriter(Protocol):
    """Framework-internal object store writer (plugins never implement this)."""

    def write(
        self,
        bucket: str,
        key: str,
        content: bytes,
        content_type: str,
    ) -> WrittenArtifact:
        """Persist bytes at ``s3://{bucket}/{key}`` and return artifact metadata."""


@dataclass
class AnalyticsContext:
    """Runtime context passed to Persistent Raster Processing Models (pRPM)."""

    job_id: str
    storage_bucket: str
    work_dir: str = "/tmp/mantle-jobs"
    _writer: ArtifactWriter | None = field(default=None, repr=False, compare=False)

    def _object_key(self, path_suffix: str) -> str:
        return path_suffix.lstrip("/")

    def write_bytes(
        self,
        path_suffix: str,
        content: bytes,
        content_type: str,
    ) -> WrittenArtifact:
        """Write raw bytes under ``jobs/…`` (or another prefix in ``path_suffix``)."""
        if self._writer is None:
            raise RuntimeError("AnalyticsContext has no artifact writer configured")
        key = self._object_key(path_suffix)
        return self._writer.write(self.storage_bucket, key, content, content_type)

    def write_json(
        self, data: dict[str, Any], *, path_suffix: str | None = None
    ) -> WrittenArtifact:
        """Serialize and write a JSON document."""
        suffix = path_suffix or f"jobs/{self.job_id}/result.json"
        content = json.dumps(data, default=str, sort_keys=True).encode("utf-8")
        return self.write_bytes(suffix, content, "application/json")

    def write_geojson(
        self, data: dict[str, Any], *, path_suffix: str | None = None
    ) -> WrittenArtifact:
        """Serialize and write a GeoJSON document."""
        suffix = path_suffix or f"jobs/{self.job_id}/result.geojson"
        content = json.dumps(data).encode("utf-8")
        return self.write_bytes(suffix, content, "application/geo+json")

    def write_cog(self, *_args: Any, **_kwargs: Any) -> WrittenArtifact:
        """Stub for future COG output support."""
        raise NotImplementedError("write_cog is not yet implemented")
