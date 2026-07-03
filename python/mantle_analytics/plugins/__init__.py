"""Mantle analytics plugin package."""

from mantle_analytics.plugins.base import (
    JobInputs,
    JobResult,
    OutputSpec,
    PersistentRasterProcessingModel,
    TileMeta,
    VirtualRasterProcessingModel,
)
from mantle_analytics.plugins.context import AnalyticsContext, WrittenArtifact

__all__ = [
    "AnalyticsContext",
    "JobInputs",
    "JobResult",
    "OutputSpec",
    "PersistentRasterProcessingModel",
    "TileMeta",
    "VirtualRasterProcessingModel",
    "WrittenArtifact",
]
