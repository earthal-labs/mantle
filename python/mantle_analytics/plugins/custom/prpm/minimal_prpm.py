"""Minimal pRPM — return JSON with mean of supplied values.

Copy this file as a starting point for Ray-backed jobs. Every pRPM plugin
must subclass ``PersistentRasterProcessingModel`` and expose ``PLUGIN = ...`` at module level.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from mantle_analytics.plugins.base import (
    AnalyticsContext,
    JobInputs,
    JobResult,
    PersistentRasterProcessingModel,
)
from mantle_analytics.plugins.parameters import ParamDirection, ParamType, ParameterSpec


class MeanValuespRPM(PersistentRasterProcessingModel):
    """Compute mean of ``params.values`` and return a JSON ``JobResult``."""

    id = "minimal_prpm"
    version = "1.0.0"

    def parameters(self) -> list[ParameterSpec]:
        return [
            ParameterSpec(
                name="values",
                param_type=ParamType.NUMBER_LIST,
                description="Numeric samples to average",
                direction=ParamDirection.INPUT,
                required=True,
            ),
            ParameterSpec(
                name="summary",
                param_type=ParamType.OUTPUT_JSON,
                direction=ParamDirection.OUTPUT,
                description="Mean and count summary JSON file",
                required=True,
                filename_template="mean.json",
                subpath="jobs",
            ),
        ]

    def run(self, inputs: JobInputs, ctx: AnalyticsContext) -> JobResult:
        _ = ctx
        numeric = np.asarray(inputs.params["values"], dtype=np.float64)
        mean = float(numeric.mean())
        data: dict[str, Any] = {
            "count": int(numeric.size),
            "mean": mean,
        }
        return JobResult(data=data, output_name="summary")


PLUGIN = MeanValuespRPM()
