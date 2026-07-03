"""Built-in zonal statistics Persistent Raster Processing Model (pRPM)."""

from __future__ import annotations

from typing import Any

import numpy as np

from mantle_analytics.raster_read import read_band_samples
from mantle_analytics.plugins.base import (
    AnalyticsContext,
    JobInputs,
    JobResult,
    PersistentRasterProcessingModel,
)
from mantle_analytics.plugins.parameters import ParamDirection, ParamType, ParameterSpec


class ZonalStatsJob(PersistentRasterProcessingModel):
    """Aggregate raster values over a zone; returns JSON statistics."""

    id = "zonal_stats"
    version = "1.0.0"

    def parameters(self) -> list[ParameterSpec]:
        return [
            ParameterSpec(
                name="values",
                param_type=ParamType.NUMBER_LIST,
                description="Pre-extracted numeric samples to aggregate (beta fast path)",
                direction=ParamDirection.INPUT,
                required=False,
            ),
            ParameterSpec(
                name="band",
                param_type=ParamType.BAND,
                description="Band index when raster zonal stats are computed from dataset_refs",
                direction=ParamDirection.INPUT,
                required=False,
                default=1,
            ),
            ParameterSpec(
                name="geometry",
                param_type=ParamType.STRING,
                description="Optional GeoJSON or WKT geometry for the zone",
                direction=ParamDirection.INPUT,
                required=False,
            ),
            ParameterSpec(
                name="statistics",
                param_type=ParamType.OUTPUT_JSON,
                direction=ParamDirection.OUTPUT,
                description="Aggregated zonal statistics result file",
                required=True,
                filename_template="zonal_stats.json",
                subpath="jobs",
            ),
        ]

    def validate_inputs(self, inputs: JobInputs) -> None:
        super().validate_inputs(inputs)
        values = inputs.params.get("values")
        has_values = isinstance(values, list) and bool(values)
        has_datasets = bool(inputs.dataset_refs)
        if not has_values and not has_datasets:
            raise ValueError(
                "zonal_stats requires params.values or dataset_refs with band"
            )
        if has_datasets and not has_values:
            band = inputs.params.get("band", 1)
            if not isinstance(band, int) or band < 1:
                raise ValueError(
                    "band must be a positive integer when using dataset_refs"
                )

    def run(self, inputs: JobInputs, ctx: AnalyticsContext) -> JobResult:
        values = inputs.params.get("values")
        read_meta: dict[str, Any] | None = None
        if values is None and inputs.dataset_refs:
            band = int(inputs.params.get("band", 1))
            numeric, read_meta = read_band_samples(inputs.dataset_refs[0], band)
        else:
            numeric = np.asarray(values, dtype=np.float64)
        count = int(numeric.size)
        total = float(numeric.sum())
        mean = float(total / count) if count else 0.0
        data: dict[str, Any] = {
            "count": count,
            "sum": total,
            "mean": mean,
            "min": float(numeric.min()) if count else None,
            "max": float(numeric.max()) if count else None,
            "geometry": inputs.params.get("geometry"),
            "dataset_refs": [
                ref.get("id")
                for ref in inputs.dataset_refs
                if ref.get("id") is not None
            ],
        }
        if read_meta is not None:
            data["raster_read"] = read_meta
        return JobResult(data=data, output_name="statistics")


PLUGIN = ZonalStatsJob()
