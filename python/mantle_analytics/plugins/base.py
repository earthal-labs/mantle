"""Base classes for Mantle Raster Processing Models (RPM)."""

from __future__ import annotations

from abc import ABC, abstractmethod

from dataclasses import dataclass, field

from typing import Any, Literal

import numpy as np

from mantle_analytics.plugins.context import AnalyticsContext

from mantle_analytics.plugins.parameters import (
    ParamType,
    ParameterSpec,
    input_parameters,
    output_parameters,
    parameters_to_json,
    validate_params_against_specs,
)

__all__ = [
    "AnalyticsContext",
    "JobInputs",
    "JobResult",
    "OutputSpec",
    "PersistentRasterProcessingModel",
    "TileMeta",
    "VirtualRasterProcessingModel",
]


@dataclass(frozen=True)
class TileMeta:
    """Tile coordinate and pixel dimensions for Virtual Raster Processing Models (vRPM)."""

    z: int
    x: int
    y: int
    width: int
    height: int
    crs: str = "EPSG:3857"


@dataclass
class JobInputs:
    """Validated inputs for a Persistent Raster Processing Model (pRPM) job."""

    params: dict[str, Any]
    dataset_refs: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class OutputSpec:
    """Framework-resolved output location derived from output parameters."""

    kind: Literal["json", "geojson", "cog", "zarr", "text"]
    filename_template: str = "{job_id}.json"
    subpath: str = "jobs"


@dataclass
class JobResult:
    """Structured result from a pRPM job (business data only)."""

    data: Any
    output_kind: Literal["cog", "zarr", "json", "geojson", "text"] | None = None
    output_name: str | None = None
    dataset_name: str | None = None


class VirtualRasterProcessingModel(ABC):
    """Per-tile pixel math on existing datasets without persisting outputs (vRPM)."""

    id: str
    version: str
    required_bands: list[str]

    @abstractmethod
    def parameters(self) -> list[ParameterSpec]:
        """Declare typed input parameters exposed via REST attach/tile APIs."""

    def validate_params(self, params: dict[str, Any]) -> None:
        """Validate params against ``parameters()`` input specs."""

        validate_params_against_specs(self.parameters(), params)

    @abstractmethod
    def compute_tile(
        self,
        bands: dict[str, np.ndarray],
        params: dict[str, Any],
        tile_meta: TileMeta,
    ) -> np.ndarray:
        """Return a single-band float32 tile (H, W) from named source bands."""

    def metadata(self) -> dict[str, Any]:
        specs = self.parameters()
        return {
            "id": self.id,
            "version": self.version,
            "required_bands": list(self.required_bands),
            "model_kind": "vrpm",
            "inputs": parameters_to_json(input_parameters(specs)),
            "outputs": parameters_to_json(output_parameters(specs)),
        }


class PersistentRasterProcessingModel(ABC):
    """Async jobs that persist artifacts or structured results to storage (pRPM)."""

    id: str
    version: str = "1.0.0"

    @abstractmethod
    def parameters(self) -> list[ParameterSpec]:
        """Declare typed job inputs and outputs exposed via OGC Processes APIs."""

    def validate_inputs(self, inputs: JobInputs) -> None:
        """Validate job inputs against input ``parameters()`` specs."""

        specs = self.parameters()
        validate_params_against_specs(specs, inputs.params)
        dataset_spec = next(
            (
                spec
                for spec in input_parameters(specs)
                if spec.param_type == ParamType.DATASET
            ),
            None,
        )
        if dataset_spec and dataset_spec.required and not inputs.dataset_refs:
            raise ValueError(f"{dataset_spec.name} requires dataset_refs")

    @abstractmethod
    def run(self, inputs: JobInputs, ctx: AnalyticsContext) -> JobResult:
        """Execute the pRPM job and return business data (no storage paths)."""

    def metadata(self) -> dict[str, Any]:
        specs = self.parameters()
        return {
            "id": self.id,
            "version": self.version,
            "model_kind": "prpm",
            "inputs": parameters_to_json(input_parameters(specs)),
            "outputs": parameters_to_json(output_parameters(specs)),
        }
