"""Built-in NDVI Virtual Raster Processing Model (vRPM)."""

from __future__ import annotations

from typing import Any

import numpy as np

from mantle_analytics.plugins.base import TileMeta, VirtualRasterProcessingModel
from mantle_analytics.plugins.parameters import ParamType, ParameterSpec


class NDVI(VirtualRasterProcessingModel):
    """Compute NDVI per tile from red and NIR source bands."""

    id = "ndvi"
    version = "1.0.0"
    required_bands = ["red", "nir"]

    def parameters(self) -> list[ParameterSpec]:
        return [
            ParameterSpec(
                name="red_band",
                param_type=ParamType.BAND,
                description="Red band index (1-based) for the parent service",
                required=False,
                default=1,
                role="red",
            ),
            ParameterSpec(
                name="nir_band",
                param_type=ParamType.BAND,
                description="NIR band index (1-based) for the parent service",
                required=False,
                default=2,
                role="nir",
            ),
        ]

    def compute_tile(
        self,
        bands: dict[str, np.ndarray],
        params: dict[str, Any],
        tile_meta: TileMeta,
    ) -> np.ndarray:
        _ = params, tile_meta
        red = bands["red"].astype(np.float32, copy=False)
        nir = bands["nir"].astype(np.float32, copy=False)
        denom = nir + red
        with np.errstate(divide="ignore", invalid="ignore"):
            ndvi = np.where(denom != 0, (nir - red) / denom, np.nan)
        return ndvi.astype(np.float32)


PLUGIN = NDVI()
