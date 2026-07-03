"""Minimal vRPM — multiply one band by a scalar.

Copy this file as a starting point for tile-time pixel math. Every vRPM plugin
must subclass ``VirtualRasterProcessingModel`` and expose ``PLUGIN = ...`` at module level.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from mantle_analytics.plugins.base import TileMeta, VirtualRasterProcessingModel
from mantle_analytics.plugins.parameters import ParamType, ParameterSpec


class ScaleBandvRPM(VirtualRasterProcessingModel):
    """Multiply a single source band by ``scale`` (default 1.0)."""
    
    id = "minimal_vrpm"
    version = "1.0.0"
    required_bands = ["source"]

    def parameters(self) -> list[ParameterSpec]:
        return [
            ParameterSpec(
                name="source_band",
                param_type=ParamType.BAND,
                description="Source band index (1-based)",
                required=False,
                default=1,
            ),
            ParameterSpec(
                name="scale",
                param_type=ParamType.NUMBER,
                description="Linear scale factor applied to source pixels",
                required=False,
                default=1.0,
                minimum=0.0,
            ),
        ]

    def compute_tile(
        self,
        bands: dict[str, np.ndarray],
        params: dict[str, Any],
        tile_meta: TileMeta,
    ) -> np.ndarray:
        _ = tile_meta
        scale = float(params.get("scale", 1.0))
        source = bands["source"].astype(np.float32, copy=False)
        return (source * scale).astype(np.float32)


PLUGIN = ScaleBandvRPM()
