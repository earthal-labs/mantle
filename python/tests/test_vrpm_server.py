"""vRPM sidecar compute tests."""

from __future__ import annotations

import base64

import numpy as np

from mantle_analytics.registry import (
    get_plugin_descriptor,
    initialize_registry,
    list_plugin_descriptors,
    reset_registry,
)
from mantle_analytics.vrpm_server import compute_tile_payload


def _b64_f32(arr: np.ndarray) -> str:
    return base64.b64encode(arr.astype(np.float32).tobytes()).decode("ascii")


def test_compute_tile_payload_ndvi() -> None:
    reset_registry()
    initialize_registry()
    width, height = 2, 2
    red = np.full((height, width), 0.1, dtype=np.float32)
    nir = np.full((height, width), 0.8, dtype=np.float32)
    body = {
        "function_id": "ndvi",
        "params": {},
        "tile_meta": {"z": 1, "x": 0, "y": 0, "width": width, "height": height},
        "bands": {
            "red": {"data": _b64_f32(red)},
            "nir": {"data": _b64_f32(nir)},
        },
    }
    result = compute_tile_payload(body)
    assert result["width"] == width
    assert result["height"] == height
    raw = base64.b64decode(result["data"])
    values = np.frombuffer(raw, dtype=np.float32)
    assert values.shape == (width * height,)
    assert values[0] > 0.5


def test_plugin_descriptors_available() -> None:
    reset_registry()
    initialize_registry()
    descriptors = list_plugin_descriptors()
    assert any(item["id"] == "ndvi" for item in descriptors)
    ndvi = get_plugin_descriptor("ndvi")
    assert ndvi["model_kind"] == "vrpm"
    assert ndvi["inputs"][0]["param_type"] == "band"