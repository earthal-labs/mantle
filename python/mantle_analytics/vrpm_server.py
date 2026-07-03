"""HTTP sidecar for vRPM tile computation (isolated from Rust API)."""

from __future__ import annotations

import base64
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import urlparse

import numpy as np

from mantle_analytics.plugins.base import TileMeta
from mantle_analytics.registry import (
    get_plugin_descriptor,
    get_vrpm_model,
    initialize_registry,
    list_plugin_descriptors,
    list_vrpm_models,
)


def _f32_to_b64(values: list[float]) -> str:
    arr = np.asarray(values, dtype=np.float32)
    return base64.b64encode(arr.tobytes()).decode("ascii")


def _b64_to_f32(data: str, width: int, height: int) -> np.ndarray:
    raw = base64.b64decode(data)
    expected = width * height * 4
    if len(raw) != expected:
        raise ValueError(f"band byte length {len(raw)} != expected {expected}")
    return np.frombuffer(raw, dtype=np.float32).reshape((height, width))


def compute_tile_payload(body: dict[str, Any]) -> dict[str, Any]:
    """Run a vRPM plugin and return encoded tile bytes."""
    function_id = str(body["function_id"])
    params = dict(body.get("params") or {})
    tile_meta_raw = body.get("tile_meta") or {}
    bands_raw = body.get("bands") or {}

    plugin = get_vrpm_model(function_id)
    plugin.validate_params(params)

    width = int(tile_meta_raw.get("width", 256))
    height = int(tile_meta_raw.get("height", 256))
    tile_meta = TileMeta(
        z=int(tile_meta_raw.get("z", 0)),
        x=int(tile_meta_raw.get("x", 0)),
        y=int(tile_meta_raw.get("y", 0)),
        width=width,
        height=height,
        crs=str(tile_meta_raw.get("crs", "EPSG:3857")),
    )

    bands: dict[str, np.ndarray] = {}
    for name, payload in bands_raw.items():
        bands[name] = _b64_to_f32(str(payload["data"]), width, height)

    for required in plugin.required_bands:
        if required not in bands:
            raise ValueError(f"missing required band: {required}")

    result = plugin.compute_tile(bands, params, tile_meta)
    if result.shape != (height, width):
        raise ValueError(
            f"plugin returned shape {result.shape}, expected ({height}, {width})"
        )

    flat = result.astype(np.float32, copy=False).ravel().tolist()
    return {
        "width": width,
        "height": height,
        "dtype": "float32",
        "data": _f32_to_b64(flat),
    }


class VrpmRequestHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler for vRPM tile compute requests."""

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        sys.stderr.write("%s - %s\n" % (self.address_string(), format % args))

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/health":
            self._send_json(200, {"status": "ok"})
            return
        if path == "/vrpm/models":
            self._send_json(200, {"models": list_vrpm_models()})
            return
        if path == "/plugins":
            self._send_json(200, {"plugins": list_plugin_descriptors()})
            return
        if path.startswith("/plugins/"):
            plugin_id = path.removeprefix("/plugins/").strip("/")
            if not plugin_id:
                self._send_json(404, {"error": "not found"})
                return
            try:
                descriptor = get_plugin_descriptor(plugin_id)
            except KeyError:
                self._send_json(404, {"error": f"unknown plugin id: {plugin_id}"})
                return
            self._send_json(200, descriptor)
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path != "/vrpm/compute-tile":
            self._send_json(404, {"error": "not found"})
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw)
            result = compute_tile_payload(body)
            self._send_json(200, result)
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            self._send_json(400, {"error": str(exc)})
        except Exception as exc:  # noqa: BLE001
            self._send_json(500, {"error": str(exc)})


def run_server(
    *,
    host: str = "127.0.0.1",
    port: int = 8090,
    extension_dirs: list[str] | None = None,
) -> None:
    initialize_registry(extension_dirs=extension_dirs)
    server = HTTPServer((host, port), VrpmRequestHandler)
    print(f"mantle vRPM compute sidecar listening on http://{host}:{port}", flush=True)
    server.serve_forever()


def main() -> None:
    host = os.environ.get("MANTLE_VRPM_BIND", "127.0.0.1")
    port = int(os.environ.get("MANTLE_VRPM_PORT", "8090"))
    run_server(host=host, port=port)


if __name__ == "__main__":
    main()
