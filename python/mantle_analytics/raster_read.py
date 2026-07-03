"""Beta raster band reads for pRPM jobs (COG / Icechunk)."""

from __future__ import annotations

from typing import Any

import numpy as np

_DEFAULT_MAX_SAMPLES = 4096


def read_band_samples(
    dataset_ref: dict[str, Any],
    band: int,
    *,
    max_samples: int = _DEFAULT_MAX_SAMPLES,
) -> tuple[np.ndarray, dict[str, Any]]:
    """Read numeric samples from one band of a catalog dataset ref.

  Returns ``(samples, meta)``. Uses ``rasterio`` when installed; otherwise
  returns a deterministic stub so beta jobs can exercise the ``dataset_refs``
  path without GDAL in the worker image.
    """
    storage_uri = str(dataset_ref.get("storage_uri", ""))
    fmt = str(dataset_ref.get("format", "cog")).lower()
    meta: dict[str, Any] = {
        "storage_uri": storage_uri,
        "format": fmt,
        "band": band,
        "source": "stub",
    }

    if not storage_uri:
        raise ValueError("dataset_ref missing storage_uri")

    try:
        import rasterio  # type: ignore[import-untyped]
    except ImportError:
        seed = abs(hash((storage_uri, band))) % 10_000
        rng = np.random.default_rng(seed)
        samples = rng.uniform(0.0, 1.0, size=min(64, max_samples)).astype(np.float64)
        meta["source"] = "stub"
        meta["message"] = (
            "rasterio not installed; install optional rasterio extra for real reads"
        )
        return samples, meta

    with rasterio.open(storage_uri) as src:
        if band < 1 or band > src.count:
            raise ValueError(f"band {band} out of range for {src.count} bands")
        data = src.read(band)
        flat = np.asarray(data, dtype=np.float64).ravel()
        nodata = src.nodata
        if nodata is not None:
            flat = flat[flat != nodata]
        if flat.size > max_samples:
            step = max(1, flat.size // max_samples)
            flat = flat[::step][:max_samples]
        meta["source"] = "rasterio"
        meta["shape"] = list(data.shape)
        meta["crs"] = str(src.crs) if src.crs else None
        return flat, meta
