"""Icechunk read helpers for multidimensional cube_slice jobs."""

from __future__ import annotations

from typing import Any


def read_cube_slice(
    storage_uri: str,
    *,
    variable: str,
    indices: dict[str, int | slice] | None = None,
) -> dict[str, Any]:
    """Read a slice from an Icechunk-backed Zarr store.

    Uses ``icechunk`` when installed; otherwise returns a stub payload for dev/tests.
    """
    indices = indices or {}

    try:
        import icechunk as ic  # type: ignore[import-untyped]

        repo = ic.Repository.open(storage_uri)
        session = repo.readonly_session("main")
        store = session.store
        import zarr  # type: ignore[import-untyped]

        root = zarr.open_group(store=store, mode="r")
        if variable not in root:
            raise KeyError(f"variable {variable!r} not in store")

        arr = root[variable]
        selector = indices or {"time": 0}
        data = arr[tuple(selector.get(dim, 0) for dim in arr.dims)]
        return {
            "variable": variable,
            "shape": list(data.shape),
            "values": data.tolist() if data.size <= 256 else "truncated",
            "storage_uri": storage_uri,
        }
    except ImportError:
        return {
            "variable": variable,
            "storage_uri": storage_uri,
            "indices": {k: str(v) for k, v in indices.items()},
            "stub": True,
            "message": "icechunk not installed; returning placeholder slice metadata",
        }
