"""VirtualiZarr → Icechunk virtual reference stub for Pathway B ingestion.

Beta status: see docs/KNOWN_LIMITATIONS.md (Virtual Zarr / external object rate limits)
and docs/operations.md (DuckDB / ingestion prerequisites).
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Any

BETA_DOC = "docs/KNOWN_LIMITATIONS.md#virtual-zarr--external-object-rate-limits"


@dataclass
class VirtualizeResult:
    storage_uri: str
    format: str = "icechunk"
    beta_stub: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "storage_uri": self.storage_uri,
            "format": self.format,
            "beta_stub": self.beta_stub,
            "todo": (
                "Implement icechunk + virtualizarr materialization; "
                f"see {BETA_DOC}"
            ),
        }


def virtualize_reference(
    source_uri: str, target_uri: str, name: str
) -> VirtualizeResult:
    """Create Icechunk virtual refs for a remote NetCDF/HDF5 dataset (beta stub).

    TODO(beta): wire ``icechunk`` + ``virtualizarr`` to materialize zero-copy
    virtual Zarr references in Mantle's S3 bucket. Until then, ingestion records
    the target URI without rewriting upstream bytes.

    See docs/KNOWN_LIMITATIONS.md (Virtual Zarr section).
    """
    try:
        import icechunk  # noqa: F401
        import virtualizarr  # noqa: F401
    except ImportError:
        pass

    _ = (name, source_uri)
    return VirtualizeResult(storage_uri=target_uri, beta_stub=True)


def main() -> int:
    payload_raw = os.environ.get("VIRTUALIZE_JSON")
    if not payload_raw and len(sys.argv) > 1:
        payload_raw = sys.argv[1]

    if not payload_raw:
        print(
            json.dumps({"error": "missing VIRTUALIZE_JSON env or argv payload"}),
            file=sys.stderr,
        )
        return 1

    payload: dict[str, Any] = json.loads(payload_raw)
    result = virtualize_reference(
        source_uri=str(payload["source_uri"]),
        target_uri=str(payload["target_uri"]),
        name=str(payload.get("name", "")),
    )
    print(json.dumps(result.to_dict()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
