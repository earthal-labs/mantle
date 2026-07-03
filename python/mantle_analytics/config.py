"""Load Mantle config.toml for analytics worker."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python <3.11
    import tomli as tomllib  # type: ignore[no-redef]


@dataclass(frozen=True)
class AnalyticsSettings:
    redis_url: str
    stream_key: str
    ray_address: str
    storage_bucket: str
    plugin_allowlist: tuple[str, ...]


def load_plugin_allowlist(config_path: str | Path | None = None) -> list[str]:
    """Return ``[analytics].plugin_allowlist`` entries from config.toml."""
    path = Path(config_path or os.environ.get("MANTLE_CONFIG", "config.toml"))
    with path.open("rb") as fh:
        raw = tomllib.load(fh)
    entries = raw.get("analytics", {}).get("plugin_allowlist", [])
    return [str(entry) for entry in entries]


def load_settings(config_path: str | Path | None = None) -> AnalyticsSettings:
    path = Path(config_path or os.environ.get("MANTLE_CONFIG", "config.toml"))
    with path.open("rb") as fh:
        raw = tomllib.load(fh)

    analytics = raw["analytics"]
    storage = raw["storage"]
    cache = raw["cache"]

    return AnalyticsSettings(
        redis_url=os.environ.get("REDIS_URL", cache["redis_url"]),
        stream_key=os.environ.get("MANTLE_JOBS_STREAM", analytics["stream_key"]),
        ray_address=os.environ.get("RAY_ADDRESS", analytics["ray_address"]),
        storage_bucket=storage["bucket"],
        plugin_allowlist=tuple(load_plugin_allowlist(path)),
    )
