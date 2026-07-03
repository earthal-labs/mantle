"""Plugin registry — built-in and config-allowlisted extensions only."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from mantle_analytics.plugins.base import (
    PersistentRasterProcessingModel,
    VirtualRasterProcessingModel,
)
from mantle_analytics.plugins.parameters import descriptor_from_plugin
from mantle_analytics.security import (
    PluginSecurityError,
    discover_builtin_modules,
    discover_custom_modules,
    plugins_root,
    register_extension_dir,
)

_BUILTIN_VRPM: dict[str, VirtualRasterProcessingModel] = {}
_BUILTIN_PRPM: dict[str, PersistentRasterProcessingModel] = {}
_CUSTOM_VRPM: dict[str, VirtualRasterProcessingModel] = {}
_CUSTOM_PRPM: dict[str, PersistentRasterProcessingModel] = {}
_initialized = False

# Default custom plugin dirs (relative to ``plugins/``).
DEFAULT_CUSTOM_ALLOWLIST = ("custom/vrpm", "custom/prpm")


def _resolve_plugin_dir(entry: str) -> Path:
    path = Path(entry)
    if path.is_absolute():
        return path.resolve()
    return (plugins_root() / entry).resolve()


def _collect_custom_dirs(extra_dirs: list[str] | None) -> list[Path]:
    seen: set[Path] = set()
    ordered: list[Path] = []

    def add(entry: str) -> None:
        resolved = _resolve_plugin_dir(entry)
        if resolved not in seen:
            seen.add(resolved)
            ordered.append(resolved)

    for entry in DEFAULT_CUSTOM_ALLOWLIST:
        add(entry)
    for entry in extra_dirs or []:
        add(entry)

    env_dirs = os.environ.get("MANTLE_PLUGIN_DIRS", "")
    for entry in env_dirs.split(os.pathsep):
        entry = entry.strip()
        if entry:
            add(entry)

    return ordered


def _register_vrpm(
    plugin: VirtualRasterProcessingModel, *, custom: bool
) -> None:
    target = _CUSTOM_VRPM if custom else _BUILTIN_VRPM
    if plugin.id in target:
        raise PluginSecurityError(f"duplicate vRPM plugin id: {plugin.id}")
    target[plugin.id] = plugin


def _register_prpm(
    plugin: PersistentRasterProcessingModel, *, custom: bool
) -> None:
    target = _CUSTOM_PRPM if custom else _BUILTIN_PRPM
    if plugin.id in target:
        raise PluginSecurityError(f"duplicate pRPM plugin id: {plugin.id}")
    target[plugin.id] = plugin


def _load_builtin_plugins() -> None:
    for plugin_id, plugin in discover_builtin_modules().items():
        if isinstance(plugin, VirtualRasterProcessingModel):
            _register_vrpm(plugin, custom=False)
        elif isinstance(plugin, PersistentRasterProcessingModel):
            _register_prpm(plugin, custom=False)


def _load_custom_plugins(directories: list[Path]) -> None:
    for plugin_id, plugin in discover_custom_modules(directories).items():
        if isinstance(plugin, VirtualRasterProcessingModel):
            _register_vrpm(plugin, custom=True)
        elif isinstance(plugin, PersistentRasterProcessingModel):
            _register_prpm(plugin, custom=True)


def _dirs_from_config() -> list[str]:
    try:
        from mantle_analytics.config import load_plugin_allowlist

        return load_plugin_allowlist()
    except (FileNotFoundError, KeyError, OSError):
        return []


def initialize_registry(*, extension_dirs: list[str] | None = None) -> None:
    """Load built-in plugins and allowlisted custom extension directories."""
    global _initialized
    if _initialized:
        return

    merged_dirs = list(extension_dirs or []) + _dirs_from_config()
    custom_dirs = _collect_custom_dirs(merged_dirs)
    for path in custom_dirs:
        register_extension_dir(path)

    _load_builtin_plugins()
    _load_custom_plugins(custom_dirs)
    _initialized = True


def reset_registry() -> None:
    """Clear registry state (for tests)."""
    global _initialized
    _BUILTIN_VRPM.clear()
    _BUILTIN_PRPM.clear()
    _CUSTOM_VRPM.clear()
    _CUSTOM_PRPM.clear()
    _initialized = False


def _ensure_initialized() -> None:
    if not _initialized:
        initialize_registry()


def _lookup_vrpm(plugin_id: str) -> VirtualRasterProcessingModel | None:
    return _CUSTOM_VRPM.get(plugin_id) or _BUILTIN_VRPM.get(plugin_id)


def _lookup_prpm(plugin_id: str) -> PersistentRasterProcessingModel | None:
    return _CUSTOM_PRPM.get(plugin_id) or _BUILTIN_PRPM.get(plugin_id)


def get_vrpm_model(plugin_id: str) -> VirtualRasterProcessingModel:
    """Return a registered Virtual Raster Processing Model by id."""
    _ensure_initialized()
    plugin = _lookup_vrpm(plugin_id)
    if plugin is None:
        raise KeyError(f"unknown vRPM plugin id: {plugin_id}")
    return plugin


def list_vrpm_models() -> list[dict[str, Any]]:
    """List metadata for all registered vRPM plugins."""
    _ensure_initialized()
    plugins = {**_BUILTIN_VRPM, **_CUSTOM_VRPM}
    return [p.metadata() for p in plugins.values()]


def get_prpm_model(plugin_id: str) -> PersistentRasterProcessingModel:
    """Return a registered Persistent Raster Processing Model by id."""
    _ensure_initialized()
    job = _lookup_prpm(plugin_id)
    if job is None:
        raise KeyError(f"unknown pRPM plugin id: {plugin_id}")
    return job


def list_prpm_models() -> list[dict[str, Any]]:
    """List metadata for all registered pRPM plugins."""
    _ensure_initialized()
    jobs = {**_BUILTIN_PRPM, **_CUSTOM_PRPM}
    return [j.metadata() for j in jobs.values()]


def list_plugin_descriptors() -> list[dict[str, Any]]:
    """List full plugin descriptors (id, model_kind, parameters) for REST APIs."""
    _ensure_initialized()
    descriptors: list[dict[str, Any]] = []
    for plugin in {**_BUILTIN_VRPM, **_CUSTOM_VRPM}.values():
        descriptors.append(descriptor_from_plugin(plugin, model_kind="vrpm"))
    for plugin in {**_BUILTIN_PRPM, **_CUSTOM_PRPM}.values():
        descriptors.append(descriptor_from_plugin(plugin, model_kind="prpm"))
    return sorted(descriptors, key=lambda entry: entry["id"])


def get_plugin_descriptor(plugin_id: str) -> dict[str, Any]:
    """Return a single plugin descriptor by id."""
    _ensure_initialized()
    plugin = _lookup_vrpm(plugin_id)
    if plugin is not None:
        return descriptor_from_plugin(plugin, model_kind="vrpm")
    job = _lookup_prpm(plugin_id)
    if job is not None:
        return descriptor_from_plugin(job, model_kind="prpm")
    raise KeyError(f"unknown plugin id: {plugin_id}")
