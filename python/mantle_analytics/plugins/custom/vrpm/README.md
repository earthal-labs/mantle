# Custom vRPM plugins

Add Virtual Raster Processing Models here for tile-time pixel math on existing catalog services.

**Do not edit `plugins/builtin/`** — those modules are read-only and ship with Mantle. Copy [`minimal_vrpm.py`](minimal_vrpm.py) as a starting point.

## Quick start

1. Copy `minimal_vrpm.py` to `my_index.py`.
2. Subclass `VirtualRasterProcessingModel`, set a unique `id`, and export `PLUGIN = MyIndex()`.
3. Restart the vRPM sidecar / analytics worker so `initialize_registry()` reloads plugins.

This directory is loaded by default. Additional paths can be registered via `config.toml` or `MANTLE_PLUGIN_DIRS` (see [plugin security](../../../../docs/plugin-security.md)).

## Security

Each `.py` file is AST-scanned at load time. Only allowlisted imports are permitted — see [Plugin security model](../../../../docs/plugin-security.md).

## Related docs

- [Virtual Raster Processing Models (vRPM)](../../../../docs/vrpm.md)
- [Built-in plugins (read-only)](../builtin/README.md)
