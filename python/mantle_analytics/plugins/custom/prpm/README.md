# Custom pRPM plugins

Add Persistent Raster Processing Models here for Ray-backed async jobs with saved outputs (JSON, COG, Zarr, GeoJSON).

**Do not edit `plugins/builtin/`** — those modules are read-only and ship with Mantle. Copy [`minimal_prpm.py`](minimal_prpm.py) as a starting point.

## Quick start

1. Copy `minimal_prpm.py` to `my_job.py`.
2. Subclass `PersistentRasterProcessingModel`, set a unique `id`, declare `parameters()` (inputs + `OUTPUT_*` outputs), and export `PLUGIN = MyJob()`.
3. Restart the analytics worker.

This directory is loaded by default. Additional paths can be registered via `config.toml` or `MANTLE_PLUGIN_DIRS`.

## Business logic only

`run()` returns `JobResult(data={...})` — the framework writes artifacts and sets `result_url`. Do not construct `s3://` URIs or import object-store clients in plugins.

## Related docs

- [Persistent Raster Processing Models (pRPM)](../../../../docs/prpm.md)
- [Built-in plugins (read-only)](../builtin/README.md)
- [Plugin security model](../../../../docs/plugin-security.md)
