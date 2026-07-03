# Built-in plugins (read-only)

Modules under `plugins/builtin/` ship with the Mantle distribution and load automatically at startup.

**Do not edit these files in production deployments** — upgrades overwrite the package. For site-specific algorithms, add plugins under [`../custom/`](../custom/) instead.

## Layout

| Path | model_kind | Plugin ID |
|------|------------|-----------|
| `builtin/vrpm/ndvi.py` | vRPM | `ndvi` |
| `builtin/prpm/zonal_stats.py` | pRPM | `zonal_stats` |

Built-in modules are discovered by scanning `builtin/vrpm/*.py` and `builtin/prpm/*.py`. Each file must export `PLUGIN = <instance>`.

## Related docs

- [Custom plugins](../custom/vrpm/README.md)
- [Virtual Raster Processing Models (vRPM)](../../../../docs/vrpm.md)
- [Persistent Raster Processing Models (pRPM)](../../../../docs/prpm.md)
