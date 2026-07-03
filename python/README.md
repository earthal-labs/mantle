# mantle-analytics

Ray worker and vRPM sidecar for Mantle async analytics.

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (`curl -LsSf https://astral.sh/uv/install.sh | sh`)

## Development setup

```bash
cd python

# Install project + dev extras into .venv
uv sync --extra dev

# Run unit tests
uv run pytest

# Start Redis job consumer (requires Ray + Redis from compose)
uv sync --extra analytics
uv run python -m mantle_analytics.worker

# vRPM tile compute HTTP sidecar
uv run python -m mantle_analytics.vrpm_server
```

Optional extras:

| Extra | Command | Purpose |
|-------|---------|---------|
| `dev` | `uv sync --extra dev` | pytest, ruff |
| `ray` | `uv sync --extra ray` | Ray client only |
| `analytics` | `uv sync --extra analytics` | Full worker image deps (Ray + boto3) |

Lockfile updates:

```bash
uv lock
```

## Lint (optional)

```bash
uv run ruff check mantle_analytics tests
```
