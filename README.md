# Mantle

Cloud-native raster engine with a Rust/Axum API, DuckLake+Postgres catalog, Redis caching, Python/Ray analytics, and full OGC/STAC APIs.

## Architecture

See [docs/architecture.md](docs/architecture.md) for the system diagram and request paths.

| Component | Crate / package | Role |
|-----------|-----------------|------|
| API | `mantle-api` | Axum HTTP server, admin ingestion, job status |
| Worker | `mantle-worker` | Background cache warmer (COG IFD / Icechunk metadata) |
| Catalog | `mantle-catalog` | DuckLake + Postgres spatial queries (append-only) |
| Raster | `mantle-raster` | oxigdal COG byte-range tiles, mosaic, WebP/PNG encode |
| Render AST | `mantle-render-ast` | JSON render rules → SIMD or Ray execution |
| Analytics | `python/mantle_analytics` | Ray remote tasks via Redis Streams |

Domain ownership and integration contracts: [AGENTS.md](AGENTS.md).

## Quickstart

```bash
# 1. Rust toolchain
cargo check

# 2. Local secrets
cp .env.example .env

# 3. Start stack
./scripts/dev-up.sh   # or: .\scripts\dev-up.ps1 on Windows
# manual: docker compose config && docker compose up -d

# 4. Health check
curl http://localhost:8080/health

# 5. Attach a vRPM and fetch a tile (requires a COG dataset_id)
curl -X POST "http://localhost:8080/admin/services/{dataset_id}/attach" \
  -H "Authorization: Bearer ${MANTLE_ADMIN_TOKEN:-dev-admin-token}" \
  -H "Content-Type: application/json" \
  -d '{"function_id":"ndvi","params_defaults":{"red_band":1,"nir_band":2},"endpoint_slug":"sentinel-ndvi"}'
curl -o tile.webp "http://localhost:8080/services/sentinel-ndvi/tiles/10/512/512?format=webp"

# 6. Offline contract tests
cargo test -p mantle-integration-tests --test contracts

# 7. Python analytics (optional)
cd python && uv sync --extra dev && uv run pytest
```

Set `MANTLE_CONFIG` to override the config file path (default: `config.toml`).

## Deployment

| Target | Guide |
|--------|-------|
| Single EC2 — **prebuilt GHCR images** (recommended) | [docs/deploy-ec2.md](docs/deploy-ec2.md#deploy-without-building-on-ec2-recommended) |
| Single EC2 — build on instance | [docs/deploy-ec2.md](docs/deploy-ec2.md#build-on-ec2-legacy) |
| AWS EKS (Helm, Aurora, IRSA) | [docs/operations.md](docs/operations.md) |

### Publish images to GHCR

**GitHub Actions:** push to `main` runs [`.github/workflows/docker-publish.yml`](.github/workflows/docker-publish.yml).

**Local (Windows):**

```powershell
docker login ghcr.io -u YOUR_GITHUB_USER
$env:GHCR_IMAGE_PREFIX = "ghcr.io/youruser"
.\scripts\build-push-images.ps1 -Tag latest
```

**EC2 pull** (no compile on server):

```bash
export GHCR_IMAGE_PREFIX=ghcr.io/youruser
export MANTLE_IMAGE_TAG=latest
docker compose -f docker-compose.yml -f docker-compose.ghcr.yml pull
docker compose -f docker-compose.yml -f docker-compose.ghcr.yml up -d
```

## API routes

All routes are served by `mantle-api` unless noted. Admin routes require `Authorization: Bearer $MANTLE_ADMIN_TOKEN`.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness / readiness |
| GET | `/console` | Barebones dev console — STAC search, dataset admin, native Leaflet tile viewer, plugin listing, job submit/poll |
| GET | `/status/{job_id}` | Async job polling (`queued` → `running` → `succeeded` / `failed`) |
| GET | `/tiles/{z}/{x}/{y}` | Legacy tile shortcut (`?dataset_id`, `?band`, `?format`, `?render`) |
| POST | `/admin/datasets/upload` | Multipart COG upload (field `file`) |
| POST | `/admin/datasets/reference` | Register external S3/HTTPS URI (Virtual Zarr → Icechunk for NetCDF) |
| GET | `/stac/` | STAC 1.0 landing page |
| GET | `/stac/collections` | List collections |
| GET | `/stac/collections/{id}` | Collection metadata |
| GET | `/stac/collections/{id}/items` | Items in collection |
| GET, POST | `/stac/search` | STAC search (bbox, datetime, CQL filters) |
| GET | `/ogc/tiles/{tms}/{z}/{y}/{x}` | OGC API – Tiles (`image/webp` default) |
| GET | `/ogc/maps/{collection_id}` | Map collection metadata |
| GET | `/ogc/maps/{collection_id}/plan` | Render AST execution plan (`?render=`) |
| GET | `/ogc/maps/{collection_id}/tiles/{tms}/{z}/{y}/{x}` | Map tile (AST pipeline) |
| GET | `/ogc/edr/collections/{id}/position` | EDR point query (CoverageJSON; `?async=true` → 202) |
| GET | `/ogc/processes` | List processes (`ndvi`, `zonal-stats`, …) |
| POST | `/ogc/processes/{id}/execution` | Submit async process → **202** + `job_id` |

## Development

### Rust workspace

```bash
cargo check --workspace
cargo build -p mantle-api
cargo run -p mantle-api
cargo run -p mantle-worker
cargo test --workspace --exclude mantle-integration-tests
```

### Python analytics

Requires [uv](https://docs.astral.sh/uv/). See [python/README.md](python/README.md).

```bash
cd python
uv sync --extra dev          # unit tests
uv sync --extra analytics    # worker + Ray deps
uv run python -m mantle_analytics.worker
uv run python -m mantle_analytics.vrpm_server
```

### Local stack (Docker Compose)

| Service | Port(s) | Description |
|---------|---------|-------------|
| `api` | 8080 | Rust `mantle-api` |
| `worker` | — | Cache warmer |
| `postgres` | 5432 | PostGIS + `migrations/` init |
| `redis` | 6379 | Cache + Redis Streams |
| `minio` | 9000, 9001 | S3-compatible storage |
| `ray-head` | 8265, 10001 | Ray dashboard + client |
| `ray-worker` | — | Ray worker |
| `analytics-worker` | — | Python Redis consumer |
| `vrpm-sidecar` | 8090 (internal) | vRPM tile compute HTTP sidecar |

```bash
docker build -f Dockerfile.api -t mantle-api:local .
docker build -f Dockerfile.worker -t mantle-worker:local .
docker build -f Dockerfile.analytics -t mantle-analytics:local .
```

### Kubernetes (Helm)

For EKS and multi-node production, see [docs/operations.md](docs/operations.md). Quick render:

```bash
helm template mantle ./helm/mantle
helm install mantle ./helm/mantle -f helm/mantle/values.yaml
helm upgrade mantle ./helm/mantle -f helm/mantle/values-prod.yaml
```

For a single Linux VM (EC2), use [docs/deploy-ec2.md](docs/deploy-ec2.md) instead of Helm.

### Integration tests

| Suite | Command | Requires Docker |
|-------|---------|-----------------|
| Contract (CI) | `cargo test -p mantle-integration-tests --test contracts` | No |
| Live flows | `cargo test -p mantle-integration-tests --features integration -- --ignored` | Yes |
| Load / latency | `MANTLE_LOAD_TEST=1 cargo test -p mantle-integration-tests tile_latency -- --ignored` | Yes + warm cache |

Flows: upload → STAC → tile, cloud ref → EDR, process 202 → poll, cache warm before tile. See [docs/KNOWN_LIMITATIONS.md](docs/KNOWN_LIMITATIONS.md).

Post-compose smoke (no COG fixture):

```bash
./scripts/smoke.sh    # or .\scripts\smoke.ps1
```

## Beta status

Mantle is in **beta**: core paths work end-to-end in Docker Compose, but several production integrations remain stubs or require manual ops.

| Area | Status | Notes |
|------|--------|-------|
| COG upload → STAC → OGC tile | Done | Requires fixture COG for live integration test |
| vRPM (NDVI) virtual tiles | Done | Sidecar + attach API |
| Redis Streams async jobs | Done | Ray + analytics-worker in compose |
| pRPM `zonal_stats` | Beta | `params.values` or `dataset_refs` + band (stub read without rasterio) |
| Virtual Zarr → Icechunk ingestion | Stub | `virtualize.py` records target URI only |
| DuckDB extensions (ducklake/spatial/postgres) | Ops | Documented in [operations.md](docs/operations.md); not bundled in API image |
| Live integration tests in CI | Partial | Contract tests in CI; full flows `#[ignore]`; use `scripts/smoke.sh` after compose |
| Tile p99 &lt; 10 ms warm cache | Benchmark | `tile_latency` test behind `MANTLE_LOAD_TEST=1` |
| Helm / EKS production | Chart ready | IRSA, KEDA, PDBs — validate in your cluster |

## Project status

Phases 0–5 scaffold complete: contracts, data plane, raster/ingestion, render AST, STAC/OGC/analytics, production hardening (Helm, CI, ops docs, integration scaffold). Beta hardening continues on the items above.

## License

MIT
