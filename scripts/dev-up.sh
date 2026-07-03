#!/usr/bin/env bash
# Start Mantle local stack, wait for health, print next steps.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  echo "Creating .env from .env.example"
  cp .env.example .env
fi

echo "Validating docker compose config..."
docker compose config >/dev/null

echo "Starting services..."
docker compose up -d

API_URL="${MANTLE_TEST_API_URL:-http://localhost:8080}"
MAX_ATTEMPTS="${MANTLE_DEV_UP_ATTEMPTS:-60}"
SLEEP_SECS="${MANTLE_DEV_UP_SLEEP:-2}"

echo "Waiting for API health at ${API_URL}/health (up to $((MAX_ATTEMPTS * SLEEP_SECS))s)..."
for ((i = 1; i <= MAX_ATTEMPTS; i++)); do
  if curl -sf "${API_URL}/health" >/dev/null; then
    echo "API is healthy."
    break
  fi
  if [[ "$i" -eq "$MAX_ATTEMPTS" ]]; then
    echo "API did not become healthy in time. Check: docker compose logs api" >&2
    exit 1
  fi
  sleep "$SLEEP_SECS"
done

cat <<EOF

Mantle dev stack is up.

  Health:     curl ${API_URL}/health
  Admin:      export MANTLE_ADMIN_TOKEN=\${MANTLE_ADMIN_TOKEN:-dev-admin-token}
  Smoke:      ./scripts/smoke.sh
  Contracts:  cargo test -p mantle-integration-tests --test contracts
  Python:     cd python && uv sync --extra dev && uv run pytest

Upload a COG then tile:
  curl -X POST "${API_URL}/admin/datasets/upload" \\
    -H "Authorization: Bearer \${MANTLE_ADMIN_TOKEN:-dev-admin-token}" \\
    -F "name=demo" -F "file=@/path/to/fixture.tif"

See README.md and docs/operations.md for integration tests.
EOF
