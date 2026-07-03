#!/usr/bin/env bash
# Post-compose smoke checks (no COG fixture required).
set -euo pipefail

API_URL="${MANTLE_TEST_API_URL:-http://localhost:8080}"
ADMIN_TOKEN="${MANTLE_ADMIN_TOKEN:-dev-admin-token}"

echo "== Mantle smoke: ${API_URL} =="

health="$(curl -sf "${API_URL}/health")"
echo "health: ${health}"

stac="$(curl -sf "${API_URL}/stac/")"
echo "stac landing: ok ($(echo "$stac" | head -c 80)...)"

processes="$(curl -sf "${API_URL}/ogc/processes")"
echo "ogc processes: ok"

# Admin route should reject missing auth
status="$(curl -s -o /dev/null -w "%{http_code}" -X POST "${API_URL}/admin/datasets/reference" \
  -H "Content-Type: application/json" \
  -d '{"name":"x","storage_uri":"s3://mantle-data/x.tif"}')"
if [[ "$status" != "401" && "$status" != "403" ]]; then
  echo "expected 401/403 without admin token, got ${status}" >&2
  exit 1
fi
echo "admin auth gate: ${status} (expected)"

# With token, empty body should fail validation (proves admin path reachable)
status_auth="$(curl -s -o /dev/null -w "%{http_code}" -X POST "${API_URL}/admin/datasets/reference" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{}')"
if [[ "$status_auth" == "401" || "$status_auth" == "403" ]]; then
  echo "admin token rejected (${status_auth})" >&2
  exit 1
fi
echo "admin route reachable: ${status_auth}"

echo "smoke passed"
