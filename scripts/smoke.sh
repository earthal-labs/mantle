#!/usr/bin/env bash
# Post-compose smoke checks (no COG fixture required).
set -euo pipefail

API_URL="${MANTLE_TEST_API_URL:-http://localhost:8080}"
ADMIN_TOKEN="${MANTLE_ADMIN_TOKEN:-dev-admin-token}"

echo "== Mantle smoke: ${API_URL} =="

curl_ok() {
  local path="$1"
  local label="$2"
  local body code
  body="$(mktemp)"
  code="$(curl -sS -o "$body" -w "%{http_code}" "${API_URL}${path}" || true)"
  if [[ "$code" != "200" ]]; then
    echo "${label}: HTTP ${code}" >&2
    head -c 500 "$body" >&2 || true
    echo >&2
    rm -f "$body"
    return 1
  fi
  echo "${label}: ok ($(head -c 80 "$body")...)"
  rm -f "$body"
}

curl_ok "/health" "health"
curl_ok "/stac" "stac landing"
curl_ok "/stac/collections" "stac collections"
curl_ok "/ogc/processes" "ogc processes"

# Admin route should reject missing auth
status="$(curl -sS -o /dev/null -w "%{http_code}" -X POST "${API_URL}/admin/datasets/reference" \
  -H "Content-Type: application/json" \
  -d '{"name":"x","storage_uri":"s3://mantle-data/x.tif"}')"
if [[ "$status" != "401" && "$status" != "403" ]]; then
  echo "expected 401/403 without admin token, got ${status}" >&2
  exit 1
fi
echo "admin auth gate: ${status} (expected)"

# With token, empty body should fail validation (proves admin path reachable)
status_auth="$(curl -sS -o /dev/null -w "%{http_code}" -X POST "${API_URL}/admin/datasets/reference" \
  -H "Authorization: Bearer ${ADMIN_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{}')"
if [[ "$status_auth" == "401" || "$status_auth" == "403" ]]; then
  echo "admin token rejected (${status_auth})" >&2
  exit 1
fi
echo "admin route reachable: ${status_auth}"

echo "smoke passed"
