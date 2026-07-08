#!/usr/bin/env bash
# Build Mantle Docker images for linux/amd64 (EC2) and push to GHCR.
#
# Prerequisites:
#   1. docker buildx create --use   (once, if no builder exists)
#   2. GitHub PAT with write:packages
#   3. docker login ghcr.io -u YOUR_GITHUB_USER
#
# Usage:
#   export GHCR_IMAGE_PREFIX=ghcr.io/youruser
#   ./scripts/build-push-images.sh [tag]
#
# Default tag: latest

set -euo pipefail

TAG="${1:-latest}"
PREFIX="${GHCR_IMAGE_PREFIX:?Set GHCR_IMAGE_PREFIX (e.g. ghcr.io/youruser)}"
PLATFORM="${MANTLE_BUILD_PLATFORM:-linux/amd64}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

cd "$ROOT"

echo "Building for ${PLATFORM} with prefix ${PREFIX} tag ${TAG}"

build_push() {
  local name="$1"
  local dockerfile="$2"
  local image="${PREFIX}/${name}:${TAG}"
  echo "==> ${image}"
  docker buildx build \
    --platform "${PLATFORM}" \
    --file "${dockerfile}" \
    --tag "${image}" \
    --push \
    .
}

build_push mantle-api Dockerfile.api
build_push mantle-worker Dockerfile.worker
build_push mantle-analytics Dockerfile.analytics

echo "Done. On EC2:"
echo "  export GHCR_IMAGE_PREFIX=${PREFIX}"
echo "  export MANTLE_IMAGE_TAG=${TAG}"
echo "  docker compose -f docker-compose.yml -f docker-compose.ghcr.yml pull"
echo "  docker compose -f docker-compose.yml -f docker-compose.ghcr.yml up -d"
