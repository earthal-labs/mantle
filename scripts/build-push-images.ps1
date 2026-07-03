# Build Mantle Docker images for linux/amd64 (EC2) and push to GHCR.
#
# Prerequisites:
#   1. docker buildx create --use   (once, if no builder exists)
#   2. GitHub PAT with write:packages
#   3. docker login ghcr.io -u YOUR_GITHUB_USER
#
# Usage:
#   $env:GHCR_IMAGE_PREFIX = "ghcr.io/youruser"
#   .\scripts\build-push-images.ps1 -Tag latest

param(
    [string]$Tag = "latest"
)

$ErrorActionPreference = "Stop"

if (-not $env:GHCR_IMAGE_PREFIX) {
    throw "Set GHCR_IMAGE_PREFIX (e.g. ghcr.io/youruser)"
}

$Prefix = $env:GHCR_IMAGE_PREFIX
$Platform = if ($env:MANTLE_BUILD_PLATFORM) { $env:MANTLE_BUILD_PLATFORM } else { "linux/amd64" }
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

Set-Location $Root

Write-Host "Building for $Platform with prefix $Prefix tag $Tag"

function Build-Push {
    param(
        [string]$Name,
        [string]$Dockerfile
    )
    $Image = "${Prefix}/${Name}:${Tag}"
    Write-Host "==> $Image"
    docker buildx build `
        --platform $Platform `
        --file $Dockerfile `
        --tag $Image `
        --push `
        .
    if ($LASTEXITCODE -ne 0) { throw "docker buildx failed for $Image" }
}

Build-Push -Name "mantle-api" -Dockerfile "Dockerfile.api"
Build-Push -Name "mantle-worker" -Dockerfile "Dockerfile.worker"
Build-Push -Name "mantle-analytics" -Dockerfile "Dockerfile.analytics"

Write-Host "Done. On EC2:"
Write-Host "  export GHCR_IMAGE_PREFIX=$Prefix"
Write-Host "  export MANTLE_IMAGE_TAG=$Tag"
Write-Host "  docker compose -f docker-compose.yml -f docker-compose.ghcr.yml pull"
Write-Host "  docker compose -f docker-compose.yml -f docker-compose.ghcr.yml up -d"
