# Start Mantle local stack, wait for health, print next steps.
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if (-not (Test-Path ".env")) {
    Write-Host "Creating .env from .env.example"
    Copy-Item ".env.example" ".env"
}

Write-Host "Validating docker compose config..."
docker compose config | Out-Null

Write-Host "Starting services..."
docker compose up -d

$ApiUrl = if ($env:MANTLE_TEST_API_URL) { $env:MANTLE_TEST_API_URL } else { "http://localhost:8080" }
$MaxAttempts = if ($env:MANTLE_DEV_UP_ATTEMPTS) { [int]$env:MANTLE_DEV_UP_ATTEMPTS } else { 60 }
$SleepSecs = if ($env:MANTLE_DEV_UP_SLEEP) { [int]$env:MANTLE_DEV_UP_SLEEP } else { 2 }

Write-Host "Waiting for API health at ${ApiUrl}/health (up to $($MaxAttempts * $SleepSecs)s)..."
$healthy = $false
for ($i = 1; $i -le $MaxAttempts; $i++) {
    try {
        $null = Invoke-WebRequest -Uri "${ApiUrl}/health" -UseBasicParsing -TimeoutSec 5
        Write-Host "API is healthy."
        $healthy = $true
        break
    } catch {
        if ($i -eq $MaxAttempts) {
            Write-Error "API did not become healthy in time. Check: docker compose logs api"
        }
        Start-Sleep -Seconds $SleepSecs
    }
}

if (-not $healthy) { exit 1 }

$token = if ($env:MANTLE_ADMIN_TOKEN) { $env:MANTLE_ADMIN_TOKEN } else { "dev-admin-token" }

Write-Host @"

Mantle dev stack is up.

  Health:     curl ${ApiUrl}/health
  Admin:      `$env:MANTLE_ADMIN_TOKEN = '$token'
  Smoke:      .\scripts\smoke.ps1
  Contracts:  cargo test -p mantle-integration-tests --test contracts
  Python:     cd python; uv sync --extra dev; uv run pytest

Upload a COG then tile — see README.md and docs/operations.md.
"@
