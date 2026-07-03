# Post-compose smoke checks (no COG fixture required).
$ErrorActionPreference = "Stop"

$ApiUrl = if ($env:MANTLE_TEST_API_URL) { $env:MANTLE_TEST_API_URL } else { "http://localhost:8080" }
$AdminToken = if ($env:MANTLE_ADMIN_TOKEN) { $env:MANTLE_ADMIN_TOKEN } else { "dev-admin-token" }

Write-Host "== Mantle smoke: $ApiUrl =="

$health = Invoke-RestMethod -Uri "${ApiUrl}/health" -TimeoutSec 10
Write-Host "health: $($health | ConvertTo-Json -Compress)"

$stac = Invoke-RestMethod -Uri "${ApiUrl}/stac/" -TimeoutSec 10
Write-Host "stac landing: ok"

$null = Invoke-RestMethod -Uri "${ApiUrl}/ogc/processes" -TimeoutSec 10
Write-Host "ogc processes: ok"

try {
    Invoke-WebRequest -Uri "${ApiUrl}/admin/datasets/reference" -Method POST `
        -ContentType "application/json" -Body '{"name":"x","storage_uri":"s3://mantle-data/x.tif"}' `
        -UseBasicParsing -TimeoutSec 10 | Out-Null
    throw "expected 401/403 without admin token"
} catch {
    $status = $_.Exception.Response.StatusCode.value__
    if ($status -ne 401 -and $status -ne 403) {
        throw "expected 401/403 without admin token, got $status"
    }
    Write-Host "admin auth gate: $status (expected)"
}

try {
    $resp = Invoke-WebRequest -Uri "${ApiUrl}/admin/datasets/reference" -Method POST `
        -Headers @{ Authorization = "Bearer $AdminToken" } `
        -ContentType "application/json" -Body '{}' `
        -UseBasicParsing -TimeoutSec 10
    Write-Host "admin route reachable: $($resp.StatusCode)"
} catch {
    $status = $_.Exception.Response.StatusCode.value__
    if ($status -eq 401 -or $status -eq 403) {
        throw "admin token rejected ($status)"
    }
    Write-Host "admin route reachable: $status"
}

Write-Host "smoke passed"
