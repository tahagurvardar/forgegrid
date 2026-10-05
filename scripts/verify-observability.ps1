# Real Docker failure isolation; leaves the local telemetry stack running.
. "$PSScriptRoot/demo-common.ps1"
function Start-ObservedJob {
    $submitted = Submit-Job @('sleep','5')
    Wait-Until { (Get-Job $submitted.id).state -eq 'RUNNING' }
    return $submitted.id
}
function Assert-ObservedJob([string]$Id) {
    Wait-Until { (Get-Job $Id).state -in @('SUCCEEDED','FAILED','CANCELLED') }
    $job = Get-Job $Id
    if ($job.state -ne 'SUCCEEDED' -or $job.attempt_count -ne 1) { throw 'Telemetry failure affected execution' }
}
Push-Location "$PSScriptRoot/.."
try {
    Invoke-Compose up -d otel-collector prometheus jaeger
    Invoke-Compose stop otel-collector prometheus jaeger
    # Recreate agents/Control Plane while all backends are unavailable.
    Invoke-Compose up -d --build --force-recreate --wait postgres control-plane worker-a worker-b worker-c
    & docker pull alpine:3.22
    if ($LASTEXITCODE -ne 0) { throw 'Image pull failed' }
    Wait-Until { @(Get-Workers | Where-Object { $_.state -eq 'ONLINE' -and $_.connected }).Count -ge 3 }
    $id = Start-ObservedJob
    Assert-ObservedJob $id
    Write-Host 'OBSERVABILITY_BACKENDS_UNAVAILABLE_AT_STARTUP_PASSED'
    foreach ($backend in @('otel-collector','jaeger','prometheus')) {
        Invoke-Compose up -d otel-collector prometheus jaeger
        $id = Start-ObservedJob
        Invoke-Compose stop $backend
        Assert-ObservedJob $id
        Write-Host "OBSERVABILITY_BACKEND_LOSS_PASSED=$backend"
    }
} finally {
    Invoke-Compose up -d otel-collector prometheus jaeger
    Pop-Location
}
