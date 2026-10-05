# Stops the Control Plane, verifies local lease enforcement, then verifies recovery.
. "$PSScriptRoot/demo-common.ps1"
Push-Location "$PSScriptRoot/.."
try {
    Invoke-Compose up -d --wait postgres control-plane worker-a worker-b worker-c
    Wait-Until { @(Get-Workers | Where-Object { $_.state -eq 'ONLINE' -and $_.connected }).Count -ge 3 }
    $submitted = Submit-Job @('sleep', '20')
    Wait-Until { (Get-Job $submitted.id).state -eq 'RUNNING' }
    $old = (Get-Job $submitted.id).attempts[0]
    Invoke-Compose stop control-plane
    Wait-Until {
        $running = & docker ps -q --filter "label=forgegrid.attempt_id=$($old.attempt_id)"
        if ($LASTEXITCODE -ne 0) { throw 'Could not inspect Docker execution' }
        [string]::IsNullOrWhiteSpace(($running -join ''))
    } 20
    Write-Host 'LOCAL_LEASE_GUARD_STOPPED_EXECUTION_WITHOUT_CONTROL_PLANE_ACKS'
    Invoke-Compose up -d --wait control-plane worker-a worker-b worker-c
    Wait-Until { (Get-Job $submitted.id).state -in @('SUCCEEDED','FAILED') }
    $job = Get-Job $submitted.id
    if ($job.state -ne 'SUCCEEDED' -or $job.attempt_count -ne 2 -or $job.attempts[0].state -ne 'LOST') { throw ($job | ConvertTo-Json -Depth 8) }
    Write-Host 'CONTROL_PLANE_RESTART_RECOVERY_PASSED'
} finally {
    Invoke-Compose up -d --wait control-plane worker-a worker-b worker-c
    Pop-Location
}
