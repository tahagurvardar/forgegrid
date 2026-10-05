# Intentionally stops workers a/c, kills worker-b with SIGKILL, then restores all workers.
. "$PSScriptRoot/demo-common.ps1"
Push-Location "$PSScriptRoot/.."
try {
    Invoke-Compose up -d --build --wait postgres control-plane
    Invoke-Compose stop worker-a worker-b worker-c
    Invoke-Compose up -d worker-b
    & docker pull alpine:3.22
    if ($LASTEXITCODE -ne 0) { throw 'Docker image pull failed' }
    Wait-Until { @(Get-Workers | Where-Object { $_.worker_id -eq 'worker-b' -and $_.state -eq 'ONLINE' -and $_.connected -and $_.active_slots -eq 0 }).Count -eq 1 }
    $submitted = Submit-Job @('/bin/sh', '-c', 'echo recovery-start; echo recovery-stderr >&2; sleep 20; echo recovery-finished')
    Wait-Until { (Get-Job $submitted.id).state -eq 'RUNNING' }
    $job = Get-Job $submitted.id
    $old = $job.attempts[0]
    if ($old.worker_id -ne 'worker-b' -or $old.attempt_number -ne 1) { throw 'Expected attempt #1 on worker-b' }
    Write-Host "RUNNING attempt=$($old.attempt_id) worker-b fence=$($old.fencing_token)"
    $workerContainer = & docker compose ps -q worker-b
    & docker update --restart=no $workerContainer
    if ($LASTEXITCODE -ne 0) { throw 'Could not disable automatic worker restart for failure demo' }
    Invoke-Compose kill -s SIGKILL worker-b
    Invoke-Compose up -d worker-c
    Wait-Until { @(Get-Workers | Where-Object { $_.worker_session_id -eq $old.worker_session_id -and $_.state -eq 'OFFLINE' }).Count -eq 1 }
    $job = Get-Job $submitted.id
    if ($job.attempt_count -ne 1 -or $job.current_attempt_id -ne $old.attempt_id -or $job.attempts[0].state -notin @('ASSIGNED','RUNNING')) { throw 'Ownership transferred before the offline observation; demo timing assumptions were not met' }
    Write-Host 'WORKER_OFFLINE: attempt #1 still owns the job; waiting for lease expiry'
    Wait-Until { (Get-Job $submitted.id).attempt_count -eq 2 }
    $job = Get-Job $submitted.id
    if ($job.attempts[0].state -ne 'LOST' -or $job.attempts[1].worker_id -ne 'worker-c' -or $job.attempts[1].fencing_token -le $old.fencing_token) { throw 'Invalid recovery transition' }
    Write-Host "LEASE_EXPIRED: attempt #1 LOST; attempt #2 worker-c fence=$($job.attempts[1].fencing_token)"
    Wait-Until { (Get-Job $submitted.id).state -in @('SUCCEEDED','FAILED') }
    $job = Get-Job $submitted.id
    if ($job.state -ne 'SUCCEEDED' -or $job.attempts[1].state -ne 'SUCCEEDED') { throw ($job | ConvertTo-Json -Depth 8) }
    Invoke-Compose run --rm --no-deps result-replay -attempt $old.attempt_id -session $old.worker_session_id -fence $old.fencing_token
    $after = Get-Job $submitted.id
    if ($after.state -ne 'SUCCEEDED' -or $after.current_attempt_id -ne $job.current_attempt_id) { throw 'Stale result changed the winner' }
    $after | ConvertTo-Json -Depth 8
    Show-Logs $after.current_attempt_id
    Write-Host 'WORKER_B_TO_WORKER_C_RECOVERY_PASSED'
} finally {
    if ($workerContainer) { & docker update --restart=unless-stopped $workerContainer | Out-Null }
    Invoke-Compose up -d worker-a worker-b worker-c
    Pop-Location
}
