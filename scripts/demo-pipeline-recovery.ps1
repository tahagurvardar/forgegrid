# Runs only worker-b until the test starts, kills it, then runs worker-c.
. "$PSScriptRoot/demo-common.ps1"
Push-Location "$PSScriptRoot/.."
try {
    Invoke-Compose up -d --build --wait postgres control-plane
    Invoke-Compose stop worker-a worker-b worker-c
    Invoke-Compose up -d worker-b
    & docker pull alpine:3.22
    if ($LASTEXITCODE -ne 0) { throw 'Image pull failed' }
    Wait-Until { @(Get-Workers | Where-Object { $_.worker_id -eq 'worker-b' -and $_.state -eq 'ONLINE' -and $_.connected -and $_.active_slots -eq 0 }).Count -eq 1 }
    $payload = @{ jobs = @(
        @{ key='build'; image='alpine:3.22'; command=@('echo','build'); timeout_seconds=60; max_attempts=2 },
        @{ key='test'; image='alpine:3.22'; command=@('sleep','20'); dependencies=@('build'); timeout_seconds=60; max_attempts=2 },
        @{ key='package'; image='alpine:3.22'; command=@('echo','package'); dependencies=@('test'); timeout_seconds=60; max_attempts=2 }
    ) } | ConvertTo-Json -Depth 8
    $response = Invoke-WebRequest -UseBasicParsing "$BaseUrl/api/v1/pipelines" -Method Post -ContentType 'application/json' -Body $payload
    $submitted = $response.Content | ConvertFrom-Json
    Write-Host "PIPELINE_RECOVERY_TRACE_ID=$($response.Headers['X-ForgeGrid-Trace-ID'])"
    $path = "$BaseUrl/api/v1/pipelines/$($submitted.id)"
    Wait-Until { @((Invoke-RestMethod $path).jobs | Where-Object { $_.key -eq 'test' -and $_.state -eq 'RUNNING' }).Count -eq 1 }
    $p = Invoke-RestMethod $path
    $test = $p.jobs | Where-Object key -eq 'test'
    $old = $test.attempts[0]
    if ($old.worker_id -ne 'worker-b' -or ($p.jobs | Where-Object key -eq 'package').state -ne 'BLOCKED') { throw 'Invalid initial DAG assignment' }
    $workerContainer = & docker compose ps -q worker-b
    & docker update --restart=no $workerContainer | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Could not disable worker restart' }
    Invoke-Compose kill -s SIGKILL worker-b
    Invoke-Compose up -d worker-c
    Wait-Until { @(Get-Workers | Where-Object { $_.worker_session_id -eq $old.worker_session_id -and $_.state -eq 'OFFLINE' }).Count -eq 1 }
    $p = Invoke-RestMethod $path
    $test = $p.jobs | Where-Object key -eq 'test'
    if ($test.attempt_count -ne 1 -or $test.current_attempt_id -ne $old.attempt_id -or ($p.jobs | Where-Object key -eq 'package').state -ne 'BLOCKED') { throw 'Dependency or ownership changed before expiry' }
    Write-Host 'PIPELINE_WORKER_OFFLINE_DEPENDENT_STILL_BLOCKED'
    Wait-Until { (Get-Job $test.id).attempt_count -eq 2 }
    $test = Get-Job $test.id
    if ($test.attempts[0].state -ne 'LOST' -or $test.attempts[1].worker_id -ne 'worker-c' -or $test.attempts[1].fencing_token -ne ($old.fencing_token + 1)) { throw 'Invalid fenced DAG recovery' }
    if (((Invoke-RestMethod $path).jobs | Where-Object key -eq 'package').state -ne 'BLOCKED') { throw 'Dependency released before retry success' }
    Invoke-Compose run --rm --no-deps result-replay -attempt $old.attempt_id -session $old.worker_session_id -fence $old.fencing_token
    Wait-Until { (Invoke-RestMethod $path).state -in @('SUCCEEDED','FAILED','CANCELLED') }
    $p = Invoke-RestMethod $path
    if ($p.state -ne 'SUCCEEDED' -or ($p.jobs | Where-Object key -eq 'package').state -ne 'SUCCEEDED') { throw 'DAG recovery did not release package' }
    $p | ConvertTo-Json -Depth 8
    Write-Host 'PIPELINE_WORKER_B_TO_WORKER_C_RECOVERY_PASSED'
} finally {
    if ($workerContainer) { & docker update --restart=unless-stopped $workerContainer | Out-Null }
    Invoke-Compose up -d worker-a worker-b worker-c
    Pop-Location
}
