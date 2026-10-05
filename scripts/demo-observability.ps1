. "$PSScriptRoot/demo-common.ps1"
function Get-Trace([string]$TraceId) {
    try { return (Invoke-RestMethod "http://localhost:16686/api/traces/$TraceId").data[0] } catch { return $null }
}
function Assert-Trace([string]$TraceId,[string[]]$Required) {
    Wait-Until {
        $trace = Get-Trace $TraceId
        if (-not $trace) { return $false }
        @($Required | Where-Object { $_ -notin $trace.spans.operationName }).Count -eq 0
    }
    $trace = Get-Trace $TraceId
    if (@($trace.processes.PSObject.Properties.Value.serviceName | Sort-Object -Unique).Count -lt 2) { throw 'Control Plane and worker traces are not correlated' }
    Write-Host "TRACE_VERIFIED: http://localhost:16686/trace/$TraceId"
}
Push-Location "$PSScriptRoot/.."
try {
    Invoke-Compose up -d --build --wait postgres control-plane worker-a worker-b worker-c otel-collector prometheus jaeger
    & docker pull alpine:3.22
    if ($LASTEXITCODE -ne 0) { throw 'Image pull failed' }
    Wait-Until { @(Get-Workers | Where-Object { $_.state -eq 'ONLINE' -and $_.connected }).Count -ge 3 }
    $payload = @{ jobs = @(
        @{ key='build'; image='alpine:3.22'; command=@('echo','build'); timeout_seconds=30; max_attempts=2 },
        @{ key='unit-test'; image='alpine:3.22'; command=@('sleep','3'); dependencies=@('build'); timeout_seconds=30; max_attempts=2 },
        @{ key='lint'; image='alpine:3.22'; command=@('sleep','3'); dependencies=@('build'); timeout_seconds=30; max_attempts=2 },
        @{ key='package'; image='alpine:3.22'; command=@('echo','package'); dependencies=@('unit-test','lint'); timeout_seconds=30; max_attempts=2 }
    ) } | ConvertTo-Json -Depth 8
    $response = Invoke-WebRequest "$BaseUrl/api/v1/pipelines" -Method Post -ContentType 'application/json' -Body $payload
    $submitted = $response.Content | ConvertFrom-Json
    $traceId = [string]($response.Headers['X-ForgeGrid-Trace-ID'] | Select-Object -First 1)
    Wait-Until { (Invoke-RestMethod "$BaseUrl/api/v1/pipelines/$($submitted.id)").state -in @('SUCCEEDED','FAILED','CANCELLED') }
    if ((Invoke-RestMethod "$BaseUrl/api/v1/pipelines/$($submitted.id)").state -ne 'SUCCEEDED') { throw 'Observed pipeline failed' }
    Assert-Trace $traceId @('pipeline.submit','scheduler.claim_job','scheduler.select_worker','scheduler.create_attempt','grpc.dispatch_assignment','worker.receive_assignment','worker.accept_assignment','worker.execute_attempt','docker.create','docker.start','docker.wait','worker.report_completion','controlplane.complete_attempt','lease.renew','dag.release_dependencies','pipeline.finalize')
    Write-Host 'OBSERVABILITY_NORMAL_TRACE_PASSED'
    $recoveryOutput = & ./scripts/demo-pipeline-recovery.ps1 6>&1
    $recoveryOutput | ForEach-Object { Write-Host $_ }
    $match = [regex]::Match(($recoveryOutput -join "`n"),'PIPELINE_RECOVERY_TRACE_ID=([a-f0-9]{32})')
    if (-not $match.Success) { throw 'Recovery trace ID missing' }
    $recoveryTraceId = $match.Groups[1].Value
    Assert-Trace $recoveryTraceId @('recovery.expire_attempt','recovery.retry_job','worker.execute_attempt','dag.release_dependencies','controlplane.complete_attempt','pipeline.finalize')
    $trace = Get-Trace $recoveryTraceId
    $retry = @($trace.spans | Where-Object { $_.operationName -eq 'scheduler.create_attempt' -and @($_.tags | Where-Object { $_.key -eq 'attempt_number' -and $_.value -eq 2 }).Count -eq 1 })
    if ($retry.Count -ne 1) { throw 'Fresh retry attempt missing from trace' }
    $workerC = @($trace.spans | Where-Object { $_.operationName -eq 'worker.execute_attempt' -and @($_.tags | Where-Object { $_.key -eq 'worker_id' -and $_.value -eq 'worker-c' }).Count -eq 1 })
    if ($workerC.Count -lt 1) { throw 'Worker-c execution missing' }
    Wait-Until {
        $metrics = (Invoke-WebRequest "$BaseUrl/metrics").Content
        $metrics -match 'forgegrid_lease_expirations_total [1-9]' -and $metrics -match 'forgegrid_attempt_retries_total\{reason="LEASE_EXPIRED"\} [1-9]'
    }
    Wait-Until {
        $result = Invoke-RestMethod 'http://localhost:9092/api/v1/query?query=up'
        @($result.data.result | Where-Object { $_.metric.job -like 'forgegrid-*' -and $_.value[1] -eq '1' }).Count -eq 4
    }
    Write-Host 'OBSERVABILITY_RECOVERY_TRACE_AND_METRICS_PASSED'
    Write-Host 'Prometheus: http://localhost:9092 — query forgegrid_workers_offline, forgegrid_lease_expirations_total, forgegrid_attempt_retries_total, forgegrid_pipeline_completions_total.'
    Write-Host 'Worker-offline gauge is transient; inspect its historical range around the kill. Traces are diagnostic; PostgreSQL remains authoritative.'
} finally { Pop-Location }
