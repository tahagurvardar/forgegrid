# Runs all three workers, submits an argv job, and verifies persisted result and both log streams.
. "$PSScriptRoot/demo-common.ps1"
Push-Location "$PSScriptRoot/.."
try {
    Invoke-Compose up -d --build --wait postgres control-plane worker-a worker-b worker-c
    & docker pull alpine:3.22
    if ($LASTEXITCODE -ne 0) { throw 'Docker image pull failed' }
    Wait-Until { @(Get-Workers | Where-Object { $_.state -eq 'ONLINE' -and $_.connected }).Count -ge 3 }
    $submitted = Submit-Job @('/bin/sh', '-c', 'echo forgegrid-stdout; echo forgegrid-stderr >&2; sleep 2')
    Wait-Until { (Get-Job $submitted.id).state -in @('SUCCEEDED', 'FAILED') }
    $job = Get-Job $submitted.id
    if ($job.state -ne 'SUCCEEDED' -or $job.attempt_count -ne 1) { throw ($job | ConvertTo-Json -Depth 8) }
    $logs = Invoke-RestMethod "$BaseUrl/api/v1/attempts/$($job.current_attempt_id)/logs"
    if (@($logs | Where-Object stream -eq 'STDOUT').Count -eq 0 -or @($logs | Where-Object stream -eq 'STDERR').Count -eq 0) { throw 'Missing stdout/stderr logs' }
    $job | ConvertTo-Json -Depth 8
    Show-Logs $job.current_attempt_id
    Write-Host 'NORMAL_EXECUTION_PASSED'
} finally { Pop-Location }
