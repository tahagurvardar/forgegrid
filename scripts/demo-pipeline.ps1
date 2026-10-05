. "$PSScriptRoot/demo-common.ps1"
Push-Location "$PSScriptRoot/.."
try {
    Invoke-Compose up -d --build --wait postgres control-plane worker-a worker-b worker-c
    & docker pull alpine:3.22
    if ($LASTEXITCODE -ne 0) { throw 'Image pull failed' }
    Wait-Until { @(Get-Workers | Where-Object { $_.state -eq 'ONLINE' -and $_.connected }).Count -ge 3 }
    foreach ($failure in @($false, $true)) {
        $testCommand = @('sleep', '3')
        if ($failure) { $testCommand = @('/bin/sh', '-c', 'sleep 1; exit 7') }
        $payload = @{ jobs = @(
            @{ key='build'; image='alpine:3.22'; command=@('echo','build'); timeout_seconds=30; max_attempts=2 },
            @{ key='unit-test'; image='alpine:3.22'; command=$testCommand; dependencies=@('build'); timeout_seconds=30; max_attempts=2 },
            @{ key='lint'; image='alpine:3.22'; command=@('sleep','3'); dependencies=@('build'); timeout_seconds=30; max_attempts=2 },
            @{ key='package'; image='alpine:3.22'; command=@('echo','package'); dependencies=@('unit-test','lint'); timeout_seconds=30; max_attempts=2 }
        ) } | ConvertTo-Json -Depth 8
        $submitted = Invoke-RestMethod "$BaseUrl/api/v1/pipelines" -Method Post -ContentType 'application/json' -Body $payload
        $path = "$BaseUrl/api/v1/pipelines/$($submitted.id)"
        if (-not $failure) {
            Wait-Until {
                $p = Invoke-RestMethod $path
                @($p.jobs | Where-Object { $_.key -in @('unit-test','lint') -and $_.state -eq 'RUNNING' }).Count -eq 2
            }
            Write-Host 'PIPELINE_PARALLEL_BRANCHES_RUNNING'
        }
        Wait-Until { (Invoke-RestMethod $path).state -in @('SUCCEEDED','FAILED','CANCELLED') }
        $p = Invoke-RestMethod $path
        $p | ConvertTo-Json -Depth 8
        $package = $p.jobs | Where-Object key -eq 'package'
        $lint = $p.jobs | Where-Object key -eq 'lint'
        if ($failure) {
            if ($p.state -ne 'FAILED' -or $package.state -ne 'SKIPPED' -or $package.attempt_count -ne 0 -or $lint.state -ne 'SUCCEEDED') { throw 'Pipeline failure semantics failed' }
            Write-Host 'PIPELINE_FAILURE_SKIPS_PACKAGE_PASSED'
        } else {
            if ($p.state -ne 'SUCCEEDED' -or @($p.jobs | Where-Object state -ne 'SUCCEEDED').Count -ne 0) { throw 'Pipeline success semantics failed' }
            Write-Host 'PIPELINE_SUCCESS_PASSED'
        }
    }
} finally { Pop-Location }
