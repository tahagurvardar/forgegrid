# Exclusive local verification: the recovery browser test temporarily stops workers.
. "$PSScriptRoot/demo-common.ps1"
$previousExternal = $env:FORGEGRID_CONSOLE_EXTERNAL
Push-Location "$PSScriptRoot/.."
try {
    Invoke-Compose --profile console up -d --build --wait postgres control-plane worker-a worker-b worker-c console
    & docker pull alpine:3.22
    if ($LASTEXITCODE -ne 0) { throw 'Workload image pull failed' }
    Wait-Until { @(Get-Workers | Where-Object { $_.current -and $_.state -eq 'ONLINE' -and $_.connected }).Count -ge 3 }
    Wait-Until { try { (Invoke-RestMethod 'http://127.0.0.1:5173/healthz').status -eq 'ok' } catch { $false } }
    Push-Location frontend
    try {
        & npm.cmd ci
        if ($LASTEXITCODE -ne 0) { throw 'Frontend dependency installation failed' }
        foreach ($suite in @('check','build','test')) {
            & npm.cmd run $suite
            if ($LASTEXITCODE -ne 0) { throw "Frontend $suite failed" }
            Write-Host "FRONTEND_PASS=$suite"
        }
        & npx.cmd playwright install chromium
        if ($LASTEXITCODE -ne 0) { throw 'Browser installation failed' }
        $env:FORGEGRID_CONSOLE_EXTERNAL = '1'
        foreach ($suite in @('test:browser','test:recovery')) {
            & npm.cmd run $suite
            if ($LASTEXITCODE -ne 0) { throw "Frontend $suite failed" }
            Write-Host "FRONTEND_PASS=$suite"
        }
    } finally { Pop-Location }
} finally {
    $env:FORGEGRID_CONSOLE_EXTERNAL = $previousExternal
    Pop-Location
}
