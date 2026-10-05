. "$PSScriptRoot/demo-common.ps1"
Push-Location "$PSScriptRoot/.."
try {
    Invoke-Compose up -d --build --wait postgres control-plane worker-a worker-b worker-c
    & docker pull alpine:3.22
    if ($LASTEXITCODE -ne 0) { throw 'Image pull failed' }
    Wait-Until { @(Get-Workers | Where-Object { $_.state -eq 'ONLINE' -and $_.connected }).Count -ge 3 }
    Invoke-Compose run --rm --build verify go test -race -tags e2e -v ./tests/e2e
} finally { Pop-Location }
