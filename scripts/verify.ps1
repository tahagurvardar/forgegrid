$ErrorActionPreference = 'Stop'
Push-Location "$PSScriptRoot/.."
try {
    & docker compose up -d --wait postgres
    if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL startup failed' }
    & docker compose run --rm --build verify sh scripts/verify.sh
    if ($LASTEXITCODE -ne 0) { throw 'ForgeGrid verification failed' }
} finally { Pop-Location }
