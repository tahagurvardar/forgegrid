$ErrorActionPreference = 'Stop'
Push-Location "$PSScriptRoot/.."
try {
    & docker compose up -d --wait postgres
    if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL startup failed' }
    & docker pull alpine:3.22
    if ($LASTEXITCODE -ne 0) { throw 'Workload image pull failed' }
    & docker compose run --rm --build -v /var/run/docker.sock:/var/run/docker.sock verify sh -c 'go vet -tags recovery ./... && go test -race -tags recovery -count=1 -v ./tests/recovery'
    if ($LASTEXITCODE -ne 0) { throw 'Process crash verification failed' }
} finally { Pop-Location }
