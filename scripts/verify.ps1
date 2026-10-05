$ErrorActionPreference = 'Stop'
Push-Location "$PSScriptRoot/.."
try {
    & docker compose up -d --wait postgres
    if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL startup failed' }
    & docker compose run --rm --build verify sh -c 'test -z "$(gofmt -l cmd internal db tests)" && go vet -tags integration ./... && go build ./... && go test -race -tags integration -count=1 ./...'
    if ($LASTEXITCODE -ne 0) { throw 'ForgeGrid verification failed' }
} finally { Pop-Location }
