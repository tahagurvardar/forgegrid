$ErrorActionPreference = 'Stop'
Push-Location "$PSScriptRoot/.."
try {
    & docker compose build verify
    if ($LASTEXITCODE -ne 0) { throw 'Tool image build failed' }
    & docker compose run --rm --no-deps verify sh -c 'mkdir -p gen/go && protoc -I api/proto --go_out=gen/go --go_opt=paths=source_relative --go-grpc_out=gen/go --go-grpc_opt=paths=source_relative api/proto/forgegrid/v1/worker.proto'
    if ($LASTEXITCODE -ne 0) { throw 'Protobuf generation failed' }
} finally { Pop-Location }
