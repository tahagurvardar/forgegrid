$ErrorActionPreference = 'Stop'
$BaseUrl = 'http://localhost:8080'
function Invoke-Compose {
    & docker compose @args
    if ($LASTEXITCODE -ne 0) { throw "docker compose failed: $args" }
}
function Wait-Until([scriptblock]$Condition, [int]$Seconds = 120) {
    $deadline = [DateTime]::UtcNow.AddSeconds($Seconds)
    do {
        if (& $Condition) { return }
        Start-Sleep -Milliseconds 200
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Timed out waiting for demo state"
}
function Get-Job([string]$Id) { Invoke-RestMethod "$BaseUrl/api/v1/jobs/$Id" }
function Get-Workers { foreach ($worker in (Invoke-RestMethod "$BaseUrl/api/v1/workers")) { $worker } }
function Submit-Job([string[]]$Command, [int]$Timeout = 60) {
    $body = @{ image = 'alpine:3.22'; command = $Command; timeout_seconds = $Timeout; max_attempts = 2 } | ConvertTo-Json -Compress
    Invoke-RestMethod "$BaseUrl/api/v1/jobs" -Method Post -ContentType 'application/json' -Body $body
}
function Show-Logs([string]$AttemptId) {
    $chunks = Invoke-RestMethod "$BaseUrl/api/v1/attempts/$AttemptId/logs"
    foreach ($chunk in $chunks) {
        $text = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($chunk.payload))
        Write-Host ("{0} {1}: {2}" -f $chunk.sequence, $chunk.stream, $text.TrimEnd())
    }
}
