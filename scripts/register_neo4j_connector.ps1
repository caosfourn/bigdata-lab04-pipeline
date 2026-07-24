param(
    [string]$ConnectUrl = "http://localhost:8083",
    [string]$ConfigPath = "connectors/neo4j-cpg-sink.json"
)

$definition = Get-Content -Raw $ConfigPath | ConvertFrom-Json
$body = $definition.config | ConvertTo-Json -Depth 20

# PUT creates the connector or updates it in place, so this script is safe to rerun.
$result = Invoke-RestMethod `
    -Method Put `
    -Uri "$ConnectUrl/connectors/$($definition.name)/config" `
    -ContentType "application/json" `
    -Body $body

$result | ConvertTo-Json -Depth 10

$statusUri = "$ConnectUrl/connectors/$($definition.name)/status"
$lastError = $null
for ($attempt = 1; $attempt -le 15; $attempt++) {
    try {
        $status = Invoke-RestMethod -Uri $statusUri
        $status | ConvertTo-Json -Depth 10
        exit 0
    }
    catch {
        $lastError = $_
        Write-Host "Waiting for connector status ($attempt/15)..."
        Start-Sleep -Seconds 2
    }
}

Write-Error "Connector was accepted but status was not available after 30 seconds: $lastError"
exit 1
