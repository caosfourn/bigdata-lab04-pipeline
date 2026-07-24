param(
    [string]$Brokers = "localhost:9092",
    [string]$Topic = "cpg.metadata",
    [string]$MongoUri = "mongodb://localhost:27017",
    [string]$MongoDatabase = "cpg",
    [string]$MongoCollection = "metadata_person3",
    [string]$CheckpointName = "person3-final"
)

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$pythonPath = Join-Path $projectRoot ".venv-spark\Scripts\python.exe"
$sparkSubmit = Join-Path $projectRoot ".venv-spark\Scripts\spark-submit.cmd"

if (-not (Test-Path -LiteralPath $pythonPath)) {
    throw "Missing Spark environment: $pythonPath"
}
if (-not (Test-Path -LiteralPath $sparkSubmit)) {
    throw "Missing spark-submit: $sparkSubmit"
}

$ivyPath = Join-Path $projectRoot "runtime\spark-ivy"
$checkpointPath = Join-Path $projectRoot "checkpoints\$CheckpointName"
New-Item -ItemType Directory -Force $ivyPath, $checkpointPath | Out-Null

$env:PYSPARK_PYTHON = $pythonPath
$env:PYSPARK_DRIVER_PYTHON = $pythonPath

Write-Host "Starting Spark metadata stream"
Write-Host "  Kafka topic : $Topic"
Write-Host "  MongoDB     : $MongoDatabase.$MongoCollection"
Write-Host "  Checkpoint  : $checkpointPath"
Write-Host "Keep this window open. Press Ctrl+C only when testing restart."

& $sparkSubmit `
    --master "local[1]" `
    --conf "spark.jars.ivy=$ivyPath" `
    --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.7.0" `
    (Join-Path $projectRoot "src\metadata_streaming_job.py") `
    --brokers $Brokers `
    --topic $Topic `
    --mongo-uri $MongoUri `
    --mongo-db $MongoDatabase `
    --mongo-collection $MongoCollection `
    --checkpoint-location $checkpointPath

if ($LASTEXITCODE -ne 0) {
    throw "Spark streaming job exited with code $LASTEXITCODE"
}
