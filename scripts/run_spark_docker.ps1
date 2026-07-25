docker compose --profile spark up spark-metadata

if ($LASTEXITCODE -ne 0) {
    throw "Docker Spark streaming job exited with code $LASTEXITCODE"
}
