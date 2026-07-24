docker compose --profile person3 up spark-person3

if ($LASTEXITCODE -ne 0) {
    throw "Docker Spark streaming job exited with code $LASTEXITCODE"
}
