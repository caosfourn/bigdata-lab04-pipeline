# Incremental Code Property Graph Streaming Pipeline

This Jupyter Book documents a pipeline that parses the Python source files in
`huggingface/lerobot` one file at a time and publishes Code Property Graph events
to Kafka. The graph topology is written directly to Neo4j by Kafka Connect,
while Spark Structured Streaming writes file metadata to MongoDB.

The repository contains reproducible source code, tests, infrastructure
configuration, and verification queries. Runtime screenshots and captured
outputs must be produced from the final team deployment before submission.

## Reproduce locally

```powershell
Copy-Item .env.example .env
docker compose up -d --build
.\scripts\create_topics.ps1
python -m pytest -q
python -m src.kafka_producer lerobot --brokers localhost:9092
```

See each task chapter for its success criteria and evidence.
