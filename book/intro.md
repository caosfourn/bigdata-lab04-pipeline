# Incremental Code Property Graph Streaming Pipeline

This Jupyter Book documents a pipeline that parses the Python source files in
`huggingface/lerobot` one file at a time and publishes Code Property Graph events
to Kafka. The graph topology is written directly to Neo4j by Kafka Connect,
while Spark Structured Streaming writes file metadata to MongoDB.

The repository contains reproducible source code, tests, infrastructure
configuration, and verification queries. Runtime screenshots and captured
outputs must be produced from the final team deployment before submission.

## Reproduce locally

```bash
cp .env.example .env
docker compose up -d --build
python -m unittest discover -s tests -v
python -m src.kafka_publisher lerobot \
  src/lerobot/__init__.py \
  --repo-id huggingface/lerobot
```

See each task chapter for its success criteria and evidence.
