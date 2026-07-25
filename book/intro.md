# Incremental Code Property Graph Streaming Pipeline

This book is the reproducible report for Lab 04. The selected public repository
is [`huggingface/lerobot`](https://github.com/huggingface/lerobot). Its Python
sources are discovered and parsed one file at a time, then emitted as versioned
events. Kafka Connect writes graph topology directly to Neo4j; Spark Structured
Streaming consumes only source metadata and upserts it into MongoDB.

## Group Information

| Full name | Student ID |
|---|---|
| Hồ Hồ Gia Bảo | 23120021 |
| Nguyễn Thanh Khánh Hà | 23120037 |
| Huỳnh Đặng Ngọc Hân | 23120042 |
| Lê Minh Nhật | 23120067 |

```text
LeRobot -> Discovery -> Parser Service -> Kafka
                                      |-> nodes/edges -> Neo4j Connector -> Neo4j
                                      |-> metadata -> Spark -> MongoDB
                                      `-> parser errors -> monitoring
```

## What is included

| Requirement | Implementation | Report chapter |
|---|---|---|
| Clone and source discovery | shallow Git clone, deterministic manifest | Task 1 |
| Incremental CPG parser | Python `ast`, AST/CFG/DFG/CALL, stable IDs | Task 2 |
| Kafka layout | four domain topics plus connector DLQ | Task 3 |
| Direct graph ingestion | Neo4j Kafka Sink, constraints and `MERGE` | Task 4 |
| Metadata ingestion | Spark Structured Streaming and MongoDB connector | Task 5 |
| Modified-file replay | revision cleanup, upsert and checkpoint restart | Task 6 |
| System design | component, data and recovery paths | Architecture |

The repository also contains JSON Schemas, Docker Compose infrastructure,
verification queries, automated tests, and the source of this Jupyter Book.

## Evidence policy

This report distinguishes two evidence scopes so preliminary fixture results
cannot be mistaken for the final repository experiment:

- **Recorded integration evidence** is copied verbatim from committed evidence
  logs. It was produced by the running Docker Compose stack on the dated
  environment shown beside each result.
- **Final LeRobot evidence** means the dated full-load and five-phase replay run
  against the pinned commit recorded in Task 1. Its values come from saved
  database/checkpoint snapshots and the automated acceptance report; values are
  never inferred from fixture results.

The fixture run checks the implementation path and its idempotency. The final
LeRobot capture records the same path running on the repository
selected in Moodle.

## Minimal reproduction

The pinned LeRobot sources require Python 3.14 for a zero-error `ast` parse.
Spark remains isolated in Docker, and CI/report tooling uses Python 3.11.

```bash
git clone --depth=1 https://github.com/huggingface/lerobot.git lerobot
python3.14 -m venv .venv
.venv/bin/python -m pip install \
  kafka-python==3.0.8 jsonschema==4.25.1 \
  neo4j==5.20.0 pymongo==4.6.3
cp .env.example .env
docker compose --profile spark up -d --build
.venv/bin/python src/discovery.py lerobot
.venv/bin/python -m src.kafka_publisher lerobot \
  src/lerobot/__init__.py --repo-id huggingface/lerobot
```

Each task chapter gives its commands, observed output, success criteria,
failure notes, and a short reflection.
