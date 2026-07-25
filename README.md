# Lab 04 — Incremental CPG Streaming Pipeline

The pipeline processes one Python file at a time and writes to two independent
data paths:

```text
Python repository → Discovery → Parser Service → Kafka
                                              ├─ cpg.nodes / cpg.edges
                                              │        ↓
                                              │  Neo4j Kafka Sink → Neo4j
                                              ├─ cpg.metadata
                                              │        ↓
                                              │  Spark Structured Streaming → MongoDB
                                              └─ cpg.errors → monitoring
```

Neo4j receives graph topology directly from Kafka without passing through
Spark. Spark consumes only `cpg.metadata` and resumes from committed offsets by
using its checkpoint.

## Project layout

```text
src/
├── discovery.py
├── parser_service.py
├── schemas.py
├── kafka_publisher.py
├── topic_config.py
├── metadata_streaming_job.py
├── spark_metadata_consumer.py
└── spark_mongo_sink.py
config/
├── schemas/                  # JSON Schema v1
└── connectors/neo4j-sink.json
infra/
├── connect/Dockerfile
├── kafka/create-topics.sh
└── neo4j/{constraints,verify}.cypher
book/                         # Jupyter Book sources
docs/                         # Task 3–4 design/evidence
tests/
docker-compose.yml
```

## Prerequisites

- Python 3.14 for discovery, parsing, and publishing against the pinned LeRobot commit
- Python 3.11 and Java 17 for CI, Jupyter Book, and Spark-specific tests
- Docker Engine or Docker Desktop with Docker Compose
- The repository assigned on Moodle, shallow-cloned into `lerobot/`

```bash
git clone --depth=1 https://github.com/huggingface/lerobot.git lerobot
python3.14 -m venv .venv
.venv/bin/python -m pip install \
  kafka-python==3.0.8 jsonschema==4.25.1 \
  neo4j==5.20.0 pymongo==4.6.3
cp .env.example .env
```

Four files in LeRobot commit `0d383d09f2051444de211739196a28cc94736861`
use syntax that Python 3.11 cannot parse. The full dry run therefore uses
Python 3.14; it processed all 490 files with no parser errors. Spark 3.5.1 runs
inside the `spark-metadata` container, so PySpark is not installed in the Python
3.14 virtual environment. On Windows, use `.venv\Scripts\python` in place of
`.venv/bin/python`.

## Tasks 1–2: Discovery and parser

```bash
.venv/bin/python src/discovery.py lerobot
.venv/bin/python src/parser_service.py \
  lerobot/src/lerobot/__init__.py \
  lerobot
```

The parser uses the following identifier contract:

- `file_id`: full SHA-256 of `repo_id + normalized relative path`.
- `node_id`: `file_id + deterministic AST structural path + node type`.
- `edge_id`: source, target, type, and a call-site discriminator when required.

This distinguishes AST singleton occurrences that have no line or column data
and keeps identifiers stable when unchanged content is parsed again.

## Tasks 3–4: Kafka and Neo4j sink

Start Kafka, topic bootstrap, Neo4j, constraints, Kafka Connect, MongoDB, and
connector registration:

```bash
docker compose up -d --build
docker compose ps
```

Publish one file:

```bash
.venv/bin/python -m src.kafka_publisher lerobot \
  src/lerobot/__init__.py \
  --repo-id huggingface/lerobot
```

Check the event contract without connecting to Kafka:

```bash
.venv/bin/python -m src.kafka_publisher lerobot \
  src/lerobot/__init__.py \
  --repo-id huggingface/lerobot \
  --dry-run
```

Required topics:

| Topic | Kafka key | Consumer |
|---|---|---|
| `cpg.nodes` | `node_id` | Neo4j Sink |
| `cpg.edges` | `edge_id` | Neo4j Sink |
| `cpg.metadata` | `file_id` | Spark; Neo4j reconciliation |
| `cpg.errors` | `file_id` | monitoring |

`cpg.neo4j.dlq` is an operational dead-letter topic and remains separate from
parser errors.

Check the connector and graph:

```bash
curl -fsS http://localhost:8083/connectors/cpg-neo4j-sink/status
docker compose exec -T neo4j cypher-shell \
  -u neo4j -p cpg-password -f /dev/stdin < infra/neo4j/verify.cypher
```

Neo4j Browser: <http://localhost:7474>. Kafka UI: <http://localhost:8080>.
The design notes and verification checklist are in
[`docs/task3_task4.md`](docs/task3_task4.md).

## Task 5: Spark metadata → MongoDB

The Spark job consumes the complete `cpg.metadata` schema, sets MongoDB
`_id = file_id`, and writes with `replace/upsert`. A new revision of a file
therefore updates one document instead of creating a duplicate.

Run Spark with the Docker profile:

```bash
docker compose --profile spark up -d spark-metadata
docker compose logs -f spark-metadata
```

The checkpoint is mounted at `./checkpoints/metadata-stream`. Keep this
path unchanged during restart testing.

Check MongoDB:

```bash
docker compose exec -T mongodb mongosh cpg --quiet --eval \
  'db.metadata.find({}, {_id:1,file_path:1,file_hash:1,parse_status:1}).toArray()'
```

Restart test:

1. Publish metadata and wait for MongoDB to update.
2. Stop `spark-metadata`.
3. Restart it with the same checkpoint without publishing a new message.
4. Confirm that Spark does not process the committed offset again and the
   MongoDB document count does not increase.
5. Edit one file, publish only that file, and confirm that the existing
   `_id/file_id` document is updated.

The checkpoint protects Kafka offsets. MongoDB `replace/upsert` also handles a
retried micro-batch safely.

The latest integration and restart results are stored in
[`docs/evidence/task5_spark_mongodb_e2e.md`](docs/evidence/task5_spark_mongodb_e2e.md).

## Test

```bash
.venv/bin/python -m unittest discover -s tests -v
docker compose --profile spark config --quiet
git diff --check
```

Spark-specific unit tests are skipped when PySpark is unavailable. Run them in
a Spark environment or container before final Task 5 verification.

## Task 6: Replay

Using one real file from the assigned repository:

1. Publish the baseline and record `file_id`, `file_hash`, parser totals, and
   Neo4j/MongoDB counts.
2. Edit exactly one file and publish only that file.
3. Wait for Neo4j consumer lag to reach zero and for Spark to finish the
   micro-batch.
4. In Neo4j, require `count = count(DISTINCT id)` and no remaining records with
   the previous revision hash.
5. In MongoDB, require exactly one `_id = file_id` document with the new hash.
6. Publish the new revision again and confirm that all counts remain unchanged.
7. Restart Spark with the existing checkpoint and confirm that the committed
   offset is not read again.

## Jupyter Book and GitHub Pages

`_toc.yml`, `_config.yml`, and `book/` contain the report sources. Each chapter
includes the method, executed commands or cells, recorded output, screenshots,
and reflection.

```bash
python3.11 -m venv .venv-report
.venv-report/bin/python -m pip install jupyter-book==0.15.1
.venv-report/bin/jupyter-book build . --all --warningiserror
```

The generated HTML is written to `_build/html`. The
`.github/workflows/ci.yml` workflow tests pull requests and manually dispatched
feature branches. Its Pages job publishes the branch selected for the report.
Configure the repository once:

1. **Settings → Pages**.
2. **Build and deployment → Source → GitHub Actions**.
3. Push the completed merge to `main` and wait for both test and deploy jobs to
   pass.
4. Open `https://caosfourn.github.io/bigdata-lab04-pipeline/` in a private window.
5. Visit every chapter and check its figures, outputs, and links.

Submit the Jupyter Book root URL from step 4 to Moodle. Before submission,
check that the final LeRobot totals in Tasks 3–6 match the committed run data
and that the Kafka, Neo4j, MongoDB, and Spark checkpoint screenshots appear in
their corresponding chapters.
