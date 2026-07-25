# Task 3 — Kafka topic design

The event contract defines four independent domain topics:

| Topic | Stable message key | Content |
|---|---|---|
| `cpg.nodes` | `node_id` | AST node events |
| `cpg.edges` | `edge_id` | AST, CFG, DFG and call edges |
| `cpg.metadata` | `file_id` | Current source-file metadata |
| `cpg.errors` | `file_id` | Parser failures |

`cpg.neo4j.dlq` is a separate operational topic for Kafka Connect failures.
Every domain event contains `schema_version`, UTC `event_time`, `repo_id`,
`file_id`, `file_path`, `file_hash` and `parse_status`.

Create topics and publish one file:

```bash
docker compose up -d --build
python -m src.kafka_publisher lerobot \
  src/lerobot/__init__.py \
  --repo-id huggingface/lerobot
```

The producer uses `acks=all`, idempotent delivery and stable keys. Kafka log
compaction reduces old exact duplicates; downstream IDs and database upserts
are the actual replay-idempotency mechanism.

## Executed Evidence (member-2 integration run, 2026-07-21)

Environment: local Docker Compose, Confluent Kafka/Connect 7.8.0.
Full log in [`docs/evidence/task3_task4_e2e.md`](https://github.com/caosfourn/bigdata-lab04-pipeline/blob/main/docs/evidence/task3_task4_e2e.md) (excluded from the built book — see the repo directly).

Broker-reported configuration for `cpg.nodes`:

```text
PartitionCount: 3
ReplicationFactor: 1
cleanup.policy=compact,delete
retention.ms=604800000
```

The bootstrap script (`infra/kafka/create-topics.sh`) applied this to all
four domain topics and created the separate `cpg.neo4j.dlq` operational
topic, which stayed at end offset `0` for the whole integration run.

### Screenshots to attach (pending)

| Evidence | File to add | Status |
|---|---|---|
| Kafka UI — topic list (4 domain + DLQ) | `docs/evidence/kafka_ui_topics.png` | ⬜ not yet captured |
| Kafka UI — one sample message per topic (key, schema_version, timestamp) | `docs/evidence/kafka_ui_sample_messages.png` | ⬜ not yet captured |

## Reflection

**What worked:** separate topics let Neo4j (via Connect) and Spark consume
only their own contract, so a schema change to `cpg.metadata` cannot break
graph ingestion. Keeping the connector DLQ (`cpg.neo4j.dlq`) separate from
parser-level errors (`cpg.errors`) meant the team could tell infrastructure
failures (bad connector config) apart from legitimate domain events (a file
that failed to parse) — the DLQ staying at offset `0` throughout the run was
itself useful evidence that no message ever failed delivery to Neo4j.

**What to still verify:** partition count and retention were checked once
against the local environment; they must be re-confirmed after the final
`docker compose up -d --build` against the Moodle-selected repository before
submission, since topic auto-creation defaults can silently override the
bootstrap script if it is skipped.
