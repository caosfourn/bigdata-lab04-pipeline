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

## Evidence to capture

- Kafka UI topic list showing all four domain topics and the DLQ.
- Topic descriptions showing partitions, retention and cleanup policy.
- One message from each populated topic, including key, schema version,
  timestamp and partition.

## Reflection

Separate topics let Neo4j and Spark consume only their contracts. Keeping the
connector DLQ separate from parser errors prevents infrastructure failures from
being mistaken for valid domain events.
