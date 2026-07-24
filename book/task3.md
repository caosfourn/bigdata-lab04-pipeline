# Task 3 — Kafka topic design

The event contract defines four independent topics:

| Topic | Stable message key | Content |
|---|---|---|
| `cpg.nodes` | `node_id` | AST node events |
| `cpg.edges` | `edge_id` | AST, CFG, DFG and call edges |
| `cpg.metadata` | `file_path` | Current source-file metadata |
| `cpg.errors` | `file_path` | Parser and connector failures |

Create the topics and publish:

```powershell
.\scripts\create_topics.ps1
python -m src.kafka_producer lerobot `
  --file lerobot/src/lerobot/__init__.py `
  --brokers localhost:9092
```

The producer uses `acks=all` and retries. Stable keys preserve entity ordering
within a partition; downstream stable IDs and upserts make replay idempotent.

## Evidence to capture

- Kafka UI topic list showing all four topics.
- One message from each populated topic.
- Key, schema version, timestamp and partition for sample events.

## Reflection

Separate topics allow Neo4j and Spark to consume only their contracts. Error
events remain observable without blocking valid files.
