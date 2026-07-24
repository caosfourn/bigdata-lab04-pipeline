# Task 3–4 — Kafka topics and idempotent Neo4j sink

## Scope and ownership

Member 2 owns the delivery boundary from Parser Service to Kafka, the topic
bootstrap, Kafka Connect worker, Neo4j service, connector registration, Cypher
mapping, constraints, and verification queries. Member 3 may extend the same
Compose project with Spark and MongoDB, but Spark must not sit between Kafka
and Neo4j.

The local version matrix is intentionally pinned:

| Component | Version |
|---|---:|
| Confluent Kafka / Kafka Connect | 7.8.0 |
| Neo4j Kafka Connector | 5.5.0 |
| Neo4j Community | 5.26 LTS |
| kafka-python | 3.0.8 |

Neo4j documents that Connector 5.5 supports Kafka Connect 3.7+ and Neo4j 5.x
Community. The connector distribution is downloaded from the official GitHub
release and verified by SHA-256 during the image build.

## Topic contract

| Topic | Key | Partitions | Cleanup | Consumer |
|---|---|---:|---|---|
| `cpg.nodes` | `node_id` | 3 | compact + 7-day delete | Neo4j Sink |
| `cpg.edges` | `edge_id` | 3 | compact + 7-day delete | Neo4j Sink |
| `cpg.metadata` | `file_id` | 3 | compact + 30-day delete | Spark → MongoDB; Neo4j reconciliation |
| `cpg.errors` | `file_id` | 1 | 7-day delete | monitoring/report |
| `cpg.neo4j.dlq` | original key | 1 | 14-day delete | connector operations |

The first four topics are the assignment contract. `cpg.neo4j.dlq` is an
additional operational topic. It is different from `cpg.errors`: parser errors
are valid domain events, whereas the DLQ stores records Kafka Connect could not
map or write.

Every domain message contains these fields:

```text
schema_version, event_time, topic, repo_id,
file_id, file_path, file_hash
```

`event_time` is UTC RFC 3339 and ends in `Z`. `repo_id` must be a stable name
such as `huggingface/lerobot`, never an absolute path on a team member's
computer. `file_id`, `node_id`, and `edge_id` are full SHA-256 identifiers.

Three partitions allow independent source files/elements to scale while the
stable keys make exact replays land on the same partition. Compaction reduces
old exact duplicates in Kafka; it is not the database idempotency mechanism.

The machine-readable contracts live under `config/schemas/`, and the canonical
topic settings live in both `src/topic_config.py` and the broker bootstrap
script `infra/kafka/create-topics.sh`.

## Neo4j graph model

The sink uses one fixed node label and one fixed relationship type:

```text
(:CPGNode {node_id, ast_label, ...})
(:CPGNode)-[:CPG_EDGE {edge_id, edge_type, ...}]->(:CPGNode)
(:SourceFile {file_id, file_hash, ...})
```

AST labels and CPG edge types remain properties. This avoids dynamic Cypher
and lets one uniqueness constraint protect every node and every relationship.

Node ingestion uses `MERGE` on `node_id`. Edge ingestion first `MERGE`s both
endpoint nodes as placeholders and then `MERGE`s the relationship on
`edge_id`. This matters because Kafka has ordering only inside a topic
partition, not across `cpg.nodes` and `cpg.edges`. A later node event fills an
internal placeholder; `CALL_EXTERNAL` deliberately resolves to an external
symbol placeholder.

Metadata events are also observed by the Neo4j connector for snapshot
reconciliation. After a successful parse, the Cypher query removes nodes and
edges from the same `file_id` whose `file_hash` belongs to an older revision.
It does not purge a previous valid graph when parsing the new revision fails.
Spark independently consumes the same metadata topic with its own consumer
group for MongoDB.

Kafka Connect is at-least-once in this Community setup. Database uniqueness
constraints plus Cypher `MERGE` make replay idempotent. Connector-level
exactly-once offset tracking is intentionally not enabled because its offset
node requires an Enterprise `NODE KEY`; the lab only requires an idempotent
result.

## Run locally

Docker must be running. From the repository root:

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps
```

`kafka-init`, `neo4j-init`, and `connector-init` are one-shot services. They
create the topics, install constraints/indexes, and idempotently register or
update `cpg-neo4j-sink` through Kafka Connect's REST API.

Check connector health:

```bash
curl -fsS http://localhost:8083/connectors/cpg-neo4j-sink/status | python -m json.tool
```

Publish one file (the required incremental unit):

```bash
python -m src.kafka_publisher lerobot \
  src/lerobot/__init__.py \
  --repo-id huggingface/lerobot
```

Omit the file argument only when a full initial load is intended. Validate all
events without Kafka using `--dry-run`.

Inspect topic samples:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic cpg.nodes \
  --from-beginning \
  --max-messages 1 \
  --property print.key=true
```

Neo4j Browser is available at <http://localhost:7474>. Run the verification
file from the CLI:

```bash
docker compose exec -T neo4j cypher-shell \
  -u neo4j -p "$NEO4J_PASSWORD" \
  -f /dev/stdin < infra/neo4j/verify.cypher
```

## Replay evidence checklist

1. Record connector status and the four topic descriptions.
2. Publish one source file; save one node event and one edge event.
3. Run `verify.cypher`; record total and distinct counts.
4. Publish the unchanged file again; counts and duplicate queries must be
   unchanged/zero.
5. Modify exactly one file and publish only that file. Wait until connector lag
   reaches zero, then run `verify.cypher` again.
6. Capture the Neo4j Browser graph and query results for the Jupyter Book.

Official references:

- [Neo4j Connector compatibility](https://neo4j.com/docs/kafka/current/)
- [Cypher sink strategy](https://neo4j.com/docs/kafka/current/sink/cypher/)
- [Neo4j sink settings](https://neo4j.com/docs/kafka/current/sink/configuration/)
- [Neo4j constraints](https://neo4j.com/docs/cypher-manual/5/constraints/)

