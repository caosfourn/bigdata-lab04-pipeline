# Task 4 — Direct Neo4j ingestion

`config/connectors/neo4j-sink.json` configures Kafka Connect to consume node
and edge topics directly. Spark is not present in this path. Metadata is also
observed only to reconcile stale elements after a successful file revision.

```bash
curl -fsS http://localhost:8083/connectors/cpg-neo4j-sink/status
docker compose exec -T neo4j cypher-shell \
  -u neo4j -p cpg-password -f /dev/stdin < infra/neo4j/verify.cypher
```

The graph model uses `:CPGNode` and `:CPG_EDGE`. Constraints enforce uniqueness
for `node_id`, `edge_id` and `file_id`; Cypher uses `MERGE` for nodes,
relationship endpoints and relationships.

```cypher
MATCH (node:CPGNode)
RETURN count(node), count(DISTINCT node.node_id);

MATCH ()-[edge:CPG_EDGE]->()
RETURN edge.edge_type, count(edge), count(DISTINCT edge.edge_id)
ORDER BY edge.edge_type;
```

On an exact replay, totals must remain unchanged. After a modified-file replay,
successful metadata reconciliation removes nodes and edges from the same
`file_id` whose `file_hash` belongs to the previous revision.

## Evidence to capture

- Kafka Connect status with connector and task state `RUNNING`.
- Neo4j constraints.
- Before/replay/after counts and duplicate queries.
- Neo4j Browser visualization for one source file.

## Reflection

Cross-topic delivery order is not guaranteed, so edge ingestion creates safe
endpoint placeholders. Later node events fill them, while uniqueness
constraints and `MERGE` keep at-least-once delivery idempotent.
