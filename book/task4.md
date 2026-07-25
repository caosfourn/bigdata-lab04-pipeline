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

## Executed Evidence (member-2 integration run, 2026-07-21)

Environment: local Docker Compose, Neo4j Community 5.26, Neo4j Kafka
Connector 5.5.0. Full log in
[`docs/evidence/task3_task4_e2e.md`](https://github.com/caosfourn/bigdata-lab04-pipeline/blob/main/docs/evidence/task3_task4_e2e.md) (excluded from the built book — see the repo directly).

Connector health:

```json
{
  "name": "cpg-neo4j-sink",
  "connector": {"state": "RUNNING", "worker_id": "connect:8083"},
  "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "connect:8083"}],
  "type": "sink"
}
```

**Exact replay** — `src/schemas.py` published twice without modification:

```text
nodes=467, distinct_node_ids=467
edges=497, distinct_edge_ids=497
placeholder_nodes=0
```

**Modified-file replay** — `tests/fixtures/replay_sample.py` published, then
edited to remove a helper, then republished:

| Revision | Parser nodes | Parser edges | Neo4j nodes | Neo4j edges |
|---|---:|---:|---:|---:|
| Before edit | 35 | 40 | 35 | 40 |
| After edit | 23 | 26 | 23 | 26 |

After reconciliation, every remaining node/edge belonged to exactly one
current `file_hash` — stale elements from the previous revision were gone:

```text
file, status, expected_nodes, nodes, edges
"replay_sample.py", "success", 23, 23, 26
"src/schemas.py", "success", 467, 467, 497
```

### Screenshots to attach (pending)

| Evidence | File to add | Status |
|---|---|---|
| Kafka Connect status JSON (`RUNNING`/`RUNNING`) | `docs/evidence/connect_status.png` | ⬜ not yet captured |
| Neo4j Browser — constraints list | `docs/evidence/neo4j_constraints.png` | ⬜ not yet captured |
| Neo4j Browser — graph visualization for one source file | `docs/evidence/neo4j_graph_view.png` | ⬜ not yet captured |

## Reflection

**What worked:** cross-topic delivery order between `cpg.nodes` and
`cpg.edges` is not guaranteed by Kafka, so edge ingestion first creates safe
endpoint placeholder nodes; later node events fill in the real properties.
Combined with the `node_id`/`edge_id` uniqueness constraints and Cypher
`MERGE`, this made at-least-once delivery (exact replay and modified-file
replay) idempotent in both tested scenarios above — node/edge counts matched
the parser output exactly with zero duplicates.

**Open risk, not fully verified:** the modified-file scenario relies on
metadata-driven reconciliation removing stale nodes/edges from the previous
`file_hash`. The evidence log confirms the *end state* was correct (no stale
elements remained after the run), but the exact ordering guarantee between
"new nodes/edges ingested" and "reconciliation runs" is not yet documented
in code or tests — this should be verified explicitly (e.g. with a
deliberately slow/delayed node ingestion) before relying on it under load.
