# Task 3–4 end-to-end evidence

Execution date: 2026-07-21  
Environment: local Docker Compose, Neo4j Community 5.26, Neo4j Kafka
Connector 5.5.0, Confluent Kafka/Connect 7.8.0.

This file records the text evidence from the member-2 integration run. The
Jupyter Book should execute the same commands against the Moodle-selected
repository and add screenshots from Neo4j Browser.

## Kafka topic

The broker reported this configuration for `cpg.nodes`:

```text
PartitionCount: 3
ReplicationFactor: 1
cleanup.policy=compact,delete
retention.ms=604800000
```

The bootstrap script applies the documented settings to all four required
domain topics and creates the separate `cpg.neo4j.dlq` operational topic.

## Connector health

```json
{
  "name": "cpg-neo4j-sink",
  "connector": {"state": "RUNNING", "worker_id": "connect:8083"},
  "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "connect:8083"}],
  "type": "sink"
}
```

The DLQ end offset was zero after the integration run:

```text
cpg.neo4j.dlq:0:0
```

## Exact replay

`src/schemas.py` was published twice without modification. Both Neo4j checks
returned the same result:

```text
nodes=467, distinct_node_ids=467
edges=497, distinct_edge_ids=497
placeholder_nodes=0
```

Therefore an at-least-once delivery/replay did not duplicate graph elements.

## Modified-file replay

`tests/fixtures/replay_sample.py` was first published with an extra helper and
then edited to remove that helper before publishing only that file again.

| Revision | Parser nodes | Parser edges | Neo4j nodes | Neo4j edges |
|---|---:|---:|---:|---:|
| Before edit | 35 | 40 | 35 | 40 |
| After edit | 23 | 26 | 23 | 26 |

After reconciliation, all remaining nodes and edges had exactly one current
`file_hash`; stale elements from the previous revision were absent. The final
database query returned:

```text
file, status, expected_nodes, nodes, edges
"replay_sample.py", "success", 23, 23, 26
"src/schemas.py", "success", 467, 467, 497
```

This evidence covers Kafka-to-Neo4j behavior. MongoDB state and Spark
checkpoint-offset evidence belong to Task 5–6 and must be supplied by members
3 and 4.
