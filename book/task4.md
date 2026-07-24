# Task 4 — Direct Neo4j ingestion

`connectors/neo4j-cpg-sink.json` configures Kafka Connect to consume node and
edge topics directly. Spark is not present in this path.

```powershell
.\scripts\register_neo4j_connector.ps1
```

Create constraints from `scripts/neo4j_constraints.cypher`, then verify:

```cypher
MATCH (node:CodeNode) RETURN count(node);
MATCH ()-[edge:CPG_EDGE]->()
RETURN edge.type, count(edge)
ORDER BY edge.type;
MATCH (node:CodeNode)
WITH node.node_id AS id, count(*) AS copies
WHERE copies > 1
RETURN id, copies;
```

The final query must return no rows.

## Evidence to capture

- Kafka Connect status with task state `RUNNING`.
- Neo4j constraints.
- Counts grouped by AST/CFG/DFG/CALL edge type.
- Neo4j Browser visualization for one source file.

## Reflection

Edges are Neo4j relationships, not intermediary `CodeEdge` nodes. `MERGE` on
stable node and relationship IDs makes replay idempotent.
