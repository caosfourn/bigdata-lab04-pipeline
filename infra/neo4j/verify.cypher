// Expected: total_nodes = distinct_node_ids and duplicate_node_ids = 0.
MATCH (node:CPGNode)
WHERE coalesce(node.external, false) = false
RETURN count(node) AS total_nodes,
       count(DISTINCT node.node_id) AS distinct_node_ids;

MATCH (node:CPGNode)
WITH node.node_id AS node_id, count(*) AS copies
WHERE copies > 1
RETURN count(*) AS duplicate_node_ids;

// Expected: total_edges = distinct_edge_ids and duplicate_edge_ids = 0.
MATCH ()-[edge:CPG_EDGE]->()
RETURN count(edge) AS total_edges,
       count(DISTINCT edge.edge_id) AS distinct_edge_ids;

MATCH ()-[edge:CPG_EDGE]->()
WITH edge.edge_id AS edge_id, count(*) AS copies
WHERE copies > 1
RETURN count(*) AS duplicate_edge_ids;

// Expected after the connector catches up: zero unresolved internal endpoints.
MATCH (node:CPGNode {placeholder: true})
WHERE coalesce(node.external, false) = false
RETURN count(node) AS unresolved_internal_placeholders;

