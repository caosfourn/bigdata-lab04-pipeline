// Stable element IDs make every write below safe to replay.
CREATE CONSTRAINT cpg_node_id_unique IF NOT EXISTS
FOR (node:CPGNode) REQUIRE node.node_id IS UNIQUE;

CREATE CONSTRAINT cpg_edge_id_unique IF NOT EXISTS
FOR ()-[edge:CPG_EDGE]-() REQUIRE edge.edge_id IS UNIQUE;

CREATE CONSTRAINT source_file_id_unique IF NOT EXISTS
FOR (file:SourceFile) REQUIRE file.file_id IS UNIQUE;

// These indexes make file-snapshot reconciliation bounded to one source file.
CREATE INDEX cpg_node_file_id IF NOT EXISTS
FOR (node:CPGNode) ON (node.file_id);

CREATE INDEX cpg_edge_file_id IF NOT EXISTS
FOR ()-[edge:CPG_EDGE]-() ON (edge.file_id);

