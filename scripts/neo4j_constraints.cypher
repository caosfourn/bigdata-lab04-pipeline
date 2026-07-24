CREATE CONSTRAINT node_id_unique IF NOT EXISTS
FOR (node:CodeNode) REQUIRE node.node_id IS UNIQUE;

CREATE CONSTRAINT metadata_file_unique IF NOT EXISTS
FOR (file:CodeFile) REQUIRE file.file_path IS UNIQUE;
