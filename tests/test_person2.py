import unittest

from src.schemas import (
    TOPIC_NODES,
    TOPIC_EDGES,
    TOPIC_METADATA,
    TOPIC_ERRORS,
    make_node_event,
    make_edge_event,
    make_metadata_event,
    make_error_event,
)
from src.neo4j_sink import (
    build_node_merge_cypher,
    build_edge_merge_cypher,
    build_metadata_merge_cypher,
    build_constraint_statements,
)


class Person2ContractTests(unittest.TestCase):
    def test_messages_include_schema_version_and_event_time(self):
        node = make_node_event(
            node_id="node_abc",
            file_path="src/app.py",
            label="FunctionDef",
            node_type="FunctionDef",
            line_number=1,
            col_offset=0,
            end_lineno=3,
            end_col_offset=4,
        )
        self.assertEqual(node["topic"], TOPIC_NODES)
        self.assertIn("schema_version", node)
        self.assertIn("event_time", node)

        edge = make_edge_event(
            edge_id="edge_abc",
            file_path="src/app.py",
            edge_type="AST_CHILD",
            source_id="node_abc",
            target_id="node_def",
        )
        self.assertEqual(edge["topic"], TOPIC_EDGES)
        self.assertIn("schema_version", edge)
        self.assertIn("event_time", edge)

        metadata = make_metadata_event(
            file_path="src/app.py",
            file_size_bytes=120,
            file_hash="hash",
            total_nodes=1,
            total_ast_edges=1,
            total_cfg_edges=0,
            total_dfg_edges=0,
            total_call_edges=0,
        )
        self.assertEqual(metadata["topic"], TOPIC_METADATA)
        self.assertIn("schema_version", metadata)
        self.assertIn("event_time", metadata)

        error = make_error_event(
            file_path="src/app.py",
            error_type="SyntaxError",
            error_message="bad syntax",
        )
        self.assertEqual(error["topic"], TOPIC_ERRORS)
        self.assertIn("schema_version", error)
        self.assertIn("event_time", error)

    def test_neo4j_cypher_uses_merge_and_constraints(self):
        node_cypher = build_node_merge_cypher(
            {
                "node_id": "node_123",
                "file_path": "src/app.py",
                "label": "FunctionDef",
                "properties": {"type": "FunctionDef", "name": "main"},
            }
        )
        self.assertIn("MERGE", node_cypher)
        self.assertIn("node_id", node_cypher)

        edge_cypher = build_edge_merge_cypher(
            {
                "edge_id": "edge_123",
                "type": "AST_CHILD",
                "source_id": "node_123",
                "target_id": "node_456",
                "properties": {"child_type": "Assign"},
            }
        )
        self.assertIn("MERGE", edge_cypher)
        self.assertIn("edge_id", edge_cypher)
        self.assertIn("CPG_EDGE", edge_cypher)
        self.assertIn("source:CodeNode", edge_cypher)
        self.assertIn("target:CodeNode", edge_cypher)

        metadata_cypher = build_metadata_merge_cypher(
            {
                "file_path": "src/app.py",
                "file_hash": "abc",
                "total_nodes": 1,
                "parser_version": "ast-stdlib-3.x",
            }
        )
        self.assertIn("MERGE", metadata_cypher)
        self.assertIn("file_path", metadata_cypher)

        constraints = build_constraint_statements()
        self.assertTrue(any("CREATE CONSTRAINT" in stmt for stmt in constraints))
        self.assertTrue(any("node_id" in stmt for stmt in constraints))


if __name__ == "__main__":
    unittest.main()
