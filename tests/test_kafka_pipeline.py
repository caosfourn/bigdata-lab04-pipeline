from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from src.kafka_publisher import (
    COMMON_REQUIRED_FIELDS,
    CPGKafkaPublisher,
    CollectingProducer,
    EventContractError,
    message_key,
    validate_event,
)
from src.parser_service import CPGParser
from src.schemas import TOPIC_EDGES, TOPIC_ERRORS, TOPIC_METADATA, TOPIC_NODES
from src.topic_config import REQUIRED_ASSIGNMENT_TOPICS, TOPIC_SPECS


ROOT = Path(__file__).resolve().parents[1]


class ParserContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo = Path(self.temp_dir.name)
        self.source = self.repo / "sample.py"
        self.source.write_text(
            "def target(value):\n"
            "    return value\n"
            "\n"
            "def caller(item):\n"
            "    first = target(item)\n"
            "    second = target(first)\n"
            "    print(second)\n"
            "    return second\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def parse(self):
        return CPGParser(
            str(self.source), str(self.repo), repo_id="example/project"
        ).parse()

    def test_ids_are_unique_and_stable(self) -> None:
        nodes_1, edges_1, metadata_1, error_1 = self.parse()
        nodes_2, edges_2, metadata_2, error_2 = self.parse()

        node_ids_1 = [event["node_id"] for event in nodes_1]
        edge_ids_1 = [event["edge_id"] for event in edges_1]
        self.assertEqual(len(node_ids_1), len(set(node_ids_1)))
        self.assertEqual(len(edge_ids_1), len(set(edge_ids_1)))
        self.assertEqual(node_ids_1, [event["node_id"] for event in nodes_2])
        self.assertEqual(edge_ids_1, [event["edge_id"] for event in edges_2])
        self.assertEqual(metadata_1["file_id"], metadata_2["file_id"])
        self.assertEqual(metadata_1["file_hash"], metadata_2["file_hash"])
        self.assertIsNone(error_1)
        self.assertIsNone(error_2)

    def test_multiple_call_sites_get_distinct_edges(self) -> None:
        _, edges, _, _ = self.parse()
        calls = [event for event in edges if event["type"] == "CALL"]
        self.assertEqual(2, len(calls))
        self.assertEqual(2, len({event["edge_id"] for event in calls}))
        self.assertEqual(2, len({event["properties"]["call_site_id"] for event in calls}))

    def test_every_event_matches_common_contract(self) -> None:
        nodes, edges, metadata, error = self.parse()
        batches = (
            (TOPIC_NODES, nodes),
            (TOPIC_EDGES, edges),
            (TOPIC_METADATA, [metadata]),
        )
        for topic, events in batches:
            for event in events:
                self.assertTrue(COMMON_REQUIRED_FIELDS.issubset(event))
                self.assertTrue(event["event_time"].endswith("Z"))
                validate_event(event, topic)
        self.assertIsNone(error)

    def test_missing_parse_status_is_rejected_as_a_common_contract_error(self) -> None:
        nodes, _, _, _ = self.parse()
        invalid = dict(nodes[0])
        invalid.pop("parse_status")

        with self.assertRaisesRegex(EventContractError, "parse_status"):
            validate_event(invalid, TOPIC_NODES)

    def test_syntax_error_is_a_valid_error_and_metadata_event(self) -> None:
        self.source.write_text("def broken(:\n", encoding="utf-8")
        nodes, edges, metadata, error = self.parse()
        self.assertEqual([], nodes)
        self.assertEqual([], edges)
        self.assertEqual("error", metadata["parse_status"])
        self.assertIsNotNone(error)
        validate_event(metadata, TOPIC_METADATA)
        validate_event(error, TOPIC_ERRORS)

    def test_emitted_events_validate_against_json_schema(self) -> None:
        try:
            from jsonschema import Draft202012Validator
            from referencing import Registry, Resource
        except ImportError:
            self.skipTest("jsonschema is installed by requirements.txt")

        schema_dir = ROOT / "config/schemas"
        documents = {
            path.name: json.loads(path.read_text(encoding="utf-8"))
            for path in schema_dir.glob("*.schema.json")
        }
        registry = Registry()
        for document in documents.values():
            Draft202012Validator.check_schema(document)
            registry = registry.with_resource(
                document["$id"], Resource.from_contents(document)
            )

        nodes, edges, metadata, _ = self.parse()
        samples = (
            ("node-event.schema.json", nodes[0]),
            ("edge-event.schema.json", edges[0]),
            ("metadata-event.schema.json", metadata),
        )
        for schema_name, event in samples:
            Draft202012Validator(
                documents[schema_name], registry=registry
            ).validate(event)

        self.source.write_text("def broken(:\n", encoding="utf-8")
        _, _, error_metadata, error = self.parse()
        Draft202012Validator(
            documents["metadata-event.schema.json"], registry=registry
        ).validate(error_metadata)
        Draft202012Validator(
            documents["error-event.schema.json"], registry=registry
        ).validate(error)


class PublisherTests(ParserContractTests):
    def test_publish_routes_all_records_and_uses_stable_keys(self) -> None:
        producer = CollectingProducer()
        result = CPGKafkaPublisher(producer).publish_file(
            str(self.source), str(self.repo), repo_id="example/project"
        )

        self.assertEqual(result.nodes + result.edges + 1, len(producer.records))
        topics = {topic for topic, _, _ in producer.records}
        self.assertEqual({TOPIC_NODES, TOPIC_EDGES, TOPIC_METADATA}, topics)
        for topic, key, event in producer.records:
            self.assertEqual(message_key(topic, event), key)
            validate_event(event, topic)


class InfrastructureContractTests(unittest.TestCase):
    def test_all_required_topics_have_specs(self) -> None:
        configured = {spec.name for spec in TOPIC_SPECS}
        self.assertTrue(REQUIRED_ASSIGNMENT_TOPICS.issubset(configured))
        self.assertEqual(5, len(configured))

    def test_connector_uses_direct_idempotent_cypher(self) -> None:
        config = json.loads(
            (ROOT / "config/connectors/neo4j-sink.json").read_text(encoding="utf-8")
        )
        subscribed = set(config["topics"].split(","))
        self.assertTrue({TOPIC_NODES, TOPIC_EDGES}.issubset(subscribed))
        self.assertEqual(
            "org.neo4j.connectors.kafka.sink.Neo4jConnector",
            config["connector.class"],
        )
        node_query = config["neo4j.cypher.topic.cpg.nodes"]
        edge_query = config["neo4j.cypher.topic.cpg.edges"]
        metadata_query = config["neo4j.cypher.topic.cpg.metadata"]
        self.assertIn("MERGE (node:CPGNode", node_query)
        self.assertIn("MERGE (source:CPGNode", edge_query)
        self.assertIn("MERGE (target:CPGNode", edge_query)
        self.assertIn("MERGE (source)-[edge:CPG_EDGE", edge_query)
        self.assertIn("event.parse_status = 'success'", metadata_query)
        ordering_guard = (
            "current_file IS NULL OR current_file.file_hash = event.file_hash "
            "OR datetime(event.event_time) > datetime(current_file.event_time)"
        )
        for query in (node_query, edge_query):
            self.assertIn(
                "OPTIONAL MATCH (current_file:SourceFile "
                "{file_id: event.file_id})",
                query,
            )
            # First load, late events from the same revision, and a genuinely
            # newer revision are accepted.  Older cross-topic events stop
            # before any graph MERGE.
            self.assertIn(ordering_guard, query)
            self.assertLess(query.index(ordering_guard), query.index("MERGE ("))

        metadata_guard = (
            "file.event_time IS NULL OR "
            "datetime(event.event_time) >= datetime(file.event_time)"
        )
        self.assertIn(metadata_guard, metadata_query)
        # A stale metadata event must stop before it can roll back SourceFile
        # or reconcile (delete) graph elements from a newer revision.
        self.assertLess(metadata_query.index(metadata_guard), metadata_query.index("SET file."))
        self.assertLess(metadata_query.index(metadata_guard), metadata_query.index("DELETE stale_edge"))
        # ``ON CREATE SET`` is a valid MERGE sub-clause; a standalone CREATE
        # pattern would violate replay idempotency.
        self.assertNotIn(" CREATE (", f" {node_query} {edge_query} ")

    def test_constraints_cover_node_edge_and_file_ids(self) -> None:
        cypher = (ROOT / "infra/neo4j/constraints.cypher").read_text(
            encoding="utf-8"
        )
        self.assertIn("node.node_id IS UNIQUE", cypher)
        self.assertIn("edge.edge_id IS UNIQUE", cypher)
        self.assertIn("file.file_id IS UNIQUE", cypher)

    def test_json_schema_files_are_valid_json(self) -> None:
        schema_dir = ROOT / "config/schemas"
        schemas = list(schema_dir.glob("*.schema.json"))
        self.assertEqual(5, len(schemas))
        for path in schemas:
            document = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(
                "https://json-schema.org/draft/2020-12/schema",
                document["$schema"],
            )


if __name__ == "__main__":
    unittest.main()
