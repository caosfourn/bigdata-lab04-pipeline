import hashlib
import json
import os
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None

if SparkSession is not None:
    from src.metadata_streaming_job import build_metadata_schema, transform_metadata


@unittest.skipIf(SparkSession is None, "pyspark is not installed")
class Person3SparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("person3-tests").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_metadata_schema_declares_every_field_non_nullable(self):
        schema = build_metadata_schema()
        self.assertTrue(all(not field.nullable for field in schema.fields))
        total_edges = schema["total_edges"].dataType
        self.assertTrue(all(not field.nullable for field in total_edges.fields))

    def test_transform_filters_invalid_payload_and_builds_stable_id(self):
        valid = {
            "schema_version": "1.0",
            "event_time": "2026-07-21T00:00:00Z",
            "topic": "cpg.metadata",
            "file_path": "src/app.py",
            "file_hash": "abc",
            "file_size_bytes": 10,
            "total_nodes": 2,
            "total_edges": {"ast": 1, "cfg": 0, "dfg": 0, "call": 0},
            "parser_version": "ast-stdlib",
            "parse_duration_ms": 1.5,
        }
        missing_nested_field = {**valid, "file_path": "src/bad.py", "total_edges": {"ast": 1}}
        wrong_topic = {**valid, "file_path": "src/wrong.py", "topic": "cpg.nodes"}
        kafka_df = self.spark.createDataFrame(
            [(json.dumps(valid),), (json.dumps(missing_nested_field),), (json.dumps(wrong_topic),), ("not-json",)],
            ["value"],
        )

        rows = transform_metadata(kafka_df, build_metadata_schema(), "cpg.metadata").collect()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["file_path"], "src/app.py")
        expected_id = hashlib.sha256(b"src/app.py").hexdigest()
        self.assertEqual(rows[0]["_id"], expected_id)


if __name__ == "__main__":
    unittest.main()
