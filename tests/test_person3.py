import unittest

from src.spark_metadata_consumer import build_schema_payload, build_spark_reader_options
from src.spark_mongo_sink import build_mongo_write_config
from src.schemas import make_metadata_event


class Person3HelperTests(unittest.TestCase):
    def test_kafka_reader_options(self):
        options = build_spark_reader_options("kafka:9092", "cpg.metadata")
        self.assertEqual(options["kafka.bootstrap.servers"], "kafka:9092")
        self.assertEqual(options["subscribe"], "cpg.metadata")
        self.assertEqual(options["startingOffsets"], "earliest")

    def test_kafka_reader_rejects_blank_required_values(self):
        with self.assertRaises(ValueError):
            build_spark_reader_options("", "cpg.metadata")
        with self.assertRaises(ValueError):
            build_spark_reader_options("kafka:9092", " ")
        with self.assertRaises(ValueError):
            build_spark_reader_options("kafka:9092", "cpg.metadata", "")

    def test_schema_matches_metadata_event_contract(self):
        schema = build_schema_payload()
        event = make_metadata_event("src/app.py", 10, "abc", 1, 2, 3, 4, 5)
        self.assertEqual(set(schema), set(event))
        self.assertEqual(schema["schema_version"], "string")
        self.assertEqual(schema["event_time"], "string")
        self.assertEqual(schema["topic"], "string")
        self.assertEqual(
            schema["total_edges"],
            {"ast": "long", "cfg": "long", "dfg": "long", "call": "long"},
        )

    def test_mongo_config_uses_upsert_by_stable_id(self):
        config = build_mongo_write_config("mongodb://mongo:27017", "cpg", "metadata")
        self.assertEqual(config["spark.mongodb.write.connection.uri"], "mongodb://mongo:27017")
        self.assertEqual(config["spark.mongodb.write.operationType"], "replace")
        self.assertEqual(config["spark.mongodb.write.idFieldList"], "_id")
        self.assertEqual(config["spark.mongodb.write.upsertDocument"], "true")

    def test_mongo_config_rejects_blank_required_values(self):
        with self.assertRaises(ValueError):
            build_mongo_write_config("", "cpg", "metadata")
        with self.assertRaises(ValueError):
            build_mongo_write_config("mongodb://mongo:27017", "", "metadata")
        with self.assertRaises(ValueError):
            build_mongo_write_config("mongodb://mongo:27017", "cpg", " ")


if __name__ == "__main__":
    unittest.main()
