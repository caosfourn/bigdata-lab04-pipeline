"""Spark Structured Streaming job to consume cpg.metadata from Kafka and write to MongoDB."""

from __future__ import annotations

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sha2
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

try:
    from .spark_metadata_consumer import build_schema_payload, build_spark_reader_options
    from .spark_mongo_sink import build_mongo_write_config
except ImportError:  # Direct execution through spark-submit.
    from spark_metadata_consumer import build_schema_payload, build_spark_reader_options
    from spark_mongo_sink import build_mongo_write_config


def build_metadata_schema() -> StructType:
    """Convert the shared metadata contract into a non-null Spark schema."""
    type_map = {
        "string": StringType,
        "long": LongType,
        "double": DoubleType,
    }

    def to_struct(payload: dict) -> StructType:
        fields = []
        for name, value in payload.items():
            data_type = to_struct(value) if isinstance(value, dict) else type_map[value]()
            fields.append(StructField(name, data_type, nullable=False))
        return StructType(fields)

    return to_struct(build_schema_payload())


def build_spark_session(app_name: str, mongo_config: dict[str, str]) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for key, value in mongo_config.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def transform_metadata(kafka_df, metadata_schema: StructType, topic: str):
    """Parse valid metadata records and attach a deterministic MongoDB _id."""
    return (
        kafka_df
        .select(from_json(col("value").cast("string"), metadata_schema).alias("payload"))
        .select("payload.*")
        .filter(
            col("schema_version").isNotNull()
            & col("event_time").isNotNull()
            & (col("topic") == topic)
            & col("file_path").isNotNull()
            & col("file_hash").isNotNull()
            & col("file_size_bytes").isNotNull()
            & col("total_nodes").isNotNull()
            & col("total_edges").isNotNull()
            & col("total_edges.ast").isNotNull()
            & col("total_edges.cfg").isNotNull()
            & col("total_edges.dfg").isNotNull()
            & col("total_edges.call").isNotNull()
            & col("parser_version").isNotNull()
            & col("parse_duration_ms").isNotNull()
        )
        # One current metadata document per source file. A changed file replaces
        # the previous document instead of creating a second version.
        .withColumn("_id", sha2(col("file_path"), 256))
    )


def run_stream(
    brokers: str,
    topic: str,
    mongo_uri: str,
    mongo_db: str,
    mongo_collection: str,
    checkpoint_location: str,
    starting_offsets: str = "earliest",
) -> None:
    spark = build_spark_session(
        app_name="cpg_metadata_stream",
        mongo_config=build_mongo_write_config(mongo_uri, mongo_db, mongo_collection),
    )

    metadata_schema = build_metadata_schema()

    reader = spark.readStream.format("kafka")
    for key, value in build_spark_reader_options(brokers, topic, starting_offsets).items():
        reader = reader.option(key, value)
    kafka_df = reader.load()

    json_df = transform_metadata(kafka_df, metadata_schema, topic)

    query = (
        json_df.writeStream
        .format("mongodb")
        .option("checkpointLocation", checkpoint_location)
        .option("spark.mongodb.connection.uri", mongo_uri)
        .option("spark.mongodb.database", mongo_db)
        .option("spark.mongodb.collection", mongo_collection)
        .option("operationType", "replace")
        .option("idFieldList", "_id")
        .option("upsertDocument", "true")
        .option("writeConcern.w", "majority")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming job for cpg.metadata.")
    parser.add_argument("--brokers", required=True, help="Kafka bootstrap servers, e.g. localhost:9092")
    parser.add_argument("--topic", default="cpg.metadata", help="Kafka topic for metadata events")
    parser.add_argument("--mongo-uri", required=True, help="MongoDB connection URI")
    parser.add_argument("--mongo-db", default="cpg", help="MongoDB database name")
    parser.add_argument("--mongo-collection", default="metadata", help="MongoDB collection name")
    parser.add_argument("--checkpoint-location", required=True, help="Spark checkpoint location")
    parser.add_argument("--starting-offsets", default="earliest", help="Kafka startingOffsets")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_stream(
        brokers=args.brokers,
        topic=args.topic,
        mongo_uri=args.mongo_uri,
        mongo_db=args.mongo_db,
        mongo_collection=args.mongo_collection,
        checkpoint_location=args.checkpoint_location,
        starting_offsets=args.starting_offsets,
    )
