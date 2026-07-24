"""Helpers for Person 3: Spark Structured Streaming -> MongoDB sink."""

from __future__ import annotations

from typing import Any


def build_mongo_write_config(uri: str, database: str, collection: str) -> dict[str, Any]:
    """Return SparkConf defaults for idempotent MongoDB writes."""
    if not uri.strip():
        raise ValueError("uri must not be empty")
    if not database.strip():
        raise ValueError("database must not be empty")
    if not collection.strip():
        raise ValueError("collection must not be empty")

    return {
        "spark.mongodb.write.connection.uri": uri,
        "spark.mongodb.write.database": database,
        "spark.mongodb.write.collection": collection,
        "spark.mongodb.write.operationType": "replace",
        "spark.mongodb.write.idFieldList": "_id",
        "spark.mongodb.write.upsertDocument": "true",
        "spark.mongodb.write.writeConcern.w": "majority",
    }
