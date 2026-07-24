"""Minimal Spark Structured Streaming consumer for metadata events."""

from __future__ import annotations

from typing import Any


def build_spark_reader_options(brokers: str, topic: str, starting_offsets: str = "earliest") -> dict[str, Any]:
    """Return Kafka options for reading metadata events into Spark."""
    if not brokers.strip():
        raise ValueError("brokers must not be empty")
    if not topic.strip():
        raise ValueError("topic must not be empty")
    if not starting_offsets.strip():
        raise ValueError("starting_offsets must not be empty")

    return {
        "kafka.bootstrap.servers": brokers,
        "subscribe": topic,
        "startingOffsets": starting_offsets,
        "failOnDataLoss": "false",
        "includeHeaders": "true",
    }


def build_schema_payload() -> dict[str, Any]:
    """Return the expected JSON payload schema for metadata events."""
    return {
        "schema_version": "string",
        "event_time": "string",
        "topic": "string",
        "file_path": "string",
        "file_hash": "string",
        "file_size_bytes": "long",
        "total_nodes": "long",
        "total_edges": {
            "ast": "long",
            "cfg": "long",
            "dfg": "long",
            "call": "long",
        },
        "parser_version": "string",
        "parse_duration_ms": "double",
    }
