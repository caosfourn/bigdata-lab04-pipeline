"""Publish one parsed Python file at a time to the CPG Kafka topics.

The parser remains independent from a Kafka client. This module
is its delivery adapter: it validates the shared event contract, selects a
stable Kafka key, and waits for acknowledgements before moving to the next
file.  Consequently memory remains bounded by one parsed source file.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Protocol

try:  # Package execution: python -m src.kafka_publisher
    from .discovery import discover_python_files
    from .parser_service import CPGParser
    from .schemas import (
        SCHEMA_VERSION,
        TOPIC_EDGES,
        TOPIC_ERRORS,
        TOPIC_METADATA,
        TOPIC_NODES,
    )
except ImportError:  # Direct execution: python src/kafka_publisher.py
    from discovery import discover_python_files
    from parser_service import CPGParser
    from schemas import (
        SCHEMA_VERSION,
        TOPIC_EDGES,
        TOPIC_ERRORS,
        TOPIC_METADATA,
        TOPIC_NODES,
    )


COMMON_REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "event_time",
        "topic",
        "repo_id",
        "file_id",
        "file_path",
        "file_hash",
        "parse_status",
    }
)

TOPIC_REQUIRED_FIELDS: dict[str, frozenset[str]] = {
    TOPIC_NODES: frozenset({"node_id", "label", "properties"}),
    TOPIC_EDGES: frozenset(
        {"edge_id", "type", "source_id", "target_id", "properties"}
    ),
    TOPIC_METADATA: frozenset(
        {
            "file_size_bytes",
            "total_nodes",
            "total_edges",
            "parser_version",
            "parse_duration_ms",
        }
    ),
    TOPIC_ERRORS: frozenset({"error_type", "error_message"}),
}


class EventContractError(ValueError):
    """Raised before publication when the Parser/Kafka contract is violated."""


class ProducerLike(Protocol):
    """Small protocol implemented by kafka-python and the unit-test fake."""

    def send(self, topic: str, key: str, value: dict[str, Any]) -> Any: ...

    def flush(self, timeout: float | None = None) -> Any: ...

    def close(self, timeout: float | None = None) -> Any: ...


@dataclass(frozen=True)
class PublishResult:
    file_path: str
    file_id: str
    nodes: int
    edges: int
    metadata: int
    errors: int


def validate_event(event: dict[str, Any], expected_topic: str) -> None:
    """Validate fields that every downstream consumer relies upon."""

    if expected_topic not in TOPIC_REQUIRED_FIELDS:
        raise EventContractError(f"Unsupported topic: {expected_topic}")

    missing = (COMMON_REQUIRED_FIELDS | TOPIC_REQUIRED_FIELDS[expected_topic]) - event.keys()
    if missing:
        raise EventContractError(
            f"{expected_topic} event is missing fields: {sorted(missing)}"
        )
    if event["topic"] != expected_topic:
        raise EventContractError(
            f"Event declares topic {event['topic']!r}, expected {expected_topic!r}"
        )
    if event["schema_version"] != SCHEMA_VERSION:
        raise EventContractError(
            f"Unsupported schema_version {event['schema_version']!r}; "
            f"publisher expects {SCHEMA_VERSION!r}"
        )
    for field in ("repo_id", "file_id", "file_path"):
        if not isinstance(event[field], str) or not event[field]:
            raise EventContractError(f"{field} must be a non-empty string")
    if not isinstance(event["file_hash"], str):
        raise EventContractError("file_hash must be a string")

    timestamp = event["event_time"]
    if not isinstance(timestamp, str) or not timestamp.endswith("Z"):
        raise EventContractError("event_time must be an RFC 3339 UTC string ending in Z")
    try:
        parsed = dt.datetime.fromisoformat(timestamp.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise EventContractError(f"Invalid event_time: {timestamp!r}") from exc
    if parsed.tzinfo is None:
        raise EventContractError("event_time must include a timezone")


def message_key(topic: str, event: dict[str, Any]) -> str:
    """Return the stable key used for partitioning and log compaction."""

    if topic == TOPIC_NODES:
        return str(event["node_id"])
    if topic == TOPIC_EDGES:
        return str(event["edge_id"])
    return str(event["file_id"])


def _assert_unique(events: Iterable[dict[str, Any]], id_field: str) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for event in events:
        element_id = str(event[id_field])
        if element_id in seen:
            duplicates.add(element_id)
        seen.add(element_id)
    if duplicates:
        sample = ", ".join(sorted(duplicates)[:3])
        raise EventContractError(
            f"Parser emitted duplicate {id_field} values ({len(duplicates)} IDs); "
            f"examples: {sample}"
        )


class CPGKafkaPublisher:
    """Parse and synchronously acknowledge one source file per call."""

    def __init__(self, producer: ProducerLike):
        self.producer = producer

    def publish_file(
        self,
        absolute_path: str,
        repo_root: str,
        repo_id: str | None = None,
    ) -> PublishResult:
        parser = CPGParser(absolute_path, repo_root, repo_id=repo_id)
        nodes, edges, metadata, error = parser.parse()

        _assert_unique(nodes, "node_id")
        _assert_unique(edges, "edge_id")

        batches: tuple[tuple[str, list[dict[str, Any]]], ...] = (
            (TOPIC_NODES, nodes),
            (TOPIC_EDGES, edges),
            (TOPIC_METADATA, [metadata]),
            (TOPIC_ERRORS, [error] if error is not None else []),
        )

        pending: list[Any] = []
        for topic, events in batches:
            for event in events:
                validate_event(event, topic)
                pending.append(
                    self.producer.send(
                        topic,
                        key=message_key(topic, event),
                        value=event,
                    )
                )

        self.producer.flush(timeout=60)
        # kafka-python futures surface broker-side delivery failures via get().
        for future in pending:
            get = getattr(future, "get", None)
            if callable(get):
                get(timeout=30)

        return PublishResult(
            file_path=metadata["file_path"],
            file_id=metadata["file_id"],
            nodes=len(nodes),
            edges=len(edges),
            metadata=1,
            errors=int(error is not None),
        )


class CollectingProducer:
    """In-memory producer used by ``--dry-run`` and unit tests."""

    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict[str, Any]]] = []

    def send(self, topic: str, key: str, value: dict[str, Any]) -> None:
        self.records.append((topic, key, value))
        return None

    def flush(self, timeout: float | None = None) -> None:
        return None

    def close(self, timeout: float | None = None) -> None:
        return None


def build_kafka_producer(bootstrap_servers: str) -> ProducerLike:
    """Construct the real kafka-python producer only when Kafka is requested."""

    try:
        from kafka import KafkaProducer
        from kafka.serializer import DefaultSerializer, SerializeWrapper
    except ImportError as exc:  # pragma: no cover - depends on local environment
        raise RuntimeError(
            "kafka-python is not installed; run `pip install -r requirements.txt`"
        ) from exc

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id="cpg-parser-service-v1",
        key_serializer=DefaultSerializer("utf-8"),
        value_serializer=SerializeWrapper(
            lambda value: json.dumps(
                value, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
        ),
        acks="all",
        retries=10,
        enable_idempotence=True,
        compression_type="gzip",
        linger_ms=10,
    )


def _resolve_files(repo_root: str, requested: list[str]) -> list[str]:
    root = Path(repo_root).resolve()
    if not requested:
        return [item["absolute_path"] for item in discover_python_files(str(root))]

    resolved: list[str] = []
    for item in requested:
        candidate = Path(item)
        if not candidate.is_absolute():
            candidate = root / candidate
        candidate = candidate.resolve()
        if candidate.suffix != ".py" or not candidate.is_file():
            raise ValueError(f"Not a readable Python source file: {candidate}")
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"File is outside repository root: {candidate}") from exc
        resolved.append(str(candidate))
    return resolved


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Parse Python files one at a time and publish CPG events to Kafka."
    )
    parser.add_argument("repo_root", help="Root of the shallow-cloned source repository")
    parser.add_argument(
        "files",
        nargs="*",
        help="Paths relative to repo_root; omit to process all discovered .py files",
    )
    parser.add_argument(
        "--repo-id",
        help="Stable namespace such as huggingface/lerobot (default: repo directory name)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and count events without contacting Kafka",
    )
    args = parser.parse_args(argv)

    repo_root = str(Path(args.repo_root).resolve())
    files = _resolve_files(repo_root, args.files)
    producer: ProducerLike = (
        CollectingProducer()
        if args.dry_run
        else build_kafka_producer(args.bootstrap_servers)
    )
    publisher = CPGKafkaPublisher(producer)

    try:
        for file_path in files:
            result = publisher.publish_file(file_path, repo_root, repo_id=args.repo_id)
            print(json.dumps(asdict(result), ensure_ascii=False))
    finally:
        producer.close(timeout=30)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
