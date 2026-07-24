"""Produce parser events to the four CPG Kafka topics."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Iterable

try:
    from .discovery import discover_python_files
    from .parser_service import CPGParser
    from .schemas import TOPIC_EDGES, TOPIC_ERRORS, TOPIC_METADATA, TOPIC_NODES
except ImportError:  # Support direct execution.
    from discovery import discover_python_files
    from parser_service import CPGParser
    from schemas import TOPIC_EDGES, TOPIC_ERRORS, TOPIC_METADATA, TOPIC_NODES

LOGGER = logging.getLogger("cpg.kafka_producer")


def event_key(event: dict) -> str:
    """Return a stable Kafka key so equal entities share a partition."""
    return str(
        event.get("node_id")
        or event.get("edge_id")
        or event.get("file_path")
        or "unknown"
    )


def build_producer(bootstrap_servers: str):
    """Create a JSON Kafka producer lazily so parser-only use needs no Kafka."""
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError(
            "kafka-python is required; run: pip install -r requirements.txt"
        ) from exc

    return KafkaProducer(
        bootstrap_servers=[item.strip() for item in bootstrap_servers.split(",")],
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8"),
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=5,
    )


def publish_file(producer, file_path: str | Path, repo_root: str | Path) -> dict:
    """Parse and publish one file, keeping the unit of work bounded to that file."""
    parser = CPGParser(str(file_path), str(repo_root))
    nodes, edges, metadata, error = parser.parse()

    futures = []
    if error:
        futures.append(
            producer.send(TOPIC_ERRORS, key=event_key(error), value=error)
        )
    else:
        futures.extend(
            producer.send(TOPIC_NODES, key=event_key(event), value=event)
            for event in nodes
        )
        futures.extend(
            producer.send(TOPIC_EDGES, key=event_key(event), value=event)
            for event in edges
        )
        futures.append(
            producer.send(TOPIC_METADATA, key=event_key(metadata), value=metadata)
        )

    for future in futures:
        future.get(timeout=30)

    return {
        "file_path": str(file_path),
        "nodes": len(nodes),
        "edges": len(edges),
        "error": error,
    }


def iter_selected_files(repo_root: Path, single_file: Path | None) -> Iterable[Path]:
    if single_file:
        resolved = single_file.resolve()
        if not resolved.is_file() or resolved.suffix != ".py":
            raise ValueError(f"Not a Python file: {single_file}")
        yield resolved
        return

    for item in discover_python_files(repo_root):
        yield Path(item["absolute_path"])


def main() -> int:
    parser = argparse.ArgumentParser(description="Parse Python files and publish CPG events.")
    parser.add_argument("repo_root", type=Path, help="Root of the cloned Python repository")
    parser.add_argument("--file", type=Path, help="Publish only one Python file")
    parser.add_argument("--brokers", default="localhost:9092", help="Kafka bootstrap servers")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    repo_root = args.repo_root.resolve()
    producer = build_producer(args.brokers)
    processed = failed = 0
    try:
        for path in iter_selected_files(repo_root, args.file):
            result = publish_file(producer, path, repo_root)
            processed += 1
            failed += int(result["error"] is not None)
            LOGGER.info(
                "%s nodes=%d edges=%d error=%s",
                path.relative_to(repo_root),
                result["nodes"],
                result["edges"],
                bool(result["error"]),
            )
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)

    LOGGER.info("completed files=%d errors=%d", processed, failed)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
