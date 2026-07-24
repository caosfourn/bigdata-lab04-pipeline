"""Kafka topic contract for the incremental CPG pipeline.

The four ``cpg.*`` topics are part of the assignment contract.  The fifth
topic is an operational dead-letter queue used by Kafka Connect; it is kept
separate from ``cpg.errors``, which contains errors produced by the parser.
"""

from __future__ import annotations

from dataclasses import dataclass

try:  # Support both ``python -m src...`` and direct scripts from ``src``.
    from .schemas import TOPIC_EDGES, TOPIC_ERRORS, TOPIC_METADATA, TOPIC_NODES
except ImportError:  # pragma: no cover - exercised by direct CLI usage
    from schemas import TOPIC_EDGES, TOPIC_ERRORS, TOPIC_METADATA, TOPIC_NODES


TOPIC_NEO4J_DLQ = "cpg.neo4j.dlq"


@dataclass(frozen=True)
class TopicSpec:
    """Declarative Kafka topic configuration used by code and documentation."""

    name: str
    key_field: str
    partitions: int
    replication_factor: int
    configs: dict[str, str]
    purpose: str


TOPIC_SPECS: tuple[TopicSpec, ...] = (
    TopicSpec(
        name=TOPIC_NODES,
        key_field="node_id",
        partitions=3,
        replication_factor=1,
        configs={
            "cleanup.policy": "compact,delete",
            "retention.ms": "604800000",  # 7 days
            "min.cleanable.dirty.ratio": "0.1",
            "min.insync.replicas": "1",
        },
        purpose="CPG node upsert events consumed directly by Neo4j Kafka Sink",
    ),
    TopicSpec(
        name=TOPIC_EDGES,
        key_field="edge_id",
        partitions=3,
        replication_factor=1,
        configs={
            "cleanup.policy": "compact,delete",
            "retention.ms": "604800000",  # 7 days
            "min.cleanable.dirty.ratio": "0.1",
            "min.insync.replicas": "1",
        },
        purpose="CPG edge upsert events consumed directly by Neo4j Kafka Sink",
    ),
    TopicSpec(
        name=TOPIC_METADATA,
        key_field="file_id",
        partitions=3,
        replication_factor=1,
        configs={
            "cleanup.policy": "compact,delete",
            "retention.ms": "2592000000",  # 30 days
            "min.cleanable.dirty.ratio": "0.1",
            "min.insync.replicas": "1",
        },
        purpose="Latest source-file metadata for Spark/MongoDB and reconciliation",
    ),
    TopicSpec(
        name=TOPIC_ERRORS,
        key_field="file_id",
        partitions=1,
        replication_factor=1,
        configs={
            "cleanup.policy": "delete",
            "retention.ms": "604800000",  # 7 days
            "min.insync.replicas": "1",
        },
        purpose="Parser failures for monitoring and reporting",
    ),
    TopicSpec(
        name=TOPIC_NEO4J_DLQ,
        key_field="original Kafka key",
        partitions=1,
        replication_factor=1,
        configs={
            "cleanup.policy": "delete",
            "retention.ms": "1209600000",  # 14 days
            "min.insync.replicas": "1",
        },
        purpose="Malformed records rejected by the Neo4j Kafka Sink connector",
    ),
)


REQUIRED_ASSIGNMENT_TOPICS = frozenset(
    {TOPIC_NODES, TOPIC_EDGES, TOPIC_METADATA, TOPIC_ERRORS}
)


def topic_spec(name: str) -> TopicSpec:
    """Return one topic specification or raise a clear error for unknown names."""

    for spec in TOPIC_SPECS:
        if spec.name == name:
            return spec
    raise KeyError(f"Unknown Kafka topic: {name}")

