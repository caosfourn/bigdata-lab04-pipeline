# Architecture

![Incremental CPG streaming architecture](images/architecture.svg)

The parser processes one Python file at a time and emits four versioned event
streams. Nodes and edges reach Neo4j directly through Kafka Connect. Metadata
is consumed independently by Spark Structured Streaming and written to MongoDB
with `file_id` as `_id`; Spark's durable checkpoint stores source offsets.
Parser failures remain observable on `cpg.errors`, while connector failures use
the separate `cpg.neo4j.dlq` topic.

Stable file, node and edge identifiers make Kafka replay safe. Neo4j uses
constraints and `MERGE`; MongoDB uses replace/upsert. Both database strategies
remain idempotent if an at-least-once consumer retries a record.
