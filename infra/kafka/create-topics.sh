#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-kafka:29092}"

kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists \
  --topic cpg.nodes --partitions 3 --replication-factor 1 \
  --config cleanup.policy=compact,delete \
  --config retention.ms=604800000 \
  --config min.cleanable.dirty.ratio=0.1 \
  --config min.insync.replicas=1

kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists \
  --topic cpg.edges --partitions 3 --replication-factor 1 \
  --config cleanup.policy=compact,delete \
  --config retention.ms=604800000 \
  --config min.cleanable.dirty.ratio=0.1 \
  --config min.insync.replicas=1

kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists \
  --topic cpg.metadata --partitions 3 --replication-factor 1 \
  --config cleanup.policy=compact,delete \
  --config retention.ms=2592000000 \
  --config min.cleanable.dirty.ratio=0.1 \
  --config min.insync.replicas=1

kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists \
  --topic cpg.errors --partitions 1 --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000 \
  --config min.insync.replicas=1

kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists \
  --topic cpg.neo4j.dlq --partitions 1 --replication-factor 1 \
  --config cleanup.policy=delete \
  --config retention.ms=1209600000 \
  --config min.insync.replicas=1

kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list

