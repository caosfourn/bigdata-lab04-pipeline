# Task 5 — Spark to MongoDB integration evidence

Execution date: 2026-07-24
Environment: Docker Compose, Spark 3.5.1, MongoDB Spark Connector 10.7.0,
MongoDB 7.0.12.

## Initial streaming batch

The merged job consumed the existing `cpg.metadata` partitions and committed
batch 0 to MongoDB:

```text
batchId: 0
numInputRows: 6
endOffset: {cpg.metadata: {0: 4, 1: 2, 2: 0}}
maxOffsetsBehindLatest: 0
MongoStreamingWrite: committed
checkpoint commit: /opt/checkpoints/metadata-stream/commits/0
```

The six Kafka records represented repeated revisions of three stable files.
MongoDB contained three current-state documents, with `_id == file_id` and no
duplicate `_id` groups:

```text
document_count: 3
duplicate_groups: []
```

## Exact replay

`tests/fixtures/replay_sample.py` was published again with this stable key:

```text
file_c5fd1983101a4aa656816fbb6d3d62075ae747c5c9df9d6a5833e4c6228c28ec
```

After Spark processed the new Kafka offset:

```text
total_documents: 3
documents_for_file_id: 1
total_nodes: 23
```

The replay replaced the existing document and did not increase collection
cardinality.

## Checkpoint restart

The Spark container was stopped and started again without publishing a new
event. The same checkpoint path was reused. Spark reported:

```text
Resuming at batch 2 with committed offsets
{cpg.metadata: {0: 5, 1: 2, 2: 0}}
available offsets
{cpg.metadata: {0: 5, 1: 2, 2: 0}}
Streaming query has been idle and waiting for new data
```

MongoDB remained unchanged after restart:

```text
total_documents: 3
documents_for_replayed_file_id: 1
event_time: 2026-07-24T16:42:23.997639Z
```

This run checks schema compatibility, MongoDB replace/upsert behavior and
resume from a persisted checkpoint at fixture scope. The completed dated
LeRobot replay and restart are recorded in
`docs/evidence/task6_lerobot_final.md`.
