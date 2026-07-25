# Task 5 — Spark metadata ingestion

Spark reads JSON from `cpg.metadata`, validates the full nested schema and
writes it through the MongoDB Spark Connector. The checkpoint directory must
be reused across restarts.

```powershell
spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.7.0 `
  src/metadata_streaming_job.py `
  --brokers localhost:9092 `
  --mongo-uri mongodb://localhost:27017 `
  --checkpoint-location checkpoints/cpg-metadata
```

MongoDB `_id` is the parser-provided, repo-scoped `file_id`. A changed hash
therefore updates the same current-file document without cross-repository path
collisions.

```javascript
db.metadata.countDocuments({})
db.metadata.findOne({file_path: "src/lerobot/__init__.py"})
db.metadata.aggregate([
  {$group: {_id: "$file_id", copies: {$sum: 1}}},
  {$match: {copies: {$gt: 1}}}
])
```

The aggregation must return an empty result.

## Executed Evidence (member-3 integration run, 2026-07-24)

Environment: Docker Compose, Spark 3.5.1, MongoDB Spark Connector 10.7.0,
MongoDB 7.0.12. Full log in
[`docs/evidence/task5_spark_mongodb_e2e.md`](https://github.com/caosfourn/bigdata-lab04-pipeline/blob/main/docs/evidence/task5_spark_mongodb_e2e.md) (excluded from the built book — see the repo directly).

**Initial streaming batch:**

```text
batchId: 0
numInputRows: 6
endOffset: {cpg.metadata: {0: 4, 1: 2, 2: 0}}
maxOffsetsBehindLatest: 0
MongoStreamingWrite: committed
checkpoint commit: /opt/checkpoints/person3-final-docker/commits/0
```

Six Kafka records (repeated revisions of three stable files) collapsed to
three current-state documents, `_id == file_id`, no duplicate groups:

```text
document_count: 3
duplicate_groups: []
```

**Exact replay** — `tests/fixtures/replay_sample.py` republished with the
same `file_id`:

```text
total_documents: 3
documents_for_file_id: 1
total_nodes: 23
```

**Checkpoint restart** — Spark container stopped and restarted with no new
publish, same checkpoint path reused:

```text
Resuming at batch 2 with committed offsets
{cpg.metadata: {0: 5, 1: 2, 2: 0}}
available offsets
{cpg.metadata: {0: 5, 1: 2, 2: 0}}
Streaming query has been idle and waiting for new data
```

MongoDB was unchanged after the restart (`total_documents: 3`,
`documents_for_replayed_file_id: 1`) — proving the checkpoint, not a
fresh full replay, drove the resume.

### Screenshots to attach (pending)

| Evidence | File to add | Status |
|---|---|---|
| Spark UI / driver log — query ID, batch ID, offsets | `docs/evidence/spark_batch_log.png` | ⬜ not yet captured |
| MongoDB Compass — document with emitted hash + counts | `docs/evidence/mongodb_metadata_doc.png` | ⬜ not yet captured |
| Terminal — restart log resuming from checkpoint | `docs/evidence/spark_restart_log.png` | ⬜ not yet captured |

Repeat this run with the Moodle-selected repository before final submission —
the numbers above are from `huggingface/lerobot` test fixtures, not the
graded commit.

## Reflection

**What worked:** checkpointing provided offset recovery — after a full
container restart, Spark resumed at the exact next batch instead of
reprocessing from the beginning, and MongoDB's deterministic
`replace`/`_id=file_id` upsert kept the collection at a stable 3 documents
across both the initial run and the exact-replay test.

**Risk designed around, not yet an observed incident:** the connector's
default write mode is plain `insert`, which would create a new
`ObjectId`-keyed document on every replay instead of updating in place. The
job explicitly sets `.option("operationType", "replace")` and
`.option("idFieldList", "_id")`, using the parser's own `file_id` as the
Mongo `_id`. The `duplicate_groups` aggregation check above is what would
catch a regression here — it returned empty in every run captured so far, so
no such failure has actually been observed in this integration log; the
check exists specifically so one would be caught before submission.
