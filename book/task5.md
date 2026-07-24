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

MongoDB `_id` is SHA-256 of `file_path`. A changed hash therefore updates the
same current-file document.

```javascript
db.metadata.countDocuments({})
db.metadata.findOne({file_path: "src/lerobot/__init__.py"})
db.metadata.aggregate([
  {$group: {_id: "$file_path", copies: {$sum: 1}}},
  {$match: {copies: {$gt: 1}}}
])
```

The aggregation must return an empty result.

## Evidence to capture

- Spark query ID, batch ID and offsets.
- MongoDB document containing the emitted hash and counts.
- Restart log using the same checkpoint.

## Reflection

Checkpointing provides offset recovery; deterministic replace/upsert provides
database idempotency. Both are necessary because either side may restart.
