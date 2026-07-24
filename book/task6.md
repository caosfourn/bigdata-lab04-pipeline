# Task 6 — Idempotent replay

Use one copied source file for a controlled before/after experiment:

1. Publish the original file and record its hash and database counts.
2. Add a small function to that file.
3. Publish only that modified file.
4. Restart Spark with the same checkpoint.
5. Run the duplicate and count queries from Tasks 4 and 5.

Capture comparable JSON snapshots before and after:

```powershell
python scripts/verify_replay.py src/lerobot/__init__.py `
  --output runtime/replay-before.json
# Append a small function and publish only this file.
python scripts/verify_replay.py src/lerobot/__init__.py `
  --output runtime/replay-after.json
```

Record final evidence in this table:

| Measurement | Before | After | Expected |
|---|---:|---:|---|
| File SHA-256 | _capture_ | _capture_ | changed |
| Neo4j nodes for file | _capture_ | _capture_ | reflects source |
| Neo4j relationships for file | _capture_ | _capture_ | reflects source |
| Duplicate node IDs | _capture_ | _capture_ | 0 |
| MongoDB documents for file | _capture_ | _capture_ | 1 |
| Spark restart starting offset | _capture_ | _capture_ | committed offset |

Do not replace these placeholders with invented values. Run the final deployed
pipeline, paste the commands and outputs, and include dated screenshots.

## Reflection

Parser ID stability alone does not prove end-to-end idempotency. This experiment
must demonstrate the broker, connector, databases and checkpoint together.
