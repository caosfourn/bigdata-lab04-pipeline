# Task 6 — Idempotent Replay Verification

**Member 4** owns this task: verifying that the complete pipeline is **idempotent**
across all three sink layers — Neo4j, MongoDB, and Spark checkpoint.

## Approach

The verification strategy targets each idempotency mechanism independently
before demonstrating end-to-end correctness:

| Layer | Mechanism | Verification method |
|---|---|---|
| Parser | Stable SHA-256 IDs (structural AST path) | Re-parse → compare `node_id` sets |
| Neo4j | `MERGE` on uniqueness-constrained `node_id` | Count nodes before & after; zero duplicates |
| MongoDB | `replace/upsert` with `_id = file_id` | Count documents; must stay at 1 |
| Spark | Committed offset checkpoint | Read `offsets/` directory; show skip behavior |

## Script: `src/replay_verifier.py`

The verifier has two phases:

```powershell
# Phase 1 — record baseline BEFORE modifying the file
python -m src.replay_verifier lerobot/lerobot/__init__.py lerobot `
  --repo-id huggingface/lerobot `
  --phase before `
  --output runtime/replay-before.json `
  --neo4j-uri bolt://localhost:7687 --neo4j-password cpg-password `
  --mongo-uri mongodb://localhost:27017

# Modify one line (see git diff below)

# Phase 2 — compare AFTER modifying the file
python -m src.replay_verifier lerobot/lerobot/__init__.py lerobot `
  --repo-id huggingface/lerobot `
  --phase after `
  --baseline runtime/replay-before.json `
  --output runtime/replay-after.json `
  --neo4j-uri bolt://localhost:7687 --neo4j-password cpg-password `
  --mongo-uri mongodb://localhost:27017
```

Offline (no database required — for CI and local testing):

```powershell
python -m src.replay_verifier lerobot/lerobot/__init__.py lerobot `
  --phase before --dry-run --output runtime/replay-before.json

python -m src.replay_verifier lerobot/lerobot/__init__.py lerobot `
  --phase after --baseline runtime/replay-before.json --dry-run `
  --output runtime/replay-after.json
```

## File Modified

Target: `lerobot/lerobot/__init__.py`

The addition appends one constant to the module:

```python
# [Task 6 replay marker] — added to test idempotent pipeline
REPLAY_TEST_VERSION = "v1.0-task6"
```

This creates **new** AST nodes (`Assign`, `Name`, `Constant`) with new stable IDs
while leaving all pre-existing nodes with their original IDs intact. Running the
pipeline again with this modified file should:

- **MERGE** (update) the existing ~N nodes in Neo4j — no duplicates
- **INSERT** only the new AST nodes for the added constant
- **Upsert** the MongoDB document at the existing `_id = file_id`
- **Skip** all other unchanged files (Spark reads from committed offset)

## Evidence Table

Run the deployed pipeline and paste real values below.

| Measurement | Before | After | Expected |
|---|---:|---:|---|
| File SHA-256 (first 16 chars) | _capture_ | _capture_ | changed |
| Parser node count | _capture_ | _capture_ | reflects source |
| Parser edge count | _capture_ | _capture_ | reflects source |
| Stable IDs preserved | — | _capture_ | > 0 |
| Neo4j nodes for `file_id` | _capture_ | _capture_ | reflects source |
| Neo4j edges for `file_id` | _capture_ | _capture_ | reflects source |
| Duplicate `node_id` count | _capture_ | _capture_ | **0** |
| MongoDB documents for file | _capture_ | _capture_ | **1** |
| Spark committed batch offset | _capture_ | _capture_ | incremented only once |

> Do not replace these placeholders with invented values. Run the final deployed
> pipeline, capture real command outputs, and include dated screenshots.

## Reflection

Parser ID stability alone does not prove end-to-end idempotency — the broker,
connector, database constraints, and Spark checkpoint must all cooperate.
The most common failure modes are:

1. **Missing Neo4j uniqueness constraint** → `MERGE` silently creates duplicates
   because Cypher `MERGE` without a constraint can match no existing node.
2. **MongoDB `insert` instead of `replace/upsert`** → each replay creates a
   new `ObjectId`-keyed document.
3. **Unstable IDs** (e.g., using Python `id(node)`, a memory address) → every
   parse produces different IDs, making MERGE miss existing nodes.

The pipeline avoids all three by design:
- `infra/neo4j/constraints.cypher` installs the constraint at startup.
- `metadata_streaming_job.py` sets `operationType=replace` and `idFieldList=_id`.
- `parser_service.py` uses `structural_path` (e.g., `root.body[0].value`)
  hashed with SHA-256 — deterministic and cross-platform.
