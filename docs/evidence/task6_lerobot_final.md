# Final LeRobot end-to-end and replay evidence

- Execution date: 2026-07-25
- Repository: `https://github.com/huggingface/lerobot.git`
- Pinned commit: `0d383d09f2051444de211739196a28cc94736861`
- Target file: `src/lerobot/__init__.py`
- Stable file ID: `file_4bcb0fb8af5f208dad26ab6d584c0d2a8e7c995ea7954b70e1d1976ce286f236`

This transcript is a compact, commit-safe summary of the ignored runtime JSON
snapshots. `scripts/verify_replay.py` produced and validated each snapshot
directly from Neo4j, MongoDB, and Spark's checkpoint.

## Initial full load

The Python 3.14 publisher processed the deterministic 490-file manifest one
source at a time:

```text
metadata events: 490
node events: 655365
edge events: 830472
parser errors: 0
```

After Kafka Connect caught up:

```text
SourceFile records: 490
internal CPG nodes: 655365
distinct internal node IDs: 655365
CPG edges: 830472
distinct edge IDs: 830472
duplicate node ID groups: 0
duplicate edge ID groups: 0
unresolved internal placeholders: 0
connector lag: 0 on every subscribed partition
cpg.neo4j.dlq end offset: 0
```

MongoDB contained 490 LeRobot documents with 490 distinct `file_id` values and
no duplicate groups. The complete collection contained 493 documents because
three repository-owned fixture documents from earlier dated integration runs
were intentionally retained.

## Controlled source change

The baseline source hash was:

```text
9de5fe33e0bf693e86e9bf55360942385a504a1baff224577a405ca91ea33838
```

The edit added this harmless function:

```python
def _lab04_replay_probe(value: str) -> str:
    """Return a normalized marker used by the Lab 04 replay experiment."""
    normalized = value.strip()
    return normalized or "lerobot"
```

The modified hash was:

```text
6c0a72b26999ff1fbaa6ba5a6a074f86e7b1e9490320093885335ebaae92f4b7
```

## Five-phase snapshot sequence

| Phase | Nodes / distinct | Edges / distinct | Hash | Mongo docs for file | Batch | Offset total |
|---|---:|---:|---|---:|---:|---:|
| Baseline | 58 / 58 | 60 / 60 | `9de5fe…33838` | 1 | 121 | 497 |
| Exact baseline replay | 58 / 58 | 60 / 60 | `9de5fe…33838` | 1 | 122 | 498 |
| Modified | 81 / 81 | 88 / 88 | `6c0a72…2f4b7` | 1 | 123 | 499 |
| Exact modified replay | 81 / 81 | 88 / 88 | `6c0a72…2f4b7` | 1 | 124 | 500 |
| Idle Spark restart | 81 / 81 | 88 / 88 | `6c0a72…2f4b7` | 1 | 124 | 500 |

Every phase had zero duplicate node and edge groups, one current graph hash,
one MongoDB document whose `_id == file_id`, and no pending checkpoint batch.
The modified 88-edge graph comprised 80 AST, 6 CFG, 1 DFG and 1 external-call
edge. The edit added 23 nodes and 28 edges, so the post-modification repository
totals were:

```text
SourceFile records: 490
CPG nodes / distinct IDs: 655388 / 655388
CPG edges / distinct IDs: 830500 / 830500
Mongo LeRobot documents / distinct file IDs: 490 / 490
whole Mongo collection documents: 493
```

## Checkpoint restart

Spark was stopped and started without a new publish and with the same checkpoint
directory. Its log reported:

```text
Resuming at batch 125 with committed offsets
{cpg.metadata: {0: 178, 1: 175, 2: 147}}
and available offsets
{cpg.metadata: {0: 178, 1: 175, 2: 147}}
Streaming query has been idle and waiting for new data
```

The target MongoDB document, including its event time, the complete collection
fingerprint, graph projection, latest committed batch 124, and offset total 500
were identical before and after the idle restart.

## Automated acceptance report

Command:

```bash
.venv/bin/python scripts/verify_replay.py \
  --verify \
    runtime/task6/baseline.json \
    runtime/task6/exact-replay.json \
    runtime/task6/modified.json \
    runtime/task6/restart.json \
  --modified-replay runtime/task6/modified-replay.json \
  --output runtime/task6/report.json
```

Result:

```json
{
  "passed": true,
  "check_count": 89,
  "failure_count": 0,
  "failures": []
}
```
