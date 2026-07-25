# Task 2 — Incremental CPG parser

The parser uses Python's standard `ast` library. Each source file is a bounded
unit of work and yields four outputs: nodes, edges, metadata, and an optional
error event.

```powershell
$env:PYTHONUTF8 = "1"
python src/parser_service.py lerobot/src/lerobot/__init__.py lerobot
```

`file_id` is repo-scoped; node IDs use deterministic AST structural paths and
full SHA-256 output, while edge IDs include their endpoints/type and a call-site
discriminator where needed. This also distinguishes locationless AST singleton
occurrences. The parser extracts AST, approximate CFG, DFG and call edges. The
executed notebook below must be rerun with the final contract and repository.

## Success criteria

- A single file can be processed without loading the whole repository.
- Every event has `schema_version` and UTC `event_time`.
- Syntax and decoding failures become `cpg.errors` events.
- Reprocessing unchanged content produces the same node and edge IDs.

The `notebooks/task6_member4.ipynb` re-parse (embedded in [Task 6](task6.md))
demonstrates this contract directly on a live file: re-parsing unchanged
content reproduces the exact same `node_id`/`edge_id` set, and a real edit
adds only new IDs while preserving the originals — see the "Node ID
analysis" output in that chapter for the executed proof.

> ⬜ **Pending:** a full-repository parser run against the final
> Moodle-selected commit, with a sample of node/edge/metadata events and any
> `cpg.errors` occurrences, supplied by Member 1.

## Reflection

**Approach and reasoning:** using Python's standard `ast` module avoids a
native/compiled parser dependency, which keeps the pipeline runnable in any
environment with just Python installed — important given the project also
needs to run in CI and in Docker without extra native toolchains.

**What worked:** treating each source file as a bounded unit of work (one
`ast.parse()` call, four output event types) makes the parser trivially
composable with both a live Kafka publisher and the offline
`CollectingProducer` used for dry-run testing in Task 6.

**Known trade-off, not a failure:** the CFG and DFG edges are educational
approximations, not a whole-program semantic analysis (no cross-function
data flow, no exception-driven control flow beyond basic try/except). This
was a deliberate scope decision to keep per-file parse time bounded and the
output deterministic — documented here so it isn't mistaken for a bug during
grading.
