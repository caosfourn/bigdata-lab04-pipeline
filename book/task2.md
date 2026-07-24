# Task 2 — Incremental CPG parser

The parser uses Python's standard `ast` library. Each source file is a bounded
unit of work and yields four outputs: nodes, edges, metadata, and an optional
error event.

```powershell
$env:PYTHONUTF8 = "1"
python src/parser_service.py lerobot/src/lerobot/__init__.py lerobot
```

Identifiers are deterministic functions of file-relative source locations and
edge endpoints. The parser extracts AST, approximate CFG, DFG and call edges.
The executed notebook below includes representative event payloads and an
ID-stability demonstration.

## Success criteria

- A single file can be processed without loading the whole repository.
- Every event has `schema_version` and UTC `event_time`.
- Syntax and decoding failures become `cpg.errors` events.
- Reprocessing unchanged content produces the same node and edge IDs.

## Reflection

The standard library avoids a native parser dependency. The CFG and DFG are
educational approximations rather than a whole-program semantic analysis, a
trade-off documented for reproducibility and bounded execution.
