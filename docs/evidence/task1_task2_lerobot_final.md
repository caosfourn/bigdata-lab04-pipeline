# Task 1–2 final LeRobot evidence

- Execution date: 2026-07-25
- Repository: `https://github.com/huggingface/lerobot.git`
- Pinned commit: `0d383d09f2051444de211739196a28cc94736861`
- Parser runtime: Python 3.14 (`ast` standard library)

This record complements the executed notebook
`notebooks/task2_parser_evidence.ipynb`. The notebook contains representative
events and the exact-replay ID comparison; this file retains the compact
repository-level acceptance totals.

## Clone and discovery

```text
is shallow repository: true
raw .py files before exclusions: 752
included Python source files: 490
duplicate relative paths: 0
excluded-pattern paths remaining: 0
```

The first deterministic manifest entry was:

```text
relative_path: scripts/ci/extract_task_descriptions.py
file_size_bytes: 8631
file_hash: 7d1235a0b11643c68de0b5ae6de60c71c999f08b575ac07c91848abb440f40cb
```

## Selected-file parser acceptance

```text
nodes: 1003
unique node IDs: 1003
AST edges: 1002
CFG edges: 81
DFG edges: 124
CALL/CALL_EXTERNAL edges: 65
all edges: 1272
unique edge IDs: 1272
stable IDs on exact replay: true
```

## Bounded full-manifest dry run

The publisher dry-run processed the manifest one source at a time. The pinned
LeRobot revision requires Python 3.14: running the same checkout under Python
3.11 produced syntax failures in four valid upstream files, so Python 3.14 was
used rather than excluding them.

```text
files processed: 490
nodes emitted: 655365
edges emitted: 830472
parser errors: 0
```

These are parser/discovery totals only. Kafka, Neo4j, Spark and MongoDB evidence
is recorded separately so fixture-scope and final-repository claims remain
distinguishable.
