# Task 1–2 — LeRobot discovery and parser evidence

Execution date: 2026-07-25
Repository: `https://github.com/huggingface/lerobot.git`
Pinned shallow-clone commit: `0d383d09f2051444de211739196a28cc94736861`

## Repository discovery

```text
is_shallow_repository: true
raw_python_files: 752
included_python_files: 490
duplicate_relative_paths: 0
excluded_pattern_matches: 0
top_level_distribution:
  src: 488
  scripts: 2
included_source_size: 5392.6 KiB
```

The final manifest excludes test/example directories, test filename patterns,
`setup.py`, `conftest.py`, generated suffixes and generated-code headers. Two
runs over the pinned checkout produced byte-identical JSON manifests.

## Incremental parser sample

The reproducible demo selected
`scripts/ci/extract_task_descriptions.py` and parsed only that file in memory.

```text
file_id: file_ae2cb41e414b074ed64d4937c0d9ce6bdd6b7b077aa08017efb95edefcfbdc54
file_hash: 7d1235a0b11643c68de0b5ae6de60c71c999f08b575ac07c91848abb440f40cb
nodes: 1003
unique_node_ids: 1003
AST_CHILD edges: 1002
CFG edges: 81
DFG edges: 124
CALL edges: 65
edges: 1272
unique_edge_ids: 1272
parser_errors: 0
```

The same file was parsed three times. The sorted `node_id` and `edge_id` sets
were identical in all three runs. Events contained `schema_version: 1.0`, an
RFC 3339 UTC `event_time`, the stable repository/file identifiers, the source
hash, and `parse_status: success`.

## Automated checks

```text
tests/test_discovery.py: 6 passed
tests/test_parser_contract.py: 6 passed
Task 1–2 total: 12 passed
```

The executed parser notebook under `notebooks/task2_parser_evidence.ipynb` contains the
full discovery sample, event samples, edge distribution and three-run stable-ID
comparison for the same pinned checkout.
