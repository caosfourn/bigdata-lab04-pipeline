# Conclusion & Team Reflection

## What We Built

This project implements a complete incremental Code Property Graph streaming
pipeline for the `huggingface/lerobot` repository:

- **Task 1–2 (Member 1):** File discovery and CPG parser service using Python's
  standard `ast` module. Every element receives a deterministic SHA-256 ID
  derived from its structural position in the AST — the foundation for
  idempotency across the entire pipeline.

- **Task 3–4 (Member 2):** Kafka topic design (4 topics, 3 partitions for
  nodes/edges/metadata) and Neo4j graph topology ingestion via Kafka Connect
  Sink 5.5 with `MERGE` semantics and uniqueness constraints.

- **Task 5 (Member 3):** Spark Structured Streaming job consuming `cpg.metadata`
  and writing to MongoDB with `replace/upsert` (`_id = file_id`) and a durable
  checkpoint location.

- **Task 6 + Diagram (Member 4):** Idempotent replay verification demonstrating
  that re-processing a modified file updates both databases in-place without
  creating duplicates, and that Spark's committed offsets correctly skip
  unchanged files.

## Key Design Decisions

1. **No Spark between Kafka and Neo4j.** Graph topology (nodes/edges) flows
   directly via Kafka Connect. This avoids the operational complexity and
   latency of a Spark job for the high-volume graph stream.

2. **Stable IDs from structural paths.** Using `root.body[0].value` as the
   hash input (not memory address `id(node)`) makes IDs reproducible across
   runs, machines, and Python versions.

3. **`file_id` as the single stable key.** The same SHA-256 of
   `repo_id + file_path` is used as Neo4j `file_id` property and MongoDB
   `_id`. This couples the two sinks without tight runtime coupling.

4. **Graceful degradation.** The parser, publisher, and verifier all work
   without live Kafka/Neo4j/MongoDB, using `CollectingProducer` dry-run.
   This allowed full testing in CI with no external services.

## Submission

The published Jupyter Book URL (replace with actual Pages URL after deploy):

```
https://caosfourn.github.io/bigdata-lab04-pipeline/
```

> Before submitting: ensure all evidence placeholders in Task 6 contain
> real captured values, all database UI screenshots are added to
> `docs/evidence/`, and the GitHub Pages deployment shows a green status.
