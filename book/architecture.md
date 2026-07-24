# Architecture

```{mermaid}
flowchart LR
    R[LeRobot repository] -->|one Python file at a time| D[Discovery]
    D --> P[CPG Parser Service]
    P --> N[cpg.nodes]
    P --> E[cpg.edges]
    P --> M[cpg.metadata]
    P --> X[cpg.errors]
    N --> C[Neo4j Kafka Connector]
    E --> C
    C -->|MERGE stable IDs| G[(Neo4j)]
    M --> S[Spark Structured Streaming]
    S -->|replace/upsert by file_path| B[(MongoDB)]
    S --> Q[(durable checkpoint)]
    X --> U[Kafka UI / monitoring]
```

Stable node and edge identifiers make Kafka replay safe. Neo4j uses `MERGE` on
those identifiers. MongoDB uses a deterministic `_id` derived from `file_path`,
so a changed version replaces the current metadata document for that file.
Spark checkpoints preserve consumed Kafka offsets across restarts.
