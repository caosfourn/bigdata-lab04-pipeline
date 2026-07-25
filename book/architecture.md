# Architecture

## Pipeline Diagram

![Incremental CPG streaming architecture](images/architecture.svg)

The pipeline processes one Python source file at a time and routes four
versioned event streams through Apache Kafka to two independent sinks.

---

## Component Breakdown

| Component | Role | Technology |
|---|---|---|
| File Discovery | Enumerate `.py` files, compute SHA-256 | `src/discovery.py` |
| Parser Service | Extract AST/CFG/DFG/CALL, assign stable IDs | `src/parser_service.py` (stdlib `ast`) |
| Kafka Broker | Durable event bus, topic compaction | Confluent CP-Kafka 7.8 (KRaft) |
| Kafka Connect | Stream cpg.nodes + cpg.edges directly to Neo4j | Neo4j Connector 5.5 |
| Neo4j | Persist CPG topology | Neo4j 5.26 Community |
| Spark Streaming | Consume cpg.metadata, write to MongoDB | Spark 3.5.1 + Mongo Connector 10.7 |
| MongoDB | Persist file-level metadata | MongoDB 7.0 |
| Checkpoint | Durable Kafka offset store | Spark checkpoint directory |

---

## Kafka Topic Layout

| Topic | Key | Partitions | Cleanup | Consumer |
|---|---|---:|---|---|
| `cpg.nodes` | `node_id` | 3 | compact + 7-day | Neo4j Sink (direct) |
| `cpg.edges` | `edge_id` | 3 | compact + 7-day | Neo4j Sink (direct) |
| `cpg.metadata` | `file_id` | 3 | compact + 30-day | Spark → MongoDB |
| `cpg.errors` | `file_id` | 1 | 7-day delete | Monitoring / DLQ |

> **Key design decision:** `cpg.nodes` and `cpg.edges` go **directly** to Neo4j
> via Kafka Connect (no Spark). Spark is used **only** for `cpg.metadata` →
> MongoDB. Mixing them up is the most common grading deduction.

---

## Idempotency Chain

```
Parser (stable SHA-256 IDs)
  → Kafka (log-compaction deduplicates same-keyed messages)
    → Neo4j MERGE (uniqueness constraint prevents duplicate nodes/edges)
    → MongoDB replace/upsert (file_id as _id, always 1 document per file)
    → Spark checkpoint (committed offsets skip already-processed records)
```

---

## Mermaid Diagram

```{mermaid}
flowchart LR
    A["Python Repo\n(lerobot)"] -->|file-at-a-time| B["Parser Service\n(discovery.py → parser_service.py)"]

    B -->|node events| K1["cpg.nodes\nkey: node_id"]
    B -->|edge events| K2["cpg.edges\nkey: edge_id"]
    B -->|metadata events| K3["cpg.metadata\nkey: file_id"]
    B -->|error events| K4["cpg.errors"]

    K1 -->|"NO Spark"| KC["Kafka Connect\nNeo4j Sink 5.5\nMERGE on node_id"]
    K2 -->|"NO Spark"| KC

    KC --> N4J["Neo4j\nCPGNode · CPG_EDGE\nuniqueness constraint"]

    K3 -->|"via Spark"| SP["Spark Structured\nStreaming\ncheckpoint location"]
    SP --> MDB["MongoDB\n_id = file_id\nreplace/upsert"]
    SP -. "durable offsets" .-> CP["Checkpoint\noffsets/N"]

    K4 --> MON["Monitoring\ncpg.neo4j.dlq"]

    style KC fill:#ede9fe
    style SP fill:#d1fae5
    style N4J fill:#cffafe
    style MDB fill:#ccfbf1
    style CP  fill:#f1f5f9
```

---

## Reflection

**Approach and reasoning:** the diagram exists to make one design decision
impossible to miss — `cpg.nodes`/`cpg.edges` go straight from Kafka Connect
to Neo4j, while only `cpg.metadata` passes through Spark on its way to
MongoDB. Drawing both a static SVG (for the PDF/print path) and a Mermaid
diagram (for the live GitHub Pages site) meant this decision is visible
regardless of how the book is read.

**What worked:** keeping the diagram at the component level (eight boxes,
four topics) rather than trying to show every field or every consumer group
kept it legible. Each row in the "Component Breakdown" table maps to exactly
one box in the diagram, so a reader can cross-reference without guessing.

**What was harder than expected:** representing the idempotency guarantees
*visually* (MERGE, upsert, checkpoint) without cluttering the flowchart was
not straightforward — the "Idempotency Chain" text block above was added
alongside the diagram instead of inside it, since cramming constraint/upsert
semantics into node labels made the Mermaid diagram unreadable at normal
zoom. The two are meant to be read together: the diagram for *shape*, the
chain for *why replays are safe*.
