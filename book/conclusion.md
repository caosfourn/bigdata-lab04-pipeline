# Conclusion and reflection

We completed the pipeline from Python source discovery to storage in Neo4j and
MongoDB. The final run used the pinned LeRobot repository and processed all 490
selected Python files without parser errors.

## Final results

| Check | Result |
|---|---:|
| Discovered Python files | 490 |
| Parser errors | 0 |
| Initial Neo4j nodes | 655,365 |
| Initial Neo4j edges | 830,472 |
| Final Neo4j nodes after the file change | 655,388 |
| Final Neo4j edges after the file change | 830,500 |
| MongoDB documents | 490 |
| Task 6 checks | 89 / 89 passed |

Replaying the same input did not increase the number of nodes, edges, or
MongoDB documents. When we changed one Python file, the file hash and graph
content changed, but the stable `file_id` still pointed to the same source file.
This allowed Neo4j to replace the old graph revision and MongoDB to update the
existing document. Restarting Spark with the same checkpoint also skipped the
offsets that had already been committed.

## What we learned

At first, we expected parsing Python to be the most difficult part. In practice,
the harder problem was keeping the same identity across Kafka, Neo4j, and
MongoDB. Stable IDs were needed in every stage; using `MERGE` only at the final
database would not have been enough.

We also learned that a streaming pipeline can temporarily look inconsistent.
Kafka does not guarantee ordering between separate topics, so an edge can reach
Neo4j before its node. Placeholder nodes and the final lag check were necessary
to handle that case. Spark checkpointing solved a different problem: resuming
from Kafka after the streaming job stopped.

## Limitations and possible improvements

- The CFG and DFG are intrafile approximations. They do not perform complete
  scope, alias, or type analysis.
- The parser processes one file at a time, but each file is still materialized
  as lists of events before publishing. A generator-based publisher would use
  less memory for very large files.
- Placeholder nodes are useful for cross-topic ordering, but they make the
  Neo4j sink queries more complicated.
- The current deployment runs all services on one Docker host. A larger project
  would need separate monitoring, resource limits, and backup policies.

## Final reflection

The most useful part of Task 6 was testing both an exact replay and a real file
change. The exact replay checked duplicate safety, while the modified-file run
checked whether old state was actually replaced. Those are different cases,
and testing only one of them would have missed an important pipeline failure.

Overall, the pipeline met the required data flow and replay behavior. The main
technical compromise is the simplified CFG/DFG analysis, which was acceptable
for this lab but would need a stronger parser for production use.
