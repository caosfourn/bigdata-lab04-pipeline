# Conclusion, limitations, and submission gate

The pipeline separates topology from file metadata while preserving one shared,
versioned identity contract. The pinned LeRobot checkout produced a deterministic
490-file source manifest. The selected parser file produced 1,003 unique nodes
and 1,272 unique AST/CFG/DFG/CALL edges, with identical IDs on exact replay.
The initial full-load graph contained 655,365 unique nodes and 830,472 unique
edges. The final five-phase LeRobot replay passed all 89 automated checks and
verified Neo4j `MERGE`, stale-revision cleanup, MongoDB replace/upsert, and Spark
checkpoint recovery together.

## What worked

- Parsing one file at a time bounded work and isolated syntax failures.
- Full deterministic IDs connected Kafka keys, Neo4j constraints and MongoDB
  `_id` without machine-specific paths.
- Direct Kafka Connect ingestion kept Spark out of the topology branch.
- Neo4j and MongoDB remained safe under exact replay.
- A persistent Spark checkpoint resumed at committed Kafka offsets.

## Limitations

The standard-library CFG and DFG are intra-file educational approximations, not
whole-program alias or type analysis. Kafka does not order records across
topics, so Neo4j needs endpoint placeholders and a post-lag verification query.
Fixture integration evidence remains useful for diagnosing individual
components, but the dated modified-LeRobot-file run is the submission-level
claim: after the edit the graph converged to 655,388 unique nodes and 830,500
unique edges, while MongoDB retained 490 unique LeRobot documents.

## Final report gate

Before publication, all of these conditions must be true:

- Task 6's result table contains one coherent LeRobot before/after/replay run.
- Kafka, Neo4j, MongoDB, and Spark restart evidence figures are embedded in
  their corresponding chapters.
- Raw logs/JSON used by the figures remain in the public repository.
- The executed Task 1–2 notebook agrees with the current full-ID code and the
  pinned commit.
- `jupyter-book build . --all --warningiserror` exits successfully.
- CI tests and the Pages deployment job are green on `main`.
- The public site opens from a signed-out/incognito browser and repository links
  do not require authentication.

## Publishing and Moodle submission

Repository administrators must select **Settings → Pages → Build and
deployment → Source: GitHub Actions** once. The workflow validates pull requests
(and manually dispatched feature branches) but intentionally deploys only a
successful build of `main`. After the completed branch is reviewed and merged,
pushing `main` uploads `_build/html` and deploys it to the `github-pages`
environment.

For this repository, the expected root address is:

```text
https://caosfourn.github.io/bigdata-lab04-pipeline/
```

Open that exact root URL in an incognito window, navigate through every chapter,
and then paste only that URL into Moodle. The assignment accepts one public
Jupyter Book URL; it does not accept a ZIP archive, PDF export, Word document,
GitHub repository URL, or a link to an individual chapter.
