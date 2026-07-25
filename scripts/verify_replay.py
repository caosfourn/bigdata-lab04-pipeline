"""Capture and verify the end-to-end Task 6 idempotent replay experiment.

The script has two modes:

* snapshot mode reads the current Neo4j, MongoDB and Spark checkpoint state;
* sequence mode compares baseline, exact-replay, modified and restart snapshots.

It is read-only. Publishing, editing the selected source file and
restarting Spark remain explicit operator actions, which makes the evidence easy
to reproduce and prevents an acceptance script from mutating source or data.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SNAPSHOT_SCHEMA_VERSION = "2.0"
DEFAULT_CHECKPOINT = "checkpoints/metadata-stream"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_safe(value: Any) -> Any:
    """Convert driver-specific values (ObjectId, temporal values) to JSON."""
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return isoformat()
        except (TypeError, ValueError):
            pass
    return str(value)


def _fingerprint(value: Any) -> str:
    rendered = json.dumps(
        _json_safe(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def _single_data(result: Any) -> dict[str, Any]:
    record = result.single()
    return dict(record) if record is not None else {}


def collect_neo4j(
    uri: str,
    user: str,
    password: str,
    file_path: str,
    *,
    file_id: str | None = None,
    repo_id: str | None = None,
    database: str | None = None,
) -> dict[str, Any]:
    """Collect one file's graph and SourceFile reconciliation state."""
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        session_options = {"database": database} if database else {}
        with driver.session(**session_options) as session:
            source_rows = list(
                session.run(
                    """
                    MATCH (file:SourceFile)
                    WHERE ($file_id IS NOT NULL AND file.file_id = $file_id)
                       OR ($file_id IS NULL AND file.file_path = $file_path
                           AND ($repo_id IS NULL OR file.repo_id = $repo_id))
                    RETURN properties(file) AS file
                    ORDER BY file.file_id
                    """,
                    file_id=file_id,
                    file_path=file_path,
                    repo_id=repo_id,
                )
            )
            source_files = [_json_safe(row["file"]) for row in source_rows]
            resolved_file_id = file_id
            if resolved_file_id is None and len(source_files) == 1:
                resolved_file_id = source_files[0].get("file_id")

            parameters = {
                "file_id": resolved_file_id,
                "file_path": file_path,
                "repo_id": repo_id,
            }
            node_summary = _single_data(
                session.run(
                    """
                    MATCH (node:CPGNode)
                    WHERE ($file_id IS NOT NULL AND node.file_id = $file_id)
                       OR ($file_id IS NULL AND node.file_path = $file_path
                           AND ($repo_id IS NULL OR node.repo_id = $repo_id))
                    RETURN count(node) AS nodes,
                           count(DISTINCT node.node_id) AS distinct_node_ids,
                           collect(DISTINCT node.file_hash) AS file_hashes
                    """,
                    **parameters,
                )
            )
            node_duplicates = _single_data(
                session.run(
                    """
                    MATCH (node:CPGNode)
                    WHERE ($file_id IS NOT NULL AND node.file_id = $file_id)
                       OR ($file_id IS NULL AND node.file_path = $file_path
                           AND ($repo_id IS NULL OR node.repo_id = $repo_id))
                    WITH node.node_id AS id, count(*) AS copies
                    WHERE copies > 1
                    RETURN count(*) AS duplicate_node_ids,
                           coalesce(sum(copies - 1), 0) AS duplicate_node_records
                    """,
                    **parameters,
                )
            )
            edge_summary = _single_data(
                session.run(
                    """
                    MATCH ()-[edge:CPG_EDGE]->()
                    WHERE ($file_id IS NOT NULL AND edge.file_id = $file_id)
                       OR ($file_id IS NULL AND edge.file_path = $file_path
                           AND ($repo_id IS NULL OR edge.repo_id = $repo_id))
                    RETURN count(edge) AS edges,
                           count(DISTINCT edge.edge_id) AS distinct_edge_ids,
                           collect(DISTINCT edge.file_hash) AS file_hashes
                    """,
                    **parameters,
                )
            )
            edge_duplicates = _single_data(
                session.run(
                    """
                    MATCH ()-[edge:CPG_EDGE]->()
                    WHERE ($file_id IS NOT NULL AND edge.file_id = $file_id)
                       OR ($file_id IS NULL AND edge.file_path = $file_path
                           AND ($repo_id IS NULL OR edge.repo_id = $repo_id))
                    WITH edge.edge_id AS id, count(*) AS copies
                    WHERE copies > 1
                    RETURN count(*) AS duplicate_edge_ids,
                           coalesce(sum(copies - 1), 0) AS duplicate_edge_records
                    """,
                    **parameters,
                )
            )
            edge_types = {
                row["edge_type"]: row["count"]
                for row in session.run(
                    """
                    MATCH ()-[edge:CPG_EDGE]->()
                    WHERE ($file_id IS NOT NULL AND edge.file_id = $file_id)
                       OR ($file_id IS NULL AND edge.file_path = $file_path
                           AND ($repo_id IS NULL OR edge.repo_id = $repo_id))
                    RETURN edge.edge_type AS edge_type, count(edge) AS count
                    ORDER BY edge.edge_type
                    """,
                    **parameters,
                )
            }
    finally:
        driver.close()

    return _json_safe(
        {
            "file_id": resolved_file_id,
            "source_file_count": len(source_files),
            "source_file": source_files[0] if len(source_files) == 1 else None,
            "source_files": source_files,
            "nodes": node_summary.get("nodes", 0),
            "distinct_node_ids": node_summary.get("distinct_node_ids", 0),
            "duplicate_node_ids": node_duplicates.get("duplicate_node_ids", 0),
            "duplicate_node_records": node_duplicates.get(
                "duplicate_node_records", 0
            ),
            "node_file_hashes": sorted(node_summary.get("file_hashes") or []),
            "edges": edge_summary.get("edges", 0),
            "distinct_edge_ids": edge_summary.get("distinct_edge_ids", 0),
            "duplicate_edge_ids": edge_duplicates.get("duplicate_edge_ids", 0),
            "duplicate_edge_records": edge_duplicates.get(
                "duplicate_edge_records", 0
            ),
            "edge_file_hashes": sorted(edge_summary.get("file_hashes") or []),
            "edge_types": edge_types,
        }
    )


def _mongo_target_filter(
    file_path: str, file_id: str | None, repo_id: str | None
) -> dict[str, Any]:
    if file_id:
        return {"$or": [{"_id": file_id}, {"file_id": file_id}]}
    query: dict[str, Any] = {"file_path": file_path}
    if repo_id:
        query["repo_id"] = repo_id
    return query


def collect_mongodb(
    uri: str,
    database: str,
    collection: str,
    file_path: str,
    *,
    file_id: str | None = None,
    repo_id: str | None = None,
) -> dict[str, Any]:
    """Collect target metadata plus a compact whole-collection fingerprint."""
    from pymongo import MongoClient

    client = MongoClient(
        uri,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        socketTimeoutMS=5000,
    )
    try:
        target = client[database][collection]
        target_filter = _mongo_target_filter(file_path, file_id, repo_id)
        documents = [_json_safe(document) for document in target.find(target_filter)]
        documents.sort(key=lambda item: (str(item.get("file_id")), str(item.get("_id"))))

        duplicate_groups = [
            _json_safe(row)
            for row in target.aggregate(
                [
                    {"$match": {"file_id": {"$type": "string"}}},
                    {"$group": {"_id": "$file_id", "copies": {"$sum": 1}}},
                    {"$match": {"copies": {"$gt": 1}}},
                    {"$sort": {"_id": 1}},
                ]
            )
        ]
        projection = {
            "_id": 1,
            "file_id": 1,
            "repo_id": 1,
            "file_path": 1,
            "file_hash": 1,
            "parse_status": 1,
            "event_time": 1,
            "file_size_bytes": 1,
            "total_nodes": 1,
            "total_edges": 1,
            "parser_version": 1,
        }
        all_versions = [_json_safe(document) for document in target.find({}, projection)]
        all_versions.sort(
            key=lambda item: (
                str(item.get("file_id")),
                str(item.get("_id")),
                str(item.get("file_path")),
            )
        )
        collection_total = target.count_documents({})
    finally:
        client.close()

    return {
        "document_count": len(documents),
        "documents": documents,
        "duplicate_file_ids": len(duplicate_groups),
        "duplicate_groups": duplicate_groups,
        "collection_document_count": collection_total,
        # The projection includes event_time. If Spark replays an already
        # committed message after restart, this fingerprint will change.
        "collection_fingerprint": _fingerprint(all_versions),
    }


def _numeric_batches(directory: Path) -> list[int]:
    if not directory.is_dir():
        return []
    return sorted(
        int(path.name)
        for path in directory.iterdir()
        if path.is_file() and path.name.isdigit()
    )


def _last_json_line(path: Path) -> Any:
    parsed: list[Any] = []
    for line in path.read_text(encoding="utf-8").splitlines()[1:]:
        line = line.strip()
        if line:
            parsed.append(json.loads(line))
    return parsed[-1] if parsed else None


def _offset_total(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, Mapping):
        return sum(_offset_total(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return sum(_offset_total(item) for item in value)
    return 0


def collect_checkpoint(location: str | Path) -> dict[str, Any]:
    """Read Spark's append-only offset/commit logs without modifying them."""
    checkpoint = Path(location)
    offset_batches = _numeric_batches(checkpoint / "offsets")
    committed_batches = _numeric_batches(checkpoint / "commits")
    latest_offset = offset_batches[-1] if offset_batches else None
    latest_commit = committed_batches[-1] if committed_batches else None
    committed_offsets = None
    errors: list[str] = []

    if latest_commit is not None:
        offset_path = checkpoint / "offsets" / str(latest_commit)
        if offset_path.is_file():
            try:
                committed_offsets = _last_json_line(offset_path)
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                errors.append(f"cannot parse offset batch {latest_commit}: {exc}")
        else:
            errors.append(f"committed batch {latest_commit} has no offset log")

    pending = [batch for batch in offset_batches if batch not in set(committed_batches)]
    return {
        "location": str(checkpoint),
        "exists": checkpoint.is_dir(),
        "offset_batches": offset_batches,
        "committed_batches": committed_batches,
        "latest_offset_batch": latest_offset,
        "latest_committed_batch": latest_commit,
        "committed_offsets": committed_offsets,
        "committed_offset_total": _offset_total(committed_offsets),
        "pending_offset_batches": pending,
        "errors": errors,
    }


def capture_snapshot(
    *,
    file_path: str,
    repo_id: str | None,
    file_id: str | None,
    neo4j_uri: str,
    neo4j_user: str,
    neo4j_password: str,
    neo4j_database: str | None,
    mongo_uri: str,
    mongo_db: str,
    mongo_collection: str,
    checkpoint_location: str,
) -> dict[str, Any]:
    neo4j = collect_neo4j(
        neo4j_uri,
        neo4j_user,
        neo4j_password,
        file_path,
        file_id=file_id,
        repo_id=repo_id,
        database=neo4j_database,
    )
    resolved_file_id = file_id or neo4j.get("file_id")
    mongodb = collect_mongodb(
        mongo_uri,
        mongo_db,
        mongo_collection,
        file_path,
        file_id=resolved_file_id,
        repo_id=repo_id,
    )
    return {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "captured_at": _utc_now(),
        # Kept at top level for compatibility with the original evidence file.
        "file_path": file_path,
        "target": {
            "repo_id": repo_id,
            "file_id": resolved_file_id,
            "file_path": file_path,
        },
        "neo4j": neo4j,
        "mongodb": mongodb,
        "checkpoint": collect_checkpoint(checkpoint_location),
    }


class _Checks:
    def __init__(self) -> None:
        self.items: list[dict[str, Any]] = []

    def expect(self, name: str, condition: bool, detail: str) -> None:
        self.items.append({"name": name, "passed": bool(condition), "detail": detail})

    def extend(self, items: list[dict[str, Any]]) -> None:
        self.items.extend(items)

    def report(self) -> dict[str, Any]:
        failures = [item for item in self.items if not item["passed"]]
        return {
            "passed": not failures,
            "check_count": len(self.items),
            "failure_count": len(failures),
            "checks": self.items,
            "failures": failures,
        }


def _current_hash(snapshot: Mapping[str, Any]) -> str | None:
    source = snapshot.get("neo4j", {}).get("source_file") or {}
    if source.get("file_hash"):
        return source["file_hash"]
    documents = snapshot.get("mongodb", {}).get("documents") or []
    return documents[0].get("file_hash") if len(documents) == 1 else None


def _target_id(snapshot: Mapping[str, Any]) -> str | None:
    return (
        snapshot.get("target", {}).get("file_id")
        or snapshot.get("neo4j", {}).get("file_id")
    )


def _expected_edge_count(source: Mapping[str, Any]) -> int | None:
    names = (
        "total_ast_edges",
        "total_cfg_edges",
        "total_dfg_edges",
        "total_call_edges",
    )
    if not all(isinstance(source.get(name), int) for name in names):
        return None
    return sum(source[name] for name in names)


def validate_snapshot(snapshot: Mapping[str, Any], phase: str) -> dict[str, Any]:
    """Validate that one snapshot is internally reconciled and duplicate-free."""
    checks = _Checks()
    neo4j = snapshot.get("neo4j") or {}
    mongodb = snapshot.get("mongodb") or {}
    checkpoint = snapshot.get("checkpoint") or {}
    source = neo4j.get("source_file") or {}
    documents = mongodb.get("documents") or []
    target_id = _target_id(snapshot)
    current_hash = _current_hash(snapshot)

    def expect(name: str, condition: bool, detail: str) -> None:
        checks.expect(f"{phase}.{name}", condition, detail)

    expect(
        "snapshot_schema",
        snapshot.get("schema_version") == SNAPSHOT_SCHEMA_VERSION,
        f"schema_version={snapshot.get('schema_version')!r}",
    )
    expect("stable_file_id", bool(target_id), f"file_id={target_id!r}")
    expect(
        "one_source_file",
        neo4j.get("source_file_count") == 1,
        f"source_file_count={neo4j.get('source_file_count')!r}",
    )
    expect(
        "node_ids_unique",
        neo4j.get("nodes") == neo4j.get("distinct_node_ids")
        and neo4j.get("duplicate_node_ids") == 0,
        (
            f"nodes={neo4j.get('nodes')!r}, "
            f"distinct={neo4j.get('distinct_node_ids')!r}, "
            f"duplicate_groups={neo4j.get('duplicate_node_ids')!r}"
        ),
    )
    expect(
        "edge_ids_unique",
        neo4j.get("edges") == neo4j.get("distinct_edge_ids")
        and neo4j.get("duplicate_edge_ids") == 0,
        (
            f"edges={neo4j.get('edges')!r}, "
            f"distinct={neo4j.get('distinct_edge_ids')!r}, "
            f"duplicate_groups={neo4j.get('duplicate_edge_ids')!r}"
        ),
    )
    expect(
        "one_mongo_document",
        mongodb.get("document_count") == 1 and len(documents) == 1,
        f"document_count={mongodb.get('document_count')!r}",
    )
    expect(
        "mongo_file_ids_unique",
        mongodb.get("duplicate_file_ids") == 0,
        f"duplicate_groups={mongodb.get('duplicate_file_ids')!r}",
    )
    if len(documents) == 1 and target_id:
        document = documents[0]
        expect(
            "mongo_uses_stable_id",
            str(document.get("_id")) == target_id
            and document.get("file_id") == target_id,
            f"_id={document.get('_id')!r}, file_id={document.get('file_id')!r}",
        )
    else:
        expect("mongo_uses_stable_id", False, "target document or file_id missing")

    expect(
        "successful_revision",
        source.get("parse_status") == "success"
        and len(documents) == 1
        and documents[0].get("parse_status") == "success",
        (
            f"neo4j_status={source.get('parse_status')!r}, "
            f"mongo_status={documents[0].get('parse_status') if len(documents) == 1 else None!r}"
        ),
    )
    expect(
        "node_count_matches_metadata",
        isinstance(source.get("total_nodes"), int)
        and source.get("total_nodes") == neo4j.get("nodes")
        and len(documents) == 1
        and documents[0].get("total_nodes") == neo4j.get("nodes"),
        (
            f"source={source.get('total_nodes')!r}, graph={neo4j.get('nodes')!r}, "
            f"mongo={documents[0].get('total_nodes') if len(documents) == 1 else None!r}"
        ),
    )
    expected_edges = _expected_edge_count(source)
    mongo_edges = documents[0].get("total_edges") if len(documents) == 1 else None
    mongo_edge_count = (
        sum(mongo_edges.values())
        if isinstance(mongo_edges, Mapping)
        and all(isinstance(value, int) for value in mongo_edges.values())
        else None
    )
    expect(
        "edge_count_matches_metadata",
        expected_edges is not None
        and expected_edges == neo4j.get("edges")
        and mongo_edge_count == neo4j.get("edges"),
        (
            f"source={expected_edges!r}, graph={neo4j.get('edges')!r}, "
            f"mongo={mongo_edge_count!r}"
        ),
    )

    observed_hashes = set()
    if source.get("file_hash"):
        observed_hashes.add(source["file_hash"])
    if len(documents) == 1 and documents[0].get("file_hash"):
        observed_hashes.add(documents[0]["file_hash"])
    observed_hashes.update(neo4j.get("node_file_hashes") or [])
    observed_hashes.update(neo4j.get("edge_file_hashes") or [])
    expect(
        "one_current_hash_everywhere",
        bool(current_hash) and observed_hashes == {current_hash},
        f"current_hash={current_hash!r}, observed_hashes={sorted(observed_hashes)!r}",
    )
    expect(
        "checkpoint_committed",
        checkpoint.get("exists") is True
        and checkpoint.get("latest_committed_batch") is not None
        and checkpoint.get("committed_offsets") is not None
        and not checkpoint.get("errors"),
        (
            f"exists={checkpoint.get('exists')!r}, "
            f"latest_commit={checkpoint.get('latest_committed_batch')!r}, "
            f"errors={checkpoint.get('errors')!r}"
        ),
    )
    expect(
        "checkpoint_has_no_pending_batch",
        checkpoint.get("pending_offset_batches") == [],
        f"pending={checkpoint.get('pending_offset_batches')!r}",
    )
    return checks.report()


def _graph_projection(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    neo4j = snapshot.get("neo4j") or {}
    return {
        key: neo4j.get(key)
        for key in ("nodes", "edges", "distinct_node_ids", "distinct_edge_ids", "edge_types")
    }


def _mongo_revision_projection(snapshot: Mapping[str, Any]) -> dict[str, Any] | None:
    documents = snapshot.get("mongodb", {}).get("documents") or []
    if len(documents) != 1:
        return None
    document = documents[0]
    return {
        key: document.get(key)
        for key in (
            "_id",
            "file_id",
            "repo_id",
            "file_path",
            "file_hash",
            "file_size_bytes",
            "total_nodes",
            "total_edges",
            "parser_version",
            "parse_status",
        )
    }


def _checkpoint_offset_total(snapshot: Mapping[str, Any]) -> int:
    value = snapshot.get("checkpoint", {}).get("committed_offset_total")
    return value if isinstance(value, int) else -1


def _compare_exact(
    checks: _Checks,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    label: str,
) -> None:
    checks.expect(
        f"{label}.stable_file_id",
        bool(_target_id(before)) and _target_id(before) == _target_id(after),
        f"before={_target_id(before)!r}, after={_target_id(after)!r}",
    )
    checks.expect(
        f"{label}.same_content_hash",
        bool(_current_hash(before)) and _current_hash(before) == _current_hash(after),
        f"before={_current_hash(before)!r}, after={_current_hash(after)!r}",
    )
    checks.expect(
        f"{label}.graph_counts_unchanged",
        _graph_projection(before) == _graph_projection(after),
        f"before={_graph_projection(before)!r}, after={_graph_projection(after)!r}",
    )
    checks.expect(
        f"{label}.mongo_revision_unchanged",
        _mongo_revision_projection(before) == _mongo_revision_projection(after),
        "stable metadata fields are identical (event_time/parse duration excluded)",
    )
    checks.expect(
        f"{label}.metadata_offset_advanced",
        _checkpoint_offset_total(after) > _checkpoint_offset_total(before),
        (
            f"before={_checkpoint_offset_total(before)}, "
            f"after={_checkpoint_offset_total(after)}"
        ),
    )


def verify_replay_sequence(
    baseline: Mapping[str, Any],
    exact_replay: Mapping[str, Any],
    modified: Mapping[str, Any],
    restart: Mapping[str, Any],
    *,
    modified_replay: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Verify Task 6 acceptance invariants across ordered snapshots."""
    checks = _Checks()
    phases: list[tuple[str, Mapping[str, Any]]] = [
        ("baseline", baseline),
        ("exact_replay", exact_replay),
        ("modified", modified),
    ]
    if modified_replay is not None:
        phases.append(("modified_replay", modified_replay))
    phases.append(("restart", restart))
    for phase, snapshot in phases:
        checks.extend(validate_snapshot(snapshot, phase)["checks"])

    _compare_exact(checks, baseline, exact_replay, "exact_replay")
    checks.expect(
        "modified.stable_file_id",
        bool(_target_id(exact_replay))
        and _target_id(exact_replay) == _target_id(modified),
        f"before={_target_id(exact_replay)!r}, after={_target_id(modified)!r}",
    )
    old_hash = _current_hash(exact_replay)
    new_hash = _current_hash(modified)
    checks.expect(
        "modified.content_hash_changed",
        bool(old_hash) and bool(new_hash) and old_hash != new_hash,
        f"before={old_hash!r}, after={new_hash!r}",
    )
    modified_hashes = set(modified.get("neo4j", {}).get("node_file_hashes") or [])
    modified_hashes.update(modified.get("neo4j", {}).get("edge_file_hashes") or [])
    source = modified.get("neo4j", {}).get("source_file") or {}
    documents = modified.get("mongodb", {}).get("documents") or []
    modified_hashes.add(source.get("file_hash"))
    if len(documents) == 1:
        modified_hashes.add(documents[0].get("file_hash"))
    modified_hashes.discard(None)
    checks.expect(
        "modified.old_hash_reconciled",
        bool(new_hash) and modified_hashes == {new_hash} and old_hash not in modified_hashes,
        f"old={old_hash!r}, current_store_hashes={sorted(modified_hashes)!r}",
    )
    checks.expect(
        "modified.mongo_cardinality_unchanged",
        exact_replay.get("mongodb", {}).get("collection_document_count")
        == modified.get("mongodb", {}).get("collection_document_count"),
        (
            f"before={exact_replay.get('mongodb', {}).get('collection_document_count')!r}, "
            f"after={modified.get('mongodb', {}).get('collection_document_count')!r}"
        ),
    )
    checks.expect(
        "modified.metadata_offset_advanced",
        _checkpoint_offset_total(modified) > _checkpoint_offset_total(exact_replay),
        (
            f"before={_checkpoint_offset_total(exact_replay)}, "
            f"after={_checkpoint_offset_total(modified)}"
        ),
    )

    before_restart = modified
    if modified_replay is not None:
        _compare_exact(checks, modified, modified_replay, "modified_replay")
        before_restart = modified_replay

    checks.expect(
        "restart.graph_unchanged",
        _graph_projection(before_restart) == _graph_projection(restart),
        "target graph projection is identical before and after restart",
    )
    checks.expect(
        "restart.mongo_target_unchanged",
        before_restart.get("mongodb", {}).get("documents")
        == restart.get("mongodb", {}).get("documents"),
        "target MongoDB document, including event_time, is identical",
    )
    checks.expect(
        "restart.mongo_collection_unchanged",
        before_restart.get("mongodb", {}).get("collection_document_count")
        == restart.get("mongodb", {}).get("collection_document_count")
        and before_restart.get("mongodb", {}).get("collection_fingerprint")
        == restart.get("mongodb", {}).get("collection_fingerprint"),
        (
            "before_count="
            f"{before_restart.get('mongodb', {}).get('collection_document_count')!r}, "
            f"after_count={restart.get('mongodb', {}).get('collection_document_count')!r}"
        ),
    )
    checks.expect(
        "restart_checkpoint.skipped_committed_offsets",
        before_restart.get("checkpoint", {}).get("latest_committed_batch")
        == restart.get("checkpoint", {}).get("latest_committed_batch")
        and before_restart.get("checkpoint", {}).get("committed_offsets")
        == restart.get("checkpoint", {}).get("committed_offsets"),
        (
            "before_batch="
            f"{before_restart.get('checkpoint', {}).get('latest_committed_batch')!r}, "
            f"after_batch={restart.get('checkpoint', {}).get('latest_committed_batch')!r}"
        ),
    )

    report = checks.report()
    return {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "verified_at": _utc_now(),
        **report,
    }


def _load_json(path: str) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"snapshot must be a JSON object: {path}")
    return value


def _render_and_write(value: Mapping[str, Any], output: str | None) -> None:
    rendered = json.dumps(value, ensure_ascii=False, indent=2, default=str)
    print(rendered)
    if output:
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture or verify Task 6 idempotent replay evidence."
    )
    parser.add_argument(
        "file_path",
        nargs="?",
        help="Repository-relative path stored in events (snapshot mode)",
    )
    parser.add_argument("--repo-id", help="Repository ID used by Parser Service")
    parser.add_argument("--file-id", help="Stable file ID; preferred over path lookup")
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="cpg-password")
    parser.add_argument("--neo4j-database", default="neo4j")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--mongo-db", default="cpg")
    parser.add_argument("--mongo-collection", default="metadata")
    parser.add_argument("--checkpoint-location", default=DEFAULT_CHECKPOINT)
    parser.add_argument(
        "--verify",
        nargs=4,
        metavar=("BASELINE", "EXACT_REPLAY", "MODIFIED", "RESTART"),
        help="Compare four ordered snapshot JSON files instead of capturing state",
    )
    parser.add_argument(
        "--modified-replay",
        help="Optional exact replay snapshot of the modified revision, before restart",
    )
    parser.add_argument("--output", help="Optional JSON snapshot/report output path")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    try:
        if args.verify:
            if args.file_path:
                parser.error("file_path cannot be used together with --verify")
            baseline, exact_replay, modified, restart = map(_load_json, args.verify)
            modified_replay = (
                _load_json(args.modified_replay) if args.modified_replay else None
            )
            report = verify_replay_sequence(
                baseline,
                exact_replay,
                modified,
                restart,
                modified_replay=modified_replay,
            )
            _render_and_write(report, args.output)
            return 0 if report["passed"] else 1

        if args.modified_replay:
            parser.error("--modified-replay requires --verify")
        if not args.file_path:
            parser.error("file_path is required in snapshot mode")
        snapshot = capture_snapshot(
            file_path=args.file_path,
            repo_id=args.repo_id,
            file_id=args.file_id,
            neo4j_uri=args.neo4j_uri,
            neo4j_user=args.neo4j_user,
            neo4j_password=args.neo4j_password,
            neo4j_database=args.neo4j_database,
            mongo_uri=args.mongo_uri,
            mongo_db=args.mongo_db,
            mongo_collection=args.mongo_collection,
            checkpoint_location=args.checkpoint_location,
        )
        snapshot["validation"] = validate_snapshot(snapshot, "snapshot")
        _render_and_write(snapshot, args.output)
        return 0 if snapshot["validation"]["passed"] else 1
    except (OSError, ValueError, RuntimeError) as exc:
        parser.exit(2, f"verify_replay: {exc}\n")
    except Exception as exc:  # Database drivers expose many optional exceptions.
        parser.exit(2, f"verify_replay: {type(exc).__name__}: {exc}\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
