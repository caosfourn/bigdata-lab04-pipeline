from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path

from scripts.verify_replay import (
    SNAPSHOT_SCHEMA_VERSION,
    _mongo_target_filter,
    collect_checkpoint,
    validate_snapshot,
    verify_replay_sequence,
)


FILE_ID = "file_123"
FILE_PATH = "src/example.py"
REPO_ID = "example/project"


def make_snapshot(
    *,
    file_hash: str = "a" * 64,
    nodes: int = 3,
    ast_edges: int = 2,
    cfg_edges: int = 1,
    offset: int = 10,
    batch: int = 1,
    event_time: str = "2026-07-25T00:00:00Z",
    fingerprint: str = "fingerprint-baseline",
) -> dict:
    edge_types = {"AST_CHILD": ast_edges, "CFG_NEXT": cfg_edges}
    total_edges = ast_edges + cfg_edges
    source_file = {
        "file_id": FILE_ID,
        "repo_id": REPO_ID,
        "file_path": FILE_PATH,
        "file_hash": file_hash,
        "file_size_bytes": 100,
        "total_nodes": nodes,
        "total_ast_edges": ast_edges,
        "total_cfg_edges": cfg_edges,
        "total_dfg_edges": 0,
        "total_call_edges": 0,
        "parser_version": "ast-stdlib",
        "parse_status": "success",
        "event_time": event_time,
    }
    document = {
        "_id": FILE_ID,
        "file_id": FILE_ID,
        "repo_id": REPO_ID,
        "file_path": FILE_PATH,
        "file_hash": file_hash,
        "file_size_bytes": 100,
        "total_nodes": nodes,
        "total_edges": {
            "ast": ast_edges,
            "cfg": cfg_edges,
            "dfg": 0,
            "call": 0,
        },
        "parser_version": "ast-stdlib",
        "parse_status": "success",
        "parse_duration_ms": 1.25,
        "event_time": event_time,
    }
    return {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "captured_at": event_time,
        "file_path": FILE_PATH,
        "target": {
            "file_id": FILE_ID,
            "repo_id": REPO_ID,
            "file_path": FILE_PATH,
        },
        "neo4j": {
            "file_id": FILE_ID,
            "source_file_count": 1,
            "source_file": source_file,
            "source_files": [source_file],
            "nodes": nodes,
            "distinct_node_ids": nodes,
            "duplicate_node_ids": 0,
            "duplicate_node_records": 0,
            "node_file_hashes": [file_hash],
            "edges": total_edges,
            "distinct_edge_ids": total_edges,
            "duplicate_edge_ids": 0,
            "duplicate_edge_records": 0,
            "edge_file_hashes": [file_hash],
            "edge_types": edge_types,
        },
        "mongodb": {
            "document_count": 1,
            "documents": [document],
            "duplicate_file_ids": 0,
            "duplicate_groups": [],
            "collection_document_count": 25,
            "collection_fingerprint": fingerprint,
        },
        "checkpoint": {
            "location": "checkpoints/test",
            "exists": True,
            "offset_batches": list(range(batch + 1)),
            "committed_batches": list(range(batch + 1)),
            "latest_offset_batch": batch,
            "latest_committed_batch": batch,
            "committed_offsets": {"cpg.metadata": {"0": offset}},
            "committed_offset_total": offset,
            "pending_offset_batches": [],
            "errors": [],
        },
    }


def make_valid_sequence() -> tuple[dict, dict, dict, dict]:
    baseline = make_snapshot()
    exact = make_snapshot(
        offset=11,
        batch=2,
        event_time="2026-07-25T00:01:00Z",
        fingerprint="fingerprint-exact",
    )
    modified = make_snapshot(
        file_hash="b" * 64,
        nodes=4,
        ast_edges=3,
        cfg_edges=1,
        offset=12,
        batch=3,
        event_time="2026-07-25T00:02:00Z",
        fingerprint="fingerprint-modified",
    )
    restart = copy.deepcopy(modified)
    restart["captured_at"] = "2026-07-25T00:03:00Z"
    return baseline, exact, modified, restart


class CheckpointReaderTests(unittest.TestCase):
    def test_reads_latest_committed_offsets_and_ignores_crc_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "offsets").mkdir()
            (root / "commits").mkdir()
            (root / "offsets" / ".1.crc").write_bytes(b"crc")
            (root / "offsets" / "0").write_text(
                'v1\n{}\n{"cpg.metadata":{"0":4,"1":2}}\n', encoding="utf-8"
            )
            (root / "offsets" / "1").write_text(
                'v1\n{}\n{"cpg.metadata":{"0":5,"1":2}}\n', encoding="utf-8"
            )
            (root / "commits" / "0").write_text('v1\n{}\n', encoding="utf-8")
            (root / "commits" / "1").write_text('v1\n{}\n', encoding="utf-8")

            state = collect_checkpoint(root)

        self.assertEqual(state["offset_batches"], [0, 1])
        self.assertEqual(state["committed_batches"], [0, 1])
        self.assertEqual(state["latest_committed_batch"], 1)
        self.assertEqual(
            state["committed_offsets"], {"cpg.metadata": {"0": 5, "1": 2}}
        )
        self.assertEqual(state["committed_offset_total"], 7)
        self.assertEqual(state["pending_offset_batches"], [])
        self.assertEqual(state["errors"], [])

    def test_reports_uncommitted_offset_batch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "offsets").mkdir()
            (root / "commits").mkdir()
            for batch in (0, 1):
                (root / "offsets" / str(batch)).write_text(
                    f'v1\n{{}}\n{{"cpg.metadata":{{"0":{batch + 1}}}}}\n',
                    encoding="utf-8",
                )
            (root / "commits" / "0").write_text('v1\n{}\n', encoding="utf-8")

            state = collect_checkpoint(root)

        self.assertEqual(state["latest_committed_batch"], 0)
        self.assertEqual(state["pending_offset_batches"], [1])


class SnapshotValidationTests(unittest.TestCase):
    def test_accepts_reconciled_duplicate_free_snapshot(self) -> None:
        report = validate_snapshot(make_snapshot(), "baseline")

        self.assertTrue(report["passed"])
        self.assertEqual(report["failure_count"], 0)

    def test_rejects_duplicate_edges_and_stale_hash(self) -> None:
        snapshot = make_snapshot()
        snapshot["neo4j"]["edges"] += 1
        snapshot["neo4j"]["duplicate_edge_ids"] = 1
        snapshot["neo4j"]["edge_file_hashes"].append("old-hash")

        report = validate_snapshot(snapshot, "modified")
        failed_names = {item["name"] for item in report["failures"]}

        self.assertFalse(report["passed"])
        self.assertIn("modified.edge_ids_unique", failed_names)
        self.assertIn("modified.one_current_hash_everywhere", failed_names)

    def test_rejects_mongo_document_with_non_stable_id(self) -> None:
        snapshot = make_snapshot()
        snapshot["mongodb"]["documents"][0]["_id"] = "different-id"

        report = validate_snapshot(snapshot, "baseline")

        self.assertIn(
            "baseline.mongo_uses_stable_id",
            {item["name"] for item in report["failures"]},
        )


class ReplaySequenceTests(unittest.TestCase):
    def test_accepts_baseline_exact_modified_and_checkpoint_restart(self) -> None:
        report = verify_replay_sequence(*make_valid_sequence())

        self.assertTrue(report["passed"])
        self.assertEqual(report["failure_count"], 0)

    def test_rejects_modified_revision_when_hash_did_not_change(self) -> None:
        baseline, exact, modified, restart = make_valid_sequence()
        unchanged_hash = exact["neo4j"]["source_file"]["file_hash"]
        for snapshot in (modified, restart):
            snapshot["neo4j"]["source_file"]["file_hash"] = unchanged_hash
            snapshot["neo4j"]["node_file_hashes"] = [unchanged_hash]
            snapshot["neo4j"]["edge_file_hashes"] = [unchanged_hash]
            snapshot["mongodb"]["documents"][0]["file_hash"] = unchanged_hash

        report = verify_replay_sequence(baseline, exact, modified, restart)

        self.assertIn(
            "modified.content_hash_changed",
            {item["name"] for item in report["failures"]},
        )

    def test_rejects_restart_that_reprocesses_committed_data(self) -> None:
        baseline, exact, modified, restart = make_valid_sequence()
        restart["checkpoint"]["latest_committed_batch"] += 1
        restart["checkpoint"]["committed_offsets"] = {
            "cpg.metadata": {"0": 13}
        }
        restart["checkpoint"]["committed_offset_total"] = 13
        restart["mongodb"]["documents"][0]["event_time"] = (
            "2026-07-25T00:04:00Z"
        )
        restart["mongodb"]["collection_fingerprint"] = "changed-after-restart"

        report = verify_replay_sequence(baseline, exact, modified, restart)
        failed_names = {item["name"] for item in report["failures"]}

        self.assertIn("restart.mongo_target_unchanged", failed_names)
        self.assertIn("restart.mongo_collection_unchanged", failed_names)
        self.assertIn("restart_checkpoint.skipped_committed_offsets", failed_names)

    def test_optionally_verifies_exact_replay_of_modified_revision(self) -> None:
        baseline, exact, modified, restart = make_valid_sequence()
        modified_replay = copy.deepcopy(modified)
        modified_replay["captured_at"] = "2026-07-25T00:02:30Z"
        modified_replay["mongodb"]["documents"][0]["event_time"] = (
            "2026-07-25T00:02:30Z"
        )
        modified_replay["checkpoint"]["latest_offset_batch"] = 4
        modified_replay["checkpoint"]["latest_committed_batch"] = 4
        modified_replay["checkpoint"]["offset_batches"].append(4)
        modified_replay["checkpoint"]["committed_batches"].append(4)
        modified_replay["checkpoint"]["committed_offsets"] = {
            "cpg.metadata": {"0": 13}
        }
        modified_replay["checkpoint"]["committed_offset_total"] = 13
        modified_replay["mongodb"]["collection_fingerprint"] = (
            "fingerprint-modified-replay"
        )
        restart = copy.deepcopy(modified_replay)
        restart["captured_at"] = "2026-07-25T00:03:00Z"

        report = verify_replay_sequence(
            baseline,
            exact,
            modified,
            restart,
            modified_replay=modified_replay,
        )

        self.assertTrue(report["passed"])


class MongoSelectorTests(unittest.TestCase):
    def test_file_id_is_preferred_over_path_and_repo(self) -> None:
        self.assertEqual(
            _mongo_target_filter(FILE_PATH, FILE_ID, REPO_ID),
            {"$or": [{"_id": FILE_ID}, {"file_id": FILE_ID}]},
        )

    def test_path_lookup_is_scoped_by_repository(self) -> None:
        self.assertEqual(
            _mongo_target_filter(FILE_PATH, None, REPO_ID),
            {"file_path": FILE_PATH, "repo_id": REPO_ID},
        )


if __name__ == "__main__":
    unittest.main()
