"""Contract tests for stable parser IDs and Kafka event context."""

from __future__ import annotations

import datetime
import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

from src.parser_service import CPGParser, _stable_file_id


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_CONTEXT = {
    "schema_version",
    "event_time",
    "repo_id",
    "file_id",
    "file_path",
    "file_hash",
    "parse_status",
}


class ParserContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo_root = Path(self.temp_dir.name)
        self.source_path = self.repo_root / "pkg" / "sample.py"
        self.source_path.parent.mkdir()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write_and_parse(self, source: str, repo_id: str = "example/repo"):
        self.source_path.write_text(source, encoding="utf-8")
        return CPGParser(
            str(self.source_path),
            str(self.repo_root),
            repo_id=repo_id,
        ).parse()

    def test_ids_are_full_sha256_unique_and_deterministic(self) -> None:
        # Add and Load are location-less singleton-style AST nodes. They exposed
        # collisions in the former (type, line, column) ID strategy.
        source = "x = a + b\ny = c + d\n"
        nodes_1, edges_1, _, _ = self._write_and_parse(source)
        nodes_2, edges_2, _, _ = self._write_and_parse(source)

        node_ids_1 = [event["node_id"] for event in nodes_1]
        edge_ids_1 = [event["edge_id"] for event in edges_1]
        self.assertEqual(len(node_ids_1), len(set(node_ids_1)))
        self.assertEqual(len(edge_ids_1), len(set(edge_ids_1)))
        self.assertTrue(all(re.fullmatch(r"node_[0-9a-f]{64}", value) for value in node_ids_1))
        self.assertTrue(all(re.fullmatch(r"edge_[0-9a-f]{64}", value) for value in edge_ids_1))
        self.assertEqual(node_ids_1, [event["node_id"] for event in nodes_2])
        self.assertEqual(edge_ids_1, [event["edge_id"] for event in edges_2])

    def test_repo_and_normalized_file_path_participate_in_identity(self) -> None:
        source = "answer = 42\n"
        nodes_a, _, metadata_a, _ = self._write_and_parse(source, repo_id="org/repo-a")
        nodes_b, _, metadata_b, _ = self._write_and_parse(source, repo_id="org/repo-b")

        self.assertNotEqual(metadata_a["file_id"], metadata_b["file_id"])
        self.assertNotEqual(nodes_a[0]["node_id"], nodes_b[0]["node_id"])
        self.assertEqual(
            _stable_file_id("org/repo", "pkg\\sample.py"),
            _stable_file_id("org/repo", "./pkg/sample.py"),
        )

    def test_each_call_site_has_a_distinct_edge(self) -> None:
        source = """\
def target():
    return 1

def caller():
    target()
    target()
    print(1)
    print(2)
"""
        _, edges, _, _ = self._write_and_parse(source)
        internal = [event for event in edges if event["type"] == "CALL"]
        external = [event for event in edges if event["type"] == "CALL_EXTERNAL"]

        self.assertEqual(2, len(internal))
        self.assertEqual(2, len({event["edge_id"] for event in internal}))
        self.assertEqual(2, len(external))
        self.assertEqual(2, len({event["edge_id"] for event in external}))
        for event in internal + external:
            self.assertRegex(event["properties"]["call_site_id"], r"^node_[0-9a-f]{64}$")

    def test_success_events_have_consistent_context_and_aware_utc_time(self) -> None:
        nodes, edges, metadata, error = self._write_and_parse("value = print(1)\n")
        self.assertIsNone(error)

        events = [*nodes, *edges, metadata]
        for event in events:
            self.assertTrue(REQUIRED_CONTEXT.issubset(event))
            self.assertEqual("example/repo", event["repo_id"])
            self.assertEqual("pkg/sample.py", event["file_path"])
            self.assertEqual("success", event["parse_status"])
            self.assertEqual(metadata["file_id"], event["file_id"])
            self.assertEqual(metadata["file_hash"], event["file_hash"])
            self.assertTrue(event["event_time"].endswith("Z"))
            timestamp = datetime.datetime.fromisoformat(event["event_time"])
            self.assertIsNotNone(timestamp.tzinfo)
            self.assertEqual(datetime.timedelta(0), timestamp.utcoffset())

    def test_syntax_error_emits_error_metadata_and_error_event(self) -> None:
        source = "def broken(:\n    pass\n"
        nodes, edges, metadata, error = self._write_and_parse(source)

        self.assertEqual([], nodes)
        self.assertEqual([], edges)
        self.assertIsNotNone(error)
        assert error is not None
        expected_hash = hashlib.sha256(source.encode("utf-8")).hexdigest()
        for event in (metadata, error):
            self.assertTrue(REQUIRED_CONTEXT.issubset(event))
            self.assertEqual("error", event["parse_status"])
            self.assertEqual(expected_hash, event["file_hash"])
            self.assertEqual(metadata["file_id"], event["file_id"])

    def test_cli_supports_script_and_module_import_modes(self) -> None:
        self.source_path.write_text("value = 1\n", encoding="utf-8")
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        commands = (
            [
                sys.executable,
                str(PROJECT_ROOT / "src" / "parser_service.py"),
                str(self.source_path),
                str(self.repo_root),
            ],
            [
                sys.executable,
                "-m",
                "src.parser_service",
                str(self.source_path),
                str(self.repo_root),
            ],
        )
        for command in commands:
            with self.subTest(command=command):
                result = subprocess.run(
                    command,
                    cwd=PROJECT_ROOT,
                    env=env,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertIn("[Parser]", result.stdout)


if __name__ == "__main__":
    unittest.main()
