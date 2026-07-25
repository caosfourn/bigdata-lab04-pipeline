"""Task 1 contract tests for deterministic repository discovery."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from src.discovery import discover_python_files


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class DiscoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo_root = Path(self.temp_dir.name) / "repository"
        self.repo_root.mkdir()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write(self, relative_path: str, content: str = "value = 1\n") -> Path:
        path = self.repo_root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def test_invalid_repository_path_fails_instead_of_looking_empty(self) -> None:
        missing = self.repo_root / "missing"
        with self.assertRaisesRegex(FileNotFoundError, "does not exist"):
            discover_python_files(missing)

        regular_file = self._write("not-a-directory.txt")
        with self.assertRaisesRegex(NotADirectoryError, "not a directory"):
            discover_python_files(regular_file)

    def test_filters_test_setup_and_generated_sources(self) -> None:
        included = {
            "manage.py",
            "src/contest.py",
            "src/core.py",
            "src/latest.py",
            "src/testimony.py",
        }
        excluded = {
            ".git/hooks/helper.py",
            "examples/example.py",
            "generated/model.py",
            "integration_tests/integration.py",
            "src/conftest.py",
            "src/helper_generated.py",
            "src/helper_gen.py",
            "src/helper_pb2.py",
            "src/helper_pb2_grpc.py",
            "src/helper_test.py",
            "src/helper_tests.py",
            "src/setup.py",
            "src/test.py",
            "src/test_helper.py",
            "src/tests.py",
            "test/unit.py",
            "tests/unit.py",
        }

        for relative_path in included | excluded:
            self._write(relative_path)

        self._write(
            "src/tool_output.py",
            "# THIS FILE WAS AUTOMATICALLY GENERATED.\nvalue = 1\n",
        )
        excluded.add("src/tool_output.py")
        self._write("src/readme.txt", "not Python\n")

        found = {item["relative_path"] for item in discover_python_files(self.repo_root)}
        self.assertEqual(included, found)
        self.assertTrue(found.isdisjoint(excluded))

    def test_manifest_is_globally_sorted_and_contains_correct_metadata(self) -> None:
        sources = {
            "z.py": "z = 1\n",
            "pkg/b.py": "b = 2\n",
            "pkg/A.py": "a = 3\n",
            "a.py": "root = 4\n",
        }
        for relative_path, content in reversed(tuple(sources.items())):
            self._write(relative_path, content)

        first = discover_python_files(self.repo_root)
        second = discover_python_files(self.repo_root)

        self.assertEqual(first, second)
        self.assertEqual(
            ["a.py", "pkg/A.py", "pkg/b.py", "z.py"],
            [item["relative_path"] for item in first],
        )
        for item in first:
            expected_content = sources[item["relative_path"]].encode("utf-8")
            self.assertEqual(len(expected_content), item["file_size_bytes"])
            self.assertEqual(
                hashlib.sha256(expected_content).hexdigest(),
                item["file_hash"],
            )
            self.assertTrue(Path(item["absolute_path"]).is_absolute())
            self.assertEqual(
                (self.repo_root / item["relative_path"]).resolve(),
                Path(item["absolute_path"]),
            )

    def test_cli_writes_reproducible_json_manifest(self) -> None:
        self._write("pkg/core.py", "answer = 42\n")
        output_path = Path(self.temp_dir.name) / "manifest.json"
        command = [
            sys.executable,
            str(PROJECT_ROOT / "src" / "discovery.py"),
            str(self.repo_root),
            "--output",
            str(output_path),
        ]
        env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")

        first = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, first.returncode, first.stderr)
        first_bytes = output_path.read_bytes()
        second = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, second.returncode, second.stderr)
        self.assertEqual(first_bytes, output_path.read_bytes())
        self.assertEqual("pkg/core.py", json.loads(first_bytes)[0]["relative_path"])

    def test_cli_reports_missing_repository_without_traceback(self) -> None:
        output_path = Path(self.temp_dir.name) / "must-not-exist.json"
        result = subprocess.run(
            [
                sys.executable,
                str(PROJECT_ROOT / "src" / "discovery.py"),
                str(self.repo_root / "missing"),
                "--output",
                str(output_path),
            ],
            cwd=PROJECT_ROOT,
            env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(2, result.returncode)
        self.assertIn("[Discovery] ERROR:", result.stderr)
        self.assertNotIn("Traceback", result.stderr)
        self.assertFalse(output_path.exists())

    def test_demo_reports_missing_repository_without_index_error(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                str(PROJECT_ROOT / "run_demo.py"),
                str(self.repo_root / "missing"),
            ],
            cwd=PROJECT_ROOT,
            env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(2, result.returncode)
        self.assertIn("ERROR: Repository path does not exist", result.stderr)
        self.assertNotIn("Traceback", result.stderr)
        self.assertNotIn("IndexError", result.stderr)


if __name__ == "__main__":
    unittest.main()
