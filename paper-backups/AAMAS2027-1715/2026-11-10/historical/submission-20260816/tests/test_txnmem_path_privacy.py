from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_path_privacy import scan_added_diff, scan_git_range  # noqa: E402


class AddedPathPrivacyTests(unittest.TestCase):
    def test_detects_personal_runtime_and_remote_run_paths_only_on_added_lines(self):
        slash = "/"
        cache_root = ".codex/plugins/cache"
        runtime_version = "26.812.1"
        diff = "\n".join(
            (
                "--- a/file",
                "+++ b/file",
                f"- {slash}Users/removed/old",
                f" context {slash}Users/not-added",
                f"+ {slash}Users/person/work/file",
                f"+ {slash}home/person/work/file",
                f"+ PYTHONPATH={slash}data/txnmem_run_20260811/src",
                f"+ {slash}opt/{cache_root}/openai-primary-runtime/documents/{runtime_version}/tool.py",
                "+ <workspace> <output-dir> <runtime> <remote-run-dir>",
            )
        )

        findings = scan_added_diff(diff)

        self.assertEqual(
            {finding["kind"] for finding in findings},
            {"personal_absolute_path", "remote_run_path", "versioned_codex_cache"},
        )
        self.assertEqual(len(findings), 4)
        self.assertEqual({finding["path"] for finding in findings}, {"file"})

    def test_scan_git_range_requests_the_full_added_diff(self):
        completed = subprocess.CompletedProcess([], 0, "", "")
        with patch("txnmem_path_privacy.subprocess.run", return_value=completed) as run:
            self.assertEqual(scan_git_range(ROOT, "base-sha"), [])

        self.assertEqual(
            run.call_args.args[0],
            ["git", "diff", "--unified=0", "base-sha"],
        )

    def test_whole_review_added_diff_contains_no_private_absolute_paths(self):
        if not (ROOT / ".git").exists():
            self.skipTest("Git range metadata is not part of a source archive")
        completed = subprocess.run(
            [
                "git",
                "diff",
                "--unified=0",
                "321e7a7d122584a96f69e1a5dd421eadc52dc4fc",
            ],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )

        self.assertEqual(scan_added_diff(completed.stdout), [])


if __name__ == "__main__":
    unittest.main()
