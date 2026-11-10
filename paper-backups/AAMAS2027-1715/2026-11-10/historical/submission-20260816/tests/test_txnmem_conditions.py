import hashlib
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest

from txnmem_conditions import (
    canonical_fingerprint,
    file_sha256,
    source_identity,
    verify_git_source_containment,
)


class TxnMemConditionTests(unittest.TestCase):
    @staticmethod
    def _git_fixture(root: Path) -> str:
        (root / "src").mkdir()
        (root / "configs").mkdir()
        (root / "src/runner.py").write_text("VALUE = 1\n", encoding="utf-8")
        (root / "configs/run.json").write_text('{"seed_count":200}\n', encoding="utf-8")
        subprocess.run(["git", "init", "-q"], cwd=root, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=root, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=root, check=True)
        subprocess.run(["git", "add", "src/runner.py", "configs/run.json"], cwd=root, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "fixture"], cwd=root, check=True)
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, check=True,
            capture_output=True, text=True,
        ).stdout.strip()

    def test_condition_fingerprint_is_canonical(self):
        self.assertEqual(
            canonical_fingerprint({"b": 2, "a": [1, 3]}),
            canonical_fingerprint({"a": [1, 3], "b": 2}),
        )

    def test_file_sha256_hashes_source_bytes(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "fixture.json"
            path.write_bytes(b"abc")
            self.assertEqual(file_sha256(path), hashlib.sha256(b"abc").hexdigest())

    def test_source_identity_uses_component_names_not_absolute_paths(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "runner.py"
            path.write_text("print('ok')\n", encoding="utf-8")
            identity = source_identity({"runner": path})

        self.assertEqual(set(identity["component_sha256"]), {"runner"})
        self.assertNotIn(tmp, str(identity))
        self.assertEqual(len(identity["fingerprint"]), 64)

    def test_git_source_containment_requires_existing_commit_tracked_paths_and_blob_hashes(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            commit = self._git_fixture(root)
            components = {
                "configs/run.json": file_sha256(root / "configs/run.json"),
                "src/runner.py": file_sha256(root / "src/runner.py"),
            }
            contained = verify_git_source_containment(root, commit, components)
            self.assertTrue(contained["contained_in_commit"])
            self.assertTrue(contained["commit_exists"])
            self.assertEqual(contained["component_count"], 2)
            self.assertNotIn(tmp, str(contained))

            (root / "src/runner.py").write_text("VALUE = 2\n", encoding="utf-8")
            dirty_components = {
                **components,
                "src/runner.py": file_sha256(root / "src/runner.py"),
            }
            dirty = verify_git_source_containment(root, commit, dirty_components)
            stale_declaration = verify_git_source_containment(root, commit, components)
            missing_commit = verify_git_source_containment(root, "0" * 40, components)
            (root / "untracked.py").write_text("pass\n", encoding="utf-8")
            untracked = verify_git_source_containment(
                root,
                commit,
                {**components, "untracked.py": file_sha256(root / "untracked.py")},
            )

        self.assertFalse(dirty["contained_in_commit"])
        self.assertEqual(dirty["component_results"]["src/runner.py"]["status"], "hash_mismatch")
        self.assertFalse(stale_declaration["contained_in_commit"])
        self.assertFalse(missing_commit["commit_exists"])
        self.assertFalse(untracked["contained_in_commit"])
        self.assertEqual(untracked["component_results"]["untracked.py"]["status"], "untracked")

    def test_git_source_containment_rejects_noncanonical_paths(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            commit = self._git_fixture(root)
            for path in ("/absolute.py", "src/../runner.py", "./src/runner.py", "src\\runner.py"):
                with self.subTest(path=path), self.assertRaises(ValueError):
                    verify_git_source_containment(root, commit, {path: "0" * 64})


if __name__ == "__main__":
    unittest.main()
