import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from txnmem_conditions import canonical_fingerprint, file_sha256, source_identity


class TxnMemConditionTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
