from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from txnmem_model_identity import fingerprint_model_artifacts


class TxnMemModelIdentityTests(unittest.TestCase):
    def test_model_fingerprint_changes_with_weight_bytes(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "config.json").write_text('{"model":"fixture"}', encoding="utf-8")
            weight = root / "model-00001-of-00001.safetensors"
            weight.write_bytes(b"weights-v1")
            first = fingerprint_model_artifacts(root)
            weight.write_bytes(b"weights-v2")
            second = fingerprint_model_artifacts(root)

        self.assertNotEqual(first["model_revision_sha256"], second["model_revision_sha256"])
        self.assertEqual(first["artifact_count"], 2)
        self.assertFalse(first["absolute_paths_retained"])


if __name__ == "__main__":
    unittest.main()
