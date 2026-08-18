from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from txnmem_artifact_audit import audit_result_paths


class TxnMemArtifactAuditTests(unittest.TestCase):
    def test_safe_sanitized_summary_passes(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "results" / "run" / "summary.json"
            path.parent.mkdir(parents=True)
            path.write_text(
                '{"argument_keys":["access_token"],"raw_reports_committed":false}',
                encoding="utf-8",
            )
            self.assertEqual(audit_result_paths([path]), [])

    def test_raw_path_and_secret_value_are_rejected(self):
        with TemporaryDirectory() as tmp:
            raw = Path(tmp) / "results" / "run" / "data" / "trace.jsonl"
            raw.parent.mkdir(parents=True)
            raw.write_text('{"password":"private"}\n', encoding="utf-8")
            findings = audit_result_paths([raw])

        codes = {finding["code"] for finding in findings}
        self.assertIn("raw_result_path", codes)
        self.assertIn("sensitive_result_key", codes)


if __name__ == "__main__":
    unittest.main()
