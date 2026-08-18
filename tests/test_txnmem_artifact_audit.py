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

    def test_exact_controlled_synthetic_allowlist_passes_but_lookalikes_fail(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            approved = [
                root / "results/final_controlled/data/generated_instances.jsonl",
                root / "results/final_controlled/data/reference_oracles.jsonl",
                root / "results/final_controlled_200/data/generated_instances.jsonl",
                root / "results/final_controlled_200/data/reference_oracles.jsonl",
            ]
            for path in approved:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('{"synthetic":true}\n', encoding="utf-8")
            self.assertEqual(audit_result_paths(approved), [])

            lookalike = root / "results/final_controlled_200-copy/data/generated_instances.jsonl"
            nested = root / "results/final_controlled_200/data/raw/generated_instances.jsonl"
            public_trace = root / "results/public_scale_20260818/data/generated_instances.jsonl"
            for path in (lookalike, nested, public_trace):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('{"synthetic":true}\n', encoding="utf-8")
            findings = audit_result_paths([lookalike, nested, public_trace])

        self.assertEqual(
            {finding["path"] for finding in findings if finding["code"] == "raw_result_path"},
            {str(lookalike), str(nested), str(public_trace)},
        )

    def test_sensitive_values_and_symlink_escapes_are_rejected(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            sensitive = root / "results/run/summary.json"
            sensitive.parent.mkdir(parents=True)
            sensitive.write_text('{"note":"sk-secret-token-value"}', encoding="utf-8")
            outside = root / "outside.jsonl"
            outside.write_text('{"synthetic":true}\n', encoding="utf-8")
            escaped = root / "results/final_controlled_200/data/generated_instances.jsonl"
            escaped.parent.mkdir(parents=True)
            escaped.symlink_to(outside)
            findings = audit_result_paths([sensitive, escaped])

        codes = {finding["code"] for finding in findings}
        self.assertIn("sensitive_result_value", codes)
        self.assertIn("result_path_escape", codes)


if __name__ == "__main__":
    unittest.main()
