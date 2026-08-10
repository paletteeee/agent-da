from __future__ import annotations

import csv
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_claim_audit import build_controlled_suite_evidence  # noqa: E402


class ControlledSuiteEvidenceTests(unittest.TestCase):
    def _fixture(self, root: Path) -> tuple[Path, Path]:
        instances_path = root / "generated_instances.jsonl"
        result_path = root / "experiment_results.csv"
        instances = [
            {"instance_id": "w1-s0", "workload": "w1", "seed": 0},
            {"instance_id": "w2-s0", "workload": "w2", "seed": 0},
        ]
        instances_path.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in instances),
            encoding="utf-8",
        )
        rows = [
            {"instance_id": "w1-s0", "workload": "w1", "variant": "TxnMem", "any_violation": 0, "oracle_match": 1},
            {"instance_id": "w2-s0", "workload": "w2", "variant": "TxnMem", "any_violation": 0, "oracle_match": 1},
            {"instance_id": "w1-s0", "workload": "w1", "variant": "Naive", "any_violation": 1, "oracle_match": 0},
            {"instance_id": "w2-s0", "workload": "w2", "variant": "Naive", "any_violation": 0, "oracle_match": 1},
        ]
        with result_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        return instances_path, result_path

    def test_builds_counts_from_rows_instead_of_accepting_handwritten_totals(self):
        with TemporaryDirectory() as tmp:
            instances_path, result_path = self._fixture(Path(tmp))
            evidence = build_controlled_suite_evidence(instances_path, result_path)

        self.assertEqual(evidence["instance_count"], 2)
        self.assertEqual(evidence["workload_family_count"], 2)
        self.assertEqual(evidence["seed_count"], 1)
        self.assertEqual(evidence["variant_count"], 2)
        self.assertEqual(evidence["variant_row_count"], 4)
        self.assertEqual(
            evidence["variants"]["TxnMem"],
            {"row_count": 2, "violation_count": 0, "oracle_match_count": 2},
        )
        self.assertEqual(
            evidence["variants"]["Naive"],
            {"row_count": 2, "violation_count": 1, "oracle_match_count": 1},
        )
        self.assertEqual(len(evidence["sources"]["instances"]["sha256"]), 64)
        self.assertEqual(len(evidence["sources"]["results"]["sha256"]), 64)
        self.assertFalse(evidence["production_latency_claim"])

    def test_rejects_missing_instance_variant_row_instead_of_reporting_old_count(self):
        with TemporaryDirectory() as tmp:
            instances_path, result_path = self._fixture(Path(tmp))
            with result_path.open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))[:-1]
            with result_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)

            with self.assertRaisesRegex(ValueError, "complete instance-by-variant Cartesian product"):
                build_controlled_suite_evidence(instances_path, result_path)

    def test_rejects_duplicate_instance_variant_row(self):
        with TemporaryDirectory() as tmp:
            instances_path, result_path = self._fixture(Path(tmp))
            with result_path.open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            rows.append(dict(rows[0]))
            with result_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)

            with self.assertRaisesRegex(ValueError, "duplicate instance/variant row"):
                build_controlled_suite_evidence(instances_path, result_path)


if __name__ == "__main__":
    unittest.main()
