import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_metrics import (  # noqa: E402
    result_row,
    summarize,
    write_repair_figure,
    write_summary,
    write_violation_figure,
)
from txnmem_simulator import run_instance  # noqa: E402
from txnmem_workloads import generate_instance  # noqa: E402


class TxnMemMetricsTests(unittest.TestCase):
    def test_result_row_contains_scope_and_violation_metrics(self):
        instance = generate_instance("scope_bypass", 40)
        row = result_row(instance, run_instance(instance, "Naive"))
        self.assertEqual(row["leak_rate"], 1.0)
        self.assertEqual(row["scope_bypass_rate"], 1.0)
        self.assertEqual(row["any_violation"], 1)

    def test_result_row_records_independent_oracle_comparison(self):
        instance = generate_instance("atomic_multi_write", 41)
        row = result_row(instance, run_instance(instance, "TxnMem"))

        self.assertEqual(row["oracle_version"], "0.1")
        self.assertEqual(row["oracle_match"], 1)
        self.assertEqual(row["allowed_outcome_count"], 1)

    def test_summary_contains_mean_and_population_std(self):
        summary = summarize(
            [
                {"workload": "w", "variant": "v", "leak_rate": 0.0},
                {"workload": "w", "variant": "v", "leak_rate": 1.0},
            ],
            ("workload", "variant"),
        )
        stats = summary["groups"]["w/v"]["leak_rate"]
        self.assertEqual(stats["mean"], 0.5)
        self.assertEqual(stats["std"], 0.5)

    def test_summary_and_svg_outputs_are_valid(self):
        summary = summarize(
            [{"workload": "w", "variant": "v", "any_violation": 0, "repair_recall": 1.0}],
            ("workload", "variant"),
        )
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_summary(summary, root / "summary.json")
            write_violation_figure(summary, root / "violations.svg")
            write_repair_figure(summary, root / "repair.svg")
            self.assertEqual(json.loads((root / "summary.json").read_text()), summary)
            self.assertTrue((root / "violations.svg").read_text().startswith("<svg"))
            self.assertTrue((root / "repair.svg").read_text().startswith("<svg"))


if __name__ == "__main__":
    unittest.main()
