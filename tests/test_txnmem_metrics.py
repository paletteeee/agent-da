import csv
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
from txnmem_experiment import write_csv  # noqa: E402
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

        self.assertEqual(row["oracle_version"], "0.3")
        self.assertEqual(row["oracle_match"], 1)
        self.assertEqual(row["allowed_outcome_count"], 1)

    def test_result_row_exports_sorted_per_transaction_states_to_csv(self):
        """Concurrent terminal states remain audit-visible in row and CSV artifacts."""

        instance = generate_instance("atomic_multi_write", 42, config={"txn_size": 1})
        agent = instance["policies"][0]["agent_id"]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_b"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_a"},
            {"op_id": "op_003", "step": 3, "agent_id": agent, "type": "write", "txn_id": "txn_b", "memory_id": "m_b", "source_ids": [], "policy_version": 1},
            {"op_id": "op_004", "step": 4, "agent_id": agent, "type": "abort", "txn_id": "txn_a"},
            {"op_id": "op_005", "step": 5, "agent_id": agent, "type": "commit", "txn_id": "txn_b"},
        ]
        instance["failure_schedule"] = []
        row = result_row(instance, run_instance(instance, "TxnMem"))

        self.assertEqual(row["transaction_states"], '{"txn_a":"aborted","txn_b":"committed"}')
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "results.csv"
            write_csv([row], path)
            with path.open(newline="", encoding="utf-8") as handle:
                exported = next(csv.DictReader(handle))
        self.assertEqual(exported["transaction_states"], row["transaction_states"])

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
