import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_invariants import check_invariants  # noqa: E402
from txnmem_simulator import run_instance  # noqa: E402
from txnmem_workloads import generate_instance  # noqa: E402


class TxnMemInvariantTests(unittest.TestCase):
    def test_scope_leak_is_reported_for_naive(self):
        instance = generate_instance("scope_bypass", 30)
        result = run_instance(instance, "Naive")
        self.assertIn("scope_leak_violation", check_invariants(instance, result))

    def test_txnmem_has_no_scope_violation(self):
        instance = generate_instance("scope_bypass", 31)
        self.assertEqual(check_invariants(instance, run_instance(instance, "TxnMem")), [])

    def test_supersession_violation_is_reported_when_old_memory_remains_active(self):
        instance = generate_instance("supersession_consistency", 32)
        result = run_instance(instance, "TxnMem-NoTxn")
        self.assertIn(
            "supersession_consistency_violation", check_invariants(instance, result)
        )

    def test_provenance_closure_is_reported_for_no_repair(self):
        instance = generate_instance("provenance_branch_repair", 33, {"branch_factor": 2})
        result = run_instance(instance, "TxnMem-NoRepair")
        self.assertIn("provenance_closure_violation", check_invariants(instance, result))

    def test_txnmem_satisfies_atomicity_and_commit_authorization(self):
        for workload in ("atomic_multi_write", "revoke_before_commit"):
            instance = generate_instance(workload, 34)
            result = run_instance(instance, "TxnMem")
            self.assertEqual(check_invariants(instance, result), [])


if __name__ == "__main__":
    unittest.main()
