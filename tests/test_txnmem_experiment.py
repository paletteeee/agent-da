import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_experiment import (  # noqa: E402
    check_invariants,
    generate_instance,
    run_instance,
)


class TxnMemExperimentTests(unittest.TestCase):
    def test_generation_is_deterministic_and_has_required_schema(self):
        for workload in (
            "atomic_multi_write",
            "revoke_before_commit",
            "provenance_chain_repair",
        ):
            config = {"txn_size": 3, "provenance_depth": 2}
            first = generate_instance(workload, seed=7, config=config)
            second = generate_instance(workload, seed=7, config=config)

            self.assertEqual(first, second)
            self.assertEqual(first["workload"], workload)
            for key in (
                "instance_id",
                "seed",
                "config",
                "initial_memories",
                "operations",
                "policies",
                "failure_schedule",
                "provenance_edges",
                "expected_outcome",
            ):
                self.assertIn(key, first)

    def test_atomic_multi_write_naive_is_partial_but_txnmem_aborts(self):
        instance = generate_instance(
            "atomic_multi_write", seed=1, config={"txn_size": 3}
        )

        naive = run_instance(instance, "Naive")
        txnmem = run_instance(instance, "TxnMem")

        self.assertEqual(naive["transaction_state"], "partial_commit")
        self.assertEqual(len(naive["committed_memory_ids"]), 1)
        self.assertIn("atomicity_violation", check_invariants(instance, naive))

        self.assertEqual(txnmem["transaction_state"], "aborted")
        self.assertEqual(txnmem["committed_memory_ids"], [])
        self.assertEqual(check_invariants(instance, txnmem), [])

    def test_revoke_before_commit_requires_commit_time_revalidation(self):
        instance = generate_instance("revoke_before_commit", seed=2, config={})

        naive = run_instance(instance, "Naive")
        txnmem = run_instance(instance, "TxnMem")

        self.assertEqual(naive["transaction_state"], "committed")
        self.assertIn("invalid_commit_violation", check_invariants(instance, naive))

        self.assertEqual(txnmem["transaction_state"], "aborted")
        self.assertEqual(check_invariants(instance, txnmem), [])

    def test_provenance_repair_invalidates_all_descendants(self):
        instance = generate_instance(
            "provenance_chain_repair", seed=3, config={"provenance_depth": 3}
        )

        naive = run_instance(instance, "Naive")
        txnmem = run_instance(instance, "TxnMem")

        self.assertEqual(naive["final_memories"]["m_derived_1"]["status"], "active")
        self.assertIn(
            "provenance_closure_violation", check_invariants(instance, naive)
        )

        self.assertEqual(
            txnmem["final_memories"]["m_derived_1"]["status"], "invalid"
        )
        self.assertEqual(
            txnmem["final_memories"]["m_derived_2"]["status"], "invalid"
        )
        self.assertEqual(check_invariants(instance, txnmem), [])

    def test_no_transaction_ablation_exposes_partial_write(self):
        instance = generate_instance(
            "atomic_multi_write", seed=4, config={"txn_size": 3}
        )

        result = run_instance(instance, "TxnMem-NoTxn")

        self.assertEqual(result["transaction_state"], "partial_commit")
        self.assertIn("atomicity_violation", check_invariants(instance, result))

    def test_no_policy_commit_ablation_allows_stale_authorization(self):
        instance = generate_instance("revoke_before_commit", seed=5, config={})

        result = run_instance(instance, "TxnMem-NoPolicyCommit")

        self.assertEqual(result["transaction_state"], "committed")
        self.assertIn("invalid_commit_violation", check_invariants(instance, result))

    def test_no_repair_ablation_leaves_invalid_descendant_active(self):
        instance = generate_instance(
            "provenance_chain_repair", seed=6, config={"provenance_depth": 3}
        )

        result = run_instance(instance, "TxnMem-NoRepair")

        self.assertEqual(result["final_memories"]["m_derived_1"]["status"], "active")
        self.assertIn(
            "provenance_closure_violation", check_invariants(instance, result)
        )


if __name__ == "__main__":
    unittest.main()
