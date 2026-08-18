import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_differential import compare_result_to_oracle  # noqa: E402
from txnmem_metrics import result_row  # noqa: E402
from txnmem_simulator import run_instance  # noqa: E402
from txnmem_workloads import WORKLOADS, generate_instance, generate_suite  # noqa: E402


class TxnMemDifferentialTests(unittest.TestCase):
    def test_every_generated_workload_is_accepted_by_full_txnmem_and_reference(self):
        for instance in generate_suite(WORKLOADS, range(3)):
            result = run_instance(instance, "TxnMem")
            row = result_row(instance, result)
            assert row["any_violation"] == 0
            assert row["oracle_match"] == 1

    def test_txnmem_matches_independent_w1_oracle(self):
        instance = generate_instance("atomic_multi_write", seed=0, config={"txn_size": 2})

        comparison = compare_result_to_oracle(instance, run_instance(instance, "TxnMem"))

        self.assertTrue(comparison["matches"])
        self.assertEqual(comparison["oracle_version"], "0.2")

    def test_naive_partial_write_is_rejected_by_w1_oracle(self):
        instance = generate_instance("atomic_multi_write", seed=1, config={"txn_size": 2})

        comparison = compare_result_to_oracle(instance, run_instance(instance, "Naive"))

        self.assertFalse(comparison["matches"])
        self.assertIn("committed_memory_ids", comparison["mismatches"])

    def test_crash_boundary_result_is_allowed_by_oracle_set(self):
        instance = generate_instance("crash_during_commit", seed=2)

        comparison = compare_result_to_oracle(instance, run_instance(instance, "TxnMem"))

        self.assertTrue(comparison["matches"])
        self.assertEqual(comparison["allowed_outcome_count"], 2)

    def test_interleaved_abort_preserves_the_other_transaction_buffer(self):
        """Aborting transaction A must neither commit nor clear transaction B's write."""

        instance = generate_instance("atomic_multi_write", seed=3, config={"txn_size": 1})
        agent = instance["policies"][0]["agent_id"]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_a"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "write", "txn_id": "txn_a", "memory_id": "m_a", "source_ids": [], "policy_version": 1},
            {"op_id": "op_003", "step": 3, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_b"},
            {"op_id": "op_004", "step": 4, "agent_id": agent, "type": "write", "txn_id": "txn_b", "memory_id": "m_b", "source_ids": [], "policy_version": 1},
            {"op_id": "op_005", "step": 5, "agent_id": agent, "type": "abort", "txn_id": "txn_a"},
            {"op_id": "op_006", "step": 6, "agent_id": agent, "type": "commit", "txn_id": "txn_b"},
        ]
        instance["failure_schedule"] = []

        result = run_instance(instance, "TxnMem")
        comparison = compare_result_to_oracle(instance, result)

        self.assertEqual(result["committed_memory_ids"], ["m_b"])
        self.assertEqual(
            result["transaction_states"], {"txn_a": "aborted", "txn_b": "committed"}
        )
        self.assertTrue(comparison["matches"])

    def test_process_crash_aborts_every_active_transaction(self):
        """A process crash clears every transaction buffer, not only the triggering one."""

        instance = generate_instance("atomic_multi_write", seed=4, config={"txn_size": 1})
        agent = instance["policies"][0]["agent_id"]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_a"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "write", "txn_id": "txn_a", "memory_id": "m_a", "source_ids": [], "policy_version": 1},
            {"op_id": "op_003", "step": 3, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_b"},
            {"op_id": "op_004", "step": 4, "agent_id": agent, "type": "write", "txn_id": "txn_b", "memory_id": "m_b", "source_ids": [], "policy_version": 1},
        ]
        instance["failure_schedule"] = [
            {"trigger": {"after_operation": "op_004"}, "type": "crash", "target": "txn_b", "phase": "after_operation"}
        ]

        result = run_instance(instance, "TxnMem")

        self.assertEqual(result["committed_memory_ids"], [])
        self.assertEqual(result["transaction_states"], {"txn_a": "aborted", "txn_b": "aborted"})
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_aborted_transaction_is_terminal_for_later_mutations_and_commit(self):
        """Later write/derive/commit calls cannot resurrect an explicitly aborted transaction."""

        instance = generate_instance("atomic_multi_write", seed=5, config={"txn_size": 1})
        agent = instance["policies"][0]["agent_id"]
        instance["initial_memories"] = [
            {"memory_id": "m_root", "agent_id": agent, "scope": "tenant:user_001", "status": "active"}
        ]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_a"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "write", "txn_id": "txn_a", "memory_id": "m_before_abort", "source_ids": [], "policy_version": 1},
            {"op_id": "op_003", "step": 3, "agent_id": agent, "type": "abort", "txn_id": "txn_a"},
            {"op_id": "op_004", "step": 4, "agent_id": agent, "type": "write", "txn_id": "txn_a", "memory_id": "m_after_abort", "source_ids": [], "policy_version": 1},
            {"op_id": "op_005", "step": 5, "agent_id": agent, "type": "derive", "txn_id": "txn_a", "memory_id": "m_derived_after_abort", "source_ids": ["m_root"], "policy_version": 1},
            {"op_id": "op_006", "step": 6, "agent_id": agent, "type": "commit", "txn_id": "txn_a"},
        ]
        instance["failure_schedule"] = []

        result = run_instance(instance, "TxnMem")

        self.assertEqual(result["transaction_states"], {"txn_a": "aborted"})
        self.assertEqual(result["committed_memory_ids"], [])
        self.assertNotIn("m_after_abort", result["final_memories"])
        self.assertNotIn("m_derived_after_abort", result["final_memories"])
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_committed_transaction_is_terminal_for_later_mutations_and_abort(self):
        """A completed transaction keeps its terminal state and committed write set."""

        instance = generate_instance("atomic_multi_write", seed=51, config={"txn_size": 1})
        agent = instance["policies"][0]["agent_id"]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_a"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "write", "txn_id": "txn_a", "memory_id": "m_committed", "source_ids": [], "policy_version": 1},
            {"op_id": "op_003", "step": 3, "agent_id": agent, "type": "commit", "txn_id": "txn_a"},
            {"op_id": "op_004", "step": 4, "agent_id": agent, "type": "write", "txn_id": "txn_a", "memory_id": "m_after_commit", "source_ids": [], "policy_version": 1},
            {"op_id": "op_005", "step": 5, "agent_id": agent, "type": "abort", "txn_id": "txn_a"},
        ]
        instance["failure_schedule"] = []

        result = run_instance(instance, "TxnMem")

        self.assertEqual(result["transaction_states"], {"txn_a": "committed"})
        self.assertEqual(result["committed_memory_ids"], ["m_committed"])
        self.assertNotIn("m_after_commit", result["final_memories"])
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_end_of_run_finalizes_empty_and_pending_transactions_like_reference(self):
        """Empty active work completes; active buffered work, including implicit work, aborts."""

        instance = generate_instance("atomic_multi_write", seed=6, config={"txn_size": 1})
        agent = instance["policies"][0]["agent_id"]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_empty"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_pending"},
            {"op_id": "op_003", "step": 3, "agent_id": agent, "type": "write", "txn_id": "txn_pending", "memory_id": "m_pending", "source_ids": [], "policy_version": 1},
            {"op_id": "op_004", "step": 4, "agent_id": agent, "type": "write", "memory_id": "m_implicit", "source_ids": [], "policy_version": 1},
        ]
        instance["failure_schedule"] = []

        result = run_instance(instance, "TxnMem")

        self.assertEqual(
            result["transaction_states"],
            {"implicit": "aborted", "txn_empty": "completed", "txn_pending": "aborted"},
        )
        self.assertEqual(result["committed_memory_ids"], [])
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_write_revocation_does_not_abort_unrelated_read_only_transaction(self):
        """Revalidation is action-specific rather than a global policy-version abort."""

        instance = generate_instance("atomic_multi_write", seed=7, config={"txn_size": 1})
        agent = instance["policies"][0]["agent_id"]
        instance["initial_memories"] = [
            {"memory_id": "m_read", "agent_id": agent, "scope": "tenant:user_001", "status": "active"}
        ]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_reader"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "read", "txn_id": "txn_reader", "memory_id": "m_read", "scope": "tenant:user_001"},
            {"op_id": "op_003", "step": 3, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_writer"},
            {"op_id": "op_004", "step": 4, "agent_id": agent, "type": "write", "txn_id": "txn_writer", "memory_id": "m_revoked", "source_ids": [], "policy_version": 1},
            {"op_id": "op_005", "step": 5, "agent_id": agent, "type": "commit", "txn_id": "txn_writer"},
            {"op_id": "op_006", "step": 6, "agent_id": agent, "type": "commit", "txn_id": "txn_reader"},
        ]
        instance["failure_schedule"] = [
            {"trigger": {"before_operation": "op_005"}, "type": "revoke", "target": "write", "phase": "before_validate"}
        ]

        result = run_instance(instance, "TxnMem")

        self.assertEqual(result["transaction_states"], {"txn_reader": "committed", "txn_writer": "aborted"})
        self.assertEqual(result["committed_memory_ids"], [])
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_aborted_supersession_does_not_mutate_committed_records(self):
        """Supersession effects stay staged until the owning transaction commits."""

        instance = generate_instance("supersession_consistency", seed=8)
        instance["operations"][-1]["type"] = "abort"
        instance["failure_schedule"] = []

        result = run_instance(instance, "TxnMem")

        self.assertEqual(result["transaction_states"], {"txn_super": "aborted"})
        self.assertEqual(result["final_memories"]["m_old"]["status"], "active")
        self.assertEqual(result["final_memories"]["m_new"]["status"], "pending")
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_concurrent_aborted_supersession_does_not_leak_into_sibling_commit(self):
        """An aborted supersession cannot alter records observed by another transaction."""

        instance = generate_instance("supersession_consistency", seed=9)
        agent = instance["policies"][0]["agent_id"]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_a"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "write", "txn_id": "txn_a", "memory_id": "m_new", "source_ids": [], "policy_version": 1, "supersedes_id": "m_old"},
            {"op_id": "op_003", "step": 3, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_b"},
            {"op_id": "op_004", "step": 4, "agent_id": agent, "type": "supersede", "txn_id": "txn_a", "old_memory_id": "m_old", "new_memory_id": "m_new"},
            {"op_id": "op_005", "step": 5, "agent_id": agent, "type": "write", "txn_id": "txn_b", "memory_id": "m_b", "source_ids": [], "policy_version": 1},
            {"op_id": "op_006", "step": 6, "agent_id": agent, "type": "abort", "txn_id": "txn_a"},
            {"op_id": "op_007", "step": 7, "agent_id": agent, "type": "commit", "txn_id": "txn_b"},
        ]
        instance["failure_schedule"] = []

        result = run_instance(instance, "TxnMem")

        self.assertEqual(result["transaction_states"], {"txn_a": "aborted", "txn_b": "committed"})
        self.assertEqual(result["committed_memory_ids"], ["m_b"])
        self.assertEqual(result["final_memories"]["m_old"]["status"], "active")
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_post_commit_process_crash_preserves_committer_and_aborts_empty_sibling(self):
        """The committing transaction survives a post-commit crash; every active sibling aborts."""

        instance = generate_instance("atomic_multi_write", seed=10, config={"txn_size": 1})
        agent = instance["policies"][0]["agent_id"]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_a"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_b"},
            {"op_id": "op_003", "step": 3, "agent_id": agent, "type": "write", "txn_id": "txn_b", "memory_id": "m_b", "source_ids": [], "policy_version": 1},
            {"op_id": "op_004", "step": 4, "agent_id": agent, "type": "commit", "txn_id": "txn_b"},
        ]
        instance["failure_schedule"] = [
            {"trigger": {"after_operation": "op_004"}, "type": "crash", "target": "txn_b", "phase": "after_operation"}
        ]

        result = run_instance(instance, "TxnMem")

        self.assertEqual(result["transaction_states"], {"txn_a": "aborted", "txn_b": "committed"})
        self.assertEqual(result["committed_memory_ids"], ["m_b"])
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])


if __name__ == "__main__":
    unittest.main()
