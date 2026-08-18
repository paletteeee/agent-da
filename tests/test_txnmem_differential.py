import sys
import unittest
import copy
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
        self.assertEqual(comparison["oracle_version"], "0.3")

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

    def test_transactional_invalidation_is_discarded_by_explicit_abort(self):
        """An active transaction stages invalidation instead of mutating committed memory."""

        instance = generate_instance("atomic_multi_write", seed=16, config={"txn_size": 1})
        agent = instance["policies"][0]["agent_id"]
        instance["initial_memories"] = [
            {"memory_id": "m_root", "agent_id": agent, "scope": "tenant:user_001", "status": "active"}
        ]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_a"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "invalidate", "txn_id": "txn_a", "memory_id": "m_root"},
            {"op_id": "op_003", "step": 3, "agent_id": agent, "type": "abort", "txn_id": "txn_a"},
        ]
        instance["failure_schedule"] = []

        result = run_instance(instance, "TxnMem")

        self.assertEqual(result["transaction_states"], {"txn_a": "aborted"})
        self.assertEqual(result["final_memories"]["m_root"]["status"], "active")
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_staged_supersession_and_invalidation_are_discarded_by_crash(self):
        """Crash clears all staged mutation categories without changing committed records."""

        instance = generate_instance("supersession_consistency", seed=17)
        instance["operations"] = instance["operations"][:-1] + [
            {"op_id": "op_004", "step": 4, "agent_id": instance["policies"][0]["agent_id"], "type": "invalidate", "txn_id": "txn_super", "memory_id": "m_old"}
        ]
        instance["failure_schedule"] = [
            {"trigger": {"after_operation": "op_004"}, "type": "crash", "target": "txn_super", "phase": "after_operation"}
        ]

        result = run_instance(instance, "TxnMem")

        self.assertEqual(result["transaction_states"], {"txn_super": "aborted"})
        self.assertEqual(result["final_memories"]["m_old"]["status"], "active")
        self.assertEqual(result["final_memories"]["m_new"]["status"], "pending")
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_sibling_overwrite_invalidates_reader_by_committed_version(self):
        """Reader commit aborts after a sibling overwrites its recorded committed version."""

        instance = generate_instance("atomic_multi_write", seed=18, config={"txn_size": 1})
        agent = instance["policies"][0]["agent_id"]
        instance["initial_memories"] = [
            {"memory_id": "m_shared", "agent_id": agent, "scope": "tenant:user_001", "status": "active", "version": 1}
        ]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_reader"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "read", "txn_id": "txn_reader", "memory_id": "m_shared", "scope": "tenant:user_001"},
            {"op_id": "op_003", "step": 3, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_writer"},
            {"op_id": "op_004", "step": 4, "agent_id": agent, "type": "write", "txn_id": "txn_writer", "memory_id": "m_shared", "source_ids": [], "policy_version": 1},
            {"op_id": "op_005", "step": 5, "agent_id": agent, "type": "commit", "txn_id": "txn_writer"},
            {"op_id": "op_006", "step": 6, "agent_id": agent, "type": "commit", "txn_id": "txn_reader"},
        ]
        instance["failure_schedule"] = []

        result = run_instance(instance, "TxnMem")

        self.assertEqual(result["transaction_states"], {"txn_reader": "aborted", "txn_writer": "committed"})
        self.assertEqual(result["final_memories"]["m_shared"]["version"], 2)
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_duplicate_begin_preserves_active_transaction_staged_state(self):
        """A duplicate active begin cannot reset staged write/supersession/invalidation work."""

        instance = generate_instance("supersession_consistency", seed=19)
        agent = instance["policies"][0]["agent_id"]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_super"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "write", "txn_id": "txn_super", "memory_id": "m_new", "source_ids": [], "policy_version": 1, "supersedes_id": "m_old"},
            {"op_id": "op_003", "step": 3, "agent_id": agent, "type": "supersede", "txn_id": "txn_super", "old_memory_id": "m_old", "new_memory_id": "m_new"},
            {"op_id": "op_004", "step": 4, "agent_id": agent, "type": "invalidate", "txn_id": "txn_super", "memory_id": "m_old"},
            {"op_id": "op_005", "step": 5, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_super"},
            {"op_id": "op_006", "step": 6, "agent_id": agent, "type": "commit", "txn_id": "txn_super"},
        ]
        instance["failure_schedule"] = []

        result = run_instance(instance, "TxnMem")

        self.assertEqual(result["transaction_states"], {"txn_super": "committed"})
        self.assertEqual(result["final_memories"]["m_old"]["status"], "invalid")
        self.assertEqual(result["final_memories"]["m_new"]["status"], "active")
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_after_operation_revoke_revalidates_the_staged_write(self):
        """A post-write revoke must be consumed before the transaction commits."""

        instance = generate_instance("atomic_multi_write", seed=21, config={"txn_size": 1})
        agent = instance["policies"][0]["agent_id"]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_a"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "write", "txn_id": "txn_a", "memory_id": "m_staged", "source_ids": [], "policy_version": 1},
            {"op_id": "op_003", "step": 3, "agent_id": agent, "type": "commit", "txn_id": "txn_a"},
        ]
        instance["failure_schedule"] = [
            {"trigger": {"after_operation": "op_002"}, "type": "revoke", "target": "write"},
        ]

        result = run_instance(instance, "TxnMem")

        self.assertEqual(result["transaction_states"], {"txn_a": "aborted"})
        self.assertNotIn("m_staged", result["final_memories"])
        self.assertIn({"step": 2, "event": "revoke", "policy_version": 2}, result["trace"])
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_after_read_invalidation_changes_version_before_revalidation(self):
        """A scheduled post-read invalidation must abort the later reader commit."""

        instance = generate_instance("atomic_multi_write", seed=22, config={"txn_size": 1})
        agent = instance["policies"][0]["agent_id"]
        instance["initial_memories"] = [
            {"memory_id": "m_root", "agent_id": agent, "scope": "tenant:user_001", "status": "active", "version": 1},
        ]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_reader"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "read", "txn_id": "txn_reader", "memory_id": "m_root", "scope": "tenant:user_001"},
            {"op_id": "op_003", "step": 3, "agent_id": agent, "type": "commit", "txn_id": "txn_reader"},
        ]
        instance["failure_schedule"] = [
            {"trigger": {"after_operation": "op_002"}, "type": "invalidate", "target": "m_root"},
        ]

        result = run_instance(instance, "TxnMem")

        self.assertEqual(result["transaction_states"], {"txn_reader": "aborted"})
        self.assertEqual(result["final_memories"]["m_root"]["status"], "invalid")
        self.assertEqual(result["final_memories"]["m_root"]["version"], 2)
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_post_events_apply_in_order_before_the_process_crash(self):
        """Post-boundary policy, delay, and invalidation each happen once before crash."""

        instance = generate_instance("atomic_multi_write", seed=23, config={"txn_size": 1})
        agent = instance["policies"][0]["agent_id"]
        instance["initial_memories"] = [
            {"memory_id": "m_root", "agent_id": agent, "scope": "tenant:user_001", "status": "active", "version": 1},
        ]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "begin_txn", "txn_id": "txn_a"},
            {"op_id": "op_002", "step": 2, "agent_id": agent, "type": "write", "txn_id": "txn_a", "memory_id": "m_staged", "source_ids": [], "policy_version": 1},
        ]
        instance["failure_schedule"] = [
            {"trigger": {"after_operation": "op_002"}, "type": "revoke", "target": "write"},
            {"trigger": {"after_operation": "op_002"}, "type": "delay"},
            {"trigger": {"after_operation": "op_002"}, "type": "invalidate", "target": "m_root"},
            {"trigger": {"after_operation": "op_002"}, "type": "crash", "target": "txn_a"},
        ]

        result = run_instance(instance, "TxnMem")
        event_names = [entry.get("event") for entry in result["trace"] if entry.get("event")]

        self.assertEqual(result["transaction_states"], {"txn_a": "aborted"})
        self.assertEqual(result["final_memories"]["m_root"]["status"], "invalid")
        self.assertEqual(result["final_memories"]["m_root"]["version"], 2)
        self.assertEqual(event_names, ["revoke", "delay", "invalidate", "crash"])
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_unbegun_transaction_labeled_invalidation_is_immediate_autocommit_repair(self):
        """An invalidate label alone must not create a staged transaction boundary."""

        instance = generate_instance("provenance_chain_repair", seed=24, config={"provenance_depth": 1})
        agent = instance["policies"][0]["agent_id"]
        instance["initial_memories"] = [
            {"memory_id": "root", "agent_id": agent, "scope": "tenant:user_001", "status": "active", "version": 1},
            {"memory_id": "child", "agent_id": agent, "scope": "tenant:user_001", "status": "active", "version": 1},
        ]
        instance["provenance_edges"] = [
            {"source_id": "root", "derived_id": "child", "relation": "read_derive"},
        ]
        instance["operations"] = [
            {"op_id": "op_001", "step": 1, "agent_id": agent, "type": "invalidate", "txn_id": "txn_unbegun", "memory_id": "root"},
        ]
        instance["failure_schedule"] = []

        result = run_instance(instance, "TxnMem")

        self.assertEqual(result["transaction_states"], {})
        self.assertEqual(result["final_memories"]["root"]["status"], "invalid")
        self.assertEqual(result["final_memories"]["root"]["version"], 2)
        self.assertEqual(result["final_memories"]["child"]["status"], "invalid")
        self.assertEqual(result["metrics"]["repair_count"], 1)
        self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_differential_rejects_missing_or_extra_transaction_state_ids(self):
        """Candidate transaction domains must exactly match an allowed oracle outcome."""

        instance = generate_instance("atomic_multi_write", seed=25, config={"txn_size": 1})
        result = run_instance(instance, "TxnMem")
        extra = copy.deepcopy(result)
        extra["transaction_states"]["txn_extra"] = "completed"
        missing = copy.deepcopy(result)
        missing["transaction_states"] = {}

        extra_comparison = compare_result_to_oracle(instance, extra)
        missing_comparison = compare_result_to_oracle(instance, missing)

        self.assertFalse(extra_comparison["matches"])
        self.assertIn("transaction_state", extra_comparison["mismatches"])
        self.assertFalse(missing_comparison["matches"])
        self.assertIn("transaction_state", missing_comparison["mismatches"])


if __name__ == "__main__":
    unittest.main()
