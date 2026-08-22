import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_simulator import run_instance  # noqa: E402
from txnmem_workloads import generate_instance  # noqa: E402


class TxnMemSimulatorTests(unittest.TestCase):
    def test_crash_during_commit_is_atomic_for_txnmem(self):
        result = run_instance(generate_instance("crash_during_commit", 20), "TxnMem")
        self.assertEqual(result["transaction_state"], "aborted")
        self.assertEqual(result["committed_memory_ids"], [])

    def test_scope_bypass_denies_direct_id_read_in_txnmem(self):
        result = run_instance(generate_instance("scope_bypass", 21), "TxnMem")
        self.assertEqual(result["metrics"]["exposed_memory_ids"], [])
        self.assertEqual(result["metrics"]["denied_reads"], 2)

    def test_naive_scope_bypass_exposes_private_memory(self):
        result = run_instance(generate_instance("scope_bypass", 22), "Naive")
        self.assertEqual(result["metrics"]["exposed_memory_ids"], ["m_private"])

    def test_supersession_updates_old_and_new_records(self):
        result = run_instance(generate_instance("supersession_consistency", 23), "TxnMem")
        old = result["final_memories"]["m_old"]
        new = result["final_memories"]["m_new"]
        self.assertEqual(new["supersedes_id"], "m_old")
        self.assertEqual(old["status"], "superseded")

    def test_branch_repair_invalidates_every_descendant(self):
        instance = generate_instance(
            "provenance_branch_repair", 24, {"branch_factor": 2, "provenance_depth": 3}
        )
        result = run_instance(instance, "TxnMem")
        self.assertTrue(
            all(memory["status"] == "invalid" for memory in result["final_memories"].values())
        )


if __name__ == "__main__":
    unittest.main()
