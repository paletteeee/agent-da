import inspect
import unittest

import txnmem_reference
from txnmem_differential import compare_result_to_oracle
from txnmem_multiagent_workloads import (
    MULTI_AGENT_WORKLOADS,
    generate_multi_agent_instance,
)
from txnmem_reference import reference_outcome
from txnmem_simulator import run_instance


class MultiAgentReferenceTests(unittest.TestCase):
    def test_reference_tracks_each_transaction_separately(self):
        oracle = reference_outcome(
            generate_multi_agent_instance("interleaved_atomicity", 0)
        )
        outcome = oracle["allowed_outcomes"][0]
        self.assertEqual(
            set(outcome["txn_states"]),
            {"txn_agent_1", "txn_agent_2"},
        )

    def test_reference_denies_other_transaction_pending_read(self):
        oracle = reference_outcome(
            generate_multi_agent_instance("multi_agent_visibility", 0)
        )
        before = [
            row
            for row in oracle["allowed_outcomes"][0]["read_observations"]
            if row["probe_role"] == "before_writer_commit"
        ]
        self.assertTrue(before)
        self.assertTrue(all(not row["visible_memory_ids"] for row in before))

    def test_reference_has_no_implementation_import(self):
        source = inspect.getsource(txnmem_reference)
        for forbidden in (
            "txnmem_simulator",
            "txnmem_invariants",
            "txnmem_multiagent_invariants",
            "txnmem_multiagent_storage",
        ):
            self.assertNotIn(forbidden, source)

    def test_full_runtime_matches_reference_for_each_family(self):
        for workload in MULTI_AGENT_WORKLOADS:
            with self.subTest(workload=workload):
                instance = generate_multi_agent_instance(workload, 0)
                comparison = compare_result_to_oracle(
                    instance,
                    run_instance(instance, "TxnMem"),
                )
                self.assertTrue(comparison["matches"], comparison)


if __name__ == "__main__":
    unittest.main()
