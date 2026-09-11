import json
import unittest

from txnmem_multiagent_storage_experiment import (
    REAL_STORAGE_SCENARIOS,
    fixture_backend_factory,
    run_real_storage_matrix,
)


class MultiAgentStorageExperimentTests(unittest.TestCase):
    def test_fixture_matrix_has_exact_paired_observation_count(self):
        report = run_real_storage_matrix(
            backend_factory=fixture_backend_factory,
            repetitions=2,
        )

        self.assertEqual(tuple(report["scenarios"]), REAL_STORAGE_SCENARIOS)
        self.assertEqual(report["scenario_count"], 4)
        self.assertEqual(report["condition_count"], 2)
        self.assertEqual(report["observation_count"], 16)
        self.assertEqual(len(report["observations"]), 16)
        self.assertFalse(report["real_service_evidence"])

    def test_every_observation_has_concurrent_readback_and_sanitized_fault_evidence(self):
        report = run_real_storage_matrix(
            backend_factory=fixture_backend_factory,
            repetitions=2,
        )

        for observation in report["observations"]:
            self.assertEqual(observation["agent_count"], 2)
            for field in (
                "scenario",
                "repetition",
                "condition",
                "actual_commit_order",
                "classification",
                "state_readback",
                "fault_evidence",
            ):
                self.assertIn(field, observation)
            serialized = json.dumps(observation["fault_evidence"])
            self.assertNotIn("password", serialized.lower())
            self.assertNotIn("api_key", serialized.lower())
            self.assertNotIn("/Users/", serialized)
        self.assertEqual(report["txnmem_counts"]["unknown_count"], 0)
        self.assertEqual(report["txnmem_counts"]["partial_visible_count"], 0)
        self.assertEqual(report["txnmem_counts"]["policy_violation_count"], 0)
        self.assertEqual(
            report["txnmem_counts"]["provenance_violation_count"],
            0,
        )
        self.assertTrue(report["all_state_readbacks_complete"])


if __name__ == "__main__":
    unittest.main()
