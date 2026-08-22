import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_adapter_contract import (  # noqa: E402
    AdapterRunError,
    CapabilityAbsentError,
    ReplayObservation,
    RuntimeAdapterError,
    UnsupportedMappingError,
    normalize_result,
)


class AdapterContractTests(unittest.TestCase):
    def test_oracle_result_has_the_required_normalized_shape(self):
        observation = ReplayObservation(
            transaction_state="completed",
            final_memories=[
                {
                    "memory_id": "m1",
                    "status": "active",
                    "scope": "tenant:user_001",
                }
            ],
            committed_memory_ids=["m1"],
            trace=[{"step": 1, "operation": "write"}],
            metrics={"operation_count": 1},
        )

        self.assertEqual(
            observation.to_oracle_result("AppendOnly"),
            {
                "variant": "AppendOnly",
                "transaction_state": "completed",
                "final_memories": {
                    "m1": {
                        "memory_id": "m1",
                        "status": "active",
                        "scope": "tenant:user_001",
                    }
                },
                "committed_memory_ids": ["m1"],
                "trace": [{"step": 1, "operation": "write"}],
                "metrics": {"operation_count": 1},
            },
        )

    def test_error_categories_remain_distinct_and_stable(self):
        self.assertEqual(CapabilityAbsentError("missing").category, "capability_absent")
        self.assertEqual(UnsupportedMappingError("unsupported").category, "unsupported_mapping")
        self.assertEqual(RuntimeAdapterError("failed").category, "runtime_error")
        self.assertEqual(AdapterRunError("failed").category, "runtime_error")

    def test_normalize_result_delegates_to_the_oracle_shape(self):
        observation = ReplayObservation(
            transaction_state="completed",
            final_memories=[],
            committed_memory_ids=[],
            trace=[],
            metrics={},
        )

        self.assertEqual(
            normalize_result(observation, "MetadataFiltered"),
            {
                "variant": "MetadataFiltered",
                "transaction_state": "completed",
                "final_memories": {},
                "committed_memory_ids": [],
                "trace": [],
                "metrics": {},
            },
        )

    def test_rejects_duplicate_normalized_memory_ids(self):
        with self.assertRaisesRegex(ValueError, "duplicate memory_id"):
            ReplayObservation(
                transaction_state="completed",
                final_memories=[
                    {"memory_id": "m1", "status": "active"},
                    {"memory_id": "m1", "status": "invalid"},
                ],
                committed_memory_ids=[],
                trace=[],
                metrics={},
            )

    def test_rejects_memory_without_status(self):
        with self.assertRaisesRegex(ValueError, "missing status"):
            ReplayObservation(
                transaction_state="completed",
                final_memories=[{"memory_id": "m1"}],
                committed_memory_ids=[],
                trace=[],
                metrics={},
            )

    def test_rejects_non_list_trace(self):
        with self.assertRaisesRegex(ValueError, "trace must be a list"):
            ReplayObservation(
                transaction_state="completed",
                final_memories=[],
                committed_memory_ids=[],
                trace=(),
                metrics={},
            )

    def test_rejects_committed_memory_not_in_final_memories_without_crash(self):
        with self.assertRaisesRegex(ValueError, "committed memory_id is absent"):
            ReplayObservation(
                transaction_state="completed",
                final_memories=[],
                committed_memory_ids=["m1"],
                trace=[],
                metrics={},
            )

    def test_crash_outcome_can_retain_unobserved_committed_id(self):
        observation = ReplayObservation(
            transaction_state="partial_commit",
            final_memories=[],
            committed_memory_ids=["m1"],
            trace=[{"event": "crash"}],
            metrics={},
        )

        self.assertEqual(observation.committed_memory_ids, ["m1"])

    def test_rejects_adapter_native_values_from_oracle_result(self):
        with self.assertRaisesRegex(TypeError, "not normalized"):
            ReplayObservation(
                transaction_state="completed",
                final_memories=[
                    {"memory_id": "m1", "status": "active", "native": object()}
                ],
                committed_memory_ids=[],
                trace=[],
                metrics={},
            )


if __name__ == "__main__":
    unittest.main()
