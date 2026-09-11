import unittest

from txnmem_backend import InstrumentedMemoryBackend
from txnmem_multiagent_storage import (
    MultiAgentStorageCoordinator,
    run_concurrent_storage_scenario,
)


class FailingBackend(InstrumentedMemoryBackend):
    def __init__(self, fail_on_write):
        super().__init__()
        self.fail_on_write = fail_on_write
        self.write_count = 0

    def write(self, memory_id, value=None, **fields):
        self.write_count += 1
        if self.write_count == self.fail_on_write:
            raise ConnectionError("injected storage failure")
        return super().write(memory_id, value=value, **fields)


class FailedCompensationBackend(FailingBackend):
    def delete(self, memory_id, **fields):
        raise ConnectionError("injected compensation failure")


class MultiAgentStorageTests(unittest.TestCase):
    def test_another_transaction_cannot_read_pending_write(self):
        coordinator = MultiAgentStorageCoordinator(InstrumentedMemoryBackend())
        coordinator.begin("txn_1", "agent_1")
        coordinator.begin("txn_2", "agent_2")
        coordinator.stage(
            "txn_1",
            {
                "type": "write",
                "memory_id": "m_pending",
                "value": "pending",
                "scope": "tenant:shared",
            },
        )

        self.assertIsNone(
            coordinator.read("txn_2", "m_pending", "tenant:shared")
        )

    def test_committing_one_transaction_does_not_flush_another(self):
        backend = InstrumentedMemoryBackend()
        coordinator = MultiAgentStorageCoordinator(backend)
        for index in (1, 2):
            coordinator.begin(f"txn_{index}", f"agent_{index}")
            coordinator.stage(
                f"txn_{index}",
                {
                    "type": "write",
                    "memory_id": f"m_{index}",
                    "value": index,
                    "scope": "tenant:shared",
                },
            )

        outcome = coordinator.commit("txn_1")

        self.assertEqual(outcome["classification"], "committed_complete")
        self.assertEqual(set(backend.snapshot()), {"m_1"})
        self.assertEqual(coordinator.report()["transactions"]["txn_2"], "active")

    def test_selective_revoke_aborts_only_matching_actor(self):
        coordinator = MultiAgentStorageCoordinator(InstrumentedMemoryBackend())
        for index in (1, 2):
            coordinator.begin(f"txn_{index}", f"agent_{index}")
            coordinator.stage(
                f"txn_{index}",
                {
                    "type": "write",
                    "memory_id": f"m_{index}",
                    "value": index,
                    "scope": "tenant:shared",
                },
            )
        coordinator.revoke("agent_1", "write", "tenant:shared")

        denied = coordinator.commit("txn_1")
        allowed = coordinator.commit("txn_2")

        self.assertEqual(denied["classification"], "aborted_clean")
        self.assertEqual(allowed["classification"], "committed_complete")

    def test_second_write_failure_compensates_first_write(self):
        backend = FailingBackend(fail_on_write=2)
        coordinator = MultiAgentStorageCoordinator(backend)
        coordinator.begin("txn_1", "agent_1")
        for index in (1, 2):
            coordinator.stage(
                "txn_1",
                {
                    "type": "write",
                    "memory_id": f"m_{index}",
                    "value": index,
                    "scope": "tenant:shared",
                },
            )

        outcome = coordinator.commit("txn_1")

        self.assertEqual(outcome["classification"], "aborted_clean")
        self.assertEqual(backend.snapshot(), {})

    def test_failed_compensation_is_unknown(self):
        backend = FailedCompensationBackend(fail_on_write=2)
        coordinator = MultiAgentStorageCoordinator(backend)
        coordinator.begin("txn_1", "agent_1")
        for index in (1, 2):
            coordinator.stage(
                "txn_1",
                {
                    "type": "write",
                    "memory_id": f"m_{index}",
                    "value": index,
                    "scope": "tenant:shared",
                },
            )

        outcome = coordinator.commit("txn_1")

        self.assertEqual(outcome["classification"], "unknown")

    def test_cross_agent_invalidation_reaches_descendants(self):
        backend = InstrumentedMemoryBackend()
        backend.write(
            "m_root",
            value="root",
            agent_id="agent_1",
            scope="tenant:shared",
        )
        coordinator = MultiAgentStorageCoordinator(backend)
        coordinator.begin("txn_derive", "agent_2")
        coordinator.stage(
            "txn_derive",
            {
                "type": "derive",
                "memory_id": "m_child",
                "source_ids": ["m_root"],
                "value": "child",
                "scope": "tenant:shared",
            },
        )
        coordinator.commit("txn_derive")

        outcome = coordinator.invalidate("agent_1", "m_root")

        self.assertEqual(outcome["classification"], "invalidated_complete")
        self.assertEqual(backend.snapshot()["m_root"]["status"], "invalid")
        self.assertEqual(backend.snapshot()["m_child"]["status"], "invalid")

    def test_two_threads_record_unique_commit_linearization_indexes(self):
        coordinator = MultiAgentStorageCoordinator(InstrumentedMemoryBackend())
        report = run_concurrent_storage_scenario(
            {
                "transactions": [
                    {
                        "txn_id": f"txn_{index}",
                        "agent_id": f"agent_{index}",
                        "actions": [
                            {
                                "type": "write",
                                "memory_id": f"m_{index}",
                                "value": index,
                                "scope": "tenant:shared",
                            }
                        ],
                    }
                    for index in (1, 2)
                ]
            },
            coordinator,
        )

        indexes = [item["linearization_index"] for item in report["commit_order"]]
        self.assertEqual(sorted(indexes), [1, 2])
        self.assertEqual(len(set(indexes)), 2)


if __name__ == "__main__":
    unittest.main()
