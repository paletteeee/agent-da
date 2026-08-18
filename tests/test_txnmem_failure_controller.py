from __future__ import annotations

import unittest

from txnmem_failure_controller import (
    FailureController,
    FailureInjectionError,
    validate_failure_schedule,
)


class FailureControllerPhaseTests(unittest.TestCase):
    def test_existing_event_schedule_behavior_is_unchanged(self):
        controller = FailureController(
            [{"trigger": {"kind": "memory_write", "count": 2}, "action": {"type": "crash"}}]
        )

        self.assertEqual(controller.observe({"kind": "memory_write", "step": 1}), [])
        with self.assertRaises(FailureInjectionError) as raised:
            controller.observe({"kind": "memory_write", "step": 2})
        self.assertEqual(raised.exception.code, "injected_crash")

    def test_phase_schedule_fires_at_exact_supported_semantic_boundary(self):
        controller = FailureController(
            [{"trigger": {"phase": "after_mutation", "count": 2}, "action": {"type": "crash"}}]
        )

        self.assertEqual(
            controller.observe_phase("after_mutation", {"mutation_count": 1}),
            [],
        )
        with self.assertRaises(FailureInjectionError) as raised:
            controller.observe_phase("after_mutation", {"mutation_count": 2})
        self.assertEqual(raised.exception.code, "injected_crash")

    def test_only_documented_commit_phase_names_are_accepted(self):
        for phase in (
            "after_prepare",
            "after_qdrant_stage",
            "after_neo4j_stage",
            "after_stage_verify",
            "after_commit_decision",
            "after_finalize",
            "after_mutation",
        ):
            with self.subTest(phase=phase):
                self.assertEqual(
                    validate_failure_schedule(
                        [{"trigger": {"phase": phase}, "action": {"type": "delay"}}]
                    )[0]["trigger"]["phase"],
                    phase,
                )

        with self.assertRaises(ValueError):
            validate_failure_schedule(
                [{"trigger": {"phase": "after_unknown"}, "action": {"type": "crash"}}]
            )


if __name__ == "__main__":
    unittest.main()
