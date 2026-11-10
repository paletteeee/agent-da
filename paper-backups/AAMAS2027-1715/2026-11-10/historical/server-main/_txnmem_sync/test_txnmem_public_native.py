import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from txnmem_public_native import (
    LoCoMoPublicAdapter,
    TauBenchPublicAdapter,
    load_public_tasks,
    run_public_native_manifest,
    write_blocked_report,
)


class PublicNativeRunnerTests(unittest.TestCase):
    def test_tau_task_conversion_has_stable_episode_id_and_native_prompt_context(self):
        with TemporaryDirectory() as tmp:
            source = Path(tmp) / "tau.json"
            source.write_text(
                json.dumps(
                    [
                        {
                            "task_id": 7,
                            "info": {"task": {"instruction": "Book the requested itinerary."}},
                            "traj": [],
                        }
                    ]
                ),
                encoding="utf-8",
            )
            tasks = load_public_tasks("tau-bench", source, limit=1)

        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].episode_id, "tau-bench:7")
        self.assertIn("Book the requested itinerary.", tasks[0].prompt)
        self.assertEqual(tasks[0].metadata["dataset"], "tau-bench")

    def test_locomo_task_conversion_uses_conversation_context_without_projection_label(self):
        with TemporaryDirectory() as tmp:
            source = Path(tmp) / "locomo.json"
            source.write_text(
                json.dumps(
                    [
                        {
                            "sample_id": "conv-1",
                            "conversation": {
                                "session_1": [{"speaker": "A", "text": "I moved to Boston."}],
                                "session_1_date_time": "2024-01-01",
                            },
                        }
                    ]
                ),
                encoding="utf-8",
            )
            tasks = load_public_tasks("locomo", source, limit=1)

        self.assertEqual(tasks[0].episode_id, "locomo:conv-1")
        self.assertIn("I moved to Boston.", tasks[0].prompt)
        self.assertEqual(tasks[0].metadata["execution_mode"], "native_contextual_agent_run")
        self.assertNotEqual(tasks[0].metadata.get("projection"), "locomo_session_summary")

    def test_blocked_report_contains_reason_but_no_raw_context(self):
        with TemporaryDirectory() as tmp:
            path = write_blocked_report(
                Path(tmp),
                dataset="appworld",
                reason="blocked_external_dependency",
                checks={"package": "missing", "raw_context": "private value"},
            )
            report = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["reason"], "blocked_external_dependency")
        self.assertNotIn("private value", json.dumps(report))
        self.assertNotIn("raw_context", report["checks"])

    def test_unavailable_public_manifest_returns_blocked_without_model(self):
        with TemporaryDirectory() as tmp:
            source = Path(tmp) / "locomo.json"
            source.write_text("[]", encoding="utf-8")
            report = run_public_native_manifest(
                {"dataset": "locomo", "source": str(source), "limit": 1},
                model=None,
                out_dir=Path(tmp) / "out",
            )

        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["reason"], "blocked_external_dependency")

    def test_environment_checks_are_explicitly_non_native_for_missing_runtime(self):
        with TemporaryDirectory() as tmp:
            source = Path(tmp) / "tau.json"
            source.write_text("[]", encoding="utf-8")
            tau = TauBenchPublicAdapter()
            locomo = LoCoMoPublicAdapter()
            self.assertIn("available", tau.check_environment(source))
            self.assertFalse(locomo.check_environment(source)["available"])
