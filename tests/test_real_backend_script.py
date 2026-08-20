from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class RealBackendScriptTests(unittest.TestCase):
    def _run_e2e_cli(self, *arguments):
        environment = os.environ.copy()
        environment["PYTHONPATH"] = os.pathsep.join(
            [str(ROOT / "src"), str(ROOT / "scripts")]
        )
        return subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "run_e2e_real_backend.py"), *arguments],
            cwd=ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )

    def test_e2e_cli_task_mode_requires_journal_and_dry_run_uses_proxy_ports(self):
        missing = self._run_e2e_cli("--dry-run", "--transaction-mode", "task")
        self.assertNotEqual(missing.returncode, 0)
        self.assertIn("--journal-path", missing.stderr)

        journal = ROOT / "results" / "dry-run-task-journal.sqlite3"
        configured = self._run_e2e_cli(
            "--dry-run",
            "--transaction-mode",
            "task",
            "--journal-path",
            str(journal),
        )
        self.assertEqual(configured.returncode, 0, configured.stderr)
        payload = json.loads(configured.stdout)
        self.assertEqual(payload["transaction_mode"], "task")
        self.assertEqual(payload["journal_path"], str(journal.resolve()))
        self.assertEqual(payload["qdrant_url"], "http://127.0.0.1:19000")
        self.assertEqual(payload["neo4j_uri"], "bolt://127.0.0.1:19001")

    def test_e2e_cli_keeps_direct_mode_as_default_without_a_journal(self):
        completed = self._run_e2e_cli("--dry-run")

        self.assertEqual(completed.returncode, 0, completed.stderr)
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["transaction_mode"], "direct")
        self.assertIsNone(payload["journal_path"])

    def test_e2e_runner_attests_model_identity_and_live_backend_health(self):
        script = (ROOT / "scripts" / "run_e2e_real_backend.py").read_text(
            encoding="utf-8"
        )

        self.assertIn('os.environ["TXNMEM_MODEL_REVISION"]', script)
        self.assertIn('os.environ["TXNMEM_MODEL_SERVER_BUILD"]', script)
        self.assertIn('os.environ["TXNMEM_RUN_ID"]', script)
        self.assertIn('f"e2e-{run_id}-tau-', script)
        self.assertIn('"backend_health": backend_health', script)
        self.assertIn("health_backend.healthcheck()", script)
        self.assertIn('"source_commit":', script)

    def test_smoke_script_creates_both_proxies_and_runs_fault_cli_through_them(self):
        script = (ROOT / "scripts" / "run_real_backend_smoke.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn('"name":"txnmem-qdrant"', script)
        self.assertIn('"upstream":"qdrant:6333"', script)
        self.assertIn('"listen":"0.0.0.0:19000"', script)
        self.assertIn('"name":"txnmem-neo4j"', script)
        self.assertIn('"upstream":"neo4j:7687"', script)
        self.assertIn('"listen":"0.0.0.0:19001"', script)
        self.assertIn("backend-performance", script)
        self.assertIn("--service-url http://127.0.0.1:19000", script)
        self.assertIn("TXNMEM_NEO4J_URI=bolt://127.0.0.1:19001", script)
        self.assertIn("TXNMEM_TOXIPROXY_URL=http://127.0.0.1:8474", script)
        self.assertIn('f["all_scenarios_state_verified"]', script)
        self.assertIn('f["all_observed_states_consistent"]', script)
        self.assertNotIn("all_scenarios_no_partial_commit", script)

    def test_smoke_script_fails_fast_when_curl_is_missing(self):
        script = (ROOT / "scripts" / "run_real_backend_smoke.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn("if ! command -v curl", script)
        self.assertIn('write_blocked "curl_not_installed"', script)

    def test_compose_does_not_publish_direct_qdrant_or_neo4j_data_ports(self):
        compose = (ROOT / "infra" / "real_backend" / "docker-compose.yml").read_text(
            encoding="utf-8"
        )

        self.assertNotIn('"6333:6333"', compose)
        self.assertNotIn('"7687:7687"', compose)
        self.assertIn('"127.0.0.1:19000:19000"', compose)
        self.assertIn('"127.0.0.1:19001:19001"', compose)
        self.assertIn('"127.0.0.1:8474:8474"', compose)
        self.assertIn("internal: true", compose)

    def test_qdrant_healthcheck_uses_tools_present_in_the_pinned_image(self):
        compose = (ROOT / "infra" / "real_backend" / "docker-compose.yml").read_text(
            encoding="utf-8"
        )

        self.assertNotIn('["CMD", "wget"', compose)
        self.assertIn("/dev/tcp/127.0.0.1/6333", compose)
        self.assertIn("GET /readyz HTTP/1.0", compose)


if __name__ == "__main__":
    unittest.main()
