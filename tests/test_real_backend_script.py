from __future__ import annotations

import json
import os
import shutil
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

    def test_formal_smoke_wrapper_is_external_credential_safe_and_candidate_free(self):
        script = (ROOT / "scripts" / "run_formal_provenance_smoke.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn("[[ $# -ne 1 ]]", script)
        self.assertIn("smoke_out", script)
        self.assertIn("TXNMEM_NEO4J_PASSWORD", script)
        self.assertIn("exec /usr/bin/env -i", script)
        self.assertIn("/opt/txnmem-formal-controller/txnmem_formal_controller.py", script)
        self.assertIn('--project-root "$PWD" smoke --out "$smoke_out"', script)
        self.assertNotIn("candidate-root", script)
        self.assertNotIn("authorization-nonce", script)

        relative = subprocess.run(
            ["/bin/bash", str(ROOT / "scripts" / "run_formal_provenance_smoke.sh"), "relative.json"],
            cwd=ROOT,
            env={"TXNMEM_NEO4J_PASSWORD": "test-only-placeholder"},
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(relative.returncode, 0)

    def test_cross_host_wrapper_adds_smoke_without_removing_existing_actions(self):
        script = (
            ROOT / "scripts" / "run_cross_host_provenance_performance.sh"
        ).read_text(encoding="utf-8")

        for action in ("measure", "material", "attest", "promote", "smoke"):
            self.assertIn(f"  {action})", script)
        self.assertIn('scripts/run_formal_provenance_smoke.sh "$1"', script)

    def test_compose_does_not_publish_direct_qdrant_or_neo4j_data_ports(self):
        compose = (ROOT / "infra" / "real_backend" / "docker-compose.yml").read_text(
            encoding="utf-8"
        )

        self.assertNotIn('"6333:6333"', compose)
        self.assertNotIn('"7687:7687"', compose)
        self.assertIn('"127.0.0.1:19000:19000"', compose)
        self.assertIn('"127.0.0.1:19001:19001"', compose)
        self.assertIn('"127.0.0.1:8474:8474"', compose)
        self.assertIn("txnmem-ingress", compose)
        self.assertEqual(compose.count("      - ingress"), 1)
        qdrant_block, remainder = compose.split("  neo4j:", 1)
        neo4j_block, toxiproxy_block = remainder.split("  toxiproxy:", 1)
        self.assertNotIn("      - ingress", qdrant_block)
        self.assertNotIn("      - ingress", neo4j_block)
        self.assertIn("      - ingress", toxiproxy_block)
        network_definitions = compose.split("\nnetworks:\n", 1)[1]
        backend_network, ingress_network = network_definitions.split(
            "  ingress:\n", 1
        )
        self.assertIn("internal: true", backend_network)
        self.assertIn("driver: bridge", backend_network)
        self.assertNotIn("internal: true", ingress_network)
        self.assertIn("driver: bridge", ingress_network)

    def test_compose_explicitly_enables_only_toxiproxy_proxy_metrics(self):
        compose = (ROOT / "infra" / "real_backend" / "docker-compose.yml").read_text(
            encoding="utf-8"
        )
        self.assertEqual(compose.count("-proxy-metrics"), 1)
        self.assertNotIn("-runtime-metrics", compose)
        self.assertIn("-host=0.0.0.0", compose)

    def test_compose_sets_qdrant_nofile_limit_for_formal_scale(self):
        compose = (ROOT / "infra" / "real_backend" / "docker-compose.yml").read_text(
            encoding="utf-8"
        )
        qdrant_block = compose.split("  neo4j:\n", 1)[0]

        self.assertIn(
            "    ulimits:\n"
            "      nofile:\n"
            "        soft: 65536\n"
            "        hard: 65536\n",
            qdrant_block,
        )

    @unittest.skipUnless(shutil.which("docker"), "Docker Compose CLI is optional")
    def test_compose_resolves_to_proxy_only_external_ingress(self):
        compose_path = ROOT / "infra" / "real_backend" / "docker-compose.yml"
        environment = os.environ.copy()
        environment["TXNMEM_NEO4J_PASSWORD"] = "compose-validation-only"
        completed = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(compose_path),
                "config",
                "--format",
                "json",
            ],
            cwd=ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        document = json.loads(completed.stdout)
        networks = document["networks"]
        services = document["services"]
        self.assertEqual(networks["backend"]["name"], "txnmem-backend")
        self.assertEqual(networks["backend"]["driver"], "bridge")
        self.assertIs(networks["backend"]["internal"], True)
        self.assertEqual(networks["ingress"]["name"], "txnmem-ingress")
        self.assertEqual(networks["ingress"]["driver"], "bridge")
        self.assertIs(networks["ingress"].get("internal", False), False)
        self.assertEqual(set(services["qdrant"]["networks"]), {"backend"})
        self.assertEqual(set(services["neo4j"]["networks"]), {"backend"})
        self.assertEqual(
            set(services["toxiproxy"]["networks"]), {"backend", "ingress"}
        )
        self.assertEqual(
            services["toxiproxy"]["command"],
            ["-host=0.0.0.0", "-proxy-metrics"],
        )
        self.assertNotIn("ports", services["qdrant"])
        self.assertNotIn("ports", services["neo4j"])
        self.assertEqual(
            services["qdrant"]["ulimits"]["nofile"],
            {"soft": 65536, "hard": 65536},
        )
        self.assertEqual(
            {
                (row["host_ip"], int(row["published"]), int(row["target"]))
                for row in services["toxiproxy"]["ports"]
            },
            {
                ("127.0.0.1", 8474, 8474),
                ("127.0.0.1", 19000, 19000),
                ("127.0.0.1", 19001, 19001),
            },
        )

    def test_qdrant_healthcheck_uses_tools_present_in_the_pinned_image(self):
        compose = (ROOT / "infra" / "real_backend" / "docker-compose.yml").read_text(
            encoding="utf-8"
        )

        self.assertNotIn('["CMD", "wget"', compose)
        self.assertIn("/dev/tcp/127.0.0.1/6333", compose)
        self.assertIn("GET /readyz HTTP/1.0", compose)


if __name__ == "__main__":
    unittest.main()
