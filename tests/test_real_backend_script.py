from __future__ import annotations

import json
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
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

    def test_formal_progress_wrapper_has_exact_arguments_and_an_isolated_read_only_environment(self):
        wrapper = ROOT / "scripts" / "read_formal_provenance_progress.sh"
        self.assertTrue(wrapper.is_file())
        script = wrapper.read_text(encoding="utf-8")

        self.assertTrue(script.startswith("#!/bin/sh\n"))
        self.assertIn('reader_install_path=/opt/txnmem-formal-controller/read_formal_provenance_progress.sh', script)
        self.assertIn('case "$0" in', script)
        self.assertIn('case "$#" in', script)
        self.assertIn("exec /usr/bin/env -i", script)
        self.assertIn("LANG=C.UTF-8", script)
        self.assertIn("LC_ALL=C.UTF-8", script)
        self.assertIn("PYTHONDONTWRITEBYTECODE=1", script)
        self.assertIn("/usr/bin/python3 -I -S -B", script)
        self.assertIn(
            "/opt/txnmem-formal-controller/txnmem_formal_controller.py",
            script,
        )
        self.assertIn(
            'progress --run-id "$1" --authorization-nonce "$2"',
            script,
        )
        self.assertNotIn("#!/usr/bin/env bash", script)
        self.assertNotIn("[[", script)
        pre_sanitization = script.split("exec /usr/bin/env -i", 1)[0]
        for forbidden_before_sanitization in ("[", "echo ", "printf ", "$PATH"):
            self.assertNotIn(forbidden_before_sanitization, pre_sanitization)
        for forbidden in (
            "TXNMEM_NEO4J_PASSWORD",
            "neo4j",
            "qdrant",
            "docker",
            "journalctl",
            "tail ",
            "grep ",
            "candidate-root",
            "progress.json",
        ):
            self.assertNotIn(forbidden, script.lower())

        seeded_run = "seeded-private-run-id"
        seeded_nonce = "/seeded/private/authorization.nonce"
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "startup-environment-ran"
            startup = Path(tmp) / "startup.sh"
            startup.write_text(f"/usr/bin/touch {marker}\n", encoding="utf-8")
            invalid = subprocess.run(
                [str(wrapper), seeded_run, seeded_nonce, "extra"],
                cwd=ROOT,
                env={
                    "LANG": "C.UTF-8",
                    "LC_ALL": "C.UTF-8",
                    "BASH_ENV": str(startup),
                    "ENV": str(startup),
                    "HOSTILE_SECRET": "must-not-survive",
                },
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(invalid.returncode, 0)
            self.assertFalse(marker.exists())
            self.assertNotIn(seeded_run, invalid.stdout + invalid.stderr)
            self.assertNotIn(seeded_nonce, invalid.stdout + invalid.stderr)

    def test_installer_registers_the_exact_progress_wrapper_bytes(self):
        installer = ROOT / "scripts" / "install_formal_provenance_runtime.sh"
        installer_text = installer.read_text(encoding="utf-8")
        try:
            embedded = installer_text.split("<<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
        except IndexError as exc:
            self.fail(f"installer embedded source exporter is unavailable: {type(exc).__name__}")

        wrapper_relative = "scripts/read_formal_provenance_progress.sh"
        wrapper_bytes = b"#!/bin/sh\n# exact committed wrapper fixture\n"
        controller_required = {
            "configs/provenance_performance_matrix.json",
            "configs/provenance_runtime_lock.json",
            "infra/real_backend/docker-compose.yml",
            "scripts/install_formal_provenance_runtime.sh",
            "scripts/run_cross_host_provenance_performance.sh",
            "scripts/run_formal_provenance_smoke.sh",
            "scripts/run_provenance_performance.sh",
            wrapper_relative,
            "src/txnmem_formal_controller.py",
            "src/txnmem_formal_smoke.py",
            "src/txnmem_provenance_execution_collector.py",
            "src/txnmem_provenance_progress.py",
            "src/txnmem_provenance_runner.py",
        }

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            repository = root / "repository"
            staging = root / "staging"
            repository.mkdir()
            staging.mkdir()
            for relative in sorted(controller_required):
                path = repository / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                if relative == "scripts/install_formal_provenance_runtime.sh":
                    path.write_bytes(installer.read_bytes())
                elif relative == wrapper_relative:
                    path.write_bytes(wrapper_bytes)
                else:
                    path.write_text(f"# {relative}\n", encoding="utf-8")
            subprocess.run(["git", "init", "-q"], cwd=repository, check=True)
            subprocess.run(
                ["git", "config", "user.email", "installer@example.invalid"],
                cwd=repository,
                check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "Installer Fixture"],
                cwd=repository,
                check=True,
            )
            subprocess.run(["git", "add", "."], cwd=repository, check=True)
            subprocess.run(
                ["git", "commit", "-q", "-m", "approved"],
                cwd=repository,
                check=True,
            )
            commit = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=repository,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            environment = {
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "SCRIPT_PATH": str(repository / "scripts" / "install_formal_provenance_runtime.sh"),
                "PROJECT_ROOT": str(repository),
                "APPROVED_COMMIT": commit,
                "STAGING": str(staging),
            }
            completed = subprocess.run(
                [sys.executable, "-I", "-S", "-B", "-c", embedded],
                cwd=repository,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            manifest = json.loads(
                (staging / "approved_source_manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            rows = {
                row["path"]: row["blob_sha256"] for row in manifest["files"]
            }
            self.assertIn(wrapper_relative, rows)
            self.assertEqual(
                rows[wrapper_relative], hashlib.sha256(wrapper_bytes).hexdigest()
            )
            self.assertEqual(
                (staging / "read_formal_provenance_progress.sh").read_bytes(),
                wrapper_bytes,
            )

        self.assertIn(
            'progress_reader_target="$controller_dir/read_formal_provenance_progress.sh"',
            installer_text,
        )
        self.assertIn(
            '"$staging/read_formal_provenance_progress.sh" "$progress_reader_new"',
            installer_text,
        )
        self.assertIn(
            '/usr/bin/mv -f "$progress_reader_new" "$progress_reader_target"',
            installer_text,
        )

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
