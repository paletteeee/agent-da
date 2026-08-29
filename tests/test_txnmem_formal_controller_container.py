import subprocess
import tempfile
import unittest
from pathlib import Path

import txnmem_formal_controller_container as container


class FormalControllerContainerTests(unittest.TestCase):
    def _paths(self):
        temporary = tempfile.TemporaryDirectory()
        root = Path(temporary.name).resolve()
        repository = root / "repository"
        wheels = root / "wheels"
        repository.mkdir()
        wheels.mkdir()
        self.addCleanup(temporary.cleanup)
        return repository, wheels

    def test_create_argv_is_the_complete_bounded_controller_contract(self):
        repository, wheels = self._paths()

        argv = container.build_create_argv(
            repository, wheels, "txnmem-formal-controller", "txnmem-formal-state",
            "txnmem-formal-controller:approved",
        )

        self.assertEqual(
            argv,
            [
                "/usr/bin/docker", "create", "--name", "txnmem-formal-controller",
                "--user", "0:0", "--network", "host", "--pid", "host",
                "--cap-drop", "ALL", "--cap-add", "NET_ADMIN", "--cap-add",
                "SYS_PTRACE", "--security-opt", "no-new-privileges=true",
                "--mount", f"type=bind,src={repository},dst=/workspace/txnmem,readonly",
                "--mount", f"type=bind,src={wheels},dst=/opt/txnmem-formal-wheel-source,readonly",
                "--mount", "type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock",
                "--mount", "type=volume,src=txnmem-formal-state,dst=/var/lib/txnmem-formal",
                "txnmem-formal-controller:approved", "/bin/sh", "-c", "exec sleep infinity",
            ],
        )
        self.assertNotIn("--privileged", argv)
        self.assertNotIn("NEO4J_PASSWORD", " ".join(argv))

    def test_create_checks_nonreuse_then_cleans_up_a_partial_lifecycle(self):
        repository, wheels = self._paths()
        calls = []

        def run(argv):
            calls.append(argv)
            code = {1: 1, 2: 1, 3: 0, 4: 42}.get(len(calls), 0)
            return subprocess.CompletedProcess(argv, code)

        with self.assertRaisesRegex(container.ControllerContainerError, "start"):
            container.create_controller(
                repository, wheels, "txnmem-formal-controller", "txnmem-formal-state",
                "txnmem-formal-controller:approved", run=run,
            )

        self.assertEqual(calls[0], ["/usr/bin/docker", "container", "inspect", "txnmem-formal-controller"])
        self.assertEqual(calls[1], ["/usr/bin/docker", "volume", "inspect", "txnmem-formal-state"])
        self.assertEqual(calls[2][0:2], ["/usr/bin/docker", "create"])
        self.assertEqual(calls[3], ["/usr/bin/docker", "start", "txnmem-formal-controller"])
        self.assertEqual(calls[4], ["/usr/bin/docker", "rm", "-f", "txnmem-formal-controller"])
        self.assertEqual(calls[5], ["/usr/bin/docker", "volume", "rm", "txnmem-formal-state"])

    def test_install_uses_only_the_fixed_installer_and_exact_commit(self):
        calls = []
        commit = "a" * 64

        container.install_controller(
            "txnmem-formal-controller", commit,
            run=lambda argv: calls.append(argv) or subprocess.CompletedProcess(argv, 0),
        )

        self.assertEqual(
            calls,
            [[
                "/usr/bin/docker", "exec", "--user", "0:0", "--env",
                "TXNMEM_FORMAL_WHEEL_SOURCE=/opt/txnmem-formal-wheel-source",
                "txnmem-formal-controller", "/bin/bash",
                "/workspace/txnmem/scripts/install_formal_provenance_runtime.sh",
                "/workspace/txnmem", commit,
            ]],
        )

    def test_invalid_inputs_fail_before_any_docker_mutation(self):
        repository, wheels = self._paths()
        calls = []
        bad_link = repository.parent / "repository-link"
        bad_link.symlink_to(repository, target_is_directory=True)

        for kwargs in (
            {"container_name": "bad/name"}, {"volume_name": "bad/name"},
            {"image": "bad image"}, {"repository": Path("/")},
            {"wheel_source": bad_link},
        ):
            with self.subTest(kwargs=kwargs):
                values = {
                    "repository": repository, "wheel_source": wheels,
                    "container_name": "txnmem-formal-controller",
                    "volume_name": "txnmem-formal-state",
                    "image": "txnmem-formal-controller:approved",
                }
                values.update(kwargs)
                with self.assertRaises(container.ControllerContainerError):
                    container.create_controller(**values, run=lambda argv: calls.append(argv))
        for commit in ("a" * 39, "A" * 40, "a" * 65, "a" * 40 + ";id"):
            with self.subTest(commit=commit):
                with self.assertRaises(container.ControllerContainerError):
                    container.install_controller("txnmem-formal-controller", commit, run=lambda argv: calls.append(argv))
        self.assertEqual(calls, [])

    def test_inspection_error_fails_closed_before_create(self):
        repository, wheels = self._paths()
        calls = []

        with self.assertRaisesRegex(container.ControllerContainerError, "inspect"):
            container.create_controller(
                repository, wheels, "txnmem-formal-controller", "txnmem-formal-state",
                "txnmem-formal-controller:approved",
                run=lambda argv: calls.append(argv) or subprocess.CompletedProcess(argv, 125),
            )

        self.assertEqual(calls, [["/usr/bin/docker", "container", "inspect", "txnmem-formal-controller"]])

    def test_dockerfile_pins_runtime_and_wrapper_has_no_passthrough(self):
        root = Path(__file__).resolve().parents[1]
        dockerfile = (root / "infra/formal_controller/Dockerfile").read_text(encoding="utf-8")
        wrapper = (root / "scripts/manage_formal_controller_container.sh").read_text(encoding="utf-8")

        self.assertIn("FROM mcr.microsoft.com/devcontainers/base@sha256:81380e4c9c14e8a629ff39029639e4b7893e67400246fa7782a0fe7dc193a02a", dockerfile)
        self.assertIn("/usr/bin/python3", dockerfile)
        self.assertIn("3.10.12", dockerfile)
        self.assertIn("/usr/bin/docker", dockerfile)
        self.assertIn("/usr/sbin/nft", dockerfile)
        self.assertNotIn("dockerd", dockerfile)
        self.assertNotIn("exec", wrapper.lower().replace("/usr/bin/python3", ""))
        self.assertNotIn("shell", wrapper.lower())
