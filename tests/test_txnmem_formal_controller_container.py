import copy
import hashlib
import json
import os
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
        configs = repository / "configs"
        configs.mkdir()
        wheel_payloads = {
            "neo4j-fixture.whl": b"neo4j fixture wheel\n",
            "pytz-fixture.whl": b"pytz fixture wheel\n",
        }
        distributions = []
        for name, (filename, payload) in zip(
            ("neo4j", "pytz"), wheel_payloads.items()
        ):
            (wheels / filename).write_bytes(payload)
            distributions.append(
                {
                    "name": name,
                    "version": "1.0",
                    "filename": filename,
                    "sha256": hashlib.sha256(payload).hexdigest(),
                    "dependency_names": [],
                    "requires_dist": [],
                }
            )
        (configs / "provenance_runtime_lock.json").write_text(
            json.dumps(
                {
                    "schema": "txnmem-provenance-runtime-lock-v1",
                    "python_versions": ["3.10.12"],
                    "distributions": distributions,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        subprocess.run(["git", "init", "-q"], cwd=repository, check=True)
        subprocess.run(
            ["git", "config", "user.email", "container-test@example.invalid"],
            cwd=repository,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Container Test"],
            cwd=repository,
            check=True,
        )
        subprocess.run(["git", "add", "."], cwd=repository, check=True)
        subprocess.run(
            ["git", "commit", "-q", "-m", "fixture"], cwd=repository, check=True
        )
        self.addCleanup(temporary.cleanup)
        return repository, wheels

    @staticmethod
    def _start_failure_runner(token, container_id, *, cleanup_raises=False):
        calls = []
        volume_created = False
        image_id = "sha256:" + "a" * 64

        def run(argv):
            nonlocal volume_created
            calls.append(argv)
            if argv[1:3] == ["image", "inspect"]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([{"Id": image_id}])
                )
            if argv == ["/usr/bin/docker", "container", "inspect", "txnmem-formal-controller"]:
                return subprocess.CompletedProcess(argv, 1, stdout="")
            if argv == ["/usr/bin/docker", "volume", "inspect", "txnmem-formal-state"]:
                if not volume_created:
                    return subprocess.CompletedProcess(argv, 1, stdout="")
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps([{"Name": "txnmem-formal-state", "Labels": {
                        "com.txnmem.formal.lifecycle": token,
                        "com.txnmem.formal.role": "controller-state",
                    }}]),
                )
            if argv[1:3] == ["volume", "create"]:
                volume_created = True
                return subprocess.CompletedProcess(argv, 0, stdout="txnmem-formal-state\n")
            if argv[1] == "create":
                return subprocess.CompletedProcess(argv, 0, stdout=container_id + "\n")
            if argv == ["/usr/bin/docker", "container", "inspect", container_id]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps([{"Id": container_id, "Name": "/txnmem-formal-controller", "Image": image_id, "Config": {"Image": image_id, "Labels": {
                        "com.txnmem.formal.lifecycle": token,
                        "com.txnmem.formal.role": "controller",
                        "com.txnmem.formal.image-id": image_id,
                    }}}]),
                )
            if argv == ["/usr/bin/docker", "start", container_id]:
                return subprocess.CompletedProcess(argv, 42, stdout="sensitive-start-output")
            if argv == ["/usr/bin/docker", "rm", "-f", container_id]:
                if cleanup_raises:
                    raise OSError("sensitive-container-cleanup-output")
                return subprocess.CompletedProcess(argv, 51, stdout="sensitive-container-cleanup-output")
            if argv == ["/usr/bin/docker", "volume", "rm", "txnmem-formal-state"]:
                return subprocess.CompletedProcess(argv, 52, stdout="sensitive-volume-cleanup-output")
            return subprocess.CompletedProcess(argv, 125, stdout="")

        return run, calls

    @staticmethod
    def _controller_inspect_document(repository, wheels, token, container_id, image_id):
        return {
            "Id": container_id,
            "Name": "/txnmem-formal-controller",
            "Image": image_id,
            "State": {"Running": True},
            "Config": {
                "User": "0:0",
                "Image": image_id,
                "Labels": {
                    "com.txnmem.formal.lifecycle": token,
                    "com.txnmem.formal.role": "controller",
                    "com.txnmem.formal.image-id": image_id,
                },
            },
            "HostConfig": {
                "NetworkMode": "host",
                "PidMode": "host",
                "CapDrop": ["ALL"],
                "CapAdd": ["NET_ADMIN", "SYS_PTRACE"],
                "SecurityOpt": ["no-new-privileges"],
                "Privileged": False,
            },
            "Mounts": [
                {
                    "Type": "bind",
                    "Source": str(repository),
                    "Destination": "/workspace/txnmem",
                    "Mode": "ro",
                    "RW": False,
                },
                {
                    "Type": "bind",
                    "Source": str(wheels),
                    "Destination": "/opt/txnmem-formal-wheel-source",
                    "Mode": "ro",
                    "RW": False,
                },
                {
                    "Type": "bind",
                    "Source": "/var/run/docker.sock",
                    "Destination": "/var/run/docker.sock",
                    "Mode": "",
                    "RW": True,
                },
                {
                    "Type": "volume",
                    "Name": "txnmem-formal-state",
                    "Source": "/var/lib/docker/volumes/txnmem-formal-state/_data",
                    "Destination": "/var/lib/txnmem-formal",
                    "Mode": "",
                    "RW": True,
                },
            ],
        }

    def test_create_argv_is_the_complete_bounded_controller_contract(self):
        repository, wheels = self._paths()

        argv = container.build_create_argv(
            repository, wheels, "txnmem-formal-controller", "txnmem-formal-state",
            "sha256:" + "b" * 64, "a" * 64,
        )

        self.assertEqual(
            argv,
            [
                "/usr/bin/docker", "create", "--name", "txnmem-formal-controller",
                "--user", "0:0", "--network", "host", "--pid", "host",
                "--cap-drop", "ALL", "--cap-add", "NET_ADMIN", "--cap-add",
                "SYS_PTRACE", "--security-opt", "no-new-privileges=true",
                "--label", f"com.txnmem.formal.lifecycle={'a' * 64}",
                "--label", "com.txnmem.formal.role=controller",
                "--label", f"com.txnmem.formal.image-id=sha256:{'b' * 64}",
                "--mount", f"type=bind,src={repository},dst=/workspace/txnmem,readonly",
                "--mount", f"type=bind,src={wheels},dst=/opt/txnmem-formal-wheel-source,readonly",
                "--mount", "type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock",
                "--mount", "type=volume,src=txnmem-formal-state,dst=/var/lib/txnmem-formal",
                "sha256:" + "b" * 64, "/bin/sh", "-c", "exec sleep infinity",
            ],
        )
        self.assertNotIn("--privileged", argv)
        self.assertNotIn("NEO4J_PASSWORD", " ".join(argv))

    def test_create_checks_nonreuse_then_cleans_up_a_partial_lifecycle(self):
        repository, wheels = self._paths()
        token = "a" * 64
        container_id = "e" * 64
        image_id = "sha256:" + "f" * 64
        calls = []
        volume_created = False

        def run(argv):
            nonlocal volume_created
            calls.append(argv)
            if argv[1:3] == ["image", "inspect"]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([{"Id": image_id}])
                )
            if argv == ["/usr/bin/docker", "container", "inspect", "txnmem-formal-controller"]:
                return subprocess.CompletedProcess(argv, 1, stdout="")
            if argv == ["/usr/bin/docker", "volume", "inspect", "txnmem-formal-state"]:
                if not volume_created:
                    return subprocess.CompletedProcess(argv, 1, stdout="")
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps([{"Name": "txnmem-formal-state", "Labels": {
                        "com.txnmem.formal.lifecycle": token,
                        "com.txnmem.formal.role": "controller-state",
                    }}]),
                )
            if argv[1:3] == ["volume", "create"]:
                volume_created = True
                return subprocess.CompletedProcess(argv, 0, stdout="txnmem-formal-state\n")
            if argv[1] == "create":
                return subprocess.CompletedProcess(argv, 0, stdout=container_id + "\n")
            if argv == ["/usr/bin/docker", "container", "inspect", container_id]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps([{"Id": container_id, "Name": "/txnmem-formal-controller", "Image": image_id, "Config": {"Image": image_id, "Labels": {
                        "com.txnmem.formal.lifecycle": token,
                        "com.txnmem.formal.role": "controller",
                        "com.txnmem.formal.image-id": image_id,
                    }}}]),
                )
            if argv == ["/usr/bin/docker", "start", container_id]:
                return subprocess.CompletedProcess(argv, 42, stdout="")
            return subprocess.CompletedProcess(argv, 0, stdout="")

        with self.assertRaisesRegex(container.ControllerContainerError, "start"):
            container.create_controller(
                repository, wheels, "txnmem-formal-controller", "txnmem-formal-state",
                "txnmem-formal-controller:approved", lifecycle_token=token, run=run,
            )

        self.assertEqual(calls[0], ["/usr/bin/docker", "image", "inspect", "txnmem-formal-controller:approved"])
        self.assertEqual(calls[1], ["/usr/bin/docker", "container", "inspect", "txnmem-formal-controller"])
        self.assertEqual(calls[2], ["/usr/bin/docker", "volume", "inspect", "txnmem-formal-state"])
        self.assertIn(["/usr/bin/docker", "start", container_id], calls)
        self.assertIn(["/usr/bin/docker", "rm", "-f", container_id], calls)
        self.assertIn(["/usr/bin/docker", "volume", "rm", "txnmem-formal-state"], calls)

    def test_install_uses_only_the_fixed_installer_and_exact_commit(self):
        repository, wheels = self._paths()
        calls = []
        commit = "a" * 64
        token = "8" * 64
        container_id = "9" * 64
        document = self._controller_inspect_document(
            repository, wheels, token, container_id, "sha256:" + "a" * 64
        )

        def run(argv):
            calls.append(argv)
            if argv == [
                "/usr/bin/docker", "container", "inspect", "txnmem-formal-controller"
            ]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([document])
                )
            if argv == [
                "/usr/bin/docker", "volume", "inspect", "txnmem-formal-state"
            ]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps([{"Name": "txnmem-formal-state", "Labels": {
                        "com.txnmem.formal.lifecycle": token,
                        "com.txnmem.formal.role": "controller-state",
                    }}]),
                )
            return subprocess.CompletedProcess(argv, 0, stdout="")

        container.install_controller(
            "txnmem-formal-controller", commit,
            lifecycle_token=token,
            run=run,
        )

        self.assertEqual(
            calls,
            [
                ["/usr/bin/docker", "container", "inspect", "txnmem-formal-controller"],
                ["/usr/bin/docker", "volume", "inspect", "txnmem-formal-state"],
                [
                "/usr/bin/docker", "exec", "--user", "0:0", "--env",
                "TXNMEM_FORMAL_WHEEL_SOURCE=/opt/txnmem-formal-wheel-source",
                container_id, "/bin/bash",
                "/workspace/txnmem/scripts/install_formal_provenance_runtime.sh",
                "/workspace/txnmem", commit,
                ],
            ],
        )

    def test_invalid_inputs_fail_before_any_docker_mutation(self):
        repository, wheels = self._paths()
        calls = []
        bad_link = repository.parent / "repository-link"
        bad_link.symlink_to(repository, target_is_directory=True)

        for kwargs in (
            {"container_name": "bad/name"}, {"volume_name": "bad/name"},
            {"image": "bad image"}, {"repository": Path("/")},
            {"wheel_source": bad_link}, {"lifecycle_token": "bad"},
        ):
            with self.subTest(kwargs=kwargs):
                values = {
                    "repository": repository, "wheel_source": wheels,
                    "container_name": "txnmem-formal-controller",
                    "volume_name": "txnmem-formal-state",
                    "image": "txnmem-formal-controller:approved",
                    "lifecycle_token": "a" * 64,
                }
                values.update(kwargs)
                with self.assertRaises(container.ControllerContainerError):
                    container.create_controller(**values, run=lambda argv: calls.append(argv))
        for commit in ("a" * 39, "A" * 40, "a" * 65, "a" * 40 + ";id"):
            with self.subTest(commit=commit):
                with self.assertRaises(container.ControllerContainerError):
                    container.install_controller(
                        "txnmem-formal-controller",
                        commit,
                        lifecycle_token="a" * 64,
                        run=lambda argv: calls.append(argv),
                    )
        self.assertEqual(calls, [])

    def test_inspection_error_fails_closed_before_create(self):
        repository, wheels = self._paths()
        calls = []

        with self.assertRaisesRegex(container.ControllerContainerError, "inspect"):
            container.create_controller(
                repository, wheels, "txnmem-formal-controller", "txnmem-formal-state",
                "txnmem-formal-controller:approved",
                lifecycle_token="a" * 64,
                run=lambda argv: calls.append(argv) or subprocess.CompletedProcess(argv, 125),
            )

        self.assertEqual(calls, [["/usr/bin/docker", "image", "inspect", "txnmem-formal-controller:approved"]])

    def test_dockerfile_pins_runtime_and_wrapper_has_no_passthrough(self):
        root = Path(__file__).resolve().parents[1]
        dockerfile = (root / "infra/formal_controller/Dockerfile").read_text(encoding="utf-8")
        wrapper = (root / "scripts/manage_formal_controller_container.sh").read_text(encoding="utf-8")

        self.assertIn("FROM mcr.microsoft.com/devcontainers/base@sha256:81380e4c9c14e8a629ff39029639e4b7893e67400246fa7782a0fe7dc193a02a", dockerfile)
        self.assertIn("/usr/bin/python3", dockerfile)
        self.assertIn("3.10.12", dockerfile)
        self.assertIn("/usr/bin/docker", dockerfile)
        self.assertIn("/usr/sbin/nft", dockerfile)
        self.assertNotIn("shell", wrapper.lower())

    def test_dockerfile_extracts_only_the_docker_cli_from_the_pinned_base(self):
        dockerfile = (
            Path(__file__).resolve().parents[1] / "infra/formal_controller/Dockerfile"
        ).read_text(encoding="utf-8")
        pinned = (
            "mcr.microsoft.com/devcontainers/base@sha256:"
            "81380e4c9c14e8a629ff39029639e4b7893e67400246fa7782a0fe7dc193a02a"
        )

        self.assertEqual(dockerfile.count(f"FROM {pinned}"), 2)
        self.assertIn(f"FROM {pinned} AS docker-cli-extractor", dockerfile)
        self.assertIn("apt-get install -y --no-install-recommends docker.io", dockerfile)
        self.assertIn(
            "COPY --from=docker-cli-extractor /usr/bin/docker /usr/bin/docker",
            dockerfile,
        )
        self.assertIn("test ! -e /usr/bin/dockerd", dockerfile)
        self.assertIn("test ! -e /usr/sbin/dockerd", dockerfile)

    def test_wrapper_resolves_with_builtin_cd_and_sanitizes_environment(self):
        source_wrapper = (
            Path(__file__).resolve().parents[1]
            / "scripts/manage_formal_controller_container.sh"
        )
        with tempfile.TemporaryDirectory() as tmp:
            fixture = Path(tmp).resolve()
            scripts = fixture / "scripts"
            source = fixture / "src"
            scripts.mkdir()
            source.mkdir()
            wrapper = scripts / source_wrapper.name
            wrapper.write_bytes(source_wrapper.read_bytes())
            boundary = source / "txnmem_formal_controller_container.py"
            boundary.write_text(
                "import json, os, sys\n"
                "print(json.dumps({'argv': sys.argv, 'danger': os.environ.get('TXNMEM_DANGEROUS')}))\n",
                encoding="utf-8",
            )
            environment = dict(os.environ)
            environment["TXNMEM_DANGEROUS"] = "must-not-cross"

            result = subprocess.run(
                ["/bin/bash", str(wrapper), "build"],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        observed = json.loads(result.stdout)
        self.assertEqual(observed["argv"], [str(boundary), "build"])
        self.assertIsNone(observed["danger"])

    def test_repository_must_be_an_exact_clean_narrow_git_top_level(self):
        repository, wheels = self._paths()
        calls = []
        invalid_repositories = [Path("/usr"), Path("/private"), repository / "configs"]
        dirty = repository / "untracked"
        dirty.write_text("dirty\n", encoding="utf-8")
        invalid_repositories.append(repository)

        for invalid in invalid_repositories:
            with self.subTest(repository=invalid):
                with self.assertRaises(container.ControllerContainerError):
                    container.create_controller(
                        invalid,
                        wheels,
                        "txnmem-formal-controller",
                        "txnmem-formal-state",
                        "txnmem-formal-controller:approved",
                        lifecycle_token="a" * 64,
                        run=lambda argv: calls.append(argv),
                    )

        self.assertEqual(calls, [])

    def test_wheel_source_must_exactly_match_the_repository_runtime_lock(self):
        mutations = ("extra", "missing", "digest", "symlink", "directory")

        for mutation in mutations:
            with self.subTest(mutation=mutation):
                repository, wheels = self._paths()
                first = wheels / "neo4j-fixture.whl"
                if mutation == "extra":
                    (wheels / "extra.whl").write_bytes(b"extra\n")
                elif mutation == "missing":
                    first.unlink()
                elif mutation == "digest":
                    first.write_bytes(b"replacement\n")
                elif mutation == "symlink":
                    first.unlink()
                    first.symlink_to(wheels / "pytz-fixture.whl")
                else:
                    (wheels / "nested").mkdir()
                calls = []

                with self.assertRaises(container.ControllerContainerError):
                    container.create_controller(
                        repository,
                        wheels,
                        "txnmem-formal-controller",
                        "txnmem-formal-state",
                        "txnmem-formal-controller:approved",
                        lifecycle_token="a" * 64,
                        run=lambda argv: calls.append(argv),
                    )

                self.assertEqual(calls, [])

    def test_failed_create_never_deletes_a_concurrent_container_claimant(self):
        repository, wheels = self._paths()
        token = "b" * 64
        image_id = "sha256:" + "7" * 64
        calls = []
        volume_created = False

        def run(argv):
            nonlocal volume_created
            calls.append(argv)
            if argv[1:3] == ["image", "inspect"]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([{"Id": image_id}])
                )
            if argv[:3] == ["/usr/bin/docker", "container", "inspect"]:
                return subprocess.CompletedProcess(argv, 1, stdout="")
            if argv[:3] == ["/usr/bin/docker", "volume", "inspect"]:
                if not volume_created:
                    return subprocess.CompletedProcess(argv, 1, stdout="")
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps(
                        [
                            {
                                "Name": "txnmem-formal-state",
                                "Labels": {
                                    "com.txnmem.formal.lifecycle": token,
                                    "com.txnmem.formal.role": "controller-state",
                                },
                            }
                        ]
                    ),
                )
            if argv[1:3] == ["volume", "create"]:
                volume_created = True
                return subprocess.CompletedProcess(
                    argv, 0, stdout="txnmem-formal-state\n"
                )
            if argv[1] == "create":
                return subprocess.CompletedProcess(argv, 1, stdout="")
            return subprocess.CompletedProcess(argv, 0, stdout="")

        with self.assertRaisesRegex(container.ControllerContainerError, "create"):
            container.create_controller(
                repository,
                wheels,
                "txnmem-formal-controller",
                "txnmem-formal-state",
                "txnmem-formal-controller:approved",
                lifecycle_token=token,
                run=run,
            )

        self.assertNotIn(
            ["/usr/bin/docker", "rm", "-f", "txnmem-formal-controller"], calls
        )
        self.assertIn(
            ["/usr/bin/docker", "volume", "rm", "txnmem-formal-state"], calls
        )

    def test_successful_create_proves_labeled_resource_identity_once(self):
        repository, wheels = self._paths()
        token = "c" * 64
        container_id = "d" * 64
        image_id = "sha256:" + "8" * 64
        calls = []
        volume_created = False

        def run(argv):
            nonlocal volume_created
            calls.append(argv)
            if argv[1:3] == ["image", "inspect"]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([{"Id": image_id}])
                )
            if argv == [
                "/usr/bin/docker",
                "container",
                "inspect",
                "txnmem-formal-controller",
            ]:
                return subprocess.CompletedProcess(argv, 1, stdout="")
            if argv == [
                "/usr/bin/docker", "volume", "inspect", "txnmem-formal-state"
            ]:
                if not volume_created:
                    return subprocess.CompletedProcess(argv, 1, stdout="")
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps(
                        [
                            {
                                "Name": "txnmem-formal-state",
                                "Labels": {
                                    "com.txnmem.formal.lifecycle": token,
                                    "com.txnmem.formal.role": "controller-state",
                                },
                            }
                        ]
                    ),
                )
            if argv[1:3] == ["volume", "create"]:
                volume_created = True
                return subprocess.CompletedProcess(
                    argv, 0, stdout="txnmem-formal-state\n"
                )
            if argv[1] == "create":
                return subprocess.CompletedProcess(argv, 0, stdout=container_id + "\n")
            if argv == [
                "/usr/bin/docker", "container", "inspect", container_id
            ]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps(
                        [
                            {
                                "Id": container_id,
                                "Name": "/txnmem-formal-controller",
                                "Image": image_id,
                                "Config": {
                                    "Image": image_id,
                                    "Labels": {
                                        "com.txnmem.formal.lifecycle": token,
                                        "com.txnmem.formal.role": "controller",
                                        "com.txnmem.formal.image-id": image_id,
                                    }
                                },
                            }
                        ]
                    ),
                )
            if argv == ["/usr/bin/docker", "start", container_id]:
                return subprocess.CompletedProcess(argv, 0, stdout=container_id + "\n")
            return subprocess.CompletedProcess(argv, 125, stdout="")

        container.create_controller(
            repository,
            wheels,
            "txnmem-formal-controller",
            "txnmem-formal-state",
            "txnmem-formal-controller:approved",
            lifecycle_token=token,
            run=run,
        )

        self.assertEqual(sum(call[1] == "create" for call in calls), 1)
        self.assertNotIn(["/usr/bin/docker", "rm", "-f", container_id], calls)
        self.assertNotIn(
            ["/usr/bin/docker", "volume", "rm", "txnmem-formal-state"], calls
        )

    def test_cleanup_return_failures_are_bounded_and_preserve_lifecycle_failure(self):
        repository, wheels = self._paths()
        token = "f" * 64
        container_id = "1" * 64
        run, calls = self._start_failure_runner(token, container_id)

        with self.assertRaisesRegex(
            container.ControllerContainerError,
            "^controller lifecycle cleanup failed$",
        ) as caught:
            container.create_controller(
                repository,
                wheels,
                "txnmem-formal-controller",
                "txnmem-formal-state",
                "txnmem-formal-controller:approved",
                lifecycle_token=token,
                run=run,
            )

        self.assertEqual(type(caught.exception).__name__, "ControllerContainerCleanupError")
        self.assertIsInstance(caught.exception.__cause__, container.ControllerContainerError)
        self.assertEqual(str(caught.exception.__cause__), "Docker start failed")
        self.assertIn(["/usr/bin/docker", "rm", "-f", container_id], calls)
        self.assertIn(["/usr/bin/docker", "volume", "rm", "txnmem-formal-state"], calls)
        self.assertNotIn("sensitive", str(caught.exception))

    def test_cleanup_invocation_error_does_not_skip_the_other_owned_target(self):
        repository, wheels = self._paths()
        token = "2" * 64
        container_id = "3" * 64
        run, calls = self._start_failure_runner(
            token, container_id, cleanup_raises=True
        )

        with self.assertRaisesRegex(
            container.ControllerContainerError,
            "^controller lifecycle cleanup failed$",
        ):
            container.create_controller(
                repository,
                wheels,
                "txnmem-formal-controller",
                "txnmem-formal-state",
                "txnmem-formal-controller:approved",
                lifecycle_token=token,
                run=run,
            )

        self.assertIn(["/usr/bin/docker", "rm", "-f", container_id], calls)
        self.assertIn(["/usr/bin/docker", "volume", "rm", "txnmem-formal-state"], calls)

    def test_install_rejects_same_name_container_with_wrong_lifecycle_identity(self):
        repository, wheels = self._paths()
        token = "4" * 64
        container_id = "5" * 64
        image_id = "sha256:" + "6" * 64
        document = self._controller_inspect_document(
            repository, wheels, "7" * 64, container_id, image_id
        )
        calls = []

        def run(argv):
            calls.append(argv)
            return subprocess.CompletedProcess(
                argv, 0, stdout=json.dumps([document])
            )

        with self.assertRaises(container.ControllerContainerError):
            container.install_controller(
                "txnmem-formal-controller",
                "a" * 40,
                lifecycle_token=token,
                run=run,
            )

        self.assertFalse(any(call[1] == "exec" for call in calls))

    def test_install_rejects_every_security_and_mount_identity_mismatch(self):
        repository, wheels = self._paths()
        token = "b" * 64
        container_id = "c" * 64
        base = self._controller_inspect_document(
            repository, wheels, token, container_id, "sha256:" + "d" * 64
        )
        cases = {
            "stopped": lambda value: value["State"].update(Running=False),
            "image": lambda value: value.update(Image="mutable:tag"),
            "uid": lambda value: value["Config"].update(User="0"),
            "network": lambda value: value["HostConfig"].update(NetworkMode="bridge"),
            "pid": lambda value: value["HostConfig"].update(PidMode=""),
            "cap-drop": lambda value: value["HostConfig"].update(CapDrop=[]),
            "cap-add": lambda value: value["HostConfig"].update(CapAdd=["NET_ADMIN"]),
            "malformed-cap-add": lambda value: value["HostConfig"].update(CapAdd=[{}]),
            "security-opt": lambda value: value["HostConfig"].update(SecurityOpt=[]),
            "privileged": lambda value: value["HostConfig"].update(Privileged=True),
            "config-image": lambda value: value["Config"].update(
                Image="sha256:" + "4" * 64
            ),
            "image-label": lambda value: value["Config"]["Labels"].update(
                {"com.txnmem.formal.image-id": "sha256:" + "5" * 64}
            ),
            "repo-rw": lambda value: value["Mounts"][0].update(RW=True),
            "extra-mount": lambda value: value["Mounts"].append(
                {
                    "Type": "bind", "Source": "/home", "Destination": "/unexpected",
                    "Mode": "ro", "RW": False,
                }
            ),
        }

        for name, mutate in cases.items():
            with self.subTest(name=name):
                document = copy.deepcopy(base)
                mutate(document)
                calls = []

                def run(argv):
                    calls.append(argv)
                    return subprocess.CompletedProcess(
                        argv, 0, stdout=json.dumps([document])
                    )

                with self.assertRaises(container.ControllerContainerError):
                    container.install_controller(
                        "txnmem-formal-controller",
                        "e" * 40,
                        lifecycle_token=token,
                        run=run,
                    )

                self.assertFalse(any(call[1] == "exec" for call in calls))

    def test_install_rejects_mismatched_state_volume_ownership(self):
        repository, wheels = self._paths()
        token = "e" * 64
        container_id = "f" * 64
        document = self._controller_inspect_document(
            repository, wheels, token, container_id, "sha256:" + "1" * 64
        )
        calls = []

        def run(argv):
            calls.append(argv)
            if argv[1:3] == ["container", "inspect"]:
                payload = [document]
            else:
                payload = [{"Name": "txnmem-formal-state", "Labels": {
                    "com.txnmem.formal.lifecycle": "2" * 64,
                    "com.txnmem.formal.role": "controller-state",
                }}]
            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(payload))

        with self.assertRaises(container.ControllerContainerError):
            container.install_controller(
                "txnmem-formal-controller",
                "3" * 40,
                lifecycle_token=token,
                run=run,
            )

        self.assertFalse(any(call[1] == "exec" for call in calls))

    def test_create_resolves_mutable_tag_to_one_labeled_exact_image_id(self):
        repository, wheels = self._paths()
        token = "4" * 64
        container_id = "5" * 64
        image_id = "sha256:" + "6" * 64
        calls = []
        volume_created = False

        def run(argv):
            nonlocal volume_created
            calls.append(argv)
            if argv == [
                "/usr/bin/docker", "image", "inspect", "txnmem-formal-controller:approved"
            ]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([{"Id": image_id}])
                )
            if argv == [
                "/usr/bin/docker", "container", "inspect", "txnmem-formal-controller"
            ]:
                return subprocess.CompletedProcess(argv, 1, stdout="")
            if argv == [
                "/usr/bin/docker", "volume", "inspect", "txnmem-formal-state"
            ]:
                if not volume_created:
                    return subprocess.CompletedProcess(argv, 1, stdout="")
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps([{"Name": "txnmem-formal-state", "Labels": {
                        "com.txnmem.formal.lifecycle": token,
                        "com.txnmem.formal.role": "controller-state",
                    }}]),
                )
            if argv[1:3] == ["volume", "create"]:
                volume_created = True
                return subprocess.CompletedProcess(argv, 0, stdout="txnmem-formal-state\n")
            if argv[1] == "create":
                return subprocess.CompletedProcess(argv, 0, stdout=container_id + "\n")
            if argv == ["/usr/bin/docker", "container", "inspect", container_id]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps([{
                        "Id": container_id,
                        "Name": "/txnmem-formal-controller",
                        "Image": image_id,
                        "Config": {"Image": image_id, "Labels": {
                            "com.txnmem.formal.lifecycle": token,
                            "com.txnmem.formal.role": "controller",
                            "com.txnmem.formal.image-id": image_id,
                            "org.opencontainers.image.ref.name": "ubuntu",
                        }},
                    }]),
                )
            return subprocess.CompletedProcess(argv, 0, stdout="")

        container.create_controller(
            repository,
            wheels,
            "txnmem-formal-controller",
            "txnmem-formal-state",
            "txnmem-formal-controller:approved",
            lifecycle_token=token,
            run=run,
        )

        create_argv = next(call for call in calls if call[1] == "create")
        self.assertEqual(calls[0][1:3], ["image", "inspect"])
        self.assertIn(image_id, create_argv)
        self.assertIn(f"com.txnmem.formal.image-id={image_id}", create_argv)
        self.assertNotIn("txnmem-formal-controller:approved", create_argv)

    def test_image_inspect_failure_ambiguity_or_malformed_id_precedes_mutation(self):
        cases = (
            (125, ""),
            (0, "[]"),
            (0, json.dumps([{"Id": "sha256:" + "1" * 64}, {"Id": "sha256:" + "2" * 64}])),
            (0, json.dumps([{"Id": "sha256:short"}])),
        )
        for returncode, stdout in cases:
            with self.subTest(returncode=returncode, stdout=stdout):
                repository, wheels = self._paths()
                calls = []

                def run(argv):
                    calls.append(argv)
                    return subprocess.CompletedProcess(
                        argv, returncode, stdout=stdout
                    )

                with self.assertRaises(container.ControllerContainerError):
                    container.create_controller(
                        repository,
                        wheels,
                        "txnmem-formal-controller",
                        "txnmem-formal-state",
                        "txnmem-formal-controller:approved",
                        lifecycle_token="3" * 64,
                        run=run,
                    )

                self.assertEqual(
                    calls,
                    [[
                        "/usr/bin/docker", "image", "inspect",
                        "txnmem-formal-controller:approved",
                    ]],
                )

    def test_concurrent_volume_claimant_is_never_removed_without_label_proof(self):
        repository, wheels = self._paths()
        token = "7" * 64
        image_id = "sha256:" + "8" * 64
        calls = []

        def run(argv):
            calls.append(argv)
            if argv[1:3] == ["image", "inspect"]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([{"Id": image_id}])
                )
            if argv[1:3] in (["container", "inspect"], ["volume", "inspect"]):
                if len([call for call in calls if call[1:3] == ["volume", "inspect"]]) == 1 and argv[1] == "volume":
                    return subprocess.CompletedProcess(argv, 1, stdout="")
                if argv[1] == "container":
                    return subprocess.CompletedProcess(argv, 1, stdout="")
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps([{"Name": "txnmem-formal-state", "Labels": {
                        "com.txnmem.formal.lifecycle": "9" * 64,
                        "com.txnmem.formal.role": "controller-state",
                    }}]),
                )
            if argv[1:3] == ["volume", "create"]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout="txnmem-formal-state\n"
                )
            return subprocess.CompletedProcess(argv, 125, stdout="")

        with self.assertRaisesRegex(container.ControllerContainerError, "ownership"):
            container.create_controller(
                repository,
                wheels,
                "txnmem-formal-controller",
                "txnmem-formal-state",
                "txnmem-formal-controller:approved",
                lifecycle_token=token,
                run=run,
            )

        self.assertNotIn(
            ["/usr/bin/docker", "volume", "rm", "txnmem-formal-state"], calls
        )
        self.assertFalse(any(call[1] == "create" for call in calls))
