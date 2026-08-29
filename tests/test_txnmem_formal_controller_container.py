import copy
import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import txnmem_formal_controller_container as container


class FormalControllerContainerTests(unittest.TestCase):
    def test_direct_module_docker_calls_use_one_local_sanitized_environment(self):
        hostile = {
            "DOCKER_HOST": "tcp://attacker.invalid:2375",
            "DOCKER_CONTEXT": "attacker-context",
            "DOCKER_CONFIG": "/tmp/attacker-config",
            "DOCKER_CERT_PATH": "/tmp/attacker-certs",
            "DOCKER_TLS_VERIFY": "1",
            "HOME": "/tmp/attacker-home",
        }
        completed = subprocess.CompletedProcess(
            ["/usr/bin/docker", "version"], 0, stdout=""
        )
        with mock.patch.dict(os.environ, hostile, clear=False):
            with mock.patch.object(
                container.subprocess, "run", return_value=completed
            ) as run:
                observed = container._run(["/usr/bin/docker", "version"])

        self.assertIs(observed, completed)
        self.assertEqual(
            run.call_args.kwargs["env"],
            {
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "HOME": "/nonexistent",
                "DOCKER_HOST": "unix:///var/run/docker.sock",
                "DOCKER_CONFIG": "/var/empty/txnmem-formal-docker-config",
            },
        )
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
    def _anonymous_state_mount(*, name="d" * 64, mode="z"):
        return {
            "Type": "volume",
            "Name": name,
            "Driver": "local",
            "Source": f"/var/lib/docker/volumes/{name}/_data",
            "Destination": "/var/lib/txnmem-formal",
            "Mode": mode,
            "RW": True,
        }

    @staticmethod
    def _anonymous_state_request(*, source_missing=True):
        request = {
            "Type": "volume",
            "Target": "/var/lib/txnmem-formal",
            "ReadOnly": False,
        }
        if not source_missing:
            request["Source"] = "d" * 64
        return request

    @staticmethod
    def _default_masked_paths():
        return [
            "/proc/acpi",
            "/proc/asound",
            "/proc/interrupts",
            "/proc/kcore",
            "/proc/keys",
            "/proc/latency_stats",
            "/proc/sched_debug",
            "/proc/scsi",
            "/proc/timer_list",
            "/proc/timer_stats",
            "/sys/devices/virtual/powercap",
            "/sys/firmware",
        ]

    @staticmethod
    def _default_readonly_paths():
        return [
            "/proc/bus",
            "/proc/fs",
            "/proc/irq",
            "/proc/sys",
            "/proc/sysrq-trigger",
        ]

    @staticmethod
    def _owned_identity_document(
        token, container_id, image_id, *, volume_name="d" * 64
    ):
        return {
            "Id": container_id,
            "Name": "/txnmem-formal-controller",
            "Image": image_id,
            "Config": {
                "Image": image_id,
                "Labels": {
                    "com.txnmem.formal.lifecycle": token,
                    "com.txnmem.formal.role": "controller",
                    "com.txnmem.formal.image-id": image_id,
                },
            },
            "HostConfig": {
                "VolumeDriver": "",
                "Mounts": [
                    FormalControllerContainerTests._anonymous_state_request()
                ],
            },
            "Mounts": [
                FormalControllerContainerTests._anonymous_state_mount(
                    name=volume_name
                )
            ],
        }

    @staticmethod
    def _start_failure_runner(token, container_id, *, cleanup_raises=False):
        calls = []
        image_id = "sha256:" + "a" * 64

        def run(argv):
            calls.append(argv)
            if argv[1:3] == ["image", "inspect"]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([{"Id": image_id}])
                )
            if argv[1:3] == ["container", "ls"]:
                return subprocess.CompletedProcess(argv, 0, stdout="")
            if argv == ["/usr/bin/docker", "container", "inspect", "txnmem-formal-controller"]:
                return subprocess.CompletedProcess(argv, 1, stdout="")
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
                        }},
                        "HostConfig": {
                            "VolumeDriver": "",
                            "Mounts": [
                                FormalControllerContainerTests._anonymous_state_request()
                            ],
                        },
                        "Mounts": [
                            FormalControllerContainerTests._anonymous_state_mount()
                        ],
                    }]),
                )
            if argv == ["/usr/bin/docker", "start", container_id]:
                return subprocess.CompletedProcess(argv, 42, stdout="sensitive-start-output")
            if argv == ["/usr/bin/docker", "rm", "-f", "-v", container_id]:
                if cleanup_raises:
                    raise OSError("sensitive-container-cleanup-output")
                return subprocess.CompletedProcess(argv, 51, stdout="sensitive-container-cleanup-output")
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
                "IpcMode": "private",
                "UTSMode": "",
                "UsernsMode": "",
                "CgroupnsMode": "private",
                "Runtime": "runc",
                "Binds": None,
                "Devices": [],
                "DeviceRequests": None,
                "DeviceCgroupRules": None,
                "GroupAdd": None,
                "VolumesFrom": None,
                "Sysctls": None,
                "Tmpfs": None,
                "MaskedPaths": FormalControllerContainerTests._default_masked_paths(),
                "ReadonlyPaths": FormalControllerContainerTests._default_readonly_paths(),
                "CapDrop": ["ALL"],
                "CapAdd": [
                    "CHOWN",
                    "DAC_OVERRIDE",
                    "FOWNER",
                    "KILL",
                    "NET_ADMIN",
                    "SETGID",
                    "SETUID",
                    "SYS_PTRACE",
                ],
                "SecurityOpt": ["no-new-privileges"],
                "Privileged": False,
                "VolumeDriver": "",
                "Mounts": [
                    {
                        "Type": "bind",
                        "Source": str(repository),
                        "Target": "/workspace/txnmem",
                        "ReadOnly": True,
                    },
                    {
                        "Type": "bind",
                        "Source": str(wheels),
                        "Target": "/opt/txnmem-formal-wheel-source",
                        "ReadOnly": True,
                    },
                    {
                        "Type": "bind",
                        "Source": "/var/run/docker.sock",
                        "Target": "/var/run/docker.sock",
                        "ReadOnly": False,
                    },
                    {
                        "Type": "volume",
                        "Source": "",
                        "Target": "/var/lib/txnmem-formal",
                        "ReadOnly": False,
                    },
                ],
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
                FormalControllerContainerTests._anonymous_state_mount(mode=""),
            ],
        }

    def test_create_argv_is_the_complete_bounded_controller_contract(self):
        repository, wheels = self._paths()

        argv = container.build_create_argv(
            repository, wheels, "txnmem-formal-controller",
            "sha256:" + "b" * 64, "a" * 64,
        )

        self.assertEqual(
            argv,
            [
                "/usr/bin/docker", "create", "--name", "txnmem-formal-controller",
                "--user", "0:0", "--network", "host", "--pid", "host",
                "--ipc", "private", "--cgroupns", "private",
                "--runtime", "runc",
                "--cap-drop", "ALL", "--cap-add", "CHOWN", "--cap-add",
                "DAC_OVERRIDE", "--cap-add", "FOWNER", "--cap-add", "KILL",
                "--cap-add", "NET_ADMIN", "--cap-add", "SETGID", "--cap-add",
                "SETUID", "--cap-add", "SYS_PTRACE", "--security-opt",
                "no-new-privileges=true",
                "--label", f"com.txnmem.formal.lifecycle={'a' * 64}",
                "--label", "com.txnmem.formal.role=controller",
                "--label", f"com.txnmem.formal.image-id=sha256:{'b' * 64}",
                "--mount", f"type=bind,src={repository},dst=/workspace/txnmem,readonly",
                "--mount", f"type=bind,src={wheels},dst=/opt/txnmem-formal-wheel-source,readonly",
                "--mount", "type=volume,dst=/var/lib/txnmem-formal",
                "--mount", "type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock",
                "sha256:" + "b" * 64, "/bin/sh", "-c", "exec sleep infinity",
            ],
        )
        self.assertNotIn("--privileged", argv)
        self.assertNotIn("NEO4J_PASSWORD", " ".join(argv))

    def test_create_argv_uses_one_anonymous_state_volume(self):
        repository, wheels = self._paths()

        argv = container.build_create_argv(
            repository,
            wheels,
            "txnmem-formal-controller",
            "sha256:" + "b" * 64,
            "a" * 64,
        )

        mounts = [
            argv[index + 1]
            for index, argument in enumerate(argv[:-1])
            if argument == "--mount"
        ]
        state_mounts = [
            value for value in mounts if "dst=/var/lib/txnmem-formal" in value
        ]
        self.assertEqual(
            state_mounts,
            ["type=volume,dst=/var/lib/txnmem-formal"],
        )
        self.assertNotIn("src=", state_mounts[0])

    def test_create_checks_nonreuse_then_cleans_up_a_partial_lifecycle(self):
        repository, wheels = self._paths()
        token = "a" * 64
        container_id = "e" * 64
        image_id = "sha256:" + "f" * 64
        calls = []
        controller_created = False

        def run(argv):
            nonlocal controller_created
            calls.append(argv)
            if argv[1:3] == ["image", "inspect"]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([{"Id": image_id}])
                )
            if argv[1:3] == ["container", "ls"]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout="txnmem-formal-controller\n" if controller_created else "",
                )
            if argv == ["/usr/bin/docker", "container", "inspect", "txnmem-formal-controller"]:
                return subprocess.CompletedProcess(argv, 1, stdout="")
            if argv[1] == "create":
                controller_created = True
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
                        }},
                        "HostConfig": {
                            "VolumeDriver": "",
                            "Mounts": [self._anonymous_state_request()],
                        },
                        "Mounts": [self._anonymous_state_mount()],
                    }]),
                )
            if argv == ["/usr/bin/docker", "start", container_id]:
                return subprocess.CompletedProcess(argv, 42, stdout="")
            if argv == ["/usr/bin/docker", "rm", "-f", "-v", container_id]:
                controller_created = False
                return subprocess.CompletedProcess(argv, 0, stdout=container_id + "\n")
            return subprocess.CompletedProcess(argv, 0, stdout="")

        with self.assertRaisesRegex(container.ControllerContainerError, "start"):
            container.create_controller(
                repository, wheels, "txnmem-formal-controller",
                "txnmem-formal-controller:approved", lifecycle_token=token, run=run,
            )

        self.assertEqual(calls[0], ["/usr/bin/docker", "image", "inspect", "txnmem-formal-controller:approved"])
        self.assertEqual(
            calls[1],
            [
                "/usr/bin/docker", "container", "ls", "--all", "--format",
                "{{.Names}}",
            ],
        )
        self.assertIn(["/usr/bin/docker", "start", container_id], calls)
        self.assertIn(["/usr/bin/docker", "rm", "-f", "-v", container_id], calls)

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
                [
                "/usr/bin/docker", "exec", "--user", "0:0", "--env",
                "TXNMEM_FORMAL_WHEEL_SOURCE=/opt/txnmem-formal-wheel-source",
                container_id, "/bin/bash",
                "/workspace/txnmem/scripts/install_formal_provenance_runtime.sh",
                "/workspace/txnmem", commit,
                ],
            ],
        )

    def test_install_accepts_equivalent_real_docker_mount_and_security_forms(self):
        repository, wheels = self._paths()
        token = "7" * 64
        container_id = "8" * 64
        document = self._controller_inspect_document(
            repository, wheels, token, container_id, "sha256:" + "9" * 64
        )
        document["HostConfig"]["SecurityOpt"] = ["no-new-privileges=true"]
        document["HostConfig"]["CapAdd"] = [
            "CAP_" + value for value in document["HostConfig"]["CapAdd"]
        ]
        document["HostConfig"]["CapDrop"] = ["CAP_ALL"]
        document["HostConfig"].update(
            {
                "Binds": None,
                "Devices": [],
                "DeviceRequests": None,
                "GroupAdd": None,
                "IpcMode": "private",
                "UTSMode": "",
                "UsernsMode": "",
                "CgroupnsMode": "private",
            }
        )
        document["HostConfig"]["MaskedPaths"].append(
            "/sys/devices/system/cpu/cpu0/thermal_throttle"
        )
        document["HostConfig"].pop("VolumeDriver")
        document["Mounts"][0]["Mode"] = ""
        document["Mounts"][1]["Mode"] = ""
        document["Mounts"][2]["Mode"] = "rw"
        document["Mounts"][3]["Mode"] = "z"
        for mount in document["Mounts"][:3]:
            mount["Propagation"] = "rprivate"
        document["Mounts"][3]["Propagation"] = ""
        for mount in document["HostConfig"]["Mounts"][:3]:
            mount["BindOptions"] = {
                "Propagation": "rprivate",
                "NonRecursive": False,
                "CreateMountpoint": False,
                "ReadOnlyNonRecursive": False,
                "ReadOnlyForceRecursive": False,
            }
        document["HostConfig"]["Mounts"][2].pop("ReadOnly")
        document["HostConfig"]["Mounts"][3].pop("Source")
        document["HostConfig"]["Mounts"][3].pop("ReadOnly")
        calls = []

        def run(argv):
            calls.append(argv)
            if argv[1:3] == ["container", "inspect"]:
                payload = [document]
            else:
                return subprocess.CompletedProcess(argv, 0, stdout="")
            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(payload))

        container.install_controller(
            "txnmem-formal-controller",
            "a" * 40,
            lifecycle_token=token,
            run=run,
        )

        self.assertTrue(any(call[1] == "exec" for call in calls))

    def test_install_accepts_only_anonymous_state_volume_proof(self):
        repository, wheels = self._paths()
        token = "1" * 64
        container_id = "2" * 64
        document = self._controller_inspect_document(
            repository, wheels, token, container_id, "sha256:" + "3" * 64
        )
        calls = []

        def run(argv):
            calls.append(argv)
            if argv[1:3] == ["container", "inspect"]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([document])
                )
            return subprocess.CompletedProcess(argv, 0, stdout="")

        container.install_controller(
            "txnmem-formal-controller",
            "5" * 40,
            lifecycle_token=token,
            run=run,
        )

        self.assertTrue(any(call[1] == "exec" for call in calls))

    def test_invalid_inputs_fail_before_any_docker_mutation(self):
        repository, wheels = self._paths()
        calls = []
        bad_link = repository.parent / "repository-link"
        bad_link.symlink_to(repository, target_is_directory=True)

        for kwargs in (
            {"container_name": "bad/name"},
            {"image": "bad image"}, {"repository": Path("/")},
            {"wheel_source": bad_link}, {"lifecycle_token": "bad"},
        ):
            with self.subTest(kwargs=kwargs):
                values = {
                    "repository": repository, "wheel_source": wheels,
                    "container_name": "txnmem-formal-controller",
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
                repository, wheels, "txnmem-formal-controller",
                "txnmem-formal-controller:approved",
                lifecycle_token="a" * 64,
                run=lambda argv: calls.append(argv) or subprocess.CompletedProcess(argv, 125),
            )

        self.assertEqual(calls, [["/usr/bin/docker", "image", "inspect", "txnmem-formal-controller:approved"]])

    def test_absence_requires_a_successful_exact_name_inventory(self):
        repository, wheels = self._paths()
        image_id = "sha256:" + "1" * 64
        calls = []

        def run(argv):
            calls.append(argv)
            if argv[1:3] == ["image", "inspect"]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([{"Id": image_id}])
                )
            if argv[1:3] == ["container", "ls"]:
                return subprocess.CompletedProcess(
                    argv, 1, stdout="", stderr="daemon unavailable"
                )
            if argv[1:3] == ["container", "inspect"]:
                return subprocess.CompletedProcess(argv, 1, stdout="", stderr="")
            return subprocess.CompletedProcess(argv, 125, stdout="", stderr="")

        with self.assertRaisesRegex(container.ControllerContainerError, "inspect"):
            container.create_controller(
                repository,
                wheels,
                "txnmem-formal-controller",
                "txnmem-formal-controller:approved",
                lifecycle_token="2" * 64,
                run=run,
            )

        self.assertIn(
            [
                "/usr/bin/docker",
                "container",
                "ls",
                "--all",
                "--format",
                "{{.Names}}",
            ],
            calls,
        )
        self.assertFalse(any(call[1] == "create" for call in calls))

    def test_create_without_an_immutable_id_never_claims_or_deletes_by_name(self):
        repository, wheels = self._paths()
        token = "3" * 64
        container_id = "4" * 64
        image_id = "sha256:" + "5" * 64
        calls = []
        controller_created = False

        def run(argv):
            nonlocal controller_created
            calls.append(argv)
            if argv[1:3] == ["image", "inspect"]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([{"Id": image_id}])
                )
            if argv[1:3] == ["container", "ls"]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout="txnmem-formal-controller\n" if controller_created else "",
                )
            if argv[1] == "create":
                controller_created = True
                raise OSError("response lost after create")
            if argv[1:3] == ["container", "inspect"]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps([{
                        "Id": container_id,
                        "Name": "/txnmem-formal-controller",
                        "Image": image_id,
                        "Config": {
                            "Image": image_id,
                            "Labels": {
                                "com.txnmem.formal.lifecycle": token,
                                "com.txnmem.formal.role": "controller",
                                "com.txnmem.formal.image-id": image_id,
                            },
                        },
                        "HostConfig": {
                            "VolumeDriver": "",
                            "Mounts": [self._anonymous_state_request()],
                        },
                        "Mounts": [self._anonymous_state_mount()],
                    }]),
                )
            if argv == ["/usr/bin/docker", "rm", "-f", "-v", container_id]:
                controller_created = False
                return subprocess.CompletedProcess(argv, 0, stdout=container_id + "\n")
            if argv == ["/usr/bin/docker", "volume", "ls", "--quiet"]:
                return subprocess.CompletedProcess(argv, 0, stdout="")
            return subprocess.CompletedProcess(argv, 125, stdout="", stderr="")

        with self.assertRaisesRegex(
            container.ControllerContainerCleanupError,
            "^controller lifecycle cleanup failed$",
        ):
            container.create_controller(
                repository,
                wheels,
                "txnmem-formal-controller",
                "txnmem-formal-controller:approved",
                lifecycle_token=token,
                run=run,
            )

        self.assertFalse(
            any(call[1:3] == ["container", "inspect"] for call in calls)
        )
        self.assertFalse(any(call[1] == "rm" for call in calls))

    def test_controller_state_uses_anonymous_volume_without_named_volume_api(self):
        repository, wheels = self._paths()
        token = "6" * 64
        container_id = "8" * 64
        image_id = "sha256:" + "7" * 64
        calls = []

        def run(argv):
            calls.append(argv)
            if argv[1:3] == ["image", "inspect"]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([{"Id": image_id}])
                )
            if argv[1:3] == ["container", "ls"]:
                return subprocess.CompletedProcess(argv, 0, stdout="")
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
                        "Config": {
                            "Image": image_id,
                            "Labels": {
                                "com.txnmem.formal.lifecycle": token,
                                "com.txnmem.formal.role": "controller",
                                "com.txnmem.formal.image-id": image_id,
                            },
                        },
                        "HostConfig": {
                            "VolumeDriver": "",
                            "Mounts": [self._anonymous_state_request()],
                        },
                        "Mounts": [self._anonymous_state_mount()],
                    }]),
                )
            if argv == ["/usr/bin/docker", "start", container_id]:
                return subprocess.CompletedProcess(argv, 0, stdout=container_id + "\n")
            return subprocess.CompletedProcess(argv, 125, stdout="", stderr="")

        container.create_controller(
            repository,
            wheels,
            "txnmem-formal-controller",
            "txnmem-formal-controller:approved",
            lifecycle_token=token,
            run=run,
        )

        self.assertFalse(any(call[1] == "volume" for call in calls))
        create_argv = next(call for call in calls if call[1] == "create")
        self.assertIn("type=volume,dst=/var/lib/txnmem-formal", create_argv)
        self.assertNotIn("src=txnmem-formal", " ".join(create_argv))

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
        self.assertNotIn("$0", wrapper)
        self.assertNotIn("script_path", wrapper)
        self.assertIn('repository_root="$(command pwd -P)"', wrapper)
        self.assertIn(
            '"$repository_root/src/txnmem_formal_controller_container.py"',
            wrapper,
        )

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
            wrapper.chmod(0o755)
            boundary = source / "txnmem_formal_controller_container.py"
            boundary.write_text(
                "import json, os, sys\n"
                "print(json.dumps({'argv': sys.argv, 'danger': os.environ.get('TXNMEM_DANGEROUS')}))\n",
                encoding="utf-8",
            )
            environment = dict(os.environ)
            environment["TXNMEM_DANGEROUS"] = "must-not-cross"
            environment["TXNMEM_FORMAL_CONTAINER_WRAPPER_SANITIZED"] = "1"
            marker = fixture / "startup-environment-ran"
            startup = fixture / "startup.sh"
            startup.write_text(
                f"/usr/bin/touch {marker}\n", encoding="utf-8"
            )
            fake_bin = fixture / "fake-bin"
            fake_bin.mkdir()
            fake_bash = fake_bin / "bash"
            fake_bash.write_text(
                f"#!/bin/sh\n/usr/bin/touch {marker}\nexit 97\n",
                encoding="utf-8",
            )
            fake_bash.chmod(0o755)
            environment["PATH"] = str(fake_bin)
            environment["BASH_ENV"] = str(startup)
            environment["ENV"] = str(startup)

            result = subprocess.run(
                [str(wrapper), "build"],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
                cwd=fixture,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        observed = json.loads(result.stdout)
        self.assertEqual(observed["argv"], [str(boundary), "build"])
        self.assertIsNone(observed["danger"])
        self.assertFalse(marker.exists())
        self.assertNotIn(
            "TXNMEM_FORMAL_CONTAINER_WRAPPER_SANITIZED",
            source_wrapper.read_text(encoding="utf-8"),
        )

    def test_wrapper_ignores_a_retargeted_invocation_path_and_uses_the_worktree(self):
        source_wrapper = (
            Path(__file__).resolve().parents[1]
            / "scripts/manage_formal_controller_container.sh"
        )
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            trusted = root / "trusted"
            trusted_scripts = trusted / "scripts"
            trusted_source = trusted / "src"
            trusted_scripts.mkdir(parents=True)
            trusted_source.mkdir()
            wrapper = trusted_scripts / source_wrapper.name
            wrapper.write_bytes(source_wrapper.read_bytes())
            wrapper.chmod(0o755)
            trusted_boundary = trusted_source / "txnmem_formal_controller_container.py"
            trusted_boundary.write_text(
                "import json, sys\n"
                "print(json.dumps({'origin': 'trusted', 'argv': sys.argv}))\n",
                encoding="utf-8",
            )

            attacker = root / "attacker"
            attacker_scripts = attacker / "scripts"
            attacker_source = attacker / "src"
            attacker_scripts.mkdir(parents=True)
            attacker_source.mkdir()
            attacker_wrapper = attacker_scripts / source_wrapper.name
            attacker_wrapper.write_bytes(source_wrapper.read_bytes())
            attacker_wrapper.chmod(0o755)
            (attacker_source / "txnmem_formal_controller_container.py").write_text(
                "raise SystemExit('attacker-adjacent source executed')\n",
                encoding="utf-8",
            )
            redirect = root / "mutable-wrapper"
            redirect.symlink_to(attacker_wrapper)

            result = subprocess.run(
                [str(redirect), "build"],
                check=False,
                capture_output=True,
                text=True,
                env={**os.environ, "TXNMEM_DANGEROUS": "must-not-cross"},
                cwd=trusted,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        observed = json.loads(result.stdout)
        self.assertEqual(observed["origin"], "trusted")
        self.assertEqual(observed["argv"], [str(trusted_boundary), "build"])

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

        def run(argv):
            calls.append(argv)
            if argv[1:3] == ["image", "inspect"]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([{"Id": image_id}])
                )
            if argv[:3] == ["/usr/bin/docker", "container", "inspect"]:
                return subprocess.CompletedProcess(argv, 1, stdout="")
            if argv[1] == "create":
                return subprocess.CompletedProcess(argv, 1, stdout="")
            return subprocess.CompletedProcess(argv, 0, stdout="")

        with self.assertRaisesRegex(
            container.ControllerContainerCleanupError,
            "^controller lifecycle cleanup failed$",
        ):
            container.create_controller(
                repository,
                wheels,
                "txnmem-formal-controller",
                "txnmem-formal-controller:approved",
                lifecycle_token=token,
                run=run,
            )

        self.assertNotIn(
            ["/usr/bin/docker", "rm", "-f", "txnmem-formal-controller"], calls
        )
        self.assertFalse(any(call[1] == "rm" for call in calls))

    def test_successful_create_proves_labeled_resource_identity_once(self):
        repository, wheels = self._paths()
        token = "c" * 64
        container_id = "d" * 64
        image_id = "sha256:" + "8" * 64
        calls = []

        def run(argv):
            calls.append(argv)
            if argv[1:3] == ["image", "inspect"]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([{"Id": image_id}])
                )
            if argv[1:3] == ["container", "ls"]:
                return subprocess.CompletedProcess(argv, 0, stdout="")
            if argv == [
                "/usr/bin/docker",
                "container",
                "inspect",
                "txnmem-formal-controller",
            ]:
                return subprocess.CompletedProcess(argv, 1, stdout="")
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
                                "HostConfig": {
                                    "VolumeDriver": "",
                                    "Mounts": [self._anonymous_state_request()],
                                },
                                "Mounts": [self._anonymous_state_mount()],
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
            "txnmem-formal-controller:approved",
            lifecycle_token=token,
            run=run,
        )

        self.assertEqual(sum(call[1] == "create" for call in calls), 1)
        self.assertNotIn(["/usr/bin/docker", "rm", "-f", "-v", container_id], calls)

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
                "txnmem-formal-controller:approved",
                lifecycle_token=token,
                run=run,
            )

        self.assertEqual(type(caught.exception).__name__, "ControllerContainerCleanupError")
        self.assertIsInstance(caught.exception.__cause__, container.ControllerContainerError)
        self.assertEqual(str(caught.exception.__cause__), "Docker start failed")
        self.assertIn(["/usr/bin/docker", "rm", "-f", "-v", container_id], calls)
        self.assertNotIn("sensitive", str(caught.exception))

    def test_cleanup_removal_invocation_error_is_bounded(self):
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
                "txnmem-formal-controller:approved",
                lifecycle_token=token,
                run=run,
            )

        self.assertIn(["/usr/bin/docker", "rm", "-f", "-v", container_id], calls)

    def test_cleanup_identity_inspection_error_is_bounded_without_deletion(self):
        calls = []

        def run(argv):
            calls.append(argv)
            raise OSError("sensitive-inspection-output")

        with self.assertRaisesRegex(
            container.ControllerContainerCleanupError,
            "^controller lifecycle cleanup failed$",
        ) as caught:
            container._cleanup_owned_resources(
                run,
                container_id="4" * 64,
                container_name="txnmem-formal-controller",
                lifecycle_token="5" * 64,
                image_id="sha256:" + "6" * 64,
            )

        self.assertEqual(
            calls,
            [["/usr/bin/docker", "container", "inspect", "4" * 64]],
        )
        self.assertNotIn("sensitive", str(caught.exception))

    def test_cleanup_success_requires_post_delete_absence_proof(self):
        token = "a" * 64
        container_id = "b" * 64
        image_id = "sha256:" + "c" * 64
        volume_name = "d" * 64
        calls = []

        def run(argv):
            calls.append(argv)
            if argv == ["/usr/bin/docker", "container", "inspect", container_id]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps([{
                        "Id": container_id,
                        "Name": "/txnmem-formal-controller",
                        "Image": image_id,
                        "Config": {
                            "Image": image_id,
                            "Labels": {
                                "com.txnmem.formal.lifecycle": token,
                                "com.txnmem.formal.role": "controller",
                                "com.txnmem.formal.image-id": image_id,
                            },
                        },
                        "HostConfig": {
                            "VolumeDriver": "",
                            "Mounts": [self._anonymous_state_request()],
                        },
                        "Mounts": [self._anonymous_state_mount(name=volume_name)],
                    }]),
                )
            if argv == ["/usr/bin/docker", "rm", "-f", "-v", container_id]:
                return subprocess.CompletedProcess(argv, 0, stdout="removed\n")
            if argv == [
                "/usr/bin/docker", "container", "ls", "--all", "--no-trunc",
                "--format", "{{.ID}}",
            ]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=container_id + "\n"
                )
            if argv == [
                "/usr/bin/docker", "container", "ls", "--all", "--format",
                "{{.Names}}",
            ]:
                return subprocess.CompletedProcess(argv, 0, stdout="")
            if argv == ["/usr/bin/docker", "volume", "ls", "--quiet"]:
                return subprocess.CompletedProcess(argv, 0, stdout="")
            return subprocess.CompletedProcess(argv, 125, stdout="", stderr="")

        with self.assertRaisesRegex(
            container.ControllerContainerCleanupError,
            "^controller lifecycle cleanup failed$",
        ):
            container._cleanup_owned_resources(
                run,
                container_id=container_id,
                container_name="txnmem-formal-controller",
                lifecycle_token=token,
                image_id=image_id,
            )

        self.assertIn(
            [
                "/usr/bin/docker", "container", "ls", "--all", "--no-trunc",
                "--format", "{{.ID}}",
            ],
            calls,
        )

    def test_cleanup_fails_closed_when_anonymous_volume_survives(self):
        token = "1" * 64
        container_id = "2" * 64
        image_id = "sha256:" + "3" * 64
        volume_name = "4" * 64
        calls = []

        def run(argv):
            calls.append(argv)
            if argv == ["/usr/bin/docker", "container", "inspect", container_id]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps([{
                        "Id": container_id,
                        "Name": "/txnmem-formal-controller",
                        "Image": image_id,
                        "Config": {
                            "Image": image_id,
                            "Labels": {
                                "com.txnmem.formal.lifecycle": token,
                                "com.txnmem.formal.role": "controller",
                                "com.txnmem.formal.image-id": image_id,
                            },
                        },
                        "HostConfig": {
                            "VolumeDriver": "",
                            "Mounts": [self._anonymous_state_request()],
                        },
                        "Mounts": [self._anonymous_state_mount(name=volume_name)],
                    }]),
                )
            if argv == ["/usr/bin/docker", "rm", "-f", "-v", container_id]:
                return subprocess.CompletedProcess(argv, 0, stdout=container_id + "\n")
            if argv == [
                "/usr/bin/docker", "container", "ls", "--all", "--no-trunc",
                "--format", "{{.ID}}",
            ]:
                return subprocess.CompletedProcess(argv, 0, stdout="")
            if argv == [
                "/usr/bin/docker", "container", "ls", "--all", "--format",
                "{{.Names}}",
            ]:
                return subprocess.CompletedProcess(argv, 0, stdout="")
            if argv == ["/usr/bin/docker", "volume", "ls", "--quiet"]:
                return subprocess.CompletedProcess(argv, 0, stdout=volume_name + "\n")
            return subprocess.CompletedProcess(argv, 125, stdout="", stderr="")

        with self.assertRaisesRegex(
            container.ControllerContainerCleanupError,
            "^controller lifecycle cleanup failed$",
        ):
            container._cleanup_owned_resources(
                run,
                container_id=container_id,
                container_name="txnmem-formal-controller",
                lifecycle_token=token,
                image_id=image_id,
            )

        self.assertIn(
            ["/usr/bin/docker", "volume", "ls", "--quiet"], calls
        )
        self.assertFalse(
            any(call[1:3] == ["volume", "rm"] for call in calls)
        )

    def test_cleanup_rejects_an_unproven_extra_anonymous_volume(self):
        token = "5" * 64
        container_id = "6" * 64
        image_id = "sha256:" + "7" * 64
        calls = []
        extra_mount = self._anonymous_state_mount(name="9" * 64)
        extra_mount["Destination"] = "/unproven-extra-volume"

        def run(argv):
            calls.append(argv)
            if argv == ["/usr/bin/docker", "container", "inspect", container_id]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps([{
                        "Id": container_id,
                        "Name": "/txnmem-formal-controller",
                        "Image": image_id,
                        "Config": {
                            "Image": image_id,
                            "Labels": {
                                "com.txnmem.formal.lifecycle": token,
                                "com.txnmem.formal.role": "controller",
                                "com.txnmem.formal.image-id": image_id,
                            },
                        },
                        "HostConfig": {
                            "VolumeDriver": "",
                            "Mounts": [self._anonymous_state_request()],
                        },
                        "Mounts": [
                            self._anonymous_state_mount(name="8" * 64),
                            extra_mount,
                        ],
                    }]),
                )
            return subprocess.CompletedProcess(argv, 125, stdout="", stderr="")

        with self.assertRaisesRegex(
            container.ControllerContainerCleanupError,
            "^controller lifecycle cleanup failed$",
        ):
            container._cleanup_owned_resources(
                run,
                container_id=container_id,
                container_name="txnmem-formal-controller",
                lifecycle_token=token,
                image_id=image_id,
            )

        self.assertFalse(any(call[1] == "rm" for call in calls))

    def test_cleanup_rejects_a_named_volume_with_an_anonymous_looking_name(self):
        token = "a" * 64
        container_id = "b" * 64
        image_id = "sha256:" + "c" * 64
        volume_name = "d" * 64
        document = self._owned_identity_document(
            token, container_id, image_id, volume_name=volume_name
        )
        document["HostConfig"]["Mounts"][0]["Source"] = volume_name
        calls = []

        def run(argv):
            calls.append(argv)
            if argv == ["/usr/bin/docker", "container", "inspect", container_id]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps([document])
                )
            return subprocess.CompletedProcess(argv, 125, stdout="", stderr="")

        with self.assertRaisesRegex(
            container.ControllerContainerCleanupError,
            "^controller lifecycle cleanup failed$",
        ):
            container._cleanup_owned_resources(
                run,
                container_id=container_id,
                container_name="txnmem-formal-controller",
                lifecycle_token=token,
                image_id=image_id,
            )

        self.assertFalse(any(call[1] == "rm" for call in calls))

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
            "legacy-binds": lambda value: value["HostConfig"].update(
                Binds=["/:/host:ro"]
            ),
            "device": lambda value: value["HostConfig"].update(
                Devices=[{
                    "PathOnHost": "/dev/kvm",
                    "PathInContainer": "/dev/kvm",
                    "CgroupPermissions": "rwm",
                }]
            ),
            "device-request": lambda value: value["HostConfig"].update(
                DeviceRequests=[{
                    "Driver": "",
                    "Count": -1,
                    "DeviceIDs": None,
                    "Capabilities": [["gpu"]],
                    "Options": {},
                }]
            ),
            "device-cgroup-rule": lambda value: value["HostConfig"].update(
                DeviceCgroupRules=["c 1:3 rwm"]
            ),
            "supplementary-group": lambda value: value["HostConfig"].update(
                GroupAdd=["123"]
            ),
            "volumes-from": lambda value: value["HostConfig"].update(
                VolumesFrom=["foreign-container:ro"]
            ),
            "tmpfs": lambda value: value["HostConfig"].update(
                Tmpfs={"/tmp": "rw"}
            ),
            "sysctl": lambda value: value["HostConfig"].update(
                Sysctls={"net.ipv4.ip_forward": "1"}
            ),
            "runtime-custom": lambda value: value["HostConfig"].update(
                Runtime="kata-runtime"
            ),
            "runtime-empty": lambda value: value["HostConfig"].update(Runtime=""),
            "masked-paths-empty": lambda value: value["HostConfig"].update(
                MaskedPaths=[]
            ),
            "readonly-paths-empty": lambda value: value["HostConfig"].update(
                ReadonlyPaths=[]
            ),
            "ipc-host": lambda value: value["HostConfig"].update(IpcMode="host"),
            "ipc-empty": lambda value: value["HostConfig"].update(IpcMode=""),
            "uts-host": lambda value: value["HostConfig"].update(UTSMode="host"),
            "userns-host": lambda value: value["HostConfig"].update(
                UsernsMode="host"
            ),
            "cgroupns-host": lambda value: value["HostConfig"].update(
                CgroupnsMode="host"
            ),
            "cgroupns-empty": lambda value: value["HostConfig"].update(
                CgroupnsMode=""
            ),
            "config-image": lambda value: value["Config"].update(
                Image="sha256:" + "4" * 64
            ),
            "image-label": lambda value: value["Config"]["Labels"].update(
                {"com.txnmem.formal.image-id": "sha256:" + "5" * 64}
            ),
            "repo-rw": lambda value: value["Mounts"][0].update(RW=True),
            "runtime-bind-propagation": lambda value: value["Mounts"][0].update(
                Propagation="rshared"
            ),
            "runtime-volume-propagation": lambda value: value["Mounts"][-1].update(
                Propagation="rshared"
            ),
            "anonymous-name": lambda value: value["Mounts"][-1].update(
                Name="not-an-anonymous-volume-id"
            ),
            "anonymous-read-only": lambda value: value["Mounts"][-1].update(
                RW=False
            ),
            "anonymous-custom-driver": lambda value: value["Mounts"][-1].update(
                Driver="foreign"
            ),
            "host-custom-volume-driver": lambda value: value["HostConfig"].update(
                VolumeDriver="foreign"
            ),
            "host-mounts-missing": lambda value: value["HostConfig"].update(
                Mounts=[]
            ),
            "host-bind-propagation": lambda value: value["HostConfig"][
                "Mounts"
            ][0].update(BindOptions={"Propagation": "rshared"}),
            "host-bind-nonrecursive": lambda value: value["HostConfig"][
                "Mounts"
            ][0].update(BindOptions={"NonRecursive": True}),
            "host-volume-bind-options": lambda value: value["HostConfig"][
                "Mounts"
            ][-1].update(BindOptions={"Propagation": "rprivate"}),
            "host-volume-tmpfs-options": lambda value: value["HostConfig"][
                "Mounts"
            ][-1].update(TmpfsOptions={"SizeBytes": 4096}),
            "host-volume-image-options": lambda value: value["HostConfig"][
                "Mounts"
            ][-1].update(ImageOptions={"Subpath": "unsafe"}),
            "host-volume-cluster-options": lambda value: value["HostConfig"][
                "Mounts"
            ][-1].update(ClusterOptions={"unsafe": True}),
            "host-state-read-only": lambda value: value["HostConfig"][
                "Mounts"
            ][-1].update(ReadOnly=True),
            "host-state-null-source": lambda value: value["HostConfig"][
                "Mounts"
            ][-1].update(Source=None),
            "host-state-null-read-only": lambda value: value["HostConfig"][
                "Mounts"
            ][-1].update(ReadOnly=None),
            "host-state-driver": lambda value: value["HostConfig"][
                "Mounts"
            ][-1].update(
                VolumeOptions={"DriverConfig": {"Name": "foreign"}}
            ),
            "extra-mount": lambda value: value["Mounts"].append(
                {
                    "Type": "bind", "Source": "/home", "Destination": "/unexpected",
                    "Mode": "ro", "RW": False,
                }
            ),
            "extra-host-mount": lambda value: value["HostConfig"][
                "Mounts"
            ].append(
                {
                    "Type": "bind",
                    "Source": "/home",
                    "Target": "/unexpected",
                    "ReadOnly": True,
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

    def test_install_rejects_named_host_managed_state_volume(self):
        repository, wheels = self._paths()
        token = "e" * 64
        container_id = "f" * 64
        document = self._controller_inspect_document(
            repository, wheels, token, container_id, "sha256:" + "1" * 64
        )
        document["HostConfig"]["Mounts"][-1]["Source"] = "foreign-state"
        document["Mounts"][-1].update(
            {
                "Name": "foreign-state",
                "Source": "/var/lib/docker/volumes/foreign-state/_data",
            }
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

        def run(argv):
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
                        "HostConfig": {
                            "VolumeDriver": "",
                            "Mounts": [self._anonymous_state_request()],
                        },
                        "Mounts": [self._anonymous_state_mount()],
                    }]),
                )
            return subprocess.CompletedProcess(argv, 0, stdout="")

        container.create_controller(
            repository,
            wheels,
            "txnmem-formal-controller",
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

    def test_cleanup_by_immutable_id_never_deletes_same_name_replacement(self):
        token = "7" * 64
        container_id = "9" * 64
        image_id = "sha256:" + "8" * 64
        calls = []

        def run(argv):
            calls.append(argv)
            if argv == ["/usr/bin/docker", "container", "inspect", container_id]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps([{
                        "Id": container_id,
                        "Name": "/txnmem-formal-controller",
                        "Image": image_id,
                        "Config": {
                            "Image": image_id,
                            "Labels": {
                                "com.txnmem.formal.lifecycle": token,
                                "com.txnmem.formal.role": "controller",
                                "com.txnmem.formal.image-id": image_id,
                            },
                        },
                        "HostConfig": {
                            "VolumeDriver": "",
                            "Mounts": [self._anonymous_state_request()],
                        },
                        "Mounts": [self._anonymous_state_mount()],
                    }]),
                )
            if argv == ["/usr/bin/docker", "rm", "-f", "-v", container_id]:
                return subprocess.CompletedProcess(
                    argv, 1, stdout="", stderr="original already gone"
                )
            if argv[1:3] == ["container", "ls"]:
                return subprocess.CompletedProcess(
                    argv, 0, stdout="txnmem-formal-controller\n"
                )
            return subprocess.CompletedProcess(argv, 125, stdout="")

        with self.assertRaisesRegex(
            container.ControllerContainerCleanupError,
            "^controller lifecycle cleanup failed$",
        ):
            container._cleanup_owned_resources(
                run,
                container_id=container_id,
                container_name="txnmem-formal-controller",
                lifecycle_token=token,
                image_id=image_id,
            )

        self.assertEqual(
            [call for call in calls if call[1] == "rm"],
            [["/usr/bin/docker", "rm", "-f", "-v", container_id]],
        )

    def test_cleanup_removes_anonymous_volume_only_with_immutable_container_id(self):
        token = "a" * 64
        container_id = "b" * 64
        image_id = "sha256:" + "c" * 64
        volume_name = "d" * 64
        calls = []

        def run(argv):
            calls.append(argv)
            if argv == ["/usr/bin/docker", "container", "inspect", container_id]:
                return subprocess.CompletedProcess(
                    argv,
                    0,
                    stdout=json.dumps([{
                        "Id": container_id,
                        "Name": "/txnmem-formal-controller",
                        "Image": image_id,
                        "Config": {
                            "Image": image_id,
                            "Labels": {
                                "com.txnmem.formal.lifecycle": token,
                                "com.txnmem.formal.role": "controller",
                                "com.txnmem.formal.image-id": image_id,
                            },
                        },
                        "HostConfig": {
                            "VolumeDriver": "",
                            "Mounts": [self._anonymous_state_request()],
                        },
                        "Mounts": [self._anonymous_state_mount(name=volume_name)],
                    }]),
                )
            if argv == ["/usr/bin/docker", "rm", "-f", "-v", container_id]:
                return subprocess.CompletedProcess(argv, 0, stdout=container_id + "\n")
            if argv == [
                "/usr/bin/docker", "container", "ls", "--all", "--no-trunc",
                "--format", "{{.ID}}",
            ]:
                return subprocess.CompletedProcess(argv, 0, stdout="")
            if argv == ["/usr/bin/docker", "volume", "ls", "--quiet"]:
                return subprocess.CompletedProcess(argv, 0, stdout="")
            return subprocess.CompletedProcess(argv, 125, stdout="", stderr="")

        container._cleanup_owned_resources(
            run,
            container_id=container_id,
            container_name="txnmem-formal-controller",
            lifecycle_token=token,
            image_id=image_id,
        )

        self.assertEqual(
            [call for call in calls if call[1] == "rm"],
            [["/usr/bin/docker", "rm", "-f", "-v", container_id]],
        )
        self.assertIn(
            ["/usr/bin/docker", "volume", "ls", "--quiet"], calls
        )
        self.assertFalse(
            any(call[1:3] == ["volume", "rm"] for call in calls)
        )
