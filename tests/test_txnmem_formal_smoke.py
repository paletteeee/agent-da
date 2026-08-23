from __future__ import annotations

import contextlib
import copy
import errno
import json
import os
import signal
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import txnmem_formal_smoke as smoke
import txnmem_provenance_execution_collector as collector
import txnmem_provenance_performance as performance
import txnmem_provenance_runner as runner
from txnmem_formal_io import canonical_json_bytes
from txnmem_toxiproxy_metrics import parse_toxiproxy_byte_counters


ROUTES = [
    {
        "role": "qdrant",
        "proxy_name": "txnmem-qdrant",
        "listen": "0.0.0.0:19000",
        "upstream": "qdrant:6333",
        "enabled": True,
        "toxics_count": 0,
    },
    {
        "role": "neo4j",
        "proxy_name": "txnmem-neo4j",
        "listen": "0.0.0.0:19001",
        "upstream": "neo4j:7687",
        "enabled": True,
        "toxics_count": 0,
    },
]

METRICS = """
# TYPE toxiproxy_proxy_received_bytes_total counter
toxiproxy_proxy_received_bytes_total{direction="upstream",proxy="txnmem-qdrant",listener="[::]:19000",upstream="qdrant:6333"} 11
toxiproxy_proxy_sent_bytes_total{proxy="txnmem-qdrant",listener="[::]:19000",upstream="qdrant:6333",direction="upstream"} 13
toxiproxy_proxy_received_bytes_total{listener="[::]:19000",upstream="qdrant:6333",direction="downstream",proxy="txnmem-qdrant"} 17
toxiproxy_proxy_sent_bytes_total{upstream="qdrant:6333",direction="downstream",proxy="txnmem-qdrant",listener="[::]:19000"} 19
toxiproxy_proxy_received_bytes_total{direction="upstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 23
toxiproxy_proxy_sent_bytes_total{direction="upstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 29
toxiproxy_proxy_received_bytes_total{direction="downstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 31
toxiproxy_proxy_sent_bytes_total{direction="downstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 37
"""


def _counter_snapshot(phase: str, *, qdrant_delta: int = 0, neo4j_delta: int = 0):
    metrics = METRICS.replace(
        'upstream="qdrant:6333"} 11\n',
        'upstream="qdrant:6333"} ' + str(11 + qdrant_delta) + "\n",
        1,
    ).replace(
        'upstream="neo4j:7687"} 23\n',
        'upstream="neo4j:7687"} ' + str(23 + neo4j_delta) + "\n",
        1,
    )
    return parse_toxiproxy_byte_counters(
        metrics, phase=phase, proxy_routes=ROUTES
    )


class _FakeProcess:
    pid = 4242

    def poll(self):
        return 0


class _FakeChild:
    def __init__(self, events):
        self.events = events
        self.process = _FakeProcess()
        self.receipt = {
            "schema": "txnmem-provenance-smoke-child-receipt-v1",
            "qdrant_proxy_ok": True,
            "neo4j_proxy_ok": True,
        }

    def release(self):
        self.events.append("child_released")

    def wait_with_receipt(self):
        return 0, dict(self.receipt)


class _FakeGuard:
    def __init__(self, events, initial):
        self.events = events
        self.initial = dict(initial)
        self.final = dict(initial)
        self.active = False

    def activate(self):
        self.active = True
        self.events.append("guard_activated")
        return dict(self.initial)

    def verify(self):
        self.events.append("guard_stable")
        return dict(self.final)

    def deactivate(self):
        self.active = False
        self.events.append("guard_removed")


class FormalSmokeOrchestrationTests(unittest.TestCase):
    def _paths(self, root: Path):
        project = root / "repository"
        reports = root / "reports"
        workspaces = root / "workspaces"
        project.mkdir()
        reports.mkdir()
        workspaces.mkdir()
        return project, reports / "smoke.json", workspaces

    @contextlib.contextmanager
    def _success_dependencies(self, root: Path):
        project, out_path, workspace_parent = self._paths(root)
        events: list[str] = []
        workspace_path = workspace_parent / "smoke-fixture"
        workspace_path.mkdir()
        workspace = smoke._SmokeWorkspace(
            path=workspace_path,
            device=1,
            inode=2,
            parent_device=3,
            parent_inode=4,
        )
        source_export = workspace_path / "source"
        runtime_snapshot = workspace_path / "runtime"
        initial_guard = {
            "schema": "txnmem-provenance-network-guard-v3",
            "ruleset_sha256": "d" * 64,
        }
        guard = _FakeGuard(events, initial_guard)
        child = _FakeChild(events)
        topology = smoke._SmokeTopology(
            raw_backend_isolation={"schema": "raw-fixture"},
            sanitized_backend_isolation={"schema": "sanitized-fixture"},
            guard_profile={
                "backend_ipv4_subnet": "192.0.2.0/28",
                "ingress_ipv4_subnet": "198.51.100.0/28",
                "backend_bridge_interface": "br-000000000001",
                "ingress_bridge_interface": "br-000000000002",
                "toxiproxy_ingress_ipv4": "198.51.100.2",
            },
            backend_ipv4_by_role={
                "qdrant": "192.0.2.2",
                "neo4j": "192.0.2.3",
                "toxiproxy_ingress": "198.51.100.2",
            },
            probe_image_id="sha256:" + "e" * 64,
            toxiproxy_manifest_digest="f" * 64,
        )
        source_identity = {
            "source_commit": "a" * 40,
            "source_manifest_sha256": "b" * 64,
            "runner_sha256": "c" * 64,
        }
        controller_context = {
            "source_commit": "a" * 40,
            "source_manifest": {
                "schema": "txnmem-provenance-source-manifest-v1",
                "source_commit": "a" * 40,
                "files": [],
            },
        }
        snapshots = {
            "baseline_a": _counter_snapshot("baseline_a"),
            "baseline_b": _counter_snapshot("baseline_b"),
            "final": _counter_snapshot(
                "final", qdrant_delta=4, neo4j_delta=6
            ),
        }
        prepare_count = 0

        def create_source(*_args, **_kwargs):
            source_export.mkdir()
            return source_export

        def create_runtime(*_args, **_kwargs):
            runtime_snapshot.mkdir()
            return runtime_snapshot, {"schema": "runtime-fixture"}

        def prepare_routes(*_args, **_kwargs):
            nonlocal prepare_count
            prepare_count += 1
            events.append(
                "routes_prepared" if prepare_count == 1 else "routes_rearmed"
            )
            return copy.deepcopy(ROUTES)

        def capture_counters(*_args, phase, **_kwargs):
            events.append(phase if phase != "final" else "final_counters")
            return copy.deepcopy(snapshots[phase])

        receipt_validator = smoke.validate_smoke_child_receipt

        def validate_receipt(value):
            normalized = receipt_validator(value)
            if normalized["qdrant_proxy_ok"] is True:
                events.append("runner_qdrant_success")
            if normalized["neo4j_proxy_ok"] is True:
                events.append("runner_neo4j_success")
            return normalized

        def remove_workspace(value):
            events.append("workspace_removed")
            shutil.rmtree(value.path)

        patches = {
            "_require_root_linux": {"return_value": None},
            "_validate_formal_controller_context": {
                "return_value": controller_context
            },
            "_create_smoke_workspace": {"return_value": workspace},
            "attest_committed_source": {
                "side_effect": lambda *_args, **_kwargs: (
                    events.append("source_verified") or source_identity
                )
            },
            "create_immutable_source_export": {"side_effect": create_source},
            "_create_locked_runtime_snapshot": {"side_effect": create_runtime},
            "_publish_formal_input_tree": {"return_value": {}},
            "_build_smoke_child_spec": {
                "return_value": smoke._SmokeChildSpec(
                    command=("/usr/bin/python3", "provenance-smoke"),
                    cwd=source_export,
                    environment={"LANG": "C.UTF-8"},
                )
            },
            "_observe_smoke_topology": {
                "side_effect": lambda: (
                    events.append("topology_observed") or topology
                )
            },
            "prepare_isolated_toxiproxy_routes": {
                "side_effect": prepare_routes
            },
            "_probe_root_health": {
                "side_effect": lambda **_kwargs: (
                    events.append("health_probed") or "2.5.0"
                )
            },
            "capture_toxiproxy_counter_snapshot": {
                "side_effect": capture_counters
            },
            "_require_formal_uid_processes": {"return_value": {}},
            "_start_gated_candidate": {
                "side_effect": lambda **_kwargs: (
                    events.append("child_gated") or child
                )
            },
            "_observe_formal_child_process": {
                "return_value": {"start_identity": "candidate:4242:99"}
            },
            "_NftNetworkGuard": {"return_value": guard},
            "_validate_smoke_guard": {
                "side_effect": lambda value, _topology: dict(value)
            },
            "_probe_root_management": {
                "side_effect": lambda *_args, **_kwargs: (
                    events.append("root_management_success") or True
                )
            },
            "_probe_root_data_denial": {
                "side_effect": lambda **_kwargs: (
                    events.append("root_data_denied") or True
                )
            },
            "_probe_direct_backend_denial": {
                "side_effect": lambda *_args, **_kwargs: (
                    events.append("direct_backend_denied") or True
                )
            },
            "_probe_forward_path_denial": {
                "side_effect": lambda **_kwargs: (
                    events.append("forward_path_denied") or True
                )
            },
            "validate_smoke_child_receipt": {"side_effect": validate_receipt},
            "observe_formal_toxiproxy_routes": {
                "return_value": copy.deepcopy(ROUTES)
            },
            "_cleanup_formal_execution_resources": {"return_value": []},
            "_remove_smoke_workspace": {"side_effect": remove_workspace},
        }
        with contextlib.ExitStack() as stack:
            mocks = {
                name: stack.enter_context(patch.object(smoke, name, **config))
                for name, config in patches.items()
            }
            yield SimpleNamespace(
                project=project,
                out_path=out_path,
                workspace_parent=workspace_parent,
                workspace=workspace,
                events=events,
                mocks=mocks,
                child=child,
                guard=guard,
                snapshots=snapshots,
            )

    @staticmethod
    def _collect(fixture):
        return smoke.collect_formal_smoke(
            project_root=fixture.project,
            out_path=fixture.out_path,
            qdrant_url="http://127.0.0.1:19000",
            neo4j_uri="bolt://127.0.0.1:19001",
            toxiproxy_url="http://127.0.0.1:8474",
            neo4j_password="test-only-placeholder",
            _controller_context={"schema": "fixture"},
        )

    def test_success_has_exact_event_order_and_never_creates_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            with self._success_dependencies(root) as fixture, patch.object(
                collector,
                "_prepare_formal_run_workspace",
                side_effect=AssertionError("candidate workspace forbidden"),
            ) as prepare_candidate, patch.object(
                collector,
                "_seal_candidate_tree",
                side_effect=AssertionError("candidate seal forbidden"),
            ) as seal_candidate, patch.object(
                performance,
                "candidate_attestation_material",
                side_effect=AssertionError("candidate material forbidden"),
            ) as candidate_material, patch.object(
                performance,
                "_load_provenance_candidate",
                side_effect=AssertionError("candidate loader forbidden"),
            ) as candidate_loader, patch.object(
                performance,
                "promote_provenance_candidate",
                side_effect=AssertionError("promotion forbidden"),
            ) as promote:
                report = self._collect(fixture)

            expected_events = [
                "source_verified",
                "topology_observed",
                "routes_prepared",
                "health_probed",
                "baseline_a",
                "child_gated",
                "guard_activated",
                "routes_rearmed",
                "baseline_b",
                "root_management_success",
                "root_data_denied",
                "direct_backend_denied",
                "forward_path_denied",
                "child_released",
                "runner_qdrant_success",
                "runner_neo4j_success",
                "final_counters",
                "guard_stable",
                "guard_removed",
                "workspace_removed",
            ]
            self.assertEqual(fixture.events, expected_events)
            self.assertFalse(
                any(path.name == "candidate" for path in root.rglob("*"))
            )
            for forbidden in (
                prepare_candidate,
                seal_candidate,
                candidate_material,
                candidate_loader,
                promote,
            ):
                forbidden.assert_not_called()
            self.assertEqual(
                json.loads(fixture.out_path.read_text(encoding="utf-8")), report
            )
            self.assertIs(smoke.validate_formal_smoke_report(report)["candidate_created"], False)

    def test_each_success_and_denial_probe_fails_closed_without_report(self):
        false_probes = (
            "_probe_root_health",
            "_probe_root_management",
            "_probe_root_data_denial",
            "_probe_direct_backend_denial",
            "_probe_forward_path_denial",
        )
        for probe_name in false_probes:
            with self.subTest(probe=probe_name), tempfile.TemporaryDirectory() as tmp:
                with self._success_dependencies(Path(tmp).resolve()) as fixture:
                    fixture.mocks[probe_name].side_effect = None
                    fixture.mocks[probe_name].return_value = False
                    with self.assertRaises(smoke.FormalSmokeError):
                        self._collect(fixture)
                    self.assertFalse(fixture.out_path.exists())

        for receipt_field in ("qdrant_proxy_ok", "neo4j_proxy_ok"):
            with self.subTest(receipt=receipt_field), tempfile.TemporaryDirectory() as tmp:
                with self._success_dependencies(Path(tmp).resolve()) as fixture:
                    fixture.child.receipt[receipt_field] = False
                    with self.assertRaises(smoke.FormalSmokeError):
                        self._collect(fixture)
                    self.assertFalse(fixture.out_path.exists())

    def test_ab_counter_or_route_drift_fails_without_report(self):
        cases = ("counter", "route")
        for case in cases:
            with self.subTest(case=case), tempfile.TemporaryDirectory() as tmp:
                with self._success_dependencies(Path(tmp).resolve()) as fixture:
                    if case == "counter":
                        fixture.snapshots["baseline_b"] = _counter_snapshot(
                            "baseline_b", qdrant_delta=1
                        )
                    else:
                        routes_b = copy.deepcopy(ROUTES)
                        routes_b[0]["toxics_count"] = 1
                        fixture.mocks[
                            "prepare_isolated_toxiproxy_routes"
                        ].side_effect = [copy.deepcopy(ROUTES), routes_b]
                    with self.assertRaises((smoke.FormalSmokeError, collector.CollectorError)):
                        self._collect(fixture)
                    self.assertFalse(fixture.out_path.exists())

    def test_zero_backend_delta_fails_without_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self._success_dependencies(Path(tmp).resolve()) as fixture:
                fixture.snapshots["final"] = _counter_snapshot(
                    "final", qdrant_delta=4, neo4j_delta=0
                )
                with self.assertRaises((smoke.FormalSmokeError, collector.CollectorError)):
                    self._collect(fixture)
                self.assertFalse(fixture.out_path.exists())

    def test_changed_guard_hash_fails_without_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self._success_dependencies(Path(tmp).resolve()) as fixture:
                fixture.guard.final["ruleset_sha256"] = "9" * 64
                with self.assertRaises(smoke.FormalSmokeError):
                    self._collect(fixture)
                self.assertFalse(fixture.out_path.exists())

    def test_child_guard_and_workspace_cleanup_failures_block_report(self):
        for cleanup in ("child", "guard", "workspace"):
            with self.subTest(cleanup=cleanup), tempfile.TemporaryDirectory() as tmp:
                with self._success_dependencies(Path(tmp).resolve()) as fixture:
                    if cleanup == "child":
                        fixture.mocks[
                            "_cleanup_formal_execution_resources"
                        ].return_value = [OSError("fixture cleanup failure")]
                    elif cleanup == "guard":
                        fixture.guard.deactivate = lambda: (_ for _ in ()).throw(
                            OSError("fixture cleanup failure")
                        )
                    else:
                        fixture.mocks[
                            "_remove_smoke_workspace"
                        ].side_effect = OSError("fixture cleanup failure")
                    with self.assertRaises(smoke.FormalSmokeError):
                        self._collect(fixture)
                    self.assertFalse(fixture.out_path.exists())

    def test_surviving_child_process_group_preserves_guard_and_blocks_report(self):
        script = (
            "import os, time\n"
            "pid = os.fork()\n"
            "if pid == 0:\n"
            "    time.sleep(30)\n"
            "else:\n"
            "    os._exit(0)\n"
        )
        survivor = subprocess.Popen(
            [sys.executable, "-c", script],
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        survivor.wait(timeout=5)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                with self._success_dependencies(Path(tmp).resolve()) as fixture:
                    fixture.child.process = survivor
                    with self.assertRaisesRegex(
                        smoke.FormalSmokeError, "cleanup"
                    ):
                        self._collect(fixture)
                    self.assertTrue(fixture.guard.active)
                    self.assertNotIn("guard_removed", fixture.events)
                    self.assertFalse(fixture.out_path.exists())
        finally:
            try:
                os.killpg(survivor.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            survivor.wait(timeout=5)

    def test_report_validator_rejects_extra_nonexact_and_rehashed_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self._success_dependencies(Path(tmp).resolve()) as fixture:
                report = self._collect(fixture)

        mutations = []
        extra = dict(report)
        extra["latency_ms"] = 1
        mutations.append(extra)
        wrong_boolean = dict(report)
        wrong_boolean["guard_stable"] = 1
        mutations.append(wrong_boolean)
        wrong_count = dict(report)
        wrong_count["proxy_metrics_series_count"] = True
        mutations.append(wrong_count)
        wrong_hash = dict(report)
        wrong_hash["report_sha256"] = "0" * 64
        mutations.append(wrong_hash)

        for value in mutations:
            with self.subTest(keys=sorted(value)):
                with self.assertRaises(smoke.FormalSmokeError):
                    smoke.validate_formal_smoke_report(value)

        unhashed = dict(report)
        digest = unhashed.pop("report_sha256")
        self.assertEqual(
            digest,
            smoke.hashlib.sha256(canonical_json_bytes(unhashed)).hexdigest(),
        )


class FormalSmokeProbeTests(unittest.TestCase):
    def test_docker_not_found_requires_exact_status_stdout_and_stderr(self):
        ref = "a" * 64
        exact = SimpleNamespace(
            returncode=1,
            stdout="",
            stderr=f"Error response from daemon: No such object: {ref}\n",
        )
        self.assertIs(smoke._docker_inspect_is_not_found(exact, ref), True)

        malformed = (
            SimpleNamespace(
                returncode=1,
                stdout="",
                stderr=f"prefix Error response from daemon: No such object: {ref}\n",
            ),
            SimpleNamespace(
                returncode=1,
                stdout="",
                stderr=f"Error response from daemon: No such object: {ref}\nextra\n",
            ),
            SimpleNamespace(
                returncode=1,
                stdout="",
                stderr=f"Error response from daemon: No such object: {ref} suffix\n",
            ),
            SimpleNamespace(
                returncode=2,
                stdout="",
                stderr=f"Error response from daemon: No such object: {ref}\n",
            ),
            SimpleNamespace(
                returncode=1,
                stdout="[]\n",
                stderr=f"Error response from daemon: No such object: {ref}\n",
            ),
            SimpleNamespace(
                returncode=1,
                stdout="",
                stderr=(
                    "permission denied while checking "
                    f"No such object: {ref}\n"
                ),
            ),
        )
        for result in malformed:
            with self.subTest(returncode=result.returncode, stdout=result.stdout, stderr=result.stderr):
                self.assertIs(smoke._docker_inspect_is_not_found(result, ref), False)

    def test_host_denial_probe_accepts_only_connection_refused(self):
        with patch.object(
            smoke.socket,
            "create_connection",
            side_effect=ConnectionRefusedError(
                errno.ECONNREFUSED, "connection refused"
            ),
        ):
            self.assertIs(smoke._tcp_connection_denied("127.0.0.1", 19000), True)

        for failure in (
            TimeoutError(errno.ETIMEDOUT, "timed out"),
            OSError(errno.ENETUNREACH, "network unreachable"),
            OSError(errno.EHOSTUNREACH, "host unreachable"),
        ):
            with self.subTest(errno=getattr(failure, "errno", None)), patch.object(
                smoke.socket, "create_connection", side_effect=failure
            ):
                with self.assertRaisesRegex(
                    smoke.FormalSmokeError, "unexpected connection probe"
                ):
                    smoke._tcp_connection_denied("127.0.0.1", 19000)

    def test_forward_probe_prohibits_pull_and_removes_ephemeral_container(self):
        calls: list[tuple[str, ...]] = []
        container_id = "a" * 64
        owner_label = "txnmem.formal-smoke.owner"
        owner_value = None
        inspect_count = 0

        def run(command, **_kwargs):
            nonlocal inspect_count, owner_value
            calls.append(tuple(command))
            operation = command[1]
            if operation == "create":
                self.assertIn("--label", command)
                owner_value = command[command.index("--label") + 1].split("=", 1)[1]
                return SimpleNamespace(returncode=0, stdout=container_id + "\n", stderr="")
            if operation == "start":
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if operation == "rm":
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if operation == "inspect":
                inspect_count += 1
                if inspect_count == 1:
                    return SimpleNamespace(
                        returncode=0,
                        stdout=json.dumps(
                            [
                                {
                                    "Id": container_id,
                                    "Config": {
                                        "Labels": {owner_label: owner_value}
                                    },
                                }
                            ]
                        ),
                        stderr="",
                    )
                return SimpleNamespace(
                    returncode=1,
                    stdout="",
                    stderr=f"Error response from daemon: No such object: {command[2]}\n",
                )
            raise AssertionError(operation)

        with patch.object(smoke.subprocess, "run", side_effect=run):
            self.assertIs(
                smoke._probe_forward_path_denial(
                    backend_ipv4_by_role={
                        "qdrant": "192.0.2.2",
                        "neo4j": "192.0.2.3",
                        "toxiproxy_ingress": "198.51.100.2",
                    },
                    image_id="sha256:" + "b" * 64,
                ),
                True,
            )

        create = calls[0]
        self.assertEqual(create[1], "create")
        self.assertIn("--pull=never", create)
        self.assertIn("--network", create)
        self.assertEqual(create[create.index("--network") + 1], "bridge")
        self.assertEqual(
            [call[1] for call in calls],
            ["create", "start", "inspect", "rm", "inspect"],
        )

    def test_forward_probe_cleanup_failure_is_a_hard_failure(self):
        container_id = "a" * 64
        owner_label = "txnmem.formal-smoke.owner"
        owner_value = None

        def run(command, **_kwargs):
            nonlocal owner_value
            operation = command[1]
            if operation == "create":
                owner_value = command[command.index("--label") + 1].split("=", 1)[1]
                return SimpleNamespace(returncode=0, stdout=container_id + "\n", stderr="")
            if operation == "start":
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if operation == "inspect":
                return SimpleNamespace(
                    returncode=0,
                    stdout=json.dumps(
                        [
                            {
                                "Id": container_id,
                                "Config": {"Labels": {owner_label: owner_value}},
                            }
                        ]
                    ),
                    stderr="",
                )
            if operation == "rm":
                return SimpleNamespace(returncode=1, stdout="", stderr="")
            raise AssertionError(operation)

        with patch.object(smoke.subprocess, "run", side_effect=run):
            with self.assertRaisesRegex(
                smoke.FormalSmokeError, "container cleanup"
            ):
                smoke._probe_forward_path_denial(
                    backend_ipv4_by_role={
                        "qdrant": "192.0.2.2",
                        "neo4j": "192.0.2.3",
                        "toxiproxy_ingress": "198.51.100.2",
                    },
                    image_id="sha256:" + "b" * 64,
                )

    def test_forward_probe_unexpected_start_status_fails_not_denied(self):
        container_id = "a" * 64
        calls: list[tuple[str, ...]] = []
        owner_label = "txnmem.formal-smoke.owner"
        owner_value = None
        inspect_count = 0

        def run(command, **_kwargs):
            nonlocal inspect_count, owner_value
            calls.append(tuple(command))
            operation = command[1]
            if operation == "create":
                owner_value = command[command.index("--label") + 1].split("=", 1)[1]
                return SimpleNamespace(returncode=0, stdout=container_id + "\n", stderr="")
            if operation == "start":
                return SimpleNamespace(returncode=92, stdout="", stderr="network unreachable\n")
            if operation == "rm":
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if operation == "inspect":
                inspect_count += 1
                if inspect_count == 1:
                    return SimpleNamespace(
                        returncode=0,
                        stdout=json.dumps(
                            [
                                {
                                    "Id": container_id,
                                    "Config": {
                                        "Labels": {owner_label: owner_value}
                                    },
                                }
                            ]
                        ),
                        stderr="",
                    )
                return SimpleNamespace(
                    returncode=1,
                    stdout="",
                    stderr=f"Error response from daemon: No such object: {command[2]}\n",
                )
            raise AssertionError(operation)

        with patch.object(smoke.subprocess, "run", side_effect=run):
            with self.assertRaisesRegex(
                smoke.FormalSmokeError, "unexpected container probe"
            ):
                smoke._probe_forward_path_denial(
                    backend_ipv4_by_role={
                        "qdrant": "192.0.2.2",
                        "neo4j": "192.0.2.3",
                        "toxiproxy_ingress": "198.51.100.2",
                    },
                    image_id="sha256:" + "b" * 64,
                )
        self.assertEqual(
            [call[1] for call in calls],
            ["create", "start", "inspect", "rm", "inspect"],
        )

    def test_forward_probe_malformed_create_output_still_removes_by_name(self):
        calls: list[tuple[str, ...]] = []
        owner_label = "txnmem.formal-smoke.owner"
        owned_id = "1" * 64
        inspect_count = 0

        def run(command, **_kwargs):
            nonlocal inspect_count
            calls.append(tuple(command))
            operation = command[1]
            if operation == "create":
                return SimpleNamespace(returncode=0, stdout="not-a-container-id\n", stderr="")
            if operation == "rm":
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if operation == "inspect":
                inspect_count += 1
                if inspect_count == 1:
                    return SimpleNamespace(
                        returncode=0,
                        stdout=json.dumps(
                            [
                                {
                                    "Id": owned_id,
                                    "Config": {
                                        "Labels": {owner_label: "1" * 24}
                                    },
                                }
                            ]
                        ),
                        stderr="",
                    )
                return SimpleNamespace(
                    returncode=1,
                    stdout="",
                    stderr=f"Error response from daemon: No such object: {command[2]}\n",
                )
            raise AssertionError(operation)

        with patch.object(smoke.secrets, "token_hex", return_value="1" * 24), patch.object(
            smoke.subprocess, "run", side_effect=run
        ):
            with self.assertRaisesRegex(
                smoke.FormalSmokeError, "could not start"
            ):
                smoke._probe_forward_path_denial(
                    backend_ipv4_by_role={
                        "qdrant": "192.0.2.2",
                        "neo4j": "192.0.2.3",
                        "toxiproxy_ingress": "198.51.100.2",
                    },
                    image_id="sha256:" + "b" * 64,
                )
        self.assertEqual(
            [call[1] for call in calls], ["create", "inspect", "rm", "inspect"]
        )
        self.assertEqual(calls[2][1:4], ("rm", "--force", owned_id))

    def test_forward_probe_create_timeout_still_removes_by_name(self):
        calls: list[tuple[str, ...]] = []
        owner_label = "txnmem.formal-smoke.owner"
        owned_id = "2" * 64
        inspect_count = 0

        def run(command, **_kwargs):
            nonlocal inspect_count
            calls.append(tuple(command))
            operation = command[1]
            if operation == "create":
                raise subprocess.TimeoutExpired(command, 15.0)
            if operation == "rm":
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if operation == "inspect":
                inspect_count += 1
                if inspect_count == 1:
                    return SimpleNamespace(
                        returncode=0,
                        stdout=json.dumps(
                            [
                                {
                                    "Id": owned_id,
                                    "Config": {
                                        "Labels": {owner_label: "2" * 24}
                                    },
                                }
                            ]
                        ),
                        stderr="",
                    )
                return SimpleNamespace(
                    returncode=1,
                    stdout="",
                    stderr=f"Error response from daemon: No such object: {command[2]}\n",
                )
            raise AssertionError(operation)

        with patch.object(smoke.secrets, "token_hex", return_value="2" * 24), patch.object(
            smoke.subprocess, "run", side_effect=run
        ):
            with self.assertRaisesRegex(
                smoke.FormalSmokeError, "container probe"
            ):
                smoke._probe_forward_path_denial(
                    backend_ipv4_by_role={
                        "qdrant": "192.0.2.2",
                        "neo4j": "192.0.2.3",
                        "toxiproxy_ingress": "198.51.100.2",
                    },
                    image_id="sha256:" + "b" * 64,
                )
        self.assertEqual(
            [call[1] for call in calls], ["create", "inspect", "rm", "inspect"]
        )
        self.assertEqual(calls[2][1:4], ("rm", "--force", owned_id))

    def test_forward_probe_inspect_permission_failure_is_cleanup_failure(self):
        container_id = "c" * 64

        def run(command, **_kwargs):
            operation = command[1]
            if operation == "create":
                return SimpleNamespace(returncode=0, stdout=container_id + "\n", stderr="")
            if operation == "start":
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if operation == "rm":
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if operation == "inspect":
                return SimpleNamespace(
                    returncode=1,
                    stdout="",
                    stderr="permission denied while inspecting container",
                )
            raise AssertionError(operation)

        with patch.object(smoke.subprocess, "run", side_effect=run):
            with self.assertRaisesRegex(
                smoke.FormalSmokeError, "container cleanup|inspect"
            ):
                smoke._probe_forward_path_denial(
                    backend_ipv4_by_role={
                        "qdrant": "192.0.2.2",
                        "neo4j": "192.0.2.3",
                        "toxiproxy_ingress": "198.51.100.2",
                    },
                    image_id="sha256:" + "b" * 64,
                )

    def test_forward_probe_duplicate_name_never_removes_unowned_container(self):
        owner_label = "txnmem.formal-smoke.owner"

        for labels in ({}, {owner_label: "foreign"}):
            with self.subTest(labels=labels):
                calls: list[tuple[str, ...]] = []

                def run(command, **_kwargs):
                    calls.append(tuple(command))
                    operation = command[1]
                    if operation == "create":
                        return SimpleNamespace(
                            returncode=1,
                            stdout="",
                            stderr="Conflict. The container name is already in use.",
                        )
                    if operation == "inspect":
                        return SimpleNamespace(
                            returncode=0,
                            stdout=json.dumps(
                                [
                                    {
                                        "Id": "3" * 64,
                                        "Config": {"Labels": labels},
                                    }
                                ]
                            ),
                            stderr="",
                        )
                    if operation == "rm":
                        return SimpleNamespace(returncode=0, stdout="", stderr="")
                    raise AssertionError(operation)

                with patch.object(smoke.secrets, "token_hex", return_value="3" * 24), patch.object(
                    smoke.subprocess, "run", side_effect=run
                ):
                    with self.assertRaisesRegex(
                        smoke.FormalSmokeError,
                        "container cleanup|container probe|could not start|ownership",
                    ):
                        smoke._probe_forward_path_denial(
                            backend_ipv4_by_role={
                                "qdrant": "192.0.2.2",
                                "neo4j": "192.0.2.3",
                                "toxiproxy_ingress": "198.51.100.2",
                            },
                            image_id="sha256:" + "b" * 64,
                        )

                self.assertNotIn("rm", [call[0] for call in calls])

    def test_forward_probe_owned_partial_create_cleanup_proves_exact_absence(self):
        owner_label = "txnmem.formal-smoke.owner"

        for mode in ("malformed", "timeout"):
            with self.subTest(mode=mode):
                token = "4" * 24
                name = "txnmem-smoke-" + token
                owned_id = "4" * 64
                calls: list[tuple[str, ...]] = []
                inspect_count = 0

                def run(command, **_kwargs):
                    nonlocal inspect_count
                    calls.append(tuple(command))
                    operation = command[1]
                    if operation == "create":
                        if mode == "timeout":
                            raise subprocess.TimeoutExpired(command, 15.0)
                        return SimpleNamespace(
                            returncode=0,
                            stdout="not-a-container-id\n",
                            stderr="",
                        )
                    if operation == "inspect":
                        inspect_count += 1
                        if inspect_count == 1:
                            return SimpleNamespace(
                                returncode=0,
                                stdout=json.dumps(
                                    [
                                        {
                                            "Id": owned_id,
                                            "Config": {
                                                "Labels": {owner_label: token}
                                            },
                                        }
                                    ]
                                ),
                                stderr="",
                            )
                        return SimpleNamespace(
                            returncode=1,
                            stdout="",
                            stderr=f"Error response from daemon: No such object: {command[2]}\n",
                        )
                    if operation == "rm":
                        return SimpleNamespace(returncode=0, stdout="", stderr="")
                    raise AssertionError(operation)

                with patch.object(smoke.secrets, "token_hex", return_value=token), patch.object(
                    smoke.subprocess, "run", side_effect=run
                ):
                    with self.assertRaises(smoke.FormalSmokeError):
                        smoke._probe_forward_path_denial(
                            backend_ipv4_by_role={
                                "qdrant": "192.0.2.2",
                                "neo4j": "192.0.2.3",
                                "toxiproxy_ingress": "198.51.100.2",
                            },
                            image_id="sha256:" + "b" * 64,
                        )

                operations = [call[1] for call in calls]
                self.assertEqual(operations, ["create", "inspect", "rm", "inspect"])
                self.assertIn("--label", calls[0])
                self.assertIn(f"{owner_label}={token}", calls[0])
                self.assertEqual(calls[2][1:4], ("rm", "--force", owned_id))

    def test_probe_cleanup_removes_bound_full_id_not_rebound_name(self):
        token = "5" * 24
        name = "txnmem-smoke-" + token
        owned_id = "a" * 64
        calls: list[tuple[str, ...]] = []

        def run(command, **_kwargs):
            docker_args = tuple(command[1:])
            calls.append(docker_args)
            if docker_args == ("inspect", name):
                return SimpleNamespace(
                    returncode=0,
                    stdout=json.dumps(
                        [
                            {
                                "Id": owned_id,
                                "Config": {
                                    "Labels": {
                                        "txnmem.formal-smoke.owner": token
                                    }
                                },
                            }
                        ]
                    ),
                    stderr="",
                )
            if docker_args[0:2] == ("rm", "--force"):
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            if docker_args == ("inspect", owned_id):
                return SimpleNamespace(
                    returncode=1,
                    stdout="",
                    stderr=(
                        "Error response from daemon: "
                        f"No such object: {owned_id}\n"
                    ),
                )
            if docker_args == ("inspect", name):
                raise AssertionError("absence must be proven by full id")
            raise AssertionError(docker_args)

        with patch.object(smoke.subprocess, "run", side_effect=run):
            smoke._remove_probe_container(name, owner_label=token)

        self.assertIn(("rm", "--force", owned_id), calls)
        self.assertNotIn(("rm", "--force", name), calls)
        self.assertEqual(calls[-1], ("inspect", owned_id))

    def test_probe_cleanup_rejects_malformed_or_ambiguous_inspect_identity(self):
        token = "6" * 24
        name = "txnmem-smoke-" + token

        malformed_documents = (
            [{"Config": {"Labels": {"txnmem.formal-smoke.owner": token}}}],
            [
                {
                    "Id": "not-a-full-container-id",
                    "Config": {"Labels": {"txnmem.formal-smoke.owner": token}},
                }
            ],
            [
                {
                    "Id": "a" * 64,
                    "Config": {"Labels": {"txnmem.formal-smoke.owner": token}},
                },
                {
                    "Id": "b" * 64,
                    "Config": {"Labels": {"txnmem.formal-smoke.owner": token}},
                },
            ],
        )
        for document in malformed_documents:
            with self.subTest(document=document):
                calls: list[tuple[str, ...]] = []

                def run(command, **_kwargs):
                    docker_args = tuple(command[1:])
                    calls.append(docker_args)
                    if docker_args == ("inspect", name):
                        return SimpleNamespace(
                            returncode=0,
                            stdout=json.dumps(document),
                            stderr="",
                        )
                    if docker_args[0:2] == ("rm", "--force"):
                        return SimpleNamespace(returncode=0, stdout="", stderr="")
                    raise AssertionError(docker_args)

                with patch.object(smoke.subprocess, "run", side_effect=run):
                    with self.assertRaisesRegex(
                        smoke.FormalSmokeError, "inspect|identity"
                    ):
                        smoke._remove_probe_container(name, owner_label=token)

                self.assertNotIn("rm", [call[0] for call in calls])

    def test_probe_partial_create_name_absence_does_not_remove(self):
        token = "7" * 24
        name = "txnmem-smoke-" + token
        calls: list[tuple[str, ...]] = []

        def run(command, **_kwargs):
            docker_args = tuple(command[1:])
            calls.append(docker_args)
            if docker_args[0] == "create":
                raise subprocess.TimeoutExpired(command, 15.0)
            if docker_args == ("inspect", name):
                return SimpleNamespace(
                    returncode=1,
                    stdout="",
                    stderr=f"Error response from daemon: No such object: {name}\n",
                )
            if docker_args[0:2] == ("rm", "--force"):
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            raise AssertionError(docker_args)

        with patch.object(smoke.secrets, "token_hex", return_value=token), patch.object(
            smoke.subprocess, "run", side_effect=run
        ):
            with self.assertRaisesRegex(
                smoke.FormalSmokeError, "container probe"
            ):
                smoke._probe_forward_path_denial(
                    backend_ipv4_by_role={
                        "qdrant": "192.0.2.2",
                        "neo4j": "192.0.2.3",
                        "toxiproxy_ingress": "198.51.100.2",
                    },
                    image_id="sha256:" + "b" * 64,
                )

        self.assertEqual([call[0] for call in calls], ["create", "inspect"])

    def test_workspace_partial_creation_cleanup_failure_is_not_silent(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            root.chmod(0o750)
            try:
                with patch.object(smoke.os, "chmod", side_effect=OSError("chmod")), patch.object(
                    smoke.os, "rmdir", side_effect=OSError("rmdir")
                ):
                    caught = None
                    try:
                        smoke._create_smoke_workspace(
                            workspace_root=root,
                            controller_uid=os.getuid(),
                            runner_gid=os.getgid(),
                        )
                    except BaseException as exc:
                        caught = exc
                    self.assertIsInstance(caught, smoke.FormalSmokeError)
                    self.assertRegex(str(caught), "rollback failed")
            finally:
                root.chmod(0o700)


class ProvenanceSmokeRunnerTests(unittest.TestCase):
    @contextlib.contextmanager
    def _runner_descriptors(self, runtime_site: Path):
        gate_read, gate_write = os.pipe()
        ready_read, ready_write = os.pipe()
        receipt_read, receipt_write = os.pipe()
        os.write(gate_write, b"G")
        os.close(gate_write)
        environment = {
            "TXNMEM_PROVENANCE_START_GATE_FD": str(gate_read),
            "TXNMEM_PROVENANCE_READY_FD": str(ready_write),
            "TXNMEM_PROVENANCE_COMPLETION_FD": str(receipt_write),
            "TXNMEM_PROVENANCE_RUNTIME_SITE": str(runtime_site),
            "TXNMEM_NEO4J_PASSWORD": "test-only-placeholder",
        }
        try:
            with patch.dict(os.environ, environment, clear=False):
                yield ready_read, receipt_read
        finally:
            for descriptor in (ready_read, receipt_read):
                try:
                    os.close(descriptor)
                except OSError:
                    pass

    def test_runner_smoke_mode_releases_only_after_gate_and_writes_receipt(self):
        receipt = {
            "schema": "txnmem-provenance-smoke-child-receipt-v1",
            "qdrant_proxy_ok": True,
            "neo4j_proxy_ok": True,
        }
        with tempfile.TemporaryDirectory() as tmp:
            runtime_site = Path(tmp).resolve()
            with self._runner_descriptors(runtime_site) as (ready, completed), patch.object(
                runner, "_provenance_smoke_receipt", return_value=receipt
            ) as probe:
                status = runner.main(["provenance-smoke"])
                self.assertEqual(os.read(ready, 1), b"R")
                payload = os.read(completed, 65537)

        self.assertEqual(status, 0)
        self.assertEqual(payload, canonical_json_bytes(receipt))
        probe.assert_called_once_with(runtime_site, "test-only-placeholder")

    def test_runner_rejects_duplicate_smoke_arguments(self):
        with tempfile.TemporaryDirectory() as tmp:
            runtime_site = Path(tmp).resolve()
            with self._runner_descriptors(runtime_site) as (ready, completed), patch.object(
                runner, "_provenance_smoke_receipt"
            ) as probe:
                status = runner.main(["provenance-smoke", "provenance-smoke"])
                self.assertEqual(os.read(ready, 1), b"R")
                self.assertEqual(os.read(completed, 1), b"")

        self.assertNotEqual(status, 0)
        probe.assert_not_called()

    def test_runner_rejects_oversized_smoke_receipt(self):
        receipt = {
            "schema": "txnmem-provenance-smoke-child-receipt-v1",
            "qdrant_proxy_ok": True,
            "neo4j_proxy_ok": True,
        }
        with tempfile.TemporaryDirectory() as tmp:
            runtime_site = Path(tmp).resolve()
            with self._runner_descriptors(runtime_site) as (ready, completed), patch.object(
                runner, "_provenance_smoke_receipt", return_value=receipt
            ), patch.object(
                runner, "_completion_payload", return_value=b"x" * 65537
            ):
                status = runner.main(["provenance-smoke"])
                self.assertEqual(os.read(ready, 1), b"R")
                self.assertEqual(os.read(completed, 1), b"")

        self.assertNotEqual(status, 0)

    def test_runner_uses_exact_loopback_probes_and_both_must_succeed(self):
        runtime_site = Path("/runtime-fixture")
        with patch.object(
            runner, "_probe_smoke_qdrant", return_value=True
        ) as qdrant, patch.object(
            runner, "_probe_smoke_neo4j", return_value=True
        ) as neo4j:
            receipt = runner._provenance_smoke_receipt(
                runtime_site, "test-only-placeholder"
            )

        self.assertEqual(
            receipt,
            {
                "schema": "txnmem-provenance-smoke-child-receipt-v1",
                "qdrant_proxy_ok": True,
                "neo4j_proxy_ok": True,
            },
        )
        qdrant.assert_called_once_with("http://127.0.0.1:19000/readyz")
        neo4j.assert_called_once_with(
            runtime_site=runtime_site,
            neo4j_uri="bolt://127.0.0.1:19001",
            neo4j_user="neo4j",
            neo4j_password="test-only-placeholder",
        )

        for failed_probe in ("_probe_smoke_qdrant", "_probe_smoke_neo4j"):
            with self.subTest(failed_probe=failed_probe), patch.object(
                runner, "_probe_smoke_qdrant", return_value=True
            ), patch.object(
                runner, "_probe_smoke_neo4j", return_value=True
            ), patch.object(runner, failed_probe, return_value=False):
                with self.assertRaises(RuntimeError):
                    runner._provenance_smoke_receipt(
                        runtime_site, "test-only-placeholder"
                    )

    def test_neo4j_probe_executes_one_explicit_return_one_transaction(self):
        events: list[object] = []

        class Result:
            def single(self, *, strict):
                events.append(("single", strict))
                return {"value": 1}

        class Transaction:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def run(self, query):
                events.append(("query", query))
                return Result()

            def commit(self):
                events.append("commit")

        class Session:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def begin_transaction(self):
                events.append("begin")
                return Transaction()

        class Driver:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def session(self):
                events.append("session")
                return Session()

        class GraphDatabase:
            @staticmethod
            def driver(uri, *, auth):
                events.append(("driver", uri, auth[0]))
                return Driver()

        with patch.object(
            runner, "_load_smoke_graph_database", return_value=GraphDatabase
        ):
            succeeded = runner._probe_smoke_neo4j(
                runtime_site=Path("/runtime-fixture"),
                neo4j_uri="bolt://127.0.0.1:19001",
                neo4j_user="neo4j",
                neo4j_password="test-only-placeholder",
            )

        self.assertIs(succeeded, True)
        self.assertEqual(
            events,
            [
                ("driver", "bolt://127.0.0.1:19001", "neo4j"),
                "session",
                "begin",
                ("query", "RETURN 1 AS value"),
                ("single", True),
                "commit",
            ],
        )


if __name__ == "__main__":
    unittest.main()
