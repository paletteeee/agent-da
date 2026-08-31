from __future__ import annotations

import contextlib
import copy
import ctypes
import errno
import hashlib
import json
import os
import signal
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import txnmem_formal_smoke as smoke
import txnmem_provenance_execution_collector as collector
import txnmem_provenance_performance as performance
import txnmem_provenance_progress as progress
import txnmem_provenance_runner as runner
from txnmem_formal_io import FormalStore, canonical_json_bytes
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
            "schema": "txnmem-provenance-smoke-child-receipt-v2",
            "scenario": "normal_prefix",
            "outcome": "succeeded",
            "completed_repetitions": 2,
            "qdrant_proxy_ok": True,
            "neo4j_proxy_ok": True,
        }

    def release(self):
        self.events.append("child_released")

    def wait_with_receipt(self, *, timeout=None):
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
        environment_snapshot = workspace_path / "environment.json"
        environment_document = {
            "schema": "txnmem-provenance-environment-v1",
            "isolation_verified": True,
            "co_tenant_load_detected": False,
            "source": "collector-observation-v2",
            "cpu_logical_count": 1,
            "memory_total_bytes": 1,
            "disk_medium": "nvme",
            "toxiproxy_version": "2.5.0",
        }
        environment_raw = canonical_json_bytes(environment_document) + b"\n"
        environment_sha256 = hashlib.sha256(
            canonical_json_bytes(environment_document)
        ).hexdigest()
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

        def write_environment(_parent, document):
            self.assertEqual(document, environment_document)
            environment_snapshot.write_bytes(environment_raw)
            return environment_snapshot, environment_raw

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

        receipt_validator = smoke._validate_smoke_v2_child_receipt

        def validate_receipt(value, *, scenario):
            normalized = receipt_validator(value, scenario=scenario)
            if (
                scenario == "normal_prefix"
                and normalized["qdrant_proxy_ok"] is True
                and "runner_qdrant_success" not in events
            ):
                events.append("runner_qdrant_success")
            if (
                scenario == "normal_prefix"
                and normalized["neo4j_proxy_ok"] is True
                and "runner_neo4j_success" not in events
            ):
                events.append("runner_neo4j_success")
            return normalized

        def remove_workspace(value):
            events.append("workspace_removed")
            shutil.rmtree(value.path)

        scenario_specs = tuple(
            smoke._SmokeV2ScenarioSpec(
                scenario=scenario,
                identity_sha256=str(index) * 64,
                directory=workspace_path / f"diagnostic-{index}",
                progress_path=workspace_path
                / f"diagnostic-{index}"
                / "progress.json",
            )
            for index, scenario in enumerate(
                smoke._SMOKE_V2_SCENARIOS, start=1
            )
        )

        def run_scenarios(**_kwargs):
            self.assertEqual(
                _kwargs["environment_attestation_path"],
                environment_snapshot,
            )
            self.assertEqual(
                _kwargs["environment_attestation_sha256"],
                environment_sha256,
            )
            events.append("child_gated")
            initial = smoke._validate_smoke_guard(guard.activate(), topology)
            routes_b = smoke.prepare_isolated_toxiproxy_routes(
                "http://127.0.0.1:8474",
                qdrant_proxy="txnmem-qdrant",
                neo4j_proxy="txnmem-neo4j",
            )
            baseline_b = smoke.capture_toxiproxy_counter_snapshot(
                "http://127.0.0.1:8474",
                phase="baseline_b",
                proxy_routes=routes_b,
            )
            smoke._validate_toxiproxy_attribution_boundary(
                snapshots["baseline_a"],
                baseline_b,
                ROUTES,
                routes_b,
            )
            if smoke._probe_root_management(
                "http://127.0.0.1:8474", routes_b
            ) is not True:
                raise smoke.FormalSmokeError("fixture management failure")
            if smoke._probe_root_data_denial(
                qdrant_url="http://127.0.0.1:19000",
                neo4j_uri="bolt://127.0.0.1:19001",
            ) is not True:
                raise smoke.FormalSmokeError("fixture proxy denial failure")
            if smoke._probe_direct_backend_denial(
                topology.backend_ipv4_by_role
            ) is not True:
                raise smoke.FormalSmokeError("fixture backend denial failure")
            if smoke._probe_forward_path_denial(
                backend_ipv4_by_role=topology.backend_ipv4_by_role,
                image_id=topology.probe_image_id,
            ) is not True:
                raise smoke.FormalSmokeError("fixture forward denial failure")
            child.release()
            exit_code, raw_receipt = child.wait_with_receipt(timeout=30.0)
            if exit_code != 0:
                raise smoke.FormalSmokeError("fixture runner failure")
            receipt = smoke._validate_smoke_v2_child_receipt(
                raw_receipt, scenario="normal_prefix"
            )
            observed_routes = smoke.observe_formal_toxiproxy_routes(
                "http://127.0.0.1:8474",
                qdrant_proxy="txnmem-qdrant",
                neo4j_proxy="txnmem-neo4j",
            )
            if observed_routes != routes_b:
                raise smoke.FormalSmokeError("fixture route drift")
            final_counters = smoke.capture_toxiproxy_counter_snapshot(
                "http://127.0.0.1:8474",
                phase="final",
                proxy_routes=observed_routes,
            )
            final_guard = smoke._validate_smoke_guard(guard.verify(), topology)
            if canonical_json_bytes(final_guard) != canonical_json_bytes(initial):
                raise smoke.FormalSmokeError("fixture guard drift")

            cleanup_failures = list(
                smoke._cleanup_formal_execution_resources(
                    execution_monitor=None,
                    network_guard=None,
                    child=child,
                )
            )
            child_quiescent = False
            try:
                smoke._verify_smoke_child_quiescence(child)
                child_quiescent = True
            except BaseException as exc:
                cleanup_failures.append(exc)
            if child_quiescent and guard.active:
                try:
                    guard.deactivate()
                except BaseException as exc:
                    cleanup_failures.append(exc)
            if cleanup_failures:
                raise smoke.FormalSmokeError("fixture cleanup failure")

            outcomes = FormalSmokeV2ContractTests()._outcomes()
            outcomes[0]["receipt"] = receipt
            return smoke._SmokeV2Execution(
                outcomes=tuple(outcomes),
                normal_receipt=receipt,
                routes_b=[dict(row) for row in routes_b],
                baseline_b=baseline_b,
                final_counters=final_counters,
                initial_guard=initial,
            )

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
            "_collect_formal_environment_attestation": {
                "return_value": copy.deepcopy(environment_document)
            },
            "_write_collected_environment_snapshot": {
                "side_effect": write_environment
            },
            "_publish_formal_input_tree": {"return_value": {}},
            "_create_smoke_v2_scenario_specs": {
                "return_value": scenario_specs
            },
            "_run_smoke_v2_scenarios": {"side_effect": run_scenarios},
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
            "_validate_smoke_v2_child_receipt": {
                "side_effect": validate_receipt
            },
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
            self.assertEqual(
                report["schema"],
                "txnmem-formal-provenance-smoke-v2",
            )
            for field in (
                "progress_monotonic",
                "formal_fail_fast",
                "backend_timeout_bounded",
                "interruption_cleanup",
                "candidate_unpublished",
            ):
                self.assertIs(report[field], True)
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
        for field in (
            "progress_monotonic",
            "formal_fail_fast",
            "backend_timeout_bounded",
            "interruption_cleanup",
            "candidate_unpublished",
        ):
            missing = dict(report)
            missing.pop(field)
            mutations.append(missing)
            type_confused = dict(report)
            type_confused[field] = 1
            mutations.append(type_confused)

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


class FormalSmokeV2ContractTests(unittest.TestCase):
    def test_child_spec_accepts_only_the_four_private_v2_scenarios(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            export = root / "source"
            runtime = root / "runtime"
            environment_path = root / "environment.json"
            (export / "src").mkdir(parents=True)
            runtime.mkdir()
            (export / "src" / "txnmem_provenance_runner.py").write_text(
                "# fixture\n", encoding="utf-8"
            )
            environment_document = {
                "schema": "txnmem-provenance-environment-v1",
                "isolation_verified": True,
                "co_tenant_load_detected": False,
                "source": "collector-observation-v2",
                "cpu_logical_count": 1,
                "memory_total_bytes": 1,
                "disk_medium": "nvme",
                "toxiproxy_version": "2.5.0",
            }
            environment_path.write_bytes(
                canonical_json_bytes(environment_document) + b"\n"
            )
            environment_hash = hashlib.sha256(
                canonical_json_bytes(environment_document)
            ).hexdigest()
            protected_metadata = SimpleNamespace(
                st_mode=stat.S_IFREG | 0o440,
                st_uid=0,
                st_gid=smoke.FORMAL_RUNNER_GID,
            )
            with patch.object(
                smoke, "_file_sha256", return_value="c" * 64
            ), patch.object(
                smoke, "verify_immutable_runtime_snapshot", return_value=None
            ), patch.object(
                Path, "lstat", return_value=protected_metadata
            ):
                for scenario in smoke._SMOKE_V2_SCENARIOS:
                    child = smoke._build_smoke_child_spec(
                        source_export=export,
                        runtime_snapshot=runtime,
                        runtime_manifest={"schema": "fixture"},
                        runner_sha256="c" * 64,
                        neo4j_password="test-only-placeholder",
                        scenario=scenario,
                        environment_attestation_path=environment_path,
                        environment_attestation_sha256=environment_hash,
                    )
                    self.assertEqual(
                        child.command[-2:],
                        ("provenance-smoke-v2", scenario),
                    )
                    self.assertEqual(
                        child.environment[
                            "TXNMEM_PROVENANCE_SMOKE_ENVIRONMENT_PATH"
                        ],
                        str(environment_path),
                    )

                with self.assertRaisesRegex(
                    smoke.FormalSmokeError, "scenario"
                ):
                    smoke._build_smoke_child_spec(
                        source_export=export,
                        runtime_snapshot=runtime,
                        runtime_manifest={"schema": "fixture"},
                        runner_sha256="c" * 64,
                        neo4j_password="test-only-placeholder",
                        scenario="normal_prefix/../../candidate",
                        environment_attestation_path=environment_path,
                        environment_attestation_sha256=environment_hash,
                    )

    @staticmethod
    def _snapshot(
        sequence: int,
        *,
        status: str,
        reason: str | None = None,
        identity_sha256: str = "a" * 64,
    ) -> dict:
        if sequence == 0:
            cell_index = 1
            graph_size = 100
            concurrency = 1
            repetition_index = 0
        else:
            cell_index = (sequence - 1) // 30 + 1
            graph_size, concurrency = progress.FORMAL_MATRIX_CELLS[
                cell_index - 1
            ]
            repetition_index = (sequence - 1) % 30 + 1
        value = {
            "schema": progress.PROGRESS_SNAPSHOT_SCHEMA,
            "run_binding_sha256": identity_sha256,
            "config_sha256": "b" * 64,
            "phase": "measurement",
            "cell_index": cell_index,
            "cell_count": 15,
            "graph_size": graph_size,
            "concurrency": concurrency,
            "repetition_index": repetition_index,
            "repetition_count": 30,
            "completed_repetitions": sequence,
            "total_repetitions": 450,
            "completed_samples": sequence * 32,
            "total_samples": 14400,
            "update_sequence": sequence,
            "status": status,
            "last_update_age_seconds": 0,
        }
        if reason is not None:
            value["terminal_reason_class"] = reason
        return json.loads(progress.canonical_snapshot_line(value))

    @staticmethod
    def _receipt(
        scenario: str,
        outcome: str,
        completed: int,
        qdrant_ok: bool,
        neo4j_ok: bool,
    ) -> dict:
        return {
            "schema": "txnmem-provenance-smoke-child-receipt-v2",
            "scenario": scenario,
            "outcome": outcome,
            "completed_repetitions": completed,
            "qdrant_proxy_ok": qdrant_ok,
            "neo4j_proxy_ok": neo4j_ok,
        }

    def _outcomes(self) -> list[dict]:
        return [
            {
                "scenario": "normal_prefix",
                "identity_sha256": "1" * 64,
                "progress": self._snapshot(
                    2,
                    status="running",
                    identity_sha256="1" * 64,
                ),
                "receipt": self._receipt(
                    "normal_prefix", "succeeded", 2, True, True
                ),
                "elapsed_milliseconds": 100,
                "child_quiescent": True,
                "guard_removed": True,
                "candidate_artifact_count": 0,
            },
            {
                "scenario": "first_ineligible",
                "identity_sha256": "2" * 64,
                "progress": self._snapshot(
                    0,
                    status="blocked",
                    reason="formal_eligibility_failed",
                    identity_sha256="2" * 64,
                ),
                "receipt": self._receipt(
                    "first_ineligible",
                    "formal_ineligible",
                    0,
                    False,
                    False,
                ),
                "elapsed_milliseconds": 50,
                "child_quiescent": True,
                "guard_removed": True,
                "candidate_artifact_count": 0,
            },
            {
                "scenario": "backend_timeout",
                "identity_sha256": "3" * 64,
                "progress": self._snapshot(
                    0,
                    status="blocked",
                    reason="backend_timeout",
                    identity_sha256="3" * 64,
                ),
                "receipt": self._receipt(
                    "backend_timeout",
                    "backend_timeout",
                    0,
                    False,
                    False,
                ),
                "elapsed_milliseconds": 1200,
                "child_quiescent": True,
                "guard_removed": True,
                "candidate_artifact_count": 0,
            },
            {
                "scenario": "interruption",
                "identity_sha256": "4" * 64,
                "progress": self._snapshot(
                    0,
                    status="interrupted",
                    reason="collector_interrupted",
                    identity_sha256="4" * 64,
                ),
                "receipt": None,
                "elapsed_milliseconds": 200,
                "child_quiescent": True,
                "guard_removed": True,
                "candidate_artifact_count": 0,
            },
        ]

    def test_scenario_specs_have_distinct_derived_identities_and_private_progress_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            workspace = Path(tmp).resolve() / "smoke-workspace"
            workspace.mkdir(mode=0o700)
            specs = smoke._create_smoke_v2_scenario_specs(
                workspace,
                source_commit="a" * 40,
                controller_uid=os.getuid(),
                controller_gid=os.getgid(),
            )

            self.assertEqual(
                [spec.scenario for spec in specs],
                [
                    "normal_prefix",
                    "first_ineligible",
                    "backend_timeout",
                    "interruption",
                ],
            )
            identities = {spec.identity_sha256 for spec in specs}
            self.assertEqual(len(identities), 4)
            for spec in specs:
                self.assertRegex(spec.identity_sha256, r"^[0-9a-f]{64}$")
                self.assertEqual(spec.progress_path.name, "progress.json")
                self.assertEqual(spec.progress_path.parent, spec.directory)
                self.assertEqual(spec.directory.parent, workspace)
                self.assertNotIn("candidate", str(spec.progress_path).lower())
                self.assertEqual(spec.directory.stat().st_mode & 0o777, 0o700)

            with self.assertRaises(smoke.FormalSmokeError):
                smoke._create_smoke_v2_scenario_specs(
                    workspace,
                    source_commit="a" * 40,
                    controller_uid=os.getuid(),
                    controller_gid=os.getgid(),
                )

    def test_v2_outcome_closure_derives_all_five_booleans_without_identity_output(self):
        outcomes = self._outcomes()
        observed = smoke._validate_smoke_v2_outcomes(outcomes)
        self.assertEqual(
            observed,
            {
                "progress_monotonic": True,
                "formal_fail_fast": True,
                "backend_timeout_bounded": True,
                "interruption_cleanup": True,
                "candidate_unpublished": True,
            },
        )
        self.assertNotIn("identity", canonical_json_bytes(observed).decode())

    def test_v2_outcome_closure_rejects_every_false_or_ambiguous_proof(self):
        mutations = []
        duplicate_identity = self._outcomes()
        duplicate_identity[1]["identity_sha256"] = duplicate_identity[0][
            "identity_sha256"
        ]
        mutations.append(duplicate_identity)
        wrong_progress = self._outcomes()
        wrong_progress[0]["progress"] = self._snapshot(1, status="running")
        mutations.append(wrong_progress)
        not_fail_fast = self._outcomes()
        not_fail_fast[1]["progress"] = self._snapshot(1, status="running")
        mutations.append(not_fail_fast)
        slow_timeout = self._outcomes()
        slow_timeout[2]["elapsed_milliseconds"] = 6001
        mutations.append(slow_timeout)
        live_child = self._outcomes()
        live_child[3]["child_quiescent"] = False
        mutations.append(live_child)
        live_guard = self._outcomes()
        live_guard[3]["guard_removed"] = False
        mutations.append(live_guard)
        candidate = self._outcomes()
        candidate[0]["candidate_artifact_count"] = 1
        mutations.append(candidate)
        extra = self._outcomes()
        extra[0]["private_path"] = "/seeded/private/candidate"
        mutations.append(extra)

        for value in mutations:
            with self.subTest(value=value):
                with self.assertRaises(smoke.FormalSmokeError):
                    smoke._validate_smoke_v2_outcomes(value)


class FormalSmokeV2LifecycleTests(unittest.TestCase):
    def test_four_scenarios_use_distinct_progress_lifecycles_and_ordered_cleanup(self):
        events = []
        children = {}
        start_calls = []
        wait_timeouts = {}
        guard_index = 0

        class Process:
            def __init__(self, pid, args):
                self.pid = pid
                self.args = args
                self.alive = True

            def poll(self):
                return None if self.alive else 0

            def wait(self, timeout=None):
                if self.alive:
                    raise subprocess.TimeoutExpired(self.args, timeout)
                return 0

        class Child:
            def __init__(self, scenario, call):
                self.scenario = scenario
                self.process = Process(4300 + len(children), call["command"])
                self.store = progress.ProgressSnapshotStore(
                    call["progress_snapshot_path"],
                    expected_uid=os.getuid(),
                    expected_gid=os.getgid(),
                )
                self.store.write_starting(
                    call["progress_binding_sha256"],
                    call["progress_config_sha256"],
                )
                self.state = progress.FormalProgressState(
                    call["progress_binding_sha256"],
                    call["progress_config_sha256"],
                )
                self.terminal = collector._GatedCandidate(
                    process=self.process,
                    _release_fd=None,
                    _receipt_fd=None,
                    ready_observed=True,
                    _progress_store=self.store,
                    _progress_state=self.state,
                )
                self.waited = False
                self.terminated = None

            def bind_process_identity(self, value):
                self.terminal.bind_process_identity(value)
                events.append(f"{self.scenario}:bound")

            def release(self):
                events.append(f"{self.scenario}:released")
                if self.scenario == "normal_prefix":
                    for sequence in (1, 2):
                        event = progress.build_progress_event(
                            run_binding_sha256=self.store.read_view()[
                                "run_binding_sha256"
                            ],
                            config_sha256=self.store.read_view()[
                                "config_sha256"
                            ],
                            cell_index=1,
                            graph_size=100,
                            concurrency=1,
                            repetition_index=sequence,
                            completed_repetitions=sequence,
                            completed_samples=sequence * 32,
                            update_sequence=sequence,
                        )
                        self.store.write_running(self.state.consume(event))

            def wait_with_receipt(self, *, timeout=None):
                self.waited = True
                self.process.alive = False
                wait_timeouts[self.scenario] = timeout
                events.append(f"{self.scenario}:waited")
                receipts = {
                    "normal_prefix": (
                        "succeeded",
                        2,
                        True,
                        True,
                    ),
                    "first_ineligible": (
                        "formal_ineligible",
                        0,
                        False,
                        False,
                    ),
                    "backend_timeout": (
                        "backend_timeout",
                        0,
                        False,
                        False,
                    ),
                }
                outcome, count, qdrant_ok, neo4j_ok = receipts[self.scenario]
                return 0, {
                    "schema": "txnmem-provenance-smoke-child-receipt-v2",
                    "scenario": self.scenario,
                    "outcome": outcome,
                    "completed_repetitions": count,
                    "qdrant_proxy_ok": qdrant_ok,
                    "neo4j_proxy_ok": neo4j_ok,
                }

            def finish_progress(self, timeout, *, allow_empty=False):
                self.assert_allow_empty = allow_empty
                events.append(f"{self.scenario}:progress_finished")
                if self.scenario == "normal_prefix":
                    return self.store.read_view()
                return None

            def read_progress(self):
                return self.store.read_view()

            def block_progress(self, reason):
                events.append(f"{self.scenario}:blocked:{reason}")
                return collector._persist_blocked_progress(self.store, reason)

            def interrupt_progress(self):
                events.append(f"{self.scenario}:interrupted")
                return self.terminal.interrupt_progress()

            def terminate_validated_group(
                self,
                *,
                term_seconds,
                kill_seconds,
                require_signal=False,
            ):
                if require_signal and not self.process.alive:
                    raise AssertionError("fixture signal target already exited")
                self.terminated = (
                    term_seconds,
                    kill_seconds,
                    require_signal,
                )
                self.process.alive = False
                events.append(f"{self.scenario}:terminated")
                return require_signal

            def require_quiescence(self):
                if self.process.alive:
                    raise AssertionError("fixture child remained live")

            def close(self):
                events.append(f"{self.scenario}:child_closed")

        class Guard:
            def __init__(self, scenario):
                self.scenario = scenario
                self.active = False
                self.attestation = {
                    "schema": "txnmem-provenance-network-guard-v3",
                    "ruleset_sha256": "d" * 64,
                }

            def activate(self):
                self.active = True
                events.append(f"{self.scenario}:guard_on")
                return dict(self.attestation)

            def verify(self):
                events.append(f"{self.scenario}:guard_verified")
                return dict(self.attestation)

            def deactivate(self):
                self.active = False
                events.append(f"{self.scenario}:guard_off")

        def start_child(**call):
            scenario = call["command"][-1]
            start_calls.append(dict(call))
            child = Child(scenario, call)
            children[scenario] = child
            events.append(f"{scenario}:started")
            return child

        def make_guard(*_args, **_kwargs):
            nonlocal guard_index
            scenario = smoke._SMOKE_V2_SCENARIOS[guard_index]
            guard_index += 1
            return Guard(scenario)

        def cleanup_child(*, execution_monitor, network_guard, child):
            self.assertIsNone(execution_monitor)
            self.assertIsNone(network_guard)
            if child.process.alive:
                child.terminate_validated_group(
                    term_seconds=5.0, kill_seconds=5.0
                )
            child.close()
            return []

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            root.chmod(0o700)
            source = root / "source"
            runtime = root / "runtime"
            environment_path = root / "environment.json"
            source.mkdir()
            runtime.mkdir()
            environment_document = {
                "schema": "txnmem-provenance-environment-v1",
                "isolation_verified": True,
                "co_tenant_load_detected": False,
                "source": "collector-observation-v2",
                "cpu_logical_count": 1,
                "memory_total_bytes": 1,
                "disk_medium": "nvme",
                "toxiproxy_version": "2.5.0",
            }
            environment_path.write_bytes(
                canonical_json_bytes(environment_document) + b"\n"
            )
            environment_sha256 = hashlib.sha256(
                canonical_json_bytes(environment_document)
            ).hexdigest()
            specs = smoke._create_smoke_v2_scenario_specs(
                root,
                source_commit="a" * 40,
                controller_uid=os.getuid(),
                controller_gid=os.getgid(),
            )
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
            with contextlib.ExitStack() as stack:
                stack.enter_context(
                    patch.object(
                        smoke,
                        "_build_smoke_child_spec",
                        side_effect=lambda **kwargs: smoke._SmokeChildSpec(
                            command=(
                                "/usr/bin/python3",
                                "provenance-smoke-v2",
                                kwargs["scenario"],
                            ),
                            cwd=source,
                            environment={"LANG": "C.UTF-8"},
                        ),
                    )
                )
                stack.enter_context(
                    patch.object(smoke, "_start_gated_candidate", side_effect=start_child)
                )
                stack.enter_context(
                    patch.object(
                        smoke,
                        "_observe_formal_child_process",
                        side_effect=lambda pid, **_kwargs: {
                            "start_identity": f"candidate:{pid}:99"
                        },
                    )
                )
                stack.enter_context(
                    patch.object(smoke, "_require_formal_uid_processes", return_value={})
                )
                stack.enter_context(patch.object(smoke, "_NftNetworkGuard", side_effect=make_guard))
                stack.enter_context(
                    patch.object(
                        smoke,
                        "_validate_smoke_guard",
                        side_effect=lambda value, _topology: dict(value),
                    )
                )
                stack.enter_context(
                    patch.object(
                        smoke,
                        "prepare_isolated_toxiproxy_routes",
                        return_value=copy.deepcopy(ROUTES),
                    )
                )
                stack.enter_context(
                    patch.object(
                        smoke,
                        "capture_toxiproxy_counter_snapshot",
                        side_effect=lambda *_args, phase, **_kwargs: _counter_snapshot(
                            phase,
                            qdrant_delta=4 if phase == "final" else 0,
                            neo4j_delta=6 if phase == "final" else 0,
                        ),
                    )
                )
                stack.enter_context(
                    patch.object(
                        smoke,
                        "_validate_toxiproxy_attribution_boundary",
                        return_value=None,
                    )
                )
                stack.enter_context(patch.object(smoke, "_probe_root_management", return_value=True))
                stack.enter_context(patch.object(smoke, "_probe_root_data_denial", return_value=True))
                stack.enter_context(patch.object(smoke, "_probe_direct_backend_denial", return_value=True))
                stack.enter_context(patch.object(smoke, "_probe_forward_path_denial", return_value=True))
                stack.enter_context(
                    patch.object(
                        smoke,
                        "observe_formal_toxiproxy_routes",
                        return_value=copy.deepcopy(ROUTES),
                    )
                )
                stack.enter_context(
                    patch.object(
                        smoke,
                        "_install_smoke_timeout_toxic",
                        side_effect=lambda *_args, **_kwargs: events.append(
                            "backend_timeout:toxic_on"
                        ),
                    )
                )
                stack.enter_context(
                    patch.object(
                        smoke,
                        "_remove_smoke_timeout_toxic",
                        side_effect=lambda *_args, **_kwargs: events.append(
                            "backend_timeout:toxic_off"
                        ),
                    )
                )
                stack.enter_context(
                    patch.object(
                        smoke,
                        "_cleanup_formal_execution_resources",
                        side_effect=cleanup_child,
                    )
                )
                stack.enter_context(
                    patch.object(
                        smoke,
                        "_verify_smoke_child_quiescence",
                        side_effect=lambda child: child.require_quiescence(),
                    )
                )
                execution = smoke._run_smoke_v2_scenarios(
                    scenario_specs=specs,
                    source_export=source,
                    runtime_snapshot=runtime,
                    runtime_manifest={"schema": "fixture"},
                    runner_sha256="c" * 64,
                    neo4j_password="test-only-placeholder",
                    environment_attestation_path=environment_path,
                    environment_attestation_sha256=environment_sha256,
                    topology=topology,
                    qdrant_url="http://127.0.0.1:19000",
                    neo4j_uri="bolt://127.0.0.1:19001",
                    toxiproxy_url="http://127.0.0.1:8474",
                    routes_a=copy.deepcopy(ROUTES),
                    baseline_a=_counter_snapshot("baseline_a"),
                )

        proofs = smoke._validate_smoke_v2_outcomes(execution.outcomes)
        self.assertTrue(all(proofs.values()))
        self.assertEqual(len(start_calls), 4)
        self.assertEqual(
            [call["command"][-1] for call in start_calls],
            list(smoke._SMOKE_V2_SCENARIOS),
        )
        self.assertEqual(
            len({call["progress_binding_sha256"] for call in start_calls}),
            4,
        )
        self.assertEqual(
            len({call["progress_snapshot_path"] for call in start_calls}),
            4,
        )
        self.assertEqual(
            wait_timeouts,
            {
                "normal_prefix": 900.0,
                "first_ineligible": 30.0,
                "backend_timeout": 6.0,
            },
        )
        for call in start_calls:
            self.assertIs(call["require_completion_receipt"], True)
            self.assertIs(call["require_progress"], True)
            self.assertIs(call["progress_allow_empty"], True)
            self.assertEqual(call["progress_expected_uid"], 0)
            self.assertEqual(call["progress_expected_gid"], 0)

        timeout_events = [
            events.index("backend_timeout:child_closed"),
            events.index("backend_timeout:toxic_off"),
            events.index("backend_timeout:guard_off"),
        ]
        self.assertEqual(timeout_events, sorted(timeout_events))
        interruption = children["interruption"]
        self.assertEqual(interruption.terminated, (5.0, 5.0, True))
        self.assertFalse(interruption.waited)
        self.assertLess(
            events.index("interruption:terminated"),
            events.index("interruption:guard_off"),
        )


class FormalSmokeProbeTests(unittest.TestCase):
    def test_timeout_toxic_has_exact_delay_and_explicit_ambiguous_cleanup(self):
        calls = []

        def request(base_url, path, **kwargs):
            calls.append((base_url, path, dict(kwargs)))
            if kwargs["method"] == "POST":
                return {
                    "name": "txnmem-formal-smoke-timeout-v2",
                    "type": "latency",
                    "stream": "downstream",
                    "toxicity": 1.0,
                    "attributes": {"latency": 2000, "jitter": 0},
                }
            return None

        with patch.object(
            smoke, "_toxiproxy_json_request", side_effect=request
        ):
            smoke._install_smoke_timeout_toxic("http://127.0.0.1:8474")
            smoke._remove_smoke_timeout_toxic(
                "http://127.0.0.1:8474", allow_not_found=True
            )

        self.assertEqual(len(calls), 2)
        self.assertEqual(
            calls[0][2],
            {
                "method": "POST",
                "payload": {
                    "name": "txnmem-formal-smoke-timeout-v2",
                    "type": "latency",
                    "stream": "downstream",
                    "toxicity": 1.0,
                    "attributes": {"latency": 2000, "jitter": 0},
                },
            },
        )
        self.assertEqual(
            calls[1][2],
            {"method": "DELETE", "allow_not_found": True},
        )

    def test_root_health_accepts_exact_plain_text_toxiproxy_version(self):
        with patch.object(
            smoke, "_http_read", return_value=(b"2.5.0", 0.0)
        ), patch.object(
            smoke, "_run_one_neo4j_transaction", return_value=True
        ):
            self.assertEqual(
                smoke._probe_root_health(
                    qdrant_url="http://127.0.0.1:19000",
                    neo4j_uri="bolt://127.0.0.1:19001",
                    toxiproxy_url="http://127.0.0.1:8474",
                    neo4j_password="test-only-placeholder",
                    runtime_snapshot=Path("/unused"),
                ),
                "2.5.0",
            )

    def test_root_health_rejects_registered_toxiproxy_version_drift(self):
        with patch.object(
            smoke, "_http_read", return_value=(b"2.9.0", 0.0)
        ), patch.object(
            smoke, "_run_one_neo4j_transaction", return_value=True
        ):
            with self.assertRaisesRegex(
                smoke.FormalSmokeError, "version drifted"
            ):
                smoke._probe_root_health(
                    qdrant_url="http://127.0.0.1:19000",
                    neo4j_uri="bolt://127.0.0.1:19001",
                    toxiproxy_url="http://127.0.0.1:8474",
                    neo4j_password="test-only-placeholder",
                    runtime_snapshot=Path("/unused"),
                )

    def test_root_health_sanitizes_collector_version_parse_failures(self):
        payload = b"2.5.0\n"
        with patch.object(
            smoke, "_http_read", return_value=(payload, 0.0)
        ), patch.object(
            smoke, "_run_one_neo4j_transaction", return_value=True
        ):
            with self.assertRaisesRegex(
                smoke.FormalSmokeError,
                "formal smoke Toxiproxy version response is invalid",
            ) as raised:
                smoke._probe_root_health(
                    qdrant_url="http://127.0.0.1:19000",
                    neo4j_uri="bolt://127.0.0.1:19001",
                    toxiproxy_url="http://127.0.0.1:8474",
                    neo4j_password="test-only-placeholder",
                    runtime_snapshot=Path("/unused"),
                )

        self.assertIsInstance(
            raised.exception.__cause__, collector.CollectorError
        )
        self.assertNotIn(repr(payload), str(raised.exception))
        self.assertNotIn(repr(payload), str(raised.exception.__cause__))

    def test_docker_not_found_requires_exact_status_stdout_and_stderr(self):
        ref = "a" * 64
        exact = SimpleNamespace(
            returncode=1,
            stdout="",
            stderr=f"Error response from daemon: No such object: {ref}\n",
        )
        self.assertIs(smoke._docker_inspect_is_not_found(exact, ref), True)
        exact_modern_cli = SimpleNamespace(
            returncode=1,
            stdout="[]\n",
            stderr=f"error: no such object: {ref}\n",
        )
        self.assertIs(
            smoke._docker_inspect_is_not_found(exact_modern_cli, ref), True
        )

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
                stderr=f"error: no such object: {ref}\n",
            ),
            SimpleNamespace(
                returncode=1,
                stdout="[]\n",
                stderr=f"error: no such object: {ref}\nextra\n",
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
        script = create[create.index("-c") + 1]
        self.assertIn(
            'probe_error=$({ exec 3<>"/dev/tcp/$1/$2"; } 2>&1)',
            script,
        )
        self.assertNotIn('probe_error=$(exec 3<>', script)
        probe_arguments = create[create.index("txnmem-forward-probe") :]
        self.assertEqual(
            probe_arguments,
            (
                "txnmem-forward-probe",
                "192.0.2.2",
                "6333",
                "192.0.2.3",
                "7687",
            ),
        )
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
    def test_first_ineligible_helper_exercises_the_real_formal_gate(self):
        self.assertIsNone(runner._prove_smoke_first_repetition_ineligible())

    def setUp(self):
        super().setUp()
        self._real_runner_harden = runner._harden_execd_formal_runner
        self._real_runner_preflight = (
            runner._require_credential_matched_publication_preflight
        )
        self._runner_harden_patch = patch.object(
            runner, "_harden_execd_formal_runner", return_value=None
        )
        self._runner_preflight_patch = patch.object(
            runner,
            "_require_credential_matched_publication_preflight",
            return_value=None,
        )
        self._runner_harden_patch.start()
        self._runner_preflight_patch.start()
        self._runner_previous_mask = None
        if hasattr(signal, "pthread_sigmask") and hasattr(signal, "sigwait"):
            self._runner_previous_mask = signal.pthread_sigmask(
                signal.SIG_SETMASK, {signal.SIGTERM}
            )
            if signal.SIGTERM in signal.sigpending():
                signal.sigwait({signal.SIGTERM})

    def tearDown(self):
        try:
            if self._runner_previous_mask is not None:
                if signal.SIGTERM in signal.sigpending():
                    signal.sigwait({signal.SIGTERM})
                signal.pthread_sigmask(
                    signal.SIG_SETMASK, self._runner_previous_mask
                )
        finally:
            self._runner_preflight_patch.stop()
            self._runner_harden_patch.stop()
            super().tearDown()

    def _run_publication_signal_scenario(self, scenario):
        import txnmem_experiment
        import txnmem_provenance_progress

        if scenario not in {
            "progress_callback",
            "before_publication",
            "during_staging",
            "during_commit",
            "after_commit_before_receipt",
        }:
            raise AssertionError("unknown publication signal scenario")
        gate_read, gate_write = os.pipe()
        ready_read, ready_write = os.pipe()
        receipt_read, receipt_write = os.pipe()
        progress_read, progress_write = os.pipe()
        os.write(gate_write, b"G")
        os.close(gate_write)
        signal_sent = False
        mask_observations = []
        real_experiment_main = txnmem_experiment.main
        real_aggregate = performance.aggregate_matrix
        real_publish = performance.publish_provenance_bundle
        real_store_write = FormalStore.write_json_exclusive

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            runtime_site = root / "runtime"
            runtime_site.mkdir()
            candidate = root / "candidate"
            config = root / "config.json"
            config.write_text(
                json.dumps(
                    {
                        "schema": "txnmem-provenance-performance-v2",
                        "graph_node_counts": [2],
                        "concurrency_levels": [1],
                        "repetitions": 1,
                        "graph_seed": 17,
                        "operations_per_type": 1,
                        "bootstrap_repetitions": 10,
                        "bootstrap_seed": 17,
                        "request_timeout_seconds": 30.0,
                    }
                ),
                encoding="utf-8",
            )
            environment = {
                "TXNMEM_PROVENANCE_START_GATE_FD": str(gate_read),
                "TXNMEM_PROVENANCE_READY_FD": str(ready_write),
                "TXNMEM_PROVENANCE_COMPLETION_FD": str(receipt_write),
                "TXNMEM_PROVENANCE_PROGRESS_FD": str(progress_write),
                "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256": "a" * 64,
                "TXNMEM_PROVENANCE_RUNTIME_SITE": str(runtime_site),
            }

            def send_sigterm_once():
                nonlocal signal_sent
                if not signal_sent:
                    signal_sent = True
                    os.kill(os.getpid(), signal.SIGTERM)

            def experiment_entry(arguments, **hooks):
                converted = list(arguments)
                backend_index = converted.index("--backend") + 1
                converted[backend_index] = "memory"
                private_hooks = {
                    "_progress_callback": hooks["_progress_callback"],
                    "_require_formal_eligibility": False,
                }
                if "_interruption_check" in hooks:
                    private_hooks["_interruption_check"] = hooks[
                        "_interruption_check"
                    ]
                return real_experiment_main(converted, **private_hooks)

            def aggregate_wrapper(*args, **kwargs):
                result = real_aggregate(*args, **kwargs)
                if scenario == "before_publication":
                    send_sigterm_once()
                return result

            def publish_wrapper(*args, **kwargs):
                if scenario == "during_commit":
                    real_precommit = kwargs.get("_precommit_check")

                    def internal_precommit():
                        if real_precommit is None:
                            raise AssertionError(
                                "experiment omitted publisher precommit gate"
                            )
                        real_precommit()
                        current_mask = signal.pthread_sigmask(
                            signal.SIG_BLOCK, set()
                        )
                        mask_observations.append(
                            signal.SIGTERM in current_mask
                        )
                        send_sigterm_once()

                    kwargs["_precommit_check"] = internal_precommit
                return real_publish(*args, **kwargs)

            def observed_store_write(store, *parts, payload):
                result = real_store_write(store, *parts, payload=payload)
                if (
                    scenario == "during_staging"
                    and parts[-1] == "COMPLETED.json"
                ):
                    send_sigterm_once()
                if (
                    scenario == "after_commit_before_receipt"
                    and parts == ("bundles", f"{report_bundle_id[0]}.json")
                ):
                    send_sigterm_once()
                return result

            def build_progress_event(**_kwargs):
                if scenario == "progress_callback":
                    send_sigterm_once()
                return {"schema": "test-only-progress"}

            def completion_material(_arguments):
                pointers = sorted((candidate / "bundles").glob("*.json"))
                if len(pointers) != 1:
                    raise AssertionError("complete candidate pointer is unavailable")
                pointer = json.loads(pointers[0].read_text(encoding="utf-8"))
                if pointer.get("publication_status") != "complete":
                    raise AssertionError("candidate pointer is incomplete")
                return {"result": "complete"}

            report_bundle_id = [None]

            def remember_bundle_id(*args, **kwargs):
                report_bundle_id[0] = kwargs["bundle_id"]
                return publish_wrapper(*args, **kwargs)

            with patch.dict(os.environ, environment, clear=True), patch.object(
                txnmem_experiment, "main", side_effect=experiment_entry
            ), patch.object(
                performance, "aggregate_matrix", side_effect=aggregate_wrapper
            ), patch.object(
                performance,
                "publish_provenance_bundle",
                side_effect=remember_bundle_id,
            ), patch.object(
                FormalStore,
                "write_json_exclusive",
                new=observed_store_write,
            ), patch.object(
                txnmem_provenance_progress,
                "build_progress_event",
                side_effect=build_progress_event,
            ), patch.object(
                txnmem_provenance_progress,
                "canonical_progress_line",
                return_value=b"{}\n",
            ), patch.object(
                runner,
                "_candidate_completion_material",
                side_effect=completion_material,
            ):
                status = runner.main(
                    [
                        "provenance-performance",
                        "--backend",
                        "vector-graph",
                        "--config",
                        str(config),
                        "--run-id",
                        f"publication-{scenario}",
                        "--out-dir",
                        str(candidate),
                    ]
                )
            ready = os.read(ready_read, 2)
            receipt = os.read(receipt_read, 65537)
            pointer_paths = sorted((candidate / "bundles").glob("*.json"))
            pointers = [
                json.loads(path.read_text(encoding="utf-8"))
                for path in pointer_paths
            ]

        for descriptor in (ready_read, receipt_read, progress_read):
            os.close(descriptor)
        return status, ready, receipt, pointers, mask_observations

    def test_signal_before_publication_leaves_no_pointer_or_receipt(self):
        status, ready, receipt, pointers, _masks = (
            self._run_publication_signal_scenario("before_publication")
        )

        self.assertEqual(ready, b"R")
        self.assertNotEqual(status, 0)
        self.assertEqual(pointers, [])
        self.assertEqual(receipt, b"")

    def test_progress_callback_signal_leaves_no_pointer_or_receipt(self):
        status, ready, receipt, pointers, _masks = (
            self._run_publication_signal_scenario("progress_callback")
        )

        self.assertEqual(ready, b"R")
        self.assertNotEqual(status, 0)
        self.assertEqual(pointers, [])
        self.assertEqual(receipt, b"")

    def test_signal_during_staging_before_precommit_leaves_no_pointer_or_receipt(self):
        status, ready, receipt, pointers, _masks = (
            self._run_publication_signal_scenario("during_staging")
        )

        self.assertEqual(ready, b"R")
        self.assertNotEqual(status, 0)
        self.assertEqual(pointers, [])
        self.assertEqual(receipt, b"")

    @unittest.skipUnless(
        hasattr(signal, "pthread_sigmask"), "POSIX signal masks only"
    )
    def test_signal_during_guarded_commit_publishes_pointer_and_receipt(self):
        status, ready, receipt, pointers, masks = (
            self._run_publication_signal_scenario("during_commit")
        )

        self.assertEqual(ready, b"R")
        self.assertEqual(status, 0)
        self.assertEqual(len(pointers), 1)
        self.assertEqual(pointers[0]["publication_status"], "complete")
        self.assertEqual(receipt, canonical_json_bytes({"result": "complete"}))
        self.assertEqual(masks, [True])

    def test_signal_after_commit_before_receipt_keeps_complete_transaction(self):
        status, ready, receipt, pointers, _masks = (
            self._run_publication_signal_scenario(
                "after_commit_before_receipt"
            )
        )

        self.assertEqual(ready, b"R")
        self.assertEqual(status, 0)
        self.assertEqual(len(pointers), 1)
        self.assertEqual(pointers[0]["publication_status"], "complete")
        self.assertEqual(receipt, canonical_json_bytes({"result": "complete"}))

    @unittest.skipUnless(
        sys.platform.startswith("linux"), "protected Linux publication gate"
    )
    def test_protected_linux_publication_gate_never_mismatches_pointer_and_receipt(self):
        cases = (
            ("during_staging", False),
            ("during_commit", True),
            ("after_commit_before_receipt", True),
        )
        for scenario, committed in cases:
            with self.subTest(scenario=scenario):
                status, ready, receipt, pointers, _masks = (
                    self._run_publication_signal_scenario(scenario)
                )
                self.assertEqual(ready, b"R")
                if committed:
                    self.assertEqual(status, 0)
                    self.assertEqual(len(pointers), 1)
                    self.assertEqual(
                        receipt,
                        canonical_json_bytes({"result": "complete"}),
                    )
                else:
                    self.assertNotEqual(status, 0)
                    self.assertEqual(pointers, [])
                    self.assertEqual(receipt, b"")
                if signal.SIGTERM in signal.sigpending():
                    signal.sigwait({signal.SIGTERM})

    def test_runner_cooperative_stop_has_no_thread_or_hard_exit_claim(self):
        state_type = getattr(runner, "_RunnerTerminationState", None)
        self.assertIsNotNone(state_type)
        if state_type is None:
            return
        state = state_type()

        state.request_stop()

        self.assertIs(type(state.stop_requested), bool)
        self.assertNotIn("thread", vars(state))
        self.assertNotIn("_thread", vars(state))
        self.assertNotIn("hard_exit", vars(state))
        self.assertNotIn("_hard_exit", vars(state))
        with self.assertRaises(runner._RunnerInterruption):
            state.raise_if_requested()

    def test_runner_pending_sigterm_is_detected_only_at_safe_boundary(self):
        state_type = getattr(runner, "_RunnerTerminationState", None)
        self.assertIsNotNone(state_type)
        if state_type is None:
            return
        state = state_type()

        with patch.object(
            runner.signal, "sigpending", return_value={signal.SIGTERM}
        ), self.assertRaises(runner._RunnerInterruption):
            state.raise_if_requested()

        self.assertIs(state.stop_requested, True)

    def test_runner_requires_exact_blocked_sigterm_mask(self):
        checker = getattr(runner, "_require_controlled_sigterm_mask", None)
        self.assertIsNotNone(checker)
        if checker is None:
            return
        accepted = ({signal.SIGTERM},)
        rejected = (set(), {signal.SIGINT}, {signal.SIGTERM, signal.SIGINT})
        for observed in accepted:
            with self.subTest(observed=observed), patch.object(
                runner.signal, "pthread_sigmask", return_value=observed
            ):
                checker()
        for observed in rejected:
            with self.subTest(observed=observed), patch.object(
                runner.signal, "pthread_sigmask", return_value=observed
            ), self.assertRaisesRegex(RuntimeError, "signal mask"):
                checker()

    def test_runner_hardening_uses_exact_prctl_credentials_groups_and_mask(self):
        harden = self._real_runner_harden
        self.assertIsNotNone(harden)
        if harden is None:
            return
        calls = []

        def prctl(option, argument, third, fourth, fifth):
            calls.append((option, argument, third, fourth, fifth))
            if option == 2:
                ctypes.c_int.from_address(argument).value = signal.SIGKILL
                return 0
            return {4: 0, 3: 0, 39: 1}[option]

        with patch.object(runner.sys, "platform", "linux"), patch.object(
            runner.os, "getuid", return_value=runner.FORMAL_RUNNER_UID
        ), patch.object(
            runner.os, "geteuid", return_value=runner.FORMAL_RUNNER_UID
        ), patch.object(
            runner.os, "getgid", return_value=runner.FORMAL_RUNNER_GID
        ), patch.object(
            runner.os, "getegid", return_value=runner.FORMAL_RUNNER_GID
        ), patch.object(
            runner.os, "getgroups", return_value=[]
        ), patch.object(
            runner, "_require_controlled_sigterm_mask"
        ) as require_mask:
            harden(prctl=prctl)

        self.assertEqual(
            calls,
            [
                (4, 0, 0, 0, 0),
                (3, 0, 0, 0, 0),
                (39, 0, 0, 0, 0),
                (2, calls[3][1], 0, 0, 0),
            ],
        )
        require_mask.assert_called_once_with()

    def test_runner_hardening_rejects_every_credential_and_kernel_mismatch(self):
        harden = self._real_runner_harden
        self.assertIsNotNone(harden)
        if harden is None:
            return

        cases = {
            "set-dumpable": ({4: 1, 3: 0, 39: 1, 2: signal.SIGKILL}, {}),
            "set-dumpable-bool": ({4: False, 3: 0, 39: 1, 2: signal.SIGKILL}, {}),
            "get-dumpable": ({4: 0, 3: 1, 39: 1, 2: signal.SIGKILL}, {}),
            "get-dumpable-bool": ({4: 0, 3: False, 39: 1, 2: signal.SIGKILL}, {}),
            "no-new-privileges": ({4: 0, 3: 0, 39: 0, 2: signal.SIGKILL}, {}),
            "no-new-privileges-bool": ({4: 0, 3: 0, 39: True, 2: signal.SIGKILL}, {}),
            "real-uid": ({4: 0, 3: 0, 39: 1, 2: signal.SIGKILL}, {"getuid": 1}),
            "effective-uid": ({4: 0, 3: 0, 39: 1, 2: signal.SIGKILL}, {"geteuid": 1}),
            "real-gid": ({4: 0, 3: 0, 39: 1, 2: signal.SIGKILL}, {"getgid": 1}),
            "effective-gid": ({4: 0, 3: 0, 39: 1, 2: signal.SIGKILL}, {"getegid": 1}),
            "groups": ({4: 0, 3: 0, 39: 1, 2: signal.SIGKILL}, {"getgroups": [1]}),
            "parent-death": ({4: 0, 3: 0, 39: 1, 2: 0}, {}),
        }
        for name, (prctl_results, overrides) in cases.items():
            with self.subTest(name=name), patch.object(
                runner.sys, "platform", "linux"
            ), patch.object(
                runner.os,
                "getuid",
                return_value=overrides.get("getuid", runner.FORMAL_RUNNER_UID),
            ), patch.object(
                runner.os,
                "geteuid",
                return_value=overrides.get("geteuid", runner.FORMAL_RUNNER_UID),
            ), patch.object(
                runner.os,
                "getgid",
                return_value=overrides.get("getgid", runner.FORMAL_RUNNER_GID),
            ), patch.object(
                runner.os,
                "getegid",
                return_value=overrides.get("getegid", runner.FORMAL_RUNNER_GID),
            ), patch.object(
                runner.os,
                "getgroups",
                return_value=overrides.get("getgroups", []),
            ), patch.object(
                runner, "_require_controlled_sigterm_mask"
            ), self.assertRaisesRegex(RuntimeError, "runner"):
                def prctl(option, argument, _third, _fourth, _fifth):
                    if option == 2:
                        ctypes.c_int.from_address(argument).value = (
                            prctl_results[option]
                        )
                        return 0
                    return prctl_results[option]

                harden(prctl=prctl)

        class HardeningPrimary(BaseException):
            pass

        primary = HardeningPrimary("hardening-primary")

        def fail_prctl(*_arguments):
            raise primary

        with patch.object(runner.sys, "platform", "linux"), self.assertRaises(
            BaseException
        ) as raised:
            harden(prctl=fail_prctl)
        self.assertIs(raised.exception, primary)

    def test_runner_credential_preflight_finishes_before_ready_byte(self):
        events = []
        with tempfile.TemporaryDirectory() as tmp:
            runtime = Path(tmp).resolve() / "runtime"
            runtime.mkdir()
            candidate = Path(tmp).resolve() / "candidate"
            candidate.mkdir()
            environment = {
                "TXNMEM_PROVENANCE_START_GATE_FD": "10",
                "TXNMEM_PROVENANCE_READY_FD": "11",
                "TXNMEM_PROVENANCE_COMPLETION_FD": "12",
                "TXNMEM_PROVENANCE_PROGRESS_FD": "13",
                "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256": "a" * 64,
                "TXNMEM_PROVENANCE_RUNTIME_SITE": str(runtime),
            }

            def write(descriptor, payload):
                events.append(("ready", descriptor, payload))
                return len(payload)

            with patch.dict(os.environ, environment, clear=True), patch.object(
                runner,
                "_harden_execd_formal_runner",
                side_effect=lambda: events.append(("harden",)),
                create=True,
            ), patch.object(
                runner,
                "_require_credential_matched_publication_preflight",
                side_effect=lambda arguments: events.append(
                    ("preflight", tuple(arguments))
                ),
                create=True,
            ), patch.object(
                runner.os, "write", side_effect=write
            ), patch.object(
                runner.os, "read", return_value=b""
            ), patch.object(
                runner.os, "close"
            ):
                status = runner.main(
                    [
                        "provenance-performance",
                        "--out-dir",
                        str(candidate),
                    ]
                )

        self.assertEqual(status, 71)
        self.assertEqual(events[0], ("harden",))
        self.assertEqual(events[1][0], "preflight")
        self.assertEqual(events[2], ("ready", 11, b"R"))

    def test_runner_preflight_uses_only_validated_internal_out_dir(self):
        preflight = self._real_runner_preflight
        self.assertIsNotNone(preflight)
        if preflight is None:
            return

        class Store:
            def __init__(self, root):
                self.root = root

            def _require_fd_bound_publication_support(self):
                observations.append(self.root)

        observations = []
        candidate = "/formal/candidate"
        with patch("txnmem_formal_io.FormalStore", Store):
            preflight(
                [
                    "provenance-performance",
                    "--out-dir",
                    candidate,
                ]
            )
        self.assertEqual(observations, [candidate])

        for arguments in (
            ["provenance-performance"],
            ["provenance-performance", "--out-dir", candidate, "--out-dir", candidate],
        ):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                preflight(arguments)

    def test_runner_preflight_failure_writes_no_ready_and_starts_no_workload(self):
        with tempfile.TemporaryDirectory() as tmp:
            runtime = Path(tmp).resolve() / "runtime"
            runtime.mkdir()
            candidate = Path(tmp).resolve() / "candidate"
            candidate.mkdir()
            environment = {
                "TXNMEM_PROVENANCE_START_GATE_FD": "10",
                "TXNMEM_PROVENANCE_READY_FD": "11",
                "TXNMEM_PROVENANCE_COMPLETION_FD": "12",
                "TXNMEM_PROVENANCE_PROGRESS_FD": "13",
                "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256": "a" * 64,
                "TXNMEM_PROVENANCE_RUNTIME_SITE": str(runtime),
            }
            with patch.dict(os.environ, environment, clear=True), patch.object(
                runner,
                "_harden_execd_formal_runner",
                return_value=None,
                create=True,
            ), patch.object(
                runner,
                "_require_credential_matched_publication_preflight",
                side_effect=RuntimeError("runner-preflight-denied"),
                create=True,
            ), patch.object(
                runner.os, "write"
            ) as write, patch.object(
                runner.os, "read"
            ) as read, patch.object(
                runner.os, "close"
            ):
                status = runner.main(
                    [
                        "provenance-performance",
                        "--out-dir",
                        str(candidate),
                    ]
                )

        self.assertEqual(status, 70)
        write.assert_not_called()
        read.assert_not_called()

    def test_runner_cleanup_attempts_all_and_preserves_interruption(self):
        import txnmem_experiment

        gate_read, gate_write = os.pipe()
        ready_read, ready_write = os.pipe()
        receipt_read, receipt_write = os.pipe()
        progress_read, progress_write = os.pipe()
        os.write(gate_write, b"G")
        os.close(gate_write)
        environment = {
            "TXNMEM_PROVENANCE_START_GATE_FD": str(gate_read),
            "TXNMEM_PROVENANCE_READY_FD": str(ready_write),
            "TXNMEM_PROVENANCE_COMPLETION_FD": str(receipt_write),
            "TXNMEM_PROVENANCE_PROGRESS_FD": str(progress_write),
            "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256": "a" * 64,
        }
        cleanup_calls = []
        real_close = os.close

        def close_descriptor(descriptor):
            cleanup_calls.append(("close", descriptor))
            if descriptor in {progress_write, receipt_write}:
                raise OSError("private-runner-fd-cleanup-failure")
            real_close(descriptor)

        with tempfile.TemporaryDirectory() as tmp:
            runtime_site = Path(tmp).resolve() / "runtime"
            runtime_site.mkdir()
            environment["TXNMEM_PROVENANCE_RUNTIME_SITE"] = str(runtime_site)
            try:
                with patch.dict(os.environ, environment, clear=True), patch.object(
                    runner.os, "close", side_effect=close_descriptor
                ), patch.object(
                    txnmem_experiment,
                    "main",
                    side_effect=runner._RunnerInterruption(
                        "primary-runner-interruption"
                    ),
                ):
                    try:
                        status = runner.main(
                            [
                                "provenance-performance",
                                "--backend",
                                "vector-graph",
                            ]
                        )
                    except BaseException as exc:
                        self.fail(
                            "runner cleanup masked interruption: "
                            f"{type(exc).__name__}"
                        )
            finally:
                for descriptor in (
                    ready_read,
                    receipt_read,
                    receipt_write,
                    progress_read,
                    progress_write,
                ):
                    try:
                        real_close(descriptor)
                    except OSError:
                        pass

        self.assertEqual(status, 75)
        self.assertIn(("close", progress_write), cleanup_calls)
        self.assertIn(("close", receipt_write), cleanup_calls)

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

    @contextlib.contextmanager
    def _runner_v2_descriptors(self, runtime_site: Path):
        gate_read, gate_write = os.pipe()
        ready_read, ready_write = os.pipe()
        receipt_read, receipt_write = os.pipe()
        progress_read, progress_write = os.pipe()
        environment_path = runtime_site / "smoke-environment.json"
        environment_path.write_text(
            json.dumps(
                {
                    "schema": "txnmem-provenance-environment-v1",
                    "isolation_verified": True,
                    "co_tenant_load_detected": False,
                    "source": "collector-observation-v2",
                    "cpu_logical_count": 1,
                    "memory_total_bytes": 1,
                    "disk_medium": "nvme",
                    "toxiproxy_version": "2.5.0",
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        os.write(gate_write, b"G")
        os.close(gate_write)
        environment = {
            "TXNMEM_PROVENANCE_START_GATE_FD": str(gate_read),
            "TXNMEM_PROVENANCE_READY_FD": str(ready_write),
            "TXNMEM_PROVENANCE_COMPLETION_FD": str(receipt_write),
            "TXNMEM_PROVENANCE_PROGRESS_FD": str(progress_write),
            "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256": "a" * 64,
            "TXNMEM_PROVENANCE_RUNTIME_SITE": str(runtime_site),
            "TXNMEM_PROVENANCE_SMOKE_ENVIRONMENT_PATH": str(
                environment_path
            ),
            "TXNMEM_NEO4J_PASSWORD": "test-only-placeholder",
        }
        try:
            with patch.dict(os.environ, environment, clear=True):
                yield ready_read, receipt_read, progress_read
        finally:
            for descriptor in (ready_read, receipt_read, progress_read):
                try:
                    os.close(descriptor)
                except OSError:
                    pass

    def test_runner_v2_normal_prefix_emits_two_real_monotonic_progress_events(self):
        backend_factory = object()

        def run_prefix(factory, graph, *, concurrency, repetitions, **kwargs):
            self.assertIs(factory, backend_factory)
            self.assertEqual(graph.node_count, 100)
            self.assertEqual(concurrency, 1)
            self.assertEqual(repetitions, 2)
            self.assertEqual(kwargs["operations_per_type"], 8)
            self.assertIs(kwargs["require_formal_eligibility"], True)
            self.assertIs(kwargs["formal"], False)
            self.assertIs(
                kwargs["environment_attestation"]["isolation_verified"],
                True,
            )
            callback = kwargs["progress_callback"]
            callback(
                {
                    "cell_id": "n100-c1",
                    "completed_repetition_count": 1,
                    "completed_operation_sample_count": 32,
                }
            )
            callback(
                {
                    "cell_id": "n100-c1",
                    "completed_repetition_count": 2,
                    "completed_operation_sample_count": 64,
                }
            )
            return {
                "schema": performance.MATRIX_SCHEMA,
                "cell_id": "n100-c1",
                "graph": {"node_count": 100},
                "concurrency": 1,
                "repetition_count": 2,
                "operations_per_type": 8,
                "samples": [{} for _index in range(64)],
                "repetitions": [
                    {"eligible_for_formal": True},
                    {"eligible_for_formal": True},
                ],
                "formal_requested": False,
                "formal_eligible": True,
            }

        with tempfile.TemporaryDirectory() as tmp:
            runtime_site = Path(tmp).resolve()
            with self._runner_v2_descriptors(runtime_site) as (
                ready,
                completed,
                progress_stream,
            ), patch.object(
                runner, "_probe_smoke_qdrant"
            ) as qdrant, patch.object(
                runner, "_probe_smoke_neo4j"
            ) as neo4j, patch.object(
                performance,
                "formal_matrix_config_sha256",
                return_value="b" * 64,
            ), patch.object(
                performance,
                "make_vector_graph_backend_factory",
                return_value=backend_factory,
            ) as make_factory, patch.object(
                performance,
                "run_matrix_cell",
                side_effect=run_prefix,
            ) as run_cell:
                status = runner.main(
                    ["provenance-smoke-v2", "normal_prefix"]
                )
                self.assertEqual(os.read(ready, 1), b"R")
                receipt_payload = os.read(completed, 65537)
                progress_payload = os.read(progress_stream, 8192)

        self.assertEqual(status, 0)
        receipt = json.loads(receipt_payload)
        self.assertEqual(
            receipt,
            {
                "schema": "txnmem-provenance-smoke-child-receipt-v2",
                "scenario": "normal_prefix",
                "outcome": "succeeded",
                "completed_repetitions": 2,
                "qdrant_proxy_ok": True,
                "neo4j_proxy_ok": True,
            },
        )
        lines = progress_payload.splitlines(keepends=True)
        self.assertEqual(len(lines), 2)
        state = progress.FormalProgressState("a" * 64, "b" * 64)
        first = state.consume(progress.decode_progress_line(lines[0]))
        second = state.consume(progress.decode_progress_line(lines[1]))
        self.assertEqual(
            (
                first["cell_index"],
                first["graph_size"],
                first["concurrency"],
                first["repetition_index"],
                first["update_sequence"],
            ),
            (1, 100, 1, 1, 1),
        )
        self.assertEqual(
            (
                second["cell_index"],
                second["graph_size"],
                second["concurrency"],
                second["repetition_index"],
                second["completed_repetitions"],
                second["completed_samples"],
                second["update_sequence"],
            ),
            (1, 100, 1, 2, 2, 64, 2),
        )
        qdrant.assert_not_called()
        neo4j.assert_not_called()
        make_factory.assert_called_once()
        run_cell.assert_called_once()

    def test_runner_v2_first_ineligible_fails_fast_before_probe_or_progress(self):
        def reject_first_repetition(*_args, **kwargs):
            self.assertIs(kwargs["require_formal_eligibility"], True)
            self.assertIs(kwargs["formal"], False)
            self.assertEqual(kwargs["repetitions"], 1)
            self.assertIs(
                kwargs["environment_attestation"]["isolation_verified"],
                False,
            )
            raise performance.ProvenancePerformanceError(
                "formal run requires verified isolation without co-tenant load"
            )

        with tempfile.TemporaryDirectory() as tmp:
            runtime_site = Path(tmp).resolve()
            with self._runner_v2_descriptors(runtime_site) as (
                ready,
                completed,
                progress_stream,
            ), patch.object(runner, "_probe_smoke_qdrant") as qdrant, patch.object(
                runner, "_probe_smoke_neo4j"
            ) as neo4j, patch.object(
                performance,
                "run_matrix_cell",
                side_effect=reject_first_repetition,
            ) as eligibility_gate:
                status = runner.main(
                    ["provenance-smoke-v2", "first_ineligible"]
                )
                self.assertEqual(os.read(ready, 1), b"R")
                receipt_payload = os.read(completed, 65537)
                progress_payload = os.read(progress_stream, 8192)

        self.assertEqual(status, 0)
        self.assertEqual(progress_payload, b"")
        self.assertEqual(
            json.loads(receipt_payload),
            {
                "schema": "txnmem-provenance-smoke-child-receipt-v2",
                "scenario": "first_ineligible",
                "outcome": "formal_ineligible",
                "completed_repetitions": 0,
                "qdrant_proxy_ok": False,
                "neo4j_proxy_ok": False,
            },
        )
        qdrant.assert_not_called()
        neo4j.assert_not_called()
        eligibility_gate.assert_called_once()

    def test_runner_v2_backend_timeout_is_closed_and_emits_no_progress(self):
        with tempfile.TemporaryDirectory() as tmp:
            runtime_site = Path(tmp).resolve()
            with self._runner_v2_descriptors(runtime_site) as (
                ready,
                completed,
                progress_stream,
            ), patch.object(
                runner,
                "_probe_smoke_qdrant",
                side_effect=TimeoutError("seeded timeout"),
            ) as qdrant, patch.object(runner, "_probe_smoke_neo4j") as neo4j:
                status = runner.main(
                    ["provenance-smoke-v2", "backend_timeout"]
                )
                self.assertEqual(os.read(ready, 1), b"R")
                receipt_payload = os.read(completed, 65537)
                progress_payload = os.read(progress_stream, 8192)

        self.assertEqual(status, 0)
        self.assertEqual(progress_payload, b"")
        self.assertEqual(
            json.loads(receipt_payload),
            {
                "schema": "txnmem-provenance-smoke-child-receipt-v2",
                "scenario": "backend_timeout",
                "outcome": "backend_timeout",
                "completed_repetitions": 0,
                "qdrant_proxy_ok": False,
                "neo4j_proxy_ok": False,
            },
        )
        qdrant.assert_called_once_with(
            "http://127.0.0.1:19000/readyz",
            timeout_seconds=1.0,
        )
        neo4j.assert_not_called()

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

    def test_runner_sigterm_unwinds_nonzero_without_partial_candidate(self):
        import txnmem_experiment

        gate_read, gate_write = os.pipe()
        ready_read, ready_write = os.pipe()
        receipt_read, receipt_write = os.pipe()
        progress_read, progress_write = os.pipe()
        os.write(gate_write, b"G")
        os.close(gate_write)
        events = []
        termination_state = runner._RunnerTerminationState()

        progress_snapshot = {
            "cell_index": 1,
            "cell_count": 15,
            "graph_size": 100,
            "concurrency": 1,
            "repetition_index": 1,
            "repetition_count": 30,
            "completed_repetitions": 1,
            "total_repetitions": 450,
            "completed_samples": 32,
            "total_samples": 14400,
            "update_sequence": 1,
        }

        def experiment_main(_arguments, **hooks):
            try:
                termination_state.request_stop()
                hooks["_progress_callback"](progress_snapshot)
                return 0
            finally:
                events.append("clients_closed")

        with tempfile.TemporaryDirectory() as tmp:
            runtime_site = Path(tmp).resolve() / "runtime"
            runtime_site.mkdir()
            candidate = Path(tmp).resolve() / "candidate"
            candidate.mkdir()
            environment = {
                "TXNMEM_PROVENANCE_START_GATE_FD": str(gate_read),
                "TXNMEM_PROVENANCE_READY_FD": str(ready_write),
                "TXNMEM_PROVENANCE_COMPLETION_FD": str(receipt_write),
                "TXNMEM_PROVENANCE_PROGRESS_FD": str(progress_write),
                "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256": "a" * 64,
                "TXNMEM_PROVENANCE_RUNTIME_SITE": str(runtime_site),
            }
            with patch.dict(os.environ, environment, clear=True), patch.object(
                runner,
                "_RunnerTerminationState",
                return_value=termination_state,
            ), patch.object(
                txnmem_experiment, "main", side_effect=experiment_main
            ), patch(
                "txnmem_provenance_performance.formal_matrix_config_sha256",
                return_value="b" * 64,
            ), patch.object(
                runner,
                "_candidate_completion_material",
                return_value={"result": "must-not-publish"},
            ) as candidate_material:
                status = runner.main(
                    [
                        "provenance-performance",
                        "--backend",
                        "vector-graph",
                        "--config",
                        "/immutable/config.json",
                        "--run-id",
                        "runner-interruption-fixture",
                        "--out-dir",
                        str(candidate),
                        "--service-url",
                        "http://127.0.0.1:19000",
                    ]
                )

            self.assertEqual(list(candidate.iterdir()), [])

        self.assertNotEqual(status, 0)
        self.assertEqual(events, ["clients_closed"])
        self.assertEqual(os.read(ready_read, 2), b"R")
        self.assertEqual(os.read(progress_read, 1), b"")
        self.assertEqual(os.read(receipt_read, 1), b"")
        candidate_material.assert_not_called()
        self.assertIs(termination_state.stop_requested, True)
        for descriptor in (ready_read, progress_read, receipt_read):
            os.close(descriptor)

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
