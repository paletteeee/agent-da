import copy
import hashlib
import inspect
import json
import os
import subprocess
import sys
import time
from contextlib import nullcontext
from types import SimpleNamespace
from types import ModuleType
import unittest
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import txnmem_provenance_execution_collector as collector_module

from txnmem_provenance_execution_collector import (
    CollectorError,
    _collect_execution_evidence,
    attest_committed_source,
    create_immutable_source_export,
    parse_toxiproxy_byte_counters,
)
from txnmem_topology_attestation import (
    FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
)


PROXY_ROUTES = [
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


def proxy_snapshot(phase, *, qdrant, neo4j):
    routes = []
    for route, values in zip(PROXY_ROUTES, (qdrant, neo4j)):
        row = {
            "role": route["role"],
            "proxy_name": route["proxy_name"],
            "listener": route["listen"],
            "upstream": route["upstream"],
            "received_upstream_bytes": values[0],
            "sent_upstream_bytes": values[1],
            "received_downstream_bytes": values[2],
            "sent_downstream_bytes": values[3],
            "total_bytes": sum(values),
        }
        routes.append(row)
    document = {
        "schema": "txnmem-provenance-proxy-counters-v1",
        "phase": phase,
        "routes": routes,
        "toxiproxy_total_bytes": sum(row["total_bytes"] for row in routes),
    }
    document["snapshot_sha256"] = hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return document


def proxy_payload_sha256(snapshot):
    payload = {
        "routes": snapshot["routes"],
        "toxiproxy_total_bytes": snapshot["toxiproxy_total_bytes"],
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


class ProvenanceExecutionCollectorTests(unittest.TestCase):
    AUTHORIZATION_NONCE = b"collector-fixture-authorization-nonce-0001"

    def test_repository_runtime_lock_accepts_protected_python_3_10_12(self):
        runtime_lock = (
            Path(__file__).resolve().parents[1]
            / "configs"
            / "provenance_runtime_lock.json"
        )

        with patch.object(
            collector_module.platform, "python_version", return_value="3.10.12"
        ):
            lock, _lock_sha256 = collector_module._load_runtime_lock(runtime_lock)

        self.assertIn("3.10.12", lock["python_versions"])

    @staticmethod
    def _runtime_manifest(
        *, executable_hash="a" * 64, version="3.11.9", file_hash="1" * 64
    ):
        files = [{"path": "neo4j/__init__.py", "sha256": file_hash}]
        return {
            "schema": "txnmem-provenance-runtime-manifest-v1",
            "python": {
                "implementation": "CPython",
                "version": version,
                "executable_sha256": executable_hash,
                "build_sha256": "2" * 64,
                "compiler_sha256": "3" * 64,
                "platform_sha256": "4" * 64,
            },
            "distributions": [
                {
                    "name": "neo4j",
                    "version": "5.28.1",
                    "files": files,
                    "files_sha256": hashlib.sha256(
                        json.dumps(
                            files, sort_keys=True, separators=(",", ":")
                        ).encode("utf-8")
                    ).hexdigest(),
                    "declared_requirements_sha256": "5" * 64,
                }
            ],
        }

    @staticmethod
    def _environment():
        return {
            "schema": "txnmem-provenance-environment-v1",
            "isolation_verified": True,
            "co_tenant_load_detected": False,
            "source": "host-observation-v1",
            "cpu_logical_count": 8,
            "memory_total_bytes": 16 * 1024**3,
            "disk_medium": "ssd",
            "toxiproxy_version": "2.9.0",
        }

    @staticmethod
    def _external_tools(python_hash):
        return [
            {
                "role": role,
                "requested_path_sha256": value * 64,
                "resolved_path_sha256": value * 64,
                "executable_sha256": python_hash if role == "python" else value * 64,
                "owner_uid": 0,
                "mode": 0o555,
            }
            for role, value in (
                ("docker", "1"),
                ("git", "2"),
                ("nft", "3"),
                ("python", "4"),
            )
        ]

    @staticmethod
    def _network_guard():
        return {
            "schema": "txnmem-provenance-network-guard-v3",
            "table_name_sha256": "a" * 64,
            "runner_uid": 65532,
            "controller_uid": 0,
            "allowed_ipv4_loopback_ports": [19000, 19001],
            "allowed_root_ingress_ports": [8474, 19000, 19001],
            "root_ingress_destination_exact": True,
            "management_port_root_only": True,
            "non_runner_proxy_traffic_blocked": True,
            "host_bridge_access_blocked": True,
            "forwarded_bridge_access_blocked": True,
            "backend_ipv4_subnet_sha256": "8" * 64,
            "ingress_ipv4_subnet_sha256": "9" * 64,
            "backend_bridge_interface_sha256": "0" * 64,
            "ingress_bridge_interface_sha256": "1" * 64,
            "toxiproxy_ingress_ipv4_sha256": hashlib.sha256(
                b"172.20.0.2"
            ).hexdigest(),
            "policy_sha256": "b" * 64,
            "ruleset_sha256": "c" * 64,
        }

    @staticmethod
    def _nft_snapshot_document(table_name):
        return {
            "nftables": [
                {"metainfo": {"json_schema_version": 1}},
                {"table": {"family": "inet", "name": table_name}},
            ]
            + [
                {
                    "chain": {
                        "family": "inet",
                        "table": table_name,
                        "name": name,
                        "type": "filter",
                        "hook": name,
                        "policy": "accept",
                    }
                }
                for name in ("output", "forward")
            ]
            + [
                {
                    "rule": {
                        "family": "inet",
                        "table": table_name,
                        "chain": "forward"
                        if comment == "txnmem-forward-bridge-deny"
                        else "output",
                        "comment": comment,
                    }
                }
                for comment in (
                    "txnmem-proxy-allow",
                    "txnmem-management-allow",
                    "txnmem-docker-proxy-ingress-allow",
                    "txnmem-runner-deny",
                    "txnmem-management-deny",
                    "txnmem-attribution-deny",
                    "txnmem-host-bridge-deny",
                    "txnmem-forward-bridge-deny",
                )
            ]
        }

    @staticmethod
    def _backend_isolation():
        from txnmem_provenance_contract import (
            FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS,
        )

        return {
            "schema": "txnmem-provenance-backend-isolation-v3",
            "network_name_sha256": "d" * 64,
            "network_id_sha256": "e" * 64,
            "ingress_network_name_sha256": "6" * 64,
            "ingress_network_id_sha256": "7" * 64,
            "toxiproxy_ingress_ipv4": "172.20.0.2",
            "toxiproxy_ingress_ipv4_sha256": hashlib.sha256(
                b"172.20.0.2"
            ).hexdigest(),
            "toxiproxy_ingress_endpoint_id_sha256": hashlib.sha256(
                ("4" * 64).encode("utf-8")
            ).hexdigest(),
            "toxiproxy_ingress_membership_verified": True,
            "ingress_unique_workload_container_verified": True,
            "backend_network_internal": True,
            "ingress_network_external": True,
            "ingress_proxy_only": True,
            "backend_network_driver": "bridge",
            "ingress_network_driver": "bridge",
            "backend_network_scope": "local",
            "ingress_network_scope": "local",
            "network_driver_options_empty": True,
            "docker_default_ipam_driver_verified": True,
            "private_non_overlapping_ipv4_subnets_verified": True,
            "backend_ipv4_subnet_sha256": "8" * 64,
            "ingress_ipv4_subnet_sha256": "9" * 64,
            "backend_bridge_interface_sha256": "0" * 64,
            "ingress_bridge_interface_sha256": "1" * 64,
            "networks_non_attachable": True,
            "networks_non_swarm_ingress": True,
            "networks_non_config_only": True,
            "direct_backend_ports_unpublished": True,
            "proxy_ports_loopback_only": True,
            "published_proxy_ports": [8474, 19000, 19001],
            "containers": [
                {
                    "role": role,
                    "container_id_sha256": value * 64,
                    "runtime_image_id_sha256": {
                        "qdrant": "d",
                        "neo4j": "e",
                        "toxiproxy": "f",
                    }[role]
                    * 64,
                    "manifest_digest": FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS[
                        role
                    ],
                }
                for role, value in (
                    ("qdrant", "a"),
                    ("neo4j", "b"),
                    ("toxiproxy", "c"),
                )
            ],
        }

    @staticmethod
    def _execution_monitor():
        return {
            "schema": "txnmem-provenance-execution-monitor-v2",
            "sampling_interval_ms": 250,
            "sample_count": 3,
            "first_sample_monotonic_ns": 1_000_000_000,
            "last_sample_monotonic_ns": 1_500_000_000,
            "gate_release_monotonic_ns": 1_100_000_000,
            "child_exit_monotonic_ns": 1_400_000_000,
            "max_observed_gap_ns": 250_000_000,
            "violation_count": 0,
            "cpu_logical_count": 8,
            "load1_limit_milli": 8000,
            "max_load1_milli": 1250,
            "invariants": [
                "backend_isolation",
                "continuous_load_ceiling",
                "host_environment",
                "network_guard",
                "runner_uid_process_set",
                "terminal_process_exit",
                "toxiproxy_routes",
            ],
            "samples_sha256": "1" * 64,
            "first_sample_sha256": "2" * 64,
            "last_sample_sha256": "3" * 64,
        }

    @staticmethod
    def _source_identity(suffix=""):
        commit = "a" * 40
        manifest = {
            "schema": "txnmem-provenance-source-manifest-v1",
            "source_commit": commit,
            "files": [
                {
                    "path": "src/txnmem_experiment.py",
                    "blob_sha256": "e" * 64,
                }
            ],
        }
        return {
            "source_commit": commit,
            "source_manifest": manifest,
            "source_manifest_sha256": (
                hashlib.sha256(
                    json.dumps(
                        manifest, sort_keys=True, separators=(",", ":")
                    ).encode("utf-8")
                ).hexdigest()
                if not suffix
                else suffix * 64
            ),
            "collector_sha256": "c" * 64,
            "runner_sha256": "d" * 64,
        }

    @staticmethod
    def _snapshot(
        proxy_counters=None,
        client_owner="candidate-process:4321:fixture-start",
    ):
        if proxy_counters is None:
            proxy_counters = proxy_snapshot(
                "baseline_a",
                qdrant=(11, 13, 17, 19),
                neo4j=(23, 29, 31, 37),
            )
        versions = {
            "client": "3.11.9",
            "qdrant": "1.15.4",
            "neo4j": "5.26.0",
            "toxiproxy": "2.9.0",
        }
        return {
            "schema": "txnmem-provenance-topology-snapshot-v3",
            "roles": [
                {
                    "role": role,
                    "host_identity": "client-host"
                    if role in {"client", "toxiproxy"}
                    else "backend-host",
                    "listener_owner": (
                        client_owner if role == "client" else f"{role}-owner"
                    ),
                    "service_version": versions[role],
                    "rtt_ms": 0.1,
                }
                for role in ("client", "qdrant", "neo4j", "toxiproxy")
            ],
            "proxy_routes": copy.deepcopy(PROXY_ROUTES),
            "proxy_counters": copy.deepcopy(proxy_counters),
            "backend_isolation": ProvenanceExecutionCollectorTests._backend_isolation(),
        }

    @staticmethod
    def _command_manifest(
        *, run_hash, config_file_hash, environment_hash, source_identity
    ):
        runtime_manifest = ProvenanceExecutionCollectorTests._runtime_manifest()
        manifest = {
            "schema": "txnmem-provenance-command-manifest-v2",
            "transport": "local_loopback",
            "argv_sha256": "8" * 64,
            "argv_template": [
                "<python-executable>",
                "-I",
                "-S",
                "-B",
                "<immutable-source>/src/txnmem_provenance_runner.py",
                "provenance-performance",
                "--backend",
                "vector-graph",
                "--config",
                "<immutable-source>/configs/provenance_performance_matrix.json",
                "--run-id",
                "<run-id>",
                "--out-dir",
                "<candidate-root>",
                "--service-url",
                "<qdrant-endpoint>",
                "--environment-attestation",
                "<environment-attestation>",
            ],
            "python_executable_path_sha256": "9" * 64,
            "python_executable_sha256": "a" * 64,
            "python_implementation": "CPython",
            "python_version": "3.11.9",
            "runtime_manifest": runtime_manifest,
            "runtime_manifest_sha256": hashlib.sha256(
                json.dumps(
                    runtime_manifest, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")
            ).hexdigest(),
            "runtime_lock_file_sha256": "6" * 64,
            "runtime_snapshot_path_sha256": "7" * 64,
            "external_tools": ProvenanceExecutionCollectorTests._external_tools(
                "a" * 64
            ),
            "working_directory_sha256": "b" * 64,
            "source_manifest_sha256": source_identity[
                "source_manifest_sha256"
            ],
            "runner_sha256": source_identity["runner_sha256"],
            "config_file_sha256": config_file_hash,
            "run_id_sha256": run_hash,
            "candidate_root_sha256": "c" * 64,
            "environment_attestation_sha256": environment_hash,
            "environment_attestation_file_sha256": "6" * 64,
            "qdrant_endpoint_sha256": "d" * 64,
            "qdrant_endpoint_port": 19000,
            "neo4j_endpoint_sha256": "e" * 64,
            "neo4j_endpoint_port": 19001,
            "toxiproxy_endpoint_sha256": "2" * 64,
            "literal_environment": {
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "PYTHONDONTWRITEBYTECODE": "1",
            },
            "hashed_environment": {
                "TXNMEM_NEO4J_URI": "f" * 64,
                "TXNMEM_NEO4J_USER": "0" * 64,
                "TXNMEM_PROVENANCE_RUNTIME_SITE": "7" * 64,
            },
            "secret_environment_variables": ["TXNMEM_NEO4J_PASSWORD"],
            "gate_environment_variable": "TXNMEM_PROVENANCE_START_GATE_FD",
            "ready_environment_variable": "TXNMEM_PROVENANCE_READY_FD",
            "completion_environment_variable": "TXNMEM_PROVENANCE_COMPLETION_FD",
            "completion_receipt_required": True,
            "runtime_environment_variable": "TXNMEM_PROVENANCE_RUNTIME_SITE",
            "inherited_environment": False,
        }
        return manifest

    @staticmethod
    def _child_process(command_manifest):
        return {
            "pid": 4321,
            "start_identity": "candidate-process:4321:fixture-start",
            "uid": 65532,
            "executable_sha256": command_manifest[
                "python_executable_sha256"
            ],
            "argv_sha256": command_manifest["argv_sha256"],
            "cmdline_sha256": "3" * 64,
        }

    def test_collector_writes_launch_before_run_and_completion_after_exact_candidate(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            project = root / "project"
            project.mkdir()
            candidate_root = root / "candidate"
            candidate_root.mkdir()
            environment_path = root / "environment.json"
            environment_path.write_text(json.dumps(self._environment()), encoding="utf-8")
            launch_path = root / "private" / "launch.json"
            completion_path = root / "private" / "completion.json"
            launch_path.parent.mkdir(mode=0o700)
            events = []
            candidate_events = []
            source_calls = iter(
                [self._source_identity(), self._source_identity()]
            )
            baseline_a = proxy_snapshot(
                "baseline_a",
                qdrant=(11, 13, 17, 19),
                neo4j=(23, 29, 31, 37),
            )
            baseline_b = proxy_snapshot(
                "baseline_b",
                qdrant=(11, 13, 17, 19),
                neo4j=(23, 29, 31, 37),
            )
            final = proxy_snapshot(
                "final",
                qdrant=(21, 33, 47, 69),
                neo4j=(73, 89, 101, 127),
            )
            probe_calls = iter(
                [self._snapshot(baseline_a), self._snapshot(final)]
            )
            config_hash = "1" * 64
            run_hash = hashlib.sha256(b"collector-fixture").hexdigest()
            candidate_id = (
                "diagnostic-vector_graph-"
                + config_hash[:16]
                + "-"
                + run_hash[:16]
            )
            material = {
                "schema": "txnmem-provenance-candidate-attestation-material-v1",
                "candidate_bundle_id": candidate_id,
                "run_id_sha256": run_hash,
                "config_sha256": config_hash,
                "config_file_sha256": "2" * 64,
                "workload_sha256": "3" * 64,
                "environment_attestation_sha256": "4" * 64,
                "evidence_manifest_sha256": "5" * 64,
                "matrix_cell_count": 15,
                "repetition_count": 450,
                "operation_sample_count": 14_400,
                "observed_service_versions": {
                    "qdrant": "1.15.4",
                    "neo4j": "5.26.0",
                    "toxiproxy": "2.9.0",
                },
                "candidate_operation_samples_sha256": "6" * 64,
                "candidate_repetitions_sha256": "7" * 64,
            }
            candidate_seal = {
                "schema": "txnmem-provenance-candidate-seal-v1",
                "root_device": 11,
                "root_inode": 22,
                "directory_count": 3,
                "file_count": 4,
                "tree_sha256": "8" * 64,
                "completion_receipt_sha256": hashlib.sha256(
                    json.dumps(
                        material, sort_keys=True, separators=(",", ":")
                    ).encode("utf-8")
                ).hexdigest(),
            }
            source_identity = self._source_identity()
            command_manifest = self._command_manifest(
                run_hash=run_hash,
                config_file_hash="2" * 64,
                environment_hash="4" * 64,
                source_identity=source_identity,
            )
            runtime_calls = iter(
                [
                    command_manifest["runtime_manifest"],
                    command_manifest["runtime_manifest"],
                ]
            )

            def source_loader(_project):
                return next(source_calls)

            def probe_loader(phase):
                events.append(f"topology_{phase}")
                return next(probe_calls)

            def runtime_loader():
                return next(runtime_calls)

            def external_tool_loader():
                return copy.deepcopy(command_manifest["external_tools"])

            def runner():
                events.append("gate_release")
                self.assertTrue(launch_path.is_file())
                self.assertFalse(completion_path.exists())
                events.append("child_exit")
                return 0, material

            def candidate_sealer(candidate, receipt):
                self.assertIn("launch_write", events)
                candidate_events.append("seal")
                self.assertEqual(candidate, candidate_root)
                self.assertEqual(receipt, material)
                return candidate_seal

            def guard_activate():
                events.extend(["guard_activate", "route_rearm", "baseline_b"])
                return {
                    "network_guard": self._network_guard(),
                    "proxy_routes": copy.deepcopy(PROXY_ROUTES),
                    "proxy_counters": copy.deepcopy(baseline_b),
                    "route_rearmed": True,
                }

            def guard_finalize():
                events.extend(["final_counters", "guard_verify"])
                return {
                    "network_guard": self._network_guard(),
                    "proxy_routes": copy.deepcopy(PROXY_ROUTES),
                    "proxy_counters": copy.deepcopy(final),
                }

            def guard_deactivate():
                events.append("guard_deactivate")

            def monitor_start():
                events.append("monitor_start")

            def monitor_finalize():
                events.append("monitor_finalize")
                return self._execution_monitor()

            def material_loader(candidate, bundle_id):
                self.assertIn("launch_write", events)
                candidate_events.append("material")
                self.assertEqual(candidate, candidate_root)
                self.assertEqual(bundle_id, candidate_id)
                return material

            original_write = collector_module.FormalStore.write_json_exclusive

            def recording_write(store, *parts, payload, sort_keys=True, mode=0o644):
                events.append(
                    "launch_write" if parts[-1] == launch_path.name else "completion_write"
                )
                return original_write(
                    store,
                    *parts,
                    payload=payload,
                    sort_keys=sort_keys,
                    mode=mode,
                )

            with patch.dict(
                FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
                {
                    run_hash: hashlib.sha256(
                        self.AUTHORIZATION_NONCE
                    ).hexdigest()
                },
                clear=True,
            ), patch.object(
                collector_module.FormalStore,
                "write_json_exclusive",
                new=recording_write,
            ):
                result = _collect_execution_evidence(
                    project_root=project,
                    candidate_root=candidate_root,
                    launch_path=launch_path,
                    completion_path=completion_path,
                    run_id="collector-fixture",
                    transport="local_loopback",
                    config_sha256=config_hash,
                    config_file_sha256="2" * 64,
                    workload_sha256="3" * 64,
                    environment_attestation_sha256="4" * 64,
                    command_manifest=command_manifest,
                    child_process=self._child_process(command_manifest),
                    authorization_nonce=self.AUTHORIZATION_NONCE,
                    network_guard_activate=guard_activate,
                    network_guard_finalize=guard_finalize,
                    network_guard_deactivate=guard_deactivate,
                    execution_monitor_start=monitor_start,
                    execution_monitor_finalize=monitor_finalize,
                    run_candidate=runner,
                    candidate_sealer=candidate_sealer,
                    topology_probe=probe_loader,
                    source_identity_loader=source_loader,
                    external_tool_identity_loader=external_tool_loader,
                    runtime_identity_loader=runtime_loader,
                    candidate_material_loader=material_loader,
                )

            launch_raw = launch_path.read_bytes()
            completion_raw = completion_path.read_bytes()
            launch = json.loads(launch_raw)
            completion = json.loads(completion_raw)
            launch_mode = launch_path.stat().st_mode & 0o777
            completion_mode = completion_path.stat().st_mode & 0o777

        self.assertEqual(
            events,
            [
                "topology_before",
                "guard_activate",
                "route_rearm",
                "baseline_b",
                "launch_write",
                "monitor_start",
                "gate_release",
                "child_exit",
                "monitor_finalize",
                "final_counters",
                "guard_verify",
                "guard_deactivate",
                "topology_after",
                "completion_write",
            ],
        )
        self.assertEqual(candidate_events, ["seal", "material"])
        self.assertEqual(result, (launch_path, completion_path))
        self.assertEqual(
            set(launch),
            {
                "schema",
                "collector_id",
                "formal_execution_requested",
                "run_id_sha256",
                "config_sha256",
                "config_file_sha256",
                "workload_sha256",
                "environment_attestation_sha256",
                "source_commit",
                "source_manifest",
                "source_manifest_sha256",
                "collector_sha256",
                "runner_sha256",
                "command_manifest",
                "command_sha256",
                "child_process",
                "network_guard",
                "backend_isolation",
                "transport",
                "matrix_cell_count",
                "repetition_count",
                "operation_sample_count",
                "roles",
                "proxy_routes",
                "proxy_counter_baseline_a",
                "proxy_counter_baseline_b",
                "proxy_route_rearm_verified",
                "authorization_nonce_sha256",
                "authorization_proof_sha256",
            },
        )
        self.assertEqual(
            set(completion),
            {
                "schema",
                "collector_id",
                "formal_execution_requested",
                "run_id_sha256",
                "config_sha256",
                "config_file_sha256",
                "workload_sha256",
                "environment_attestation_sha256",
                "source_commit",
                "source_manifest",
                "source_manifest_sha256",
                "collector_sha256",
                "runner_sha256",
                "command_manifest",
                "command_sha256",
                "child_process",
                "network_guard",
                "backend_isolation",
                "transport",
                "matrix_cell_count",
                "repetition_count",
                "operation_sample_count",
                "launch_file_sha256",
                "exit_code",
                "candidate_bundle_id",
                "evidence_manifest_sha256",
                "candidate_operation_samples_sha256",
                "candidate_repetitions_sha256",
                "candidate_seal",
                "execution_monitor",
                "roles",
                "proxy_routes",
                "proxy_counter_baseline_b_sha256",
                "proxy_counter_final",
                "proxy_counter_deltas",
                "authorization_nonce_sha256",
                "authorization_proof_sha256",
            },
        )
        self.assertEqual(launch["schema"], "txnmem-provenance-execution-launch-raw-v4")
        self.assertEqual(
            completion["schema"],
            "txnmem-provenance-execution-completion-raw-v5",
        )
        self.assertEqual(launch["proxy_counter_baseline_a"], baseline_a)
        self.assertEqual(launch["proxy_counter_baseline_b"], baseline_b)
        self.assertIs(launch["proxy_route_rearm_verified"], True)
        self.assertEqual(
            completion["proxy_counter_baseline_b_sha256"],
            proxy_payload_sha256(baseline_b),
        )
        self.assertEqual(completion["proxy_counter_final"], final)
        self.assertEqual(
            [row["total_bytes"] for row in completion["proxy_counter_deltas"]["routes"]],
            [110, 270],
        )
        self.assertEqual(
            completion["proxy_counter_deltas"]["toxiproxy_total_bytes"], 380
        )
        self.assertEqual(completion["exit_code"], 0)
        self.assertEqual(completion["candidate_bundle_id"], candidate_id)
        self.assertEqual(completion["execution_monitor"]["violation_count"], 0)
        self.assertEqual(launch_mode, 0o600)
        self.assertEqual(completion_mode, 0o600)

    def test_attribution_boundary_failure_deactivates_guard_before_launch(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            project = root / "project"
            project.mkdir()
            candidate = root / "candidate"
            candidate.mkdir()
            private = root / "private"
            private.mkdir(mode=0o700)
            launch = private / "launch.json"
            completion = private / "completion.json"
            run_id = "boundary-cleanup-fixture"
            run_hash = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
            source_identity = self._source_identity()
            command_manifest = self._command_manifest(
                run_hash=run_hash,
                config_file_hash="2" * 64,
                environment_hash="4" * 64,
                source_identity=source_identity,
            )
            baseline_a = proxy_snapshot(
                "baseline_a",
                qdrant=(11, 13, 17, 19),
                neo4j=(23, 29, 31, 37),
            )
            drifted_b = proxy_snapshot(
                "baseline_b",
                qdrant=(12, 13, 17, 19),
                neo4j=(23, 29, 31, 37),
            )
            events = []

            with patch.dict(
                FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
                {
                    run_hash: hashlib.sha256(
                        self.AUTHORIZATION_NONCE
                    ).hexdigest()
                },
                clear=True,
            ):
                with self.assertRaisesRegex(CollectorError, "not quiescent"):
                    _collect_execution_evidence(
                        project_root=project,
                        candidate_root=candidate,
                        launch_path=launch,
                        completion_path=completion,
                        run_id=run_id,
                        transport="local_loopback",
                        config_sha256="1" * 64,
                        config_file_sha256="2" * 64,
                        workload_sha256="3" * 64,
                        environment_attestation_sha256="4" * 64,
                        command_manifest=command_manifest,
                        child_process=self._child_process(command_manifest),
                        authorization_nonce=self.AUTHORIZATION_NONCE,
                        network_guard_activate=lambda: {
                            "network_guard": self._network_guard(),
                            "proxy_routes": copy.deepcopy(PROXY_ROUTES),
                            "proxy_counters": drifted_b,
                            "route_rearmed": True,
                        },
                        network_guard_finalize=lambda: self.fail(
                            "finalization must not run"
                        ),
                        network_guard_deactivate=lambda: events.append(
                            "guard_deactivate"
                        ),
                        execution_monitor_start=lambda: self.fail(
                            "monitor must not start"
                        ),
                        execution_monitor_finalize=lambda: self.fail(
                            "monitor must not finalize"
                        ),
                        run_candidate=lambda: self.fail(
                            "candidate must remain gated"
                        ),
                        candidate_sealer=lambda *_args: self.fail(
                            "candidate must not be sealed"
                        ),
                        topology_probe=lambda _phase: self._snapshot(baseline_a),
                        source_identity_loader=lambda _root: source_identity,
                        external_tool_identity_loader=lambda: copy.deepcopy(
                            command_manifest["external_tools"]
                        ),
                        runtime_identity_loader=lambda: copy.deepcopy(
                            command_manifest["runtime_manifest"]
                        ),
                        candidate_material_loader=lambda *_args: self.fail(
                            "candidate material must not be loaded"
                        ),
                    )

        self.assertEqual(events, ["guard_deactivate"])
        self.assertFalse(launch.exists())
        self.assertFalse(completion.exists())

    def test_gated_child_cannot_execute_before_launch_release_or_inherit_parent_secrets(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            script = root / "gate_fixture.py"
            result = root / "child.json"
            script.write_text(
                "\n".join(
                    [
                        "import json, os, sys",
                        "ready_fd = int(os.environ.pop('TXNMEM_PROVENANCE_READY_FD'))",
                        "os.write(ready_fd, b'R')",
                        "os.close(ready_fd)",
                        "fd = int(os.environ.pop('TXNMEM_PROVENANCE_START_GATE_FD'))",
                        "token = os.read(fd, 1)",
                        "os.close(fd)",
                        "with open(sys.argv[1], 'w', encoding='utf-8') as stream:",
                        "    json.dump({'token': token.decode('ascii'), 'env': dict(os.environ)}, stream)",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            command = (sys.executable, "-I", "-B", str(script), str(result))
            with patch.dict(
                os.environ,
                {"TXNMEM_TEST_PARENT_SECRET": "must-not-reach-child"},
                clear=False,
            ):
                child = collector_module._start_gated_candidate(
                    command=command,
                    cwd=root,
                    environment={"TXNMEM_CHILD_SAFE": "1"},
                )
            try:
                self.assertTrue(child.ready_observed)
                self.assertIsNone(child.process.poll())
                self.assertFalse(result.exists())
                child.release()
                self.assertEqual(child.process.wait(timeout=5), 0)
            finally:
                child.close()

            payload = json.loads(result.read_text(encoding="utf-8"))
            self.assertEqual(payload["token"], "G")
            self.assertEqual(payload["env"]["TXNMEM_CHILD_SAFE"], "1")
            self.assertNotIn("TXNMEM_TEST_PARENT_SECRET", payload["env"])
            self.assertNotIn("TXNMEM_PROVENANCE_START_GATE_FD", payload["env"])
            self.assertNotIn("TXNMEM_PROVENANCE_READY_FD", payload["env"])

    def test_gated_child_must_signal_readiness_before_launch_attestation(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            script = root / "never_ready.py"
            script.write_text("raise SystemExit(0)\n", encoding="utf-8")

            with self.assertRaisesRegex(CollectorError, "readiness"):
                collector_module._start_gated_candidate(
                    command=(sys.executable, "-I", "-B", str(script)),
                    cwd=root,
                    environment={},
                )

    def test_gated_child_completion_receipt_is_canonical_and_parent_observed(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            script = root / "receipt_fixture.py"
            receipt = {"result": "sealed", "value": 7}
            script.write_text(
                "\n".join(
                    [
                        "import json, os",
                        "ready_fd = int(os.environ.pop('TXNMEM_PROVENANCE_READY_FD'))",
                        "gate_fd = int(os.environ.pop('TXNMEM_PROVENANCE_START_GATE_FD'))",
                        "receipt_fd = int(os.environ.pop('TXNMEM_PROVENANCE_COMPLETION_FD'))",
                        "os.write(ready_fd, b'R')",
                        "os.close(ready_fd)",
                        "token = os.read(gate_fd, 1)",
                        "os.close(gate_fd)",
                        "payload = json.dumps({'result':'sealed','value':7}, sort_keys=True, separators=(',', ':')).encode('utf-8')",
                        "os.write(receipt_fd, payload)",
                        "os.close(receipt_fd)",
                        "raise SystemExit(0 if token == b'G' else 9)",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            child = collector_module._start_gated_candidate(
                command=(sys.executable, "-I", "-B", str(script)),
                cwd=root,
                environment={},
                require_completion_receipt=True,
            )
            try:
                child.release()
                exit_code, observed = child.wait_with_receipt(timeout=5)
            finally:
                child.close()

            self.assertEqual(exit_code, 0)
            self.assertEqual(observed, receipt)

    def test_child_identity_is_observed_from_proc_not_copied_from_manifest(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            proc = root / "proc"
            process = proc / "4321"
            process.mkdir(parents=True)
            executable = root / "python"
            executable.write_bytes(b"pinned-python-binary")
            executable.chmod(0o500)
            (process / "exe").symlink_to(executable)
            command = (str(executable), "-I", "-S", "-B", "/sealed/runner.py")
            (process / "cmdline").write_bytes(
                b"\0".join(item.encode("utf-8") for item in command) + b"\0"
            )
            (process / "status").write_text(
                "Name:\tpython\nUid:\t65532\t65532\t65532\t65532\n",
                encoding="utf-8",
            )
            fields = ["S"] * 22
            fields[19] = "987654"
            (process / "stat").write_text(
                "4321 (python) " + " ".join(fields) + "\n", encoding="utf-8"
            )

            observed = collector_module._observe_formal_child_process(
                4321,
                expected_command=command,
                expected_uid=65532,
                proc_root=proc,
            )

            self.assertEqual(observed["pid"], 4321)
            self.assertEqual(observed["uid"], 65532)
            self.assertEqual(
                observed["executable_sha256"],
                hashlib.sha256(executable.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                observed["argv_sha256"],
                hashlib.sha256("\0".join(command).encode("utf-8")).hexdigest(),
            )
            self.assertRegex(observed["cmdline_sha256"], r"^[0-9a-f]{64}$")

            (process / "cmdline").write_bytes(b"/different\0")
            with self.assertRaisesRegex(CollectorError, "command line"):
                collector_module._observe_formal_child_process(
                    4321,
                    expected_command=command,
                    expected_uid=65532,
                    proc_root=proc,
                )

    def test_formal_runner_uid_process_inventory_is_exact(self):
        with TemporaryDirectory() as tmp:
            proc = Path(tmp).resolve() / "proc"
            for pid, uid, start in ((101, 65532, 7001), (202, 1000, 7002)):
                process = proc / str(pid)
                process.mkdir(parents=True)
                (process / "status").write_text(
                    f"Name:\tworker\nUid:\t{uid}\t{uid}\t{uid}\t{uid}\n",
                    encoding="utf-8",
                )
                fields = ["S"] * 22
                fields[19] = str(start)
                (process / "stat").write_text(
                    f"{pid} (worker) " + " ".join(fields) + "\n",
                    encoding="utf-8",
                )

            observed = collector_module._formal_uid_processes(
                65532, proc_root=proc
            )

            self.assertEqual(observed, {101: "7001"})
            collector_module._require_formal_uid_processes(
                65532, expected={101: "7001"}, proc_root=proc
            )
            with self.assertRaisesRegex(CollectorError, "process set"):
                collector_module._require_formal_uid_processes(
                    65532, expected={}, proc_root=proc
                )

    def test_formal_child_sets_and_verifies_no_new_privileges(self):
        calls = []

        def prctl(option, arg2, arg3, arg4, arg5):
            calls.append((option, arg2, arg3, arg4, arg5))
            if option == 38:
                return 0
            if option == 39:
                return 1
            return -1

        collector_module._set_no_new_privileges(prctl=prctl)

        self.assertEqual(
            calls,
            [(38, 1, 0, 0, 0), (39, 0, 0, 0, 0)],
        )

        with self.assertRaisesRegex(CollectorError, "no-new-privileges"):
            collector_module._set_no_new_privileges(
                prctl=lambda *_arguments: -1
            )

    def test_external_executable_attestation_binds_root_protected_bytes(self):
        with TemporaryDirectory() as tmp:
            executable = Path(tmp).resolve() / "tool"
            executable.write_bytes(b"formal-tool-bytes")
            executable.chmod(0o555)

            attested = collector_module._attest_external_executable(
                executable,
                role="git",
                expected_uid=os.getuid(),
                require_protected_parents=False,
            )

            self.assertEqual(attested["role"], "git")
            self.assertEqual(attested["owner_uid"], os.getuid())
            self.assertEqual(attested["mode"], 0o555)
            self.assertEqual(
                attested["executable_sha256"],
                hashlib.sha256(b"formal-tool-bytes").hexdigest(),
            )
            executable.chmod(0o775)
            with self.assertRaisesRegex(CollectorError, "writable"):
                collector_module._attest_external_executable(
                    executable,
                    role="git",
                    expected_uid=os.getuid(),
                    require_protected_parents=False,
                )

    def test_root_topology_probe_loads_neo4j_only_from_locked_runtime(self):
        with TemporaryDirectory() as tmp:
            runtime = Path(tmp).resolve() / "runtime"
            package = runtime / "neo4j"
            package.mkdir(parents=True)
            (package / "__init__.py").write_text(
                "class GraphDatabase:\n    marker = 'locked-runtime'\n",
                encoding="utf-8",
            )

            with collector_module._locked_neo4j_graph_database(runtime) as graph:
                self.assertEqual(graph.marker, "locked-runtime")

            self.assertNotIn("neo4j", sys.modules)
            injected = ModuleType("neo4j")
            with patch.dict(sys.modules, {"neo4j": injected}, clear=False):
                with self.assertRaisesRegex(CollectorError, "already imported"):
                    with collector_module._locked_neo4j_graph_database(runtime):
                        pass

    def test_nft_network_guard_policy_allows_only_two_loopback_proxy_ports(self):
        run_hash = "5" * 64
        table_name = collector_module._formal_network_table_name(run_hash)
        batch = collector_module._nft_guard_batch(
            table_name,
            runner_uid=65532,
            backend_ipv4_subnet="172.19.0.0/16",
            ingress_ipv4_subnet="172.20.0.0/16",
            backend_bridge_interface="br-aaaaaaaaaaaa",
            ingress_bridge_interface="br-bbbbbbbbbbbb",
            toxiproxy_ingress_ipv4="172.20.0.2",
        )

        self.assertEqual(table_name, "txnmem_" + run_hash[:16])
        self.assertIn("meta skuid 65532", batch)
        self.assertIn("tcp dport { 19000, 19001 } accept", batch)
        self.assertIn(
            "meta skuid 0 ip daddr 127.0.0.1 tcp dport 8474 accept",
            batch,
        )
        root_management = "txnmem-management-allow"
        root_ingress = (
            "meta skuid 0 ip daddr 172.20.0.2 "
            "tcp dport { 8474, 19000, 19001 } accept "
            'comment "txnmem-docker-proxy-ingress-allow"'
        )
        self.assertIn(root_ingress, batch)
        self.assertLess(batch.index(root_management), batch.index(root_ingress))
        self.assertLess(batch.index(root_ingress), batch.index("txnmem-runner-deny"))
        self.assertLess(
            batch.index(root_ingress), batch.index("txnmem-host-bridge-deny")
        )
        self.assertNotIn(
            "meta skuid 0 ip daddr 172.20.0.0/16",
            batch,
        )
        self.assertNotIn(
            "meta skuid 0 ip daddr { 172.19.0.0/16, 172.20.0.0/16 } accept",
            batch,
        )
        self.assertIn("meta skuid 65532 reject", batch)
        self.assertIn("tcp dport 8474 reject", batch)
        self.assertIn(
            "ip daddr 127.0.0.1 tcp dport { 19000, 19001 } reject",
            batch,
        )
        self.assertIn(
            "ip daddr { 172.19.0.0/16, 172.20.0.0/16 } reject",
            batch,
        )
        self.assertIn("chain forward", batch)
        self.assertIn(
            'iifname != { "br-aaaaaaaaaaaa", "br-bbbbbbbbbbbb" }',
            batch,
        )
        self.assertIn('comment "txnmem-forward-bridge-deny"', batch)
        self.assertEqual(batch.count(" accept comment"), 3)
        self.assertEqual(batch.count(" reject comment"), 5)

    def test_nft_network_guard_rejects_nonexclusive_ingress_addresses(self):
        for name, address in (
            ("ipv6", "fd00::2"),
            ("loopback", "127.0.0.2"),
            ("network", "172.20.0.0"),
            ("gateway", "172.20.0.1"),
            ("broadcast", "172.20.255.255"),
            ("outside_ingress", "172.21.0.2"),
            ("inside_backend", "172.19.0.2"),
        ):
            with self.subTest(name=name):
                with self.assertRaisesRegex(CollectorError, "ingress address"):
                    collector_module._nft_guard_batch(
                        "txnmem_" + "5" * 16,
                        runner_uid=65532,
                        backend_ipv4_subnet="172.19.0.0/16",
                        ingress_ipv4_subnet="172.20.0.0/16",
                        backend_bridge_interface="br-aaaaaaaaaaaa",
                        ingress_bridge_interface="br-bbbbbbbbbbbb",
                        toxiproxy_ingress_ipv4=address,
                    )

    def test_nft_network_guard_v3_requires_exact_rule_closure(self):
        table_name = "txnmem_" + "5" * 16
        document = self._nft_snapshot_document(table_name)

        normalized = collector_module._normalize_nft_snapshot(
            document, table_name=table_name
        )
        self.assertEqual(len(normalized["nftables"]), 11)

        missing = copy.deepcopy(document)
        missing["nftables"] = [
            item
            for item in missing["nftables"]
            if item.get("rule", {}).get("comment")
            != "txnmem-docker-proxy-ingress-allow"
        ]
        with self.assertRaisesRegex(CollectorError, "closure"):
            collector_module._normalize_nft_snapshot(
                missing, table_name=table_name
            )

        extra = copy.deepcopy(document)
        extra["nftables"].append(
            {
                "rule": {
                    "family": "inet",
                    "table": table_name,
                    "chain": "output",
                    "comment": "txnmem-extra-allow",
                }
            }
        )
        with self.assertRaisesRegex(CollectorError, "closure"):
            collector_module._normalize_nft_snapshot(extra, table_name=table_name)

    def test_nft_network_guard_v3_hashes_exact_ingress_without_disclosing_it(self):
        table_name = "txnmem_" + "5" * 16
        document = self._nft_snapshot_document(table_name)

        snapshots = []
        for address in ("172.20.0.2", "172.20.0.3"):
            guard = collector_module._NftNetworkGuard(
                table_name,
                backend_ipv4_subnet="172.19.0.0/16",
                ingress_ipv4_subnet="172.20.0.0/16",
                backend_bridge_interface="br-aaaaaaaaaaaa",
                ingress_bridge_interface="br-bbbbbbbbbbbb",
                toxiproxy_ingress_ipv4=address,
                active=True,
            )
            with patch.object(
                guard,
                "_run",
                return_value=SimpleNamespace(stdout=json.dumps(document)),
            ):
                snapshots.append(guard.snapshot())

        snapshot = snapshots[0]
        self.assertEqual(
            set(snapshot),
            {
                "schema",
                "table_name_sha256",
                "runner_uid",
                "controller_uid",
                "allowed_ipv4_loopback_ports",
                "allowed_root_ingress_ports",
                "root_ingress_destination_exact",
                "management_port_root_only",
                "non_runner_proxy_traffic_blocked",
                "host_bridge_access_blocked",
                "forwarded_bridge_access_blocked",
                "backend_ipv4_subnet_sha256",
                "ingress_ipv4_subnet_sha256",
                "backend_bridge_interface_sha256",
                "ingress_bridge_interface_sha256",
                "toxiproxy_ingress_ipv4_sha256",
                "policy_sha256",
                "ruleset_sha256",
            },
        )
        self.assertEqual(snapshot["schema"], "txnmem-provenance-network-guard-v3")
        self.assertEqual(snapshot["allowed_root_ingress_ports"], [8474, 19000, 19001])
        self.assertIs(snapshot["root_ingress_destination_exact"], True)
        self.assertEqual(
            snapshot["toxiproxy_ingress_ipv4_sha256"],
            hashlib.sha256(b"172.20.0.2").hexdigest(),
        )
        self.assertNotIn("172.20.0.2", json.dumps(snapshot, sort_keys=True))
        self.assertNotEqual(
            snapshots[0]["toxiproxy_ingress_ipv4_sha256"],
            snapshots[1]["toxiproxy_ingress_ipv4_sha256"],
        )
        self.assertNotEqual(
            snapshots[0]["policy_sha256"], snapshots[1]["policy_sha256"]
        )

    def test_nft_guard_activation_rollback_failure_preserves_active_state(self):
        table_name = "txnmem_" + "5" * 16
        guard = collector_module._NftNetworkGuard(
            table_name,
            backend_ipv4_subnet="172.19.0.0/16",
            ingress_ipv4_subnet="172.20.0.0/16",
            backend_bridge_interface="br-aaaaaaaaaaaa",
            ingress_bridge_interface="br-bbbbbbbbbbbb",
            toxiproxy_ingress_ipv4="172.20.0.2",
        )
        table_names = iter((set(), {table_name}, {table_name}))
        calls: list[tuple[str, ...]] = []

        def run(arguments, *, stdin=None, check=True):
            calls.append(tuple(arguments))
            if arguments == ("-f", "-"):
                raise CollectorError("apply failed")
            if arguments == ("delete", "table", "inet", table_name):
                return SimpleNamespace(returncode=1, stdout="", stderr="delete failed")
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with patch.object(guard, "_table_names", side_effect=lambda: next(table_names)), patch.object(
            guard, "_run", side_effect=run
        ):
            with self.assertRaisesRegex(CollectorError, "rollback"):
                guard.activate()

        self.assertTrue(guard.active)
        self.assertIn(("delete", "table", "inet", table_name), calls)

    def test_nft_guard_apply_failure_inventory_error_remains_active_for_cleanup(self):
        table_name = "txnmem_" + "6" * 16
        guard = collector_module._NftNetworkGuard(
            table_name,
            backend_ipv4_subnet="172.19.0.0/16",
            ingress_ipv4_subnet="172.20.0.0/16",
            backend_bridge_interface="br-aaaaaaaaaaaa",
            ingress_bridge_interface="br-bbbbbbbbbbbb",
            toxiproxy_ingress_ipv4="172.20.0.2",
        )
        calls: list[tuple[str, ...]] = []
        table_queries = 0

        def table_names():
            nonlocal table_queries
            table_queries += 1
            if table_queries == 1:
                return set()
            if table_queries == 2:
                raise CollectorError("inventory unavailable")
            return set()

        def run(arguments, *, stdin=None, check=True):
            calls.append(tuple(arguments))
            if arguments == ("-f", "-"):
                raise CollectorError("apply failed after partial create")
            if arguments == ("delete", "table", "inet", table_name):
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with patch.object(guard, "_table_names", side_effect=table_names), patch.object(
            guard, "_run", side_effect=run
        ):
            with self.assertRaisesRegex(CollectorError, "rollback"):
                guard.activate()
            self.assertTrue(guard.active)
            cleanup_failures = collector_module._cleanup_formal_execution_resources(
                execution_monitor=None,
                network_guard=guard,
                child=None,
            )

        self.assertEqual(cleanup_failures, [])
        self.assertFalse(guard.active)
        self.assertIn(("delete", "table", "inet", table_name), calls)

    def test_topology_snapshot_v3_binds_structured_counters_and_backend_isolation_v3(self):
        roles, routes, counters, isolation = collector_module._snapshot_components(
            self._snapshot()
        )

        self.assertEqual(len(roles), 4)
        self.assertEqual(len(routes), 2)
        self.assertEqual(counters["phase"], "baseline_a")
        self.assertEqual(
            isolation["schema"], "txnmem-provenance-backend-isolation-v3"
        )

        legacy = self._snapshot()
        legacy["schema"] = "txnmem-provenance-topology-snapshot-v1"
        with self.assertRaisesRegex(CollectorError, "snapshot"):
            collector_module._snapshot_components(legacy)

    def test_docker_backend_isolation_requires_proxy_only_ingress_network(self):
        from txnmem_provenance_contract import (
            FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS,
        )

        backend_network_id = "a" * 64
        ingress_network_id = "9" * 64

        def container(role, ports):
            digest = FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS[role]
            repository = {
                "qdrant": "qdrant/qdrant:v1.11.5",
                "neo4j": "neo4j:5.22-community",
                "toxiproxy": "shopify/toxiproxy:2.5.0",
            }[role]
            networks = {
                "txnmem-backend": {"NetworkID": backend_network_id}
            }
            if role == "toxiproxy":
                networks["txnmem-ingress"] = {
                    "NetworkID": ingress_network_id,
                    "EndpointID": "4" * 64,
                    "IPAddress": "172.20.0.2",
                    "IPPrefixLen": 16,
                }
            return {
                "Id": {
                    "qdrant": "d",
                    "neo4j": "e",
                    "toxiproxy": "f",
                }[role]
                * 64,
                "Image": "sha256:" + ("b" if role == "qdrant" else "c") * 64,
                "Config": {"Image": repository + "@sha256:" + digest},
                "NetworkSettings": {
                    "Networks": networks,
                    "Ports": ports,
                },
            }

        containers = {
            "qdrant": container("qdrant", {"6333/tcp": None, "6334/tcp": None}),
            "neo4j": container("neo4j", {"7474/tcp": None, "7687/tcp": None}),
            "toxiproxy": container(
                "toxiproxy",
                {
                    "8474/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8474"}],
                    "19000/tcp": [{"HostIp": "127.0.0.1", "HostPort": "19000"}],
                    "19001/tcp": [{"HostIp": "127.0.0.1", "HostPort": "19001"}],
                },
            ),
        }
        backend_network = {
            "Id": backend_network_id,
            "Name": "txnmem-backend",
            "Driver": "bridge",
            "Scope": "local",
            "Internal": True,
            "Attachable": False,
            "Ingress": False,
            "ConfigOnly": False,
            "EnableIPv4": True,
            "EnableIPv6": False,
            "Options": {},
            "IPAM": {
                "Driver": "default",
                "Options": None,
                "Config": [
                    {
                        "Subnet": "172.19.0.0/16",
                        "IPRange": "",
                        "Gateway": "172.19.0.1",
                    }
                ],
            },
            "Containers": {
                row["Id"]: {} for row in containers.values()
            },
        }
        ingress_network = {
            "Id": ingress_network_id,
            "Name": "txnmem-ingress",
            "Driver": "bridge",
            "Scope": "local",
            "Internal": False,
            "Attachable": False,
            "Ingress": False,
            "ConfigOnly": False,
            "EnableIPv4": True,
            "EnableIPv6": False,
            "Options": {},
            "IPAM": {
                "Driver": "default",
                "Options": None,
                "Config": [
                    {
                        "Subnet": "172.20.0.0/16",
                        "IPRange": "",
                        "Gateway": "172.20.0.1",
                    }
                ],
            },
            "Containers": {
                containers["toxiproxy"]["Id"]: {
                    "Name": "txnmem-toxiproxy",
                    "EndpointID": "4" * 64,
                    "MacAddress": "02:42:ac:14:00:02",
                    "IPv4Address": "172.20.0.2/16",
                    "IPv6Address": "",
                }
            },
        }

        attested = collector_module._normalize_docker_backend_isolation(
            containers, backend_network, ingress_network
        )

        self.assertTrue(attested["backend_network_internal"])
        self.assertTrue(attested["ingress_network_external"])
        self.assertTrue(attested["ingress_proxy_only"])
        self.assertEqual(attested["backend_network_driver"], "bridge")
        self.assertEqual(attested["ingress_network_driver"], "bridge")
        self.assertEqual(attested["backend_network_scope"], "local")
        self.assertEqual(attested["ingress_network_scope"], "local")
        self.assertTrue(attested["network_driver_options_empty"])
        self.assertTrue(attested["docker_default_ipam_driver_verified"])
        self.assertTrue(
            attested["private_non_overlapping_ipv4_subnets_verified"]
        )
        self.assertEqual(
            attested["backend_ipv4_subnet_sha256"],
            hashlib.sha256(b"172.19.0.0/16").hexdigest(),
        )
        self.assertEqual(
            attested["ingress_ipv4_subnet_sha256"],
            hashlib.sha256(b"172.20.0.0/16").hexdigest(),
        )
        self.assertEqual(
            attested["backend_bridge_interface_sha256"],
            hashlib.sha256(b"br-aaaaaaaaaaaa").hexdigest(),
        )
        self.assertEqual(
            attested["ingress_bridge_interface_sha256"],
            hashlib.sha256(b"br-999999999999").hexdigest(),
        )
        self.assertTrue(attested["networks_non_attachable"])
        self.assertTrue(attested["networks_non_swarm_ingress"])
        self.assertTrue(attested["networks_non_config_only"])
        self.assertTrue(attested["direct_backend_ports_unpublished"])
        self.assertTrue(attested["proxy_ports_loopback_only"])
        self.assertEqual(attested["published_proxy_ports"], [8474, 19000, 19001])
        self.assertEqual(
            attested["schema"], "txnmem-provenance-backend-isolation-v3"
        )
        self.assertEqual(attested["toxiproxy_ingress_ipv4"], "172.20.0.2")
        self.assertEqual(
            attested["toxiproxy_ingress_ipv4_sha256"],
            hashlib.sha256(b"172.20.0.2").hexdigest(),
        )
        self.assertEqual(
            attested["toxiproxy_ingress_endpoint_id_sha256"],
            hashlib.sha256(("4" * 64).encode("utf-8")).hexdigest(),
        )
        self.assertTrue(attested["toxiproxy_ingress_membership_verified"])
        self.assertTrue(attested["ingress_unique_workload_container_verified"])

        addressed_containers = copy.deepcopy(containers)
        addressed_backend = copy.deepcopy(backend_network)
        for role, address in (("qdrant", "172.19.0.2"), ("neo4j", "172.19.0.3")):
            addressed_containers[role]["NetworkSettings"]["Networks"][
                "txnmem-backend"
            ].update({"IPAddress": address, "IPPrefixLen": 16})
            addressed_backend["Containers"][containers[role]["Id"]] = {
                "IPv4Address": address + "/16"
            }
        self.assertEqual(
            collector_module._validated_backend_ipv4_by_role(
                addressed_containers, addressed_backend, ingress_network
            ),
            {
                "qdrant": "172.19.0.2",
                "neo4j": "172.19.0.3",
                "toxiproxy_ingress": "172.20.0.2",
            },
        )

        unsafe_backend_addresses = copy.deepcopy(addressed_containers)
        unsafe_backend_network = copy.deepcopy(addressed_backend)
        unsafe_backend_addresses["qdrant"]["NetworkSettings"]["Networks"][
            "txnmem-backend"
        ]["IPAddress"] = "169.254.0.2"
        unsafe_backend_network["Containers"][containers["qdrant"]["Id"]][
            "IPv4Address"
        ] = "169.254.0.2/16"
        with self.assertRaisesRegex(CollectorError, "address"):
            collector_module._validated_backend_ipv4_by_role(
                unsafe_backend_addresses, unsafe_backend_network, ingress_network
            )

        published_backend = copy.deepcopy(containers)
        published_backend["qdrant"]["NetworkSettings"]["Ports"]["6333/tcp"] = [
            {"HostIp": "0.0.0.0", "HostPort": "6333"}
        ]
        with self.assertRaisesRegex(CollectorError, "direct backend"):
            collector_module._normalize_docker_backend_isolation(
                published_backend, backend_network, ingress_network
            )

        for name, mutation in (
            (
                "backend_on_ingress",
                lambda values: values["qdrant"]["NetworkSettings"][
                    "Networks"
                ].update(
                    {"txnmem-ingress": {"NetworkID": ingress_network_id}}
                ),
            ),
            (
                "neo4j_on_ingress",
                lambda values: values["neo4j"]["NetworkSettings"][
                    "Networks"
                ].update(
                    {"txnmem-ingress": {"NetworkID": ingress_network_id}}
                ),
            ),
            (
                "proxy_missing_ingress",
                lambda values: values["toxiproxy"]["NetworkSettings"][
                    "Networks"
                ].pop("txnmem-ingress"),
            ),
        ):
            with self.subTest(name=name):
                drifted = copy.deepcopy(containers)
                mutation(drifted)
                with self.assertRaisesRegex(CollectorError, "network"):
                    collector_module._normalize_docker_backend_isolation(
                        drifted, backend_network, ingress_network
                    )

        extra_ingress_member = copy.deepcopy(ingress_network)
        extra_ingress_member["Containers"][containers["qdrant"]["Id"]] = {}
        with self.assertRaisesRegex(CollectorError, "ingress"):
            collector_module._normalize_docker_backend_isolation(
                containers, backend_network, extra_ingress_member
            )

        for name, container_address, network_address in (
            ("outside_subnet", "172.19.0.2", "172.19.0.2/16"),
            ("gateway", "172.20.0.1", "172.20.0.1/16"),
            ("network", "172.20.0.0", "172.20.0.0/16"),
            ("broadcast", "172.20.255.255", "172.20.255.255/16"),
        ):
            with self.subTest(name=name):
                drifted_containers = copy.deepcopy(containers)
                drifted_ingress = copy.deepcopy(ingress_network)
                drifted_containers["toxiproxy"]["NetworkSettings"]["Networks"][
                    "txnmem-ingress"
                ]["IPAddress"] = container_address
                drifted_ingress["Containers"][containers["toxiproxy"]["Id"]][
                    "IPv4Address"
                ] = network_address
                with self.assertRaisesRegex(CollectorError, "ingress"):
                    collector_module._normalize_docker_backend_isolation(
                        drifted_containers, backend_network, drifted_ingress
                    )

        in_subnet_address_disagreement = copy.deepcopy(containers)
        in_subnet_address_disagreement["toxiproxy"]["NetworkSettings"][
            "Networks"
        ]["txnmem-ingress"]["IPAddress"] = "172.20.0.3"
        with self.assertRaisesRegex(CollectorError, "ingress"):
            collector_module._normalize_docker_backend_isolation(
                in_subnet_address_disagreement, backend_network, ingress_network
            )

        container_prefix_disagreement = copy.deepcopy(containers)
        container_prefix_disagreement["toxiproxy"]["NetworkSettings"][
            "Networks"
        ]["txnmem-ingress"]["IPPrefixLen"] = 24
        with self.assertRaisesRegex(CollectorError, "ingress"):
            collector_module._normalize_docker_backend_isolation(
                container_prefix_disagreement, backend_network, ingress_network
            )

        endpoint_mismatch = copy.deepcopy(containers)
        endpoint_mismatch["toxiproxy"]["NetworkSettings"]["Networks"][
            "txnmem-ingress"
        ]["EndpointID"] = "5" * 64
        with self.assertRaisesRegex(CollectorError, "ingress"):
            collector_module._normalize_docker_backend_isolation(
                endpoint_mismatch, backend_network, ingress_network
            )

        prefix_mismatch = copy.deepcopy(ingress_network)
        prefix_mismatch["Containers"][containers["toxiproxy"]["Id"]][
            "IPv4Address"
        ] = "172.20.0.2/24"
        with self.assertRaisesRegex(CollectorError, "ingress"):
            collector_module._normalize_docker_backend_isolation(
                containers, backend_network, prefix_mismatch
            )

        missing_address = copy.deepcopy(containers)
        missing_address["toxiproxy"]["NetworkSettings"]["Networks"][
            "txnmem-ingress"
        ].pop("IPAddress")
        with self.assertRaisesRegex(CollectorError, "ingress"):
            collector_module._normalize_docker_backend_isolation(
                missing_address, backend_network, ingress_network
            )

        internal_ingress = copy.deepcopy(ingress_network)
        internal_ingress["Internal"] = True
        with self.assertRaisesRegex(CollectorError, "ingress"):
            collector_module._normalize_docker_backend_isolation(
                containers, backend_network, internal_ingress
            )

        for name, target, field, unsafe_value in (
            ("backend_macvlan", "backend", "Driver", "macvlan"),
            ("ingress_overlay", "ingress", "Driver", "overlay"),
            ("backend_global_scope", "backend", "Scope", "global"),
            ("ingress_attachable", "ingress", "Attachable", True),
            ("backend_swarm_ingress", "backend", "Ingress", True),
            ("ingress_config_only", "ingress", "ConfigOnly", True),
            ("backend_ipv4_disabled", "backend", "EnableIPv4", False),
            ("ingress_ipv6_enabled", "ingress", "EnableIPv6", True),
        ):
            with self.subTest(name=name):
                drifted_backend = copy.deepcopy(backend_network)
                drifted_ingress = copy.deepcopy(ingress_network)
                target_network = (
                    drifted_backend if target == "backend" else drifted_ingress
                )
                target_network[field] = unsafe_value
                with self.assertRaisesRegex(CollectorError, "network"):
                    collector_module._normalize_docker_backend_isolation(
                        containers, drifted_backend, drifted_ingress
                    )

        routed_ingress = copy.deepcopy(ingress_network)
        routed_ingress["Options"] = {
            "com.docker.network.bridge.gateway_mode_ipv4": "routed"
        }
        with self.assertRaisesRegex(CollectorError, "network"):
            collector_module._normalize_docker_backend_isolation(
                containers, backend_network, routed_ingress
            )

        custom_ipam = copy.deepcopy(backend_network)
        custom_ipam["IPAM"]["Driver"] = "custom"
        with self.assertRaisesRegex(CollectorError, "IPAM"):
            collector_module._normalize_docker_backend_isolation(
                containers, custom_ipam, ingress_network
            )

        for name, subnet, gateway in (
            ("public", "8.8.8.0/24", "8.8.8.1"),
            ("loopback", "127.10.0.0/16", "127.10.0.1"),
            ("link_local", "169.254.0.0/16", "169.254.0.1"),
            ("multicast", "224.0.0.0/24", "224.0.0.1"),
        ):
            with self.subTest(name=name):
                unsafe_ipam = copy.deepcopy(backend_network)
                unsafe_ipam["IPAM"]["Config"][0]["Subnet"] = subnet
                unsafe_ipam["IPAM"]["Config"][0]["Gateway"] = gateway
                with self.assertRaisesRegex(CollectorError, "private"):
                    collector_module._normalize_docker_backend_isolation(
                        containers, unsafe_ipam, ingress_network
                    )

        overlapping_ingress = copy.deepcopy(ingress_network)
        overlapping_ingress["IPAM"]["Config"][0]["Subnet"] = "172.19.0.0/16"
        overlapping_ingress["IPAM"]["Config"][0]["Gateway"] = "172.19.0.2"
        with self.assertRaisesRegex(CollectorError, "overlap"):
            collector_module._normalize_docker_backend_isolation(
                containers, backend_network, overlapping_ingress
            )

        with TemporaryDirectory() as tmp:
            sys_class_net = Path(tmp).resolve()
            for interface_name in ("br-aaaaaaaaaaaa", "br-999999999999"):
                (sys_class_net / interface_name / "bridge").mkdir(parents=True)
            profile = collector_module._normalize_docker_network_guard_profile(
                backend_network,
                ingress_network,
                sys_class_net=sys_class_net,
            )
            self.assertEqual(
                profile,
                {
                    "backend_ipv4_subnet": "172.19.0.0/16",
                    "ingress_ipv4_subnet": "172.20.0.0/16",
                    "backend_bridge_interface": "br-aaaaaaaaaaaa",
                    "ingress_bridge_interface": "br-999999999999",
                },
            )
            (sys_class_net / "br-999999999999" / "bridge").rmdir()
            with self.assertRaisesRegex(CollectorError, "bridge interface"):
                collector_module._normalize_docker_network_guard_profile(
                    backend_network,
                    ingress_network,
                    sys_class_net=sys_class_net,
                )

        unpublished_proxy = copy.deepcopy(containers)
        unpublished_proxy["toxiproxy"]["NetworkSettings"]["Ports"] = {
            "8474/tcp": None,
            "19000/tcp": None,
            "19001/tcp": None,
        }
        with self.assertRaisesRegex(CollectorError, "proxy port"):
            collector_module._normalize_docker_backend_isolation(
                unpublished_proxy, backend_network, ingress_network
            )

    def test_docker_backend_collector_uses_typed_container_and_network_inspection(self):
        containers = [{"container": index} for index in range(3)]
        backend_network = {"Name": "txnmem-backend"}
        ingress_network = {"Name": "txnmem-ingress"}
        networks = [ingress_network, backend_network]
        normalized = {"schema": "normalized"}
        with patch.object(
            collector_module.subprocess,
            "run",
            side_effect=[
                SimpleNamespace(stdout=b"containers"),
                SimpleNamespace(stdout=b"network"),
            ],
        ) as run, patch.object(
            collector_module,
            "_strict_json_bytes",
            side_effect=[containers, networks],
        ), patch.object(
            collector_module,
            "_normalize_docker_backend_isolation",
            return_value=normalized,
        ) as normalize:
            observed = collector_module._collect_docker_backend_isolation(
                qdrant_container="txnmem-qdrant",
                neo4j_container="txnmem-neo4j",
                toxiproxy_container="txnmem-toxiproxy",
            )

        self.assertEqual(observed, normalized)
        self.assertEqual(run.call_count, 2)
        self.assertEqual(run.call_args_list[0].args[0][1], "inspect")
        self.assertEqual(run.call_args_list[1].args[0][1:3], ["network", "inspect"])
        self.assertEqual(
            run.call_args_list[1].args[0][-2:],
            ["txnmem-backend", "txnmem-ingress"],
        )
        self.assertEqual(
            normalize.call_args.args[1:],
            (backend_network, ingress_network),
        )

    def test_docker_network_guard_profile_binds_same_raw_ingress_address(self):
        networks = {
            "txnmem-backend": {"Name": "txnmem-backend"},
            "txnmem-ingress": {"Name": "txnmem-ingress"},
        }
        raw_isolation = {
            "toxiproxy_ingress_ipv4": "172.20.0.2",
            "toxiproxy_ingress_ipv4_sha256": hashlib.sha256(
                b"172.20.0.2"
            ).hexdigest(),
        }
        with patch.object(
            collector_module,
            "_inspect_docker_backend_isolation_documents",
            return_value=(
                {"qdrant": {}, "neo4j": {}, "toxiproxy": {}},
                networks["txnmem-backend"],
                networks["txnmem-ingress"],
            ),
        ), patch.object(
            collector_module,
            "_normalize_docker_network_guard_profile",
            return_value={
                "backend_ipv4_subnet": "172.19.0.0/16",
                "ingress_ipv4_subnet": "172.20.0.0/16",
                "backend_bridge_interface": "br-aaaaaaaaaaaa",
                "ingress_bridge_interface": "br-999999999999",
            },
        ), patch.object(
            collector_module,
            "_normalize_docker_backend_isolation",
            return_value=raw_isolation,
        ) as normalize:
            profile = collector_module._collect_docker_network_guard_profile(
                toxiproxy_container="txnmem-toxiproxy"
            )

        self.assertEqual(
            profile,
            {
                "backend_ipv4_subnet": "172.19.0.0/16",
                "ingress_ipv4_subnet": "172.20.0.0/16",
                "backend_bridge_interface": "br-aaaaaaaaaaaa",
                "ingress_bridge_interface": "br-999999999999",
                "toxiproxy_ingress_ipv4": "172.20.0.2",
            },
        )
        normalize.assert_called_once_with(
            {"qdrant": {}, "neo4j": {}, "toxiproxy": {}},
            networks["txnmem-backend"],
            networks["txnmem-ingress"],
        )

    def test_docker_network_guard_profile_rejects_raw_ingress_hash_mismatch(self):
        networks = {
            "txnmem-backend": {"Name": "txnmem-backend"},
            "txnmem-ingress": {"Name": "txnmem-ingress"},
        }
        with patch.object(
            collector_module,
            "_inspect_docker_backend_isolation_documents",
            return_value=(
                {"qdrant": {}, "neo4j": {}, "toxiproxy": {}},
                networks["txnmem-backend"],
                networks["txnmem-ingress"],
            ),
        ), patch.object(
            collector_module,
            "_normalize_docker_backend_isolation",
            return_value={
                "toxiproxy_ingress_ipv4": "172.20.0.2",
                "toxiproxy_ingress_ipv4_sha256": "0" * 64,
            },
        ), patch.object(
            collector_module,
            "_normalize_docker_network_guard_profile",
            return_value={
                "backend_ipv4_subnet": "172.19.0.0/16",
                "ingress_ipv4_subnet": "172.20.0.0/16",
                "backend_bridge_interface": "br-aaaaaaaaaaaa",
                "ingress_bridge_interface": "br-999999999999",
            },
        ):
            with self.assertRaisesRegex(CollectorError, "identity hash"):
                collector_module._collect_docker_network_guard_profile(
                    toxiproxy_container="txnmem-toxiproxy"
                )

    def test_formal_candidate_root_is_derived_from_run_and_nonce(self):
        with TemporaryDirectory() as tmp:
            base = Path(tmp).resolve()
            run_hash = "1" * 64
            nonce_hash = "2" * 64

            expected = collector_module._formal_candidate_root(
                run_hash,
                nonce_hash,
                runs_root=base,
            )

            self.assertEqual(
                expected,
                base / f"run-{run_hash}-{nonce_hash[:16]}" / "candidate",
            )
            self.assertEqual(
                collector_module._require_derived_candidate_root(
                    expected,
                    run_hash=run_hash,
                    nonce_hash=nonce_hash,
                    runs_root=base,
                ),
                expected,
            )
            with self.assertRaisesRegex(CollectorError, "derived formal run"):
                collector_module._require_derived_candidate_root(
                    base / "caller-selected" / "candidate",
                    run_hash=run_hash,
                    nonce_hash=nonce_hash,
                    runs_root=base,
                )

    def test_formal_run_workspace_is_exclusive_and_identity_bound(self):
        with TemporaryDirectory() as tmp:
            base = Path(tmp).resolve() / "runs"
            base.mkdir(mode=0o750)
            run_hash = "3" * 64
            nonce_hash = "4" * 64

            workspace = collector_module._prepare_formal_run_workspace(
                run_hash,
                nonce_hash,
                runs_root=base,
                controller_uid=os.getuid(),
                runner_uid=os.getuid(),
                runner_gid=os.getgid(),
                require_root=False,
            )

            self.assertEqual(workspace.root.stat().st_mode & 0o777, 0o750)
            self.assertEqual(workspace.candidate.stat().st_mode & 0o777, 0o700)
            self.assertEqual(
                (workspace.root_device, workspace.root_inode),
                (workspace.root.stat().st_dev, workspace.root.stat().st_ino),
            )
            with self.assertRaisesRegex(CollectorError, "already exists"):
                collector_module._prepare_formal_run_workspace(
                    run_hash,
                    nonce_hash,
                    runs_root=base,
                    controller_uid=os.getuid(),
                    runner_uid=os.getuid(),
                    runner_gid=os.getgid(),
                    require_root=False,
                )

    def test_formal_input_tree_is_root_owned_group_read_only_and_byte_bound(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve() / "inputs"
            root.mkdir(mode=0o700)
            nested = root / "source" / "src"
            nested.mkdir(parents=True, mode=0o700)
            payload = nested / "runner.py"
            payload.write_bytes(b"print('sealed')\n")

            manifest = collector_module._publish_formal_input_tree(
                root,
                controller_uid=os.getuid(),
                runner_gid=os.getgid(),
            )

            self.assertEqual(root.stat().st_mode & 0o777, 0o550)
            self.assertEqual(nested.stat().st_mode & 0o777, 0o550)
            self.assertEqual(payload.stat().st_mode & 0o777, 0o440)
            self.assertEqual(manifest["schema"], "txnmem-provenance-input-tree-v1")
            self.assertEqual(manifest["file_count"], 1)
            self.assertEqual(manifest["directory_count"], 3)
            self.assertRegex(manifest["tree_sha256"], r"^[0-9a-f]{64}$")
            self.assertEqual(
                collector_module._verify_formal_input_tree(
                    root,
                    controller_uid=os.getuid(),
                    runner_gid=os.getgid(),
                ),
                manifest,
            )

    def test_formal_privilege_drop_sets_closed_supplementary_groups_first(self):
        calls = []
        with patch.object(
            collector_module,
            "_set_no_new_privileges",
            side_effect=lambda: calls.append(("no-new-privileges", True)),
        ), patch.object(collector_module.os, "geteuid", side_effect=[0, 65532]), patch.object(
            collector_module.os, "setgroups", side_effect=lambda value: calls.append(("groups", value))
        ), patch.object(
            collector_module.os, "setgid", side_effect=lambda value: calls.append(("gid", value))
        ), patch.object(
            collector_module.os, "setuid", side_effect=lambda value: calls.append(("uid", value))
        ), patch.object(
            collector_module.os, "getgid", return_value=65532
        ), patch.object(
            collector_module.os, "getegid", return_value=65532
        ), patch.object(
            collector_module.os, "umask"
        ), patch.object(
            collector_module.os, "getuid", return_value=65532
        ):
            collector_module._drop_formal_child_privileges(65532, 65532)

        self.assertEqual(
            calls,
            [
                ("no-new-privileges", True),
                ("groups", []),
                ("gid", 65532),
                ("uid", 65532),
            ],
        )

    def test_candidate_seal_is_tree_complete_and_receipt_bound(self):
        with TemporaryDirectory() as tmp:
            candidate = Path(tmp).resolve() / "candidate"
            candidate.mkdir(mode=0o700)
            bundle = candidate / "bundle"
            bundle.mkdir(mode=0o700)
            first = bundle / "first.json"
            second = candidate / "second.jsonl"
            first.write_bytes(b'{"value":1}\n')
            second.write_bytes(b'{"value":2}\n')
            receipt = {
                "schema": "txnmem-provenance-candidate-attestation-material-v1",
                "candidate_bundle_id": "diagnostic-vector_graph-" + "1" * 16 + "-" + "2" * 16,
                "run_id_sha256": "2" * 64,
                "config_sha256": "1" * 64,
                "config_file_sha256": "3" * 64,
                "workload_sha256": "4" * 64,
                "environment_attestation_sha256": "5" * 64,
                "evidence_manifest_sha256": "6" * 64,
                "matrix_cell_count": 15,
                "repetition_count": 450,
                "operation_sample_count": 14400,
                "observed_service_versions": {
                    "qdrant": "1.15.4",
                    "neo4j": "5.26.0",
                    "toxiproxy": "2.9.0",
                },
                "candidate_operation_samples_sha256": "7" * 64,
                "candidate_repetitions_sha256": "8" * 64,
            }

            sealed = collector_module._seal_candidate_tree(
                candidate,
                expected_owner_uid=os.getuid(),
                sealed_owner_uid=os.getuid(),
                sealed_owner_gid=os.getgid(),
                completion_receipt=receipt,
            )

            self.assertEqual(sealed["schema"], "txnmem-provenance-candidate-seal-v1")
            self.assertEqual(sealed["file_count"], 2)
            self.assertEqual(sealed["directory_count"], 2)
            self.assertRegex(sealed["tree_sha256"], r"^[0-9a-f]{64}$")
            self.assertEqual(
                sealed["completion_receipt_sha256"],
                hashlib.sha256(
                    json.dumps(
                        receipt, sort_keys=True, separators=(",", ":")
                    ).encode("utf-8")
                ).hexdigest(),
            )
            self.assertEqual(candidate.stat().st_mode & 0o777, 0o500)
            self.assertEqual(bundle.stat().st_mode & 0o777, 0o500)
            self.assertEqual(first.stat().st_mode & 0o777, 0o400)
            self.assertEqual(second.stat().st_mode & 0o777, 0o400)

    def test_immutable_runner_requires_and_consumes_runtime_snapshot(self):
        import txnmem_provenance_runner as runner_module

        with TemporaryDirectory() as tmp:
            runtime = Path(tmp).resolve()
            with patch.dict(
                os.environ,
                {
                    "TXNMEM_PROVENANCE_START_GATE_FD": "10",
                    "TXNMEM_PROVENANCE_READY_FD": "11",
                },
                clear=True,
            ), patch.object(runner_module.os, "write", return_value=1), patch.object(
                runner_module.os, "read", return_value=b"G"
            ), patch.object(runner_module.os, "close"):
                self.assertEqual(runner_module.main(["invalid-command"]), 70)

            with patch.dict(
                os.environ,
                {
                    "TXNMEM_PROVENANCE_START_GATE_FD": "10",
                    "TXNMEM_PROVENANCE_READY_FD": "11",
                    "TXNMEM_PROVENANCE_RUNTIME_SITE": str(runtime),
                    "TXNMEM_PROVENANCE_COMPLETION_FD": "12",
                },
                clear=True,
            ), patch.object(runner_module.os, "write", return_value=1), patch.object(
                runner_module.os, "read", return_value=b"G"
            ), patch.object(runner_module.os, "close"):
                self.assertEqual(runner_module.main(["invalid-command"]), 72)
                self.assertNotIn(
                    "TXNMEM_PROVENANCE_RUNTIME_SITE", os.environ
                )

    def test_formal_child_spec_uses_only_immutable_export_and_redacts_secret(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            export = root / "source-export"
            (export / "src").mkdir(parents=True)
            (export / "configs").mkdir()
            runner = export / "src" / "txnmem_provenance_runner.py"
            runner.write_text("# immutable runner\n", encoding="utf-8")
            config = export / "configs" / "provenance_performance_matrix.json"
            config.write_text("{}\n", encoding="utf-8")
            runtime_lock = export / "configs" / "provenance_runtime_lock.json"
            runtime_lock.write_bytes(
                (
                    Path(__file__).resolve().parents[1]
                    / "configs"
                    / "provenance_runtime_lock.json"
                ).read_bytes()
            )
            candidate = root / "candidate"
            candidate.mkdir()
            environment = root / "environment.json"
            environment.write_text(
                json.dumps(self._environment(), sort_keys=True) + "\n",
                encoding="utf-8",
            )
            environment_hash = hashlib.sha256(
                collector_module.canonical_json_bytes(self._environment())
            ).hexdigest()
            source_manifest_hash = "a" * 64
            password = "fixture-secret-password"

            python_hash = collector_module._file_sha256(
                Path(sys.executable).resolve(), "test Python executable"
            )
            runtime_snapshot = root / "runtime-snapshot"
            runtime_package = runtime_snapshot / "neo4j" / "__init__.py"
            runtime_package.parent.mkdir(parents=True)
            runtime_package.write_bytes(b"# immutable neo4j runtime\n")
            runtime_package.chmod(0o400)
            runtime_package.parent.chmod(0o500)
            runtime_snapshot.chmod(0o500)
            runtime_manifest = self._runtime_manifest(
                executable_hash=python_hash,
                version=collector_module.platform.python_version(),
                file_hash=hashlib.sha256(runtime_package.read_bytes()).hexdigest(),
            )
            spec = collector_module._build_formal_child_spec(
                    source_export=export,
                    runtime_snapshot=runtime_snapshot,
                    runtime_manifest=runtime_manifest,
                    candidate_root=candidate,
                    environment_attestation_path=environment,
                    run_id="formal-fixture-run",
                    transport="local_loopback",
                    qdrant_url="http://127.0.0.1:19000",
                    neo4j_uri="bolt://127.0.0.1:19001",
                    toxiproxy_url="http://127.0.0.1:8474",
                    neo4j_user="neo4j",
                    neo4j_password=password,
                    source_manifest_sha256=source_manifest_hash,
                    runner_sha256=hashlib.sha256(runner.read_bytes()).hexdigest(),
                    config_file_sha256=hashlib.sha256(config.read_bytes()).hexdigest(),
                    environment_attestation_sha256=environment_hash,
                    external_tools=self._external_tools(python_hash),
                )

            joined_command = "\0".join(spec.command)
            encoded_manifest = json.dumps(spec.command_manifest, sort_keys=True)
            self.assertEqual(
                spec.command[:4], (sys.executable, "-I", "-S", "-B")
            )
            self.assertEqual(Path(spec.command[4]), runner)
            self.assertEqual(spec.cwd, export)
            self.assertNotIn(str(Path.cwd()), joined_command)
            self.assertNotIn(password, encoded_manifest)
            self.assertNotIn(password, joined_command)
            self.assertEqual(spec.environment["TXNMEM_NEO4J_PASSWORD"], password)
            self.assertNotIn("PYTHONPATH", spec.environment)
            self.assertEqual(
                set(spec.environment),
                {
                    "LANG",
                    "LC_ALL",
                    "PYTHONDONTWRITEBYTECODE",
                    "TXNMEM_NEO4J_URI",
                    "TXNMEM_NEO4J_USER",
                    "TXNMEM_NEO4J_PASSWORD",
                    "TXNMEM_PROVENANCE_RUNTIME_SITE",
                },
            )
            self.assertEqual(
                Path(spec.environment["TXNMEM_PROVENANCE_RUNTIME_SITE"]),
                runtime_snapshot,
            )
            self.assertEqual(
                spec.command_manifest["source_manifest_sha256"],
                source_manifest_hash,
            )
            self.assertEqual(
                spec.command_manifest["argv_sha256"],
                hashlib.sha256(joined_command.encode("utf-8")).hexdigest(),
            )
            self.assertEqual(
                spec.command_manifest["secret_environment_variables"],
                ["TXNMEM_NEO4J_PASSWORD"],
            )
            self.assertEqual(spec.command_manifest["qdrant_endpoint_port"], 19000)
            self.assertEqual(spec.command_manifest["neo4j_endpoint_port"], 19001)
            self.assertEqual(
                spec.command_manifest["environment_attestation_file_sha256"],
                hashlib.sha256(environment.read_bytes()).hexdigest(),
            )
            self.assertEqual(
                spec.command_manifest["runtime_manifest"], runtime_manifest
            )

            with self.assertRaisesRegex(
                CollectorError, "environment attestation hash"
            ):
                collector_module._build_formal_child_spec(
                        source_export=export,
                        runtime_snapshot=runtime_snapshot,
                        runtime_manifest=runtime_manifest,
                        candidate_root=candidate,
                        environment_attestation_path=environment,
                        run_id="formal-fixture-run",
                        transport="local_loopback",
                        qdrant_url="http://127.0.0.1:19000",
                        neo4j_uri="bolt://127.0.0.1:19001",
                        toxiproxy_url="http://127.0.0.1:8474",
                        neo4j_user="neo4j",
                        neo4j_password=password,
                        source_manifest_sha256=source_manifest_hash,
                        runner_sha256=hashlib.sha256(runner.read_bytes()).hexdigest(),
                        config_file_sha256=hashlib.sha256(config.read_bytes()).hexdigest(),
                        environment_attestation_sha256="0" * 64,
                        external_tools=self._external_tools(python_hash),
                    )

            with self.assertRaisesRegex(CollectorError, "Toxiproxy"):
                collector_module._build_formal_child_spec(
                    source_export=export,
                    runtime_snapshot=runtime_snapshot,
                    runtime_manifest=runtime_manifest,
                    candidate_root=candidate,
                    environment_attestation_path=environment,
                    run_id="formal-fixture-run",
                    transport="local_loopback",
                    qdrant_url="http://127.0.0.1:6333",
                    neo4j_uri="bolt://127.0.0.1:19001",
                    toxiproxy_url="http://127.0.0.1:8474",
                    neo4j_user="neo4j",
                    neo4j_password=password,
                    source_manifest_sha256=source_manifest_hash,
                    runner_sha256=hashlib.sha256(runner.read_bytes()).hexdigest(),
                    config_file_sha256=hashlib.sha256(config.read_bytes()).hexdigest(),
                    environment_attestation_sha256=environment_hash,
                    external_tools=self._external_tools(python_hash),
                )

            with self.assertRaisesRegex(CollectorError, "Toxiproxy"):
                collector_module._build_formal_child_spec(
                    source_export=export,
                    runtime_snapshot=runtime_snapshot,
                    runtime_manifest=runtime_manifest,
                    candidate_root=candidate,
                    environment_attestation_path=environment,
                    run_id="formal-fixture-run",
                    transport="local_loopback",
                    qdrant_url="http://127.0.0.1:19000",
                    neo4j_uri="neo4j://127.0.0.1:19001",
                    toxiproxy_url="http://127.0.0.1:8474",
                    neo4j_user="neo4j",
                    neo4j_password=password,
                    source_manifest_sha256=source_manifest_hash,
                    runner_sha256=hashlib.sha256(runner.read_bytes()).hexdigest(),
                    config_file_sha256=hashlib.sha256(config.read_bytes()).hexdigest(),
                    environment_attestation_sha256=environment_hash,
                    external_tools=self._external_tools(python_hash),
                )

            with self.assertRaisesRegex(CollectorError, "Toxiproxy"):
                collector_module._build_formal_child_spec(
                    source_export=export,
                    runtime_snapshot=runtime_snapshot,
                    runtime_manifest=runtime_manifest,
                    candidate_root=candidate,
                    environment_attestation_path=environment,
                    run_id="formal-fixture-run",
                    transport="local_loopback",
                    qdrant_url="http://192.0.2.1:19000",
                    neo4j_uri="bolt://127.0.0.1:19001",
                    toxiproxy_url="http://127.0.0.1:8474",
                    neo4j_user="neo4j",
                    neo4j_password=password,
                    source_manifest_sha256=source_manifest_hash,
                    runner_sha256=hashlib.sha256(runner.read_bytes()).hexdigest(),
                    config_file_sha256=hashlib.sha256(config.read_bytes()).hexdigest(),
                    environment_attestation_sha256=environment_hash,
                    external_tools=self._external_tools(python_hash),
                )

    def test_formal_execution_api_has_no_caller_injected_runner_probe_or_hashes(self):
        parameters = set(
            inspect.signature(
                collector_module.collect_formal_execution
            ).parameters
        )
        self.assertTrue(
            {
                "project_root",
                "candidate_root",
                "launch_path",
                "completion_path",
                "authorization_nonce_path",
                "run_id",
                "transport",
                "qdrant_url",
                "neo4j_uri",
                "toxiproxy_url",
            }.issubset(parameters)
        )
        self.assertTrue(
            {
                "run_candidate",
                "topology_probe",
                "source_identity_loader",
                "candidate_material_loader",
                "command_sha256",
                "qdrant_proxy",
                "neo4j_proxy",
                "qdrant_container",
                "neo4j_container",
                "toxiproxy_container",
                "neo4j_user",
                "environment_attestation_path",
            }.isdisjoint(parameters)
        )
        self.assertFalse(
            hasattr(collector_module, "collect_execution_evidence")
        )

    def test_environment_attestation_is_collected_and_written_by_controller(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            with patch.object(
                collector_module, "_linux_memory_total_bytes", return_value=32 * 1024**3
            ), patch.object(
                collector_module, "_linux_disk_medium", return_value="nvme"
            ), patch.object(
                collector_module,
                "_measure_background_cpu_busy_permille",
                return_value=75,
            ), patch.object(
                collector_module.os, "cpu_count", return_value=16
            ), patch.object(
                collector_module,
                "_http_read",
                return_value=(b'{"version":"2.9.0"}', 0.1),
            ):
                document = collector_module._collect_formal_environment_attestation(
                    toxiproxy_url="http://127.0.0.1:8474",
                    storage_path=root,
                )
                path, raw = collector_module._write_collected_environment_snapshot(
                    root, document
                )

            self.assertEqual(document["source"], "collector-observation-v2")
            self.assertTrue(document["isolation_verified"])
            self.assertFalse(document["co_tenant_load_detected"])
            self.assertEqual(document["cpu_logical_count"], 16)
            self.assertEqual(document["memory_total_bytes"], 32 * 1024**3)
            self.assertEqual(document["disk_medium"], "nvme")
            self.assertEqual(document["toxiproxy_version"], "2.9.0")
            self.assertEqual(path.read_bytes(), raw)
            self.assertEqual(path.stat().st_mode & 0o777, 0o400)
            self.assertEqual(
                json.loads(raw), document
            )

    def test_collected_environment_fails_closed_on_background_cpu_load(self):
        with TemporaryDirectory() as tmp, patch.object(
            collector_module, "_linux_memory_total_bytes", return_value=1024
        ), patch.object(
            collector_module, "_linux_disk_medium", return_value="ssd"
        ), patch.object(
            collector_module,
            "_measure_background_cpu_busy_permille",
            return_value=201,
        ), patch.object(
            collector_module.os, "cpu_count", return_value=4
        ), patch.object(
            collector_module,
            "_http_read",
            return_value=(b'"2.9.0"', 0.1),
        ):
            with self.assertRaisesRegex(CollectorError, "co-tenant load"):
                collector_module._collect_formal_environment_attestation(
                    toxiproxy_url="http://127.0.0.1:8474",
                    storage_path=Path(tmp),
                )

    def test_execution_integrity_monitor_samples_continuously_and_detects_drift(self):
        baseline = {
            "network_guard": self._network_guard(),
            "toxiproxy_routes": self._snapshot()["proxy_routes"],
            "backend_isolation": self._backend_isolation(),
            "runner_uid_processes": [{"pid": 7, "start_identity": "42"}],
            "host_environment": {
                "host_identity_sha256": "4" * 64,
                "cpu_logical_count": 8,
                "memory_total_bytes": 1024,
                "disk_medium": "ssd",
            },
            "load1_milli": 100,
        }
        terminal = copy.deepcopy(baseline)
        terminal["runner_uid_processes"] = []

        monitor = collector_module._ExecutionIntegrityMonitor(
            probe=lambda: copy.deepcopy(baseline),
            terminal_probe=lambda: copy.deepcopy(terminal),
            interval_seconds=0.005,
        )
        monitor.start()
        gate_release = time.monotonic_ns()
        time.sleep(0.025)
        child_exit = time.monotonic_ns()
        summary = monitor.finalize(
            gate_release_monotonic_ns=gate_release,
            child_exit_monotonic_ns=child_exit,
        )

        self.assertGreaterEqual(summary["sample_count"], 2)
        self.assertEqual(summary["violation_count"], 0)
        self.assertRegex(summary["samples_sha256"], r"^[0-9a-f]{64}$")

        calls = 0

        def drifting_probe():
            nonlocal calls
            calls += 1
            sample = copy.deepcopy(baseline)
            if calls >= 2:
                sample["backend_isolation"]["network_id_sha256"] = "0" * 64
            return sample

        monitor = collector_module._ExecutionIntegrityMonitor(
            probe=drifting_probe,
            terminal_probe=lambda: copy.deepcopy(terminal),
            interval_seconds=0.005,
        )
        monitor.start()
        gate_release = time.monotonic_ns()
        time.sleep(0.02)
        child_exit = time.monotonic_ns()
        with self.assertRaisesRegex(CollectorError, "integrity monitor"):
            monitor.finalize(
                gate_release_monotonic_ns=gate_release,
                child_exit_monotonic_ns=child_exit,
            )

    def test_execution_monitor_rejects_excessive_load_and_terminal_drift(self):
        baseline = {
            "network_guard": self._network_guard(),
            "toxiproxy_routes": self._snapshot()["proxy_routes"],
            "backend_isolation": self._backend_isolation(),
            "runner_uid_processes": [{"pid": 7, "start_identity": "42"}],
            "host_environment": {
                "host_identity_sha256": "4" * 64,
                "cpu_logical_count": 2,
                "memory_total_bytes": 1024,
                "disk_medium": "ssd",
            },
            "load1_milli": 2001,
        }
        with self.assertRaisesRegex(CollectorError, "excessive host load"):
            collector_module._normalize_execution_monitor_probe(baseline)

        baseline["load1_milli"] = 100
        terminal = copy.deepcopy(baseline)
        terminal["runner_uid_processes"] = []
        terminal["backend_isolation"]["network_id_sha256"] = "0" * 64
        monitor = collector_module._ExecutionIntegrityMonitor(
            probe=lambda: copy.deepcopy(baseline),
            terminal_probe=lambda: copy.deepcopy(terminal),
            interval_seconds=0.005,
        )
        monitor.start()
        gate_release = time.monotonic_ns()
        time.sleep(0.01)
        child_exit = time.monotonic_ns()
        with self.assertRaisesRegex(CollectorError, "integrity monitor"):
            monitor.finalize(
                gate_release_monotonic_ns=gate_release,
                child_exit_monotonic_ns=child_exit,
            )

    def test_formal_cleanup_attempts_monitor_guard_and_child_independently(self):
        events = []

        class Monitor:
            def abort(self):
                events.append("monitor")
                raise RuntimeError("monitor cleanup")

        class Guard:
            active = True

            def deactivate(self):
                events.append("guard")
                raise RuntimeError("guard cleanup")

        class Process:
            def poll(self):
                return 0

        class Child:
            process = Process()

            def close(self):
                events.append("child")
                raise RuntimeError("child cleanup")

        failures = collector_module._cleanup_formal_execution_resources(
            execution_monitor=Monitor(), network_guard=Guard(), child=Child()
        )

        self.assertEqual(events, ["monitor", "guard", "child"])
        self.assertEqual(len(failures), 3)

    def test_collector_rejects_output_inside_candidate_and_existing_launch(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            project = root / "project"
            project.mkdir()
            candidate = root / "candidate"
            candidate.mkdir()
            environment = root / "environment.json"
            environment.write_text(json.dumps(self._environment()), encoding="utf-8")
            private = root / "private"
            private.mkdir(mode=0o700)
            common = {
                "project_root": project,
                "candidate_root": candidate,
                "completion_path": private / "completion.json",
                "run_id": "unsafe",
                "transport": "local_loopback",
                "config_sha256": "1" * 64,
                "config_file_sha256": "2" * 64,
                "workload_sha256": "3" * 64,
                "environment_attestation_sha256": "4" * 64,
                "authorization_nonce": self.AUTHORIZATION_NONCE,
                "network_guard_activate": lambda: self._network_guard(),
                "network_guard_finalize": lambda: self._network_guard(),
                "network_guard_deactivate": lambda: None,
                "execution_monitor_start": lambda: None,
                "execution_monitor_finalize": lambda: self._execution_monitor(),
                "run_candidate": lambda: 0,
                "candidate_sealer": lambda _candidate, _receipt: {},
                "topology_probe": lambda _phase: self._snapshot(),
                "source_identity_loader": lambda _root: self._source_identity(),
                "external_tool_identity_loader": lambda: unsafe_command[
                    "external_tools"
                ],
                "runtime_identity_loader": lambda: self._runtime_manifest(),
                "candidate_material_loader": lambda *_args: {},
            }
            unsafe_source = self._source_identity()
            unsafe_command = self._command_manifest(
                run_hash=hashlib.sha256(b"unsafe").hexdigest(),
                config_file_hash="2" * 64,
                environment_hash="4" * 64,
                source_identity=unsafe_source,
            )
            common["command_manifest"] = unsafe_command
            common["child_process"] = self._child_process(unsafe_command)
            with self.assertRaises(CollectorError):
                _collect_execution_evidence(
                    launch_path=candidate / "launch.json", **common
                )
            launch = private / "launch.json"
            launch.write_text("sentinel", encoding="utf-8")
            with self.assertRaises(CollectorError):
                _collect_execution_evidence(launch_path=launch, **common)
            self.assertEqual(launch.read_text(encoding="utf-8"), "sentinel")

            private.chmod(0o500)
            try:
                with self.assertRaisesRegex(CollectorError, "mode 0700"):
                    collector_module._preflight_external_outputs(
                        project,
                        candidate,
                        private / "new-launch.json",
                        private / "new-completion.json",
                    )
            finally:
                private.chmod(0o700)

    def test_toxiproxy_route_normalization_rejects_drift_and_active_toxics(self):
        route = collector_module._normalize_toxiproxy_proxy(
            {
                "name": "txnmem-qdrant",
                "listen": "0.0.0.0:19000",
                "upstream": "qdrant:6333",
                "enabled": True,
                "toxics": [],
            },
            role="qdrant",
        )
        self.assertEqual(
            route,
            {
                "role": "qdrant",
                "proxy_name": "txnmem-qdrant",
                "listen": "0.0.0.0:19000",
                "upstream": "qdrant:6333",
                "enabled": True,
                "toxics_count": 0,
            },
        )

        ipv6_wildcard_route = collector_module._normalize_toxiproxy_proxy(
            {
                "name": "txnmem-qdrant",
                "listen": "[::]:19000",
                "upstream": "qdrant:6333",
                "enabled": True,
                "toxics": [],
                "Logger": {},
            },
            role="qdrant",
        )
        self.assertEqual(ipv6_wildcard_route, route)

        mutations = (
            {"name": "other"},
            {"listen": "0.0.0.0:6333"},
            {"listen": "[::1]:19000"},
            {"listen": "[2001:db8::1]:19000"},
            {"Logger": {"level": "debug"}},
            {"Logger": []},
            {"upstream": "unrelated:6333"},
            {"enabled": False},
            {"toxics": [{"name": "latency"}]},
        )
        base = {
            "name": "txnmem-qdrant",
            "listen": "0.0.0.0:19000",
            "upstream": "qdrant:6333",
            "enabled": True,
            "toxics": [],
        }
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                document = {**base, **mutation}
                with self.assertRaises(CollectorError):
                    collector_module._normalize_toxiproxy_proxy(
                        document, role="qdrant"
                    )

    def test_topology_metrics_call_uses_strict_snapshot_parser(self):
        routes = [
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
        metrics = "\n".join(
            (
                'toxiproxy_proxy_received_bytes_total{direction="upstream",proxy="txnmem-qdrant",listener="[::]:19000",upstream="qdrant:6333"} 11',
                'toxiproxy_proxy_sent_bytes_total{direction="upstream",proxy="txnmem-qdrant",listener="[::]:19000",upstream="qdrant:6333"} 13',
                'toxiproxy_proxy_received_bytes_total{direction="downstream",proxy="txnmem-qdrant",listener="[::]:19000",upstream="qdrant:6333"} 17',
                'toxiproxy_proxy_sent_bytes_total{direction="downstream",proxy="txnmem-qdrant",listener="[::]:19000",upstream="qdrant:6333"} 19',
                'toxiproxy_proxy_received_bytes_total{direction="upstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 23',
                'toxiproxy_proxy_sent_bytes_total{direction="upstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 29',
                'toxiproxy_proxy_received_bytes_total{direction="downstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 31',
                'toxiproxy_proxy_sent_bytes_total{direction="downstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 37',
            )
        )
        events = []

        class Driver:
            def get_server_info(self):
                return SimpleNamespace(agent="Neo4j/5.26.0")

            def close(self):
                pass

        class GraphDatabase:
            @staticmethod
            def driver(_uri, auth):
                self.assertEqual(auth, ("neo4j", "password"))
                events.append("neo4j_health")
                return Driver()

        def http_read(url):
            if url.endswith("/metrics"):
                events.append("baseline_a")
                return metrics.encode("utf-8"), 0.0
            if url.endswith("/version"):
                events.append("toxiproxy_health")
                return b'"2.5.0"', 0.0
            self.assertTrue(url.endswith("/"))
            events.append("qdrant_health")
            return b'{"version":"1.15.4"}', 0.0

        def prepare_routes(*_args, **_kwargs):
            events.append("routes")
            return copy.deepcopy(routes)

        with patch.object(
            collector_module,
            "prepare_isolated_toxiproxy_routes",
            side_effect=prepare_routes,
        ), patch.object(
            collector_module, "_http_read", side_effect=http_read
        ), patch.object(
            collector_module,
            "_locked_neo4j_graph_database",
            return_value=nullcontext(GraphDatabase),
        ), patch.object(
            collector_module, "_host_identity", return_value="host"
        ), patch.object(
            collector_module, "_docker_owner", return_value="0:0"
        ), patch.object(
            collector_module, "_collect_docker_backend_isolation", return_value={}
        ):
            snapshot = collector_module.collect_docker_topology_snapshot(
                "before",
                qdrant_url="http://qdrant",
                neo4j_uri="bolt://neo4j",
                toxiproxy_url="http://toxiproxy",
                neo4j_auth=("neo4j", "password"),
                qdrant_proxy="txnmem-qdrant",
                neo4j_proxy="txnmem-neo4j",
                qdrant_container="txnmem-qdrant",
                neo4j_container="txnmem-neo4j",
                toxiproxy_container="txnmem-toxiproxy",
                client_owner="0:0",
                client_python_version="3.11.9",
                runtime_snapshot=Path("/unused"),
            )

        self.assertEqual(events, ["routes", "qdrant_health", "neo4j_health", "toxiproxy_health", "baseline_a"])
        self.assertEqual(snapshot["schema"], "txnmem-provenance-topology-snapshot-v3")
        self.assertEqual(
            set(snapshot),
            {"schema", "roles", "proxy_routes", "proxy_counters", "backend_isolation"},
        )
        self.assertTrue(
            all("proxy_counter_bytes" not in row for row in snapshot["roles"])
        )
        self.assertEqual(
            snapshot["proxy_counters"],
            proxy_snapshot(
                "baseline_a",
                qdrant=(11, 13, 17, 19),
                neo4j=(23, 29, 31, 37),
            ),
        )

    def test_completion_metrics_translate_strict_parser_errors_without_payload(self):
        routes = [
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
        payload = "not-a-toxiproxy-metric"

        with patch.object(
            collector_module, "_http_read", return_value=(payload.encode("utf-8"), 0.0)
        ), patch.object(
            collector_module, "observe_formal_toxiproxy_routes", return_value=routes
        ):
            with self.assertRaisesRegex(
                CollectorError, "^formal Toxiproxy metrics are invalid$"
            ) as raised:
                collector_module.capture_toxiproxy_counter_snapshot(
                    "http://toxiproxy",
                    phase="final",
                    proxy_routes=routes,
                )

        self.assertNotIn(payload, str(raised.exception))

    def test_toxiproxy_attribution_baseline_requires_zero_and_exact_routes(self):
        baseline_a = proxy_snapshot(
            "baseline_a",
            qdrant=(11, 13, 17, 19),
            neo4j=(23, 29, 31, 37),
        )
        baseline_b = proxy_snapshot(
            "baseline_b",
            qdrant=(11, 13, 17, 19),
            neo4j=(23, 29, 31, 37),
        )

        self.assertIsNone(
            collector_module._validate_toxiproxy_attribution_boundary(
                baseline_a,
                baseline_b,
                PROXY_ROUTES,
                PROXY_ROUTES,
            )
        )

        drifted = proxy_snapshot(
            "baseline_b",
            qdrant=(12, 13, 17, 19),
            neo4j=(23, 29, 31, 37),
        )
        with self.assertRaisesRegex(CollectorError, "not quiescent"):
            collector_module._validate_toxiproxy_attribution_boundary(
                baseline_a,
                drifted,
                PROXY_ROUTES,
                PROXY_ROUTES,
            )
        with self.assertRaisesRegex(CollectorError, "routes changed"):
            collector_module._validate_toxiproxy_attribution_boundary(
                baseline_a,
                baseline_b,
                PROXY_ROUTES,
                list(reversed(PROXY_ROUTES)),
            )

    def test_toxiproxy_attribution_rejects_final_regression_and_zero_backend_delta(self):
        baseline_b = proxy_snapshot(
            "baseline_b",
            qdrant=(11, 13, 17, 19),
            neo4j=(23, 29, 31, 37),
        )
        final = proxy_snapshot(
            "final",
            qdrant=(21, 33, 47, 69),
            neo4j=(73, 89, 101, 127),
        )

        deltas = collector_module._derive_toxiproxy_attribution_deltas(
            baseline_b, final
        )
        self.assertEqual([row["total_bytes"] for row in deltas["routes"]], [110, 270])
        self.assertEqual(deltas["toxiproxy_total_bytes"], 380)

        cases = {
            "regression": proxy_snapshot(
                "final",
                qdrant=(10, 33, 47, 69),
                neo4j=(73, 89, 101, 127),
            ),
            "qdrant_zero": proxy_snapshot(
                "final",
                qdrant=(11, 13, 17, 19),
                neo4j=(73, 89, 101, 127),
            ),
            "neo4j_zero": proxy_snapshot(
                "final",
                qdrant=(21, 33, 47, 69),
                neo4j=(23, 29, 31, 37),
            ),
        }
        for name, candidate in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(CollectorError):
                    collector_module._derive_toxiproxy_attribution_deltas(
                        baseline_b, candidate
                    )

    def test_source_attestation_rejects_a_dirty_formal_runner_blob(self):
        import txnmem_provenance_execution_collector as collector_module
        from unittest.mock import patch

        paths = (
            "src/txnmem_provenance_execution_collector.py",
            "src/txnmem_provenance_runner.py",
            "src/txnmem_experiment.py",
            "src/txnmem_backend.py",
        )
        with TemporaryDirectory() as tmp, patch.object(
            collector_module, "_SOURCE_PATHS_FOR_TESTS", paths
        ):
            workspace = Path(tmp).resolve()
            root = workspace / "project"
            (root / "src").mkdir(parents=True)
            for relative in paths:
                (root / relative).write_text(f"# {relative}\n", encoding="utf-8")
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            subprocess.run(
                ["git", "config", "user.email", "collector@example.invalid"],
                cwd=root,
                check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "Collector Fixture"],
                cwd=root,
                check=True,
            )
            subprocess.run(["git", "add", "src"], cwd=root, check=True)
            subprocess.run(
                ["git", "commit", "-q", "-m", "fixture"], cwd=root, check=True
            )

            identity = attest_committed_source(root)
            private = workspace / "private"
            private.mkdir(mode=0o700)
            exported = create_immutable_source_export(root, private, identity)
            exported_backend_before = (
                exported / "src" / "txnmem_backend.py"
            ).read_bytes()
            exported_runner = (
                exported / "src" / "txnmem_provenance_runner.py"
            ).read_bytes()
            (root / "src" / "txnmem_backend.py").write_text(
                "# modified\n", encoding="utf-8"
            )
            with self.assertRaises(CollectorError):
                attest_committed_source(root)
            exported_backend_after = (
                exported / "src" / "txnmem_backend.py"
            ).read_bytes()
            exported_mode = (
                exported / "src" / "txnmem_backend.py"
            ).stat().st_mode & 0o777

        self.assertRegex(identity["source_commit"], r"^[0-9a-f]{40}$")
        self.assertRegex(identity["source_manifest_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(exported_backend_before, exported_backend_after)
        self.assertEqual(exported_mode, 0o400)
        self.assertEqual(
            identity["runner_sha256"],
            hashlib.sha256(exported_runner).hexdigest(),
        )
        self.assertIn(
            "src/txnmem_backend.py",
            {row["path"] for row in identity["source_manifest"]["files"]},
        )

    def test_source_attestation_rejects_head_change_from_controller_commit(self):
        paths = (
            "src/txnmem_provenance_execution_collector.py",
            "src/txnmem_provenance_runner.py",
        )
        with TemporaryDirectory() as tmp, patch.object(
            collector_module, "_SOURCE_PATHS_FOR_TESTS", paths
        ):
            root = Path(tmp).resolve()
            (root / "src").mkdir()
            for relative in paths:
                (root / relative).write_text(
                    f"# approved {relative}\n", encoding="utf-8"
                )
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            subprocess.run(
                ["git", "config", "user.email", "collector@example.invalid"],
                cwd=root,
                check=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "Collector Fixture"],
                cwd=root,
                check=True,
            )
            subprocess.run(["git", "add", "src"], cwd=root, check=True)
            subprocess.run(
                ["git", "commit", "-q", "-m", "approved"], cwd=root, check=True
            )
            approved = attest_committed_source(root)
            (root / paths[0]).write_text("# replacement\n", encoding="utf-8")
            subprocess.run(["git", "add", paths[0]], cwd=root, check=True)
            subprocess.run(
                ["git", "commit", "-q", "-m", "replacement"],
                cwd=root,
                check=True,
            )

            with self.assertRaisesRegex(CollectorError, "approved commit"):
                attest_committed_source(
                    root,
                    expected_commit=approved["source_commit"],
                    expected_source_manifest=approved["source_manifest"],
                )

    def test_controller_context_is_bound_to_root_approval_manifest_hash(self):
        rows = [
            {"path": path, "blob_sha256": hashlib.sha256(path.encode()).hexdigest()}
            for path in sorted(collector_module._REQUIRED_SOURCE_PATHS)
        ]
        commit = "a" * 40
        source_manifest = {
            "schema": "txnmem-provenance-source-manifest-v1",
            "source_commit": commit,
            "files": rows,
        }
        approval = {
            "schema": "txnmem-formal-approved-source-v1",
            "source_commit": commit,
            "files": rows,
        }
        context = {
            "schema": "txnmem-formal-controller-context-v1",
            "source_commit": commit,
            "source_manifest": source_manifest,
            "approval_manifest_sha256": hashlib.sha256(
                collector_module.canonical_json_bytes(approval)
            ).hexdigest(),
        }

        self.assertEqual(
            collector_module._validate_formal_controller_context(context),
            context,
        )
        context["approval_manifest_sha256"] = "0" * 64
        with self.assertRaisesRegex(CollectorError, "approval manifest hash"):
            collector_module._validate_formal_controller_context(context)

    def test_environment_input_snapshot_is_private_immutable_and_byte_exact(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            source = root / "environment.json"
            original = b'{"environment":"exact"}\n'
            source.write_bytes(original)
            private = root / "private"
            private.mkdir(mode=0o700)

            snapshot = collector_module._create_immutable_input_snapshot(
                source,
                private,
                expected_file_sha256=hashlib.sha256(original).hexdigest(),
                label="environment",
            )
            source.write_bytes(b'{"environment":"changed"}\n')

            self.assertEqual(snapshot.read_bytes(), original)
            self.assertEqual(snapshot.stat().st_mode & 0o777, 0o400)
            self.assertEqual(snapshot.parent, private)
            with self.assertRaises(CollectorError):
                collector_module._create_immutable_input_snapshot(
                    source,
                    private,
                    expected_file_sha256=hashlib.sha256(original).hexdigest(),
                    label="environment",
                )

    def test_locked_runtime_snapshot_accepts_only_registered_wheel_bytes(self):
        def write_wheel(path, *, package, version, requires=()):
            dist_info = f"{package}-{version}.dist-info"
            metadata = [f"Name: {package}", f"Version: {version}"]
            metadata.extend(f"Requires-Dist: {item}" for item in requires)
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr(f"{package}/__init__.py", "# locked runtime\n")
                archive.writestr(
                    f"{dist_info}/METADATA", "\n".join(metadata) + "\n"
                )

        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            private = root / "private"
            private.mkdir(mode=0o700)
            wheels = root / "wheels"
            wheels.mkdir()
            neo4j_wheel = wheels / "neo4j-5.28.1-py3-none-any.whl"
            pytz_wheel = wheels / "pytz-2025.2-py3-none-any.whl"
            write_wheel(
                neo4j_wheel,
                package="neo4j",
                version="5.28.1",
                requires=("pytz",),
            )
            write_wheel(pytz_wheel, package="pytz", version="2025.2")
            lock = {
                "schema": "txnmem-provenance-runtime-lock-v1",
                "python_versions": [collector_module.platform.python_version()],
                "distributions": [
                    {
                        "name": "neo4j",
                        "version": "5.28.1",
                        "filename": neo4j_wheel.name,
                        "sha256": hashlib.sha256(neo4j_wheel.read_bytes()).hexdigest(),
                        "dependency_names": ["pytz"],
                        "requires_dist": ["pytz"],
                    },
                    {
                        "name": "pytz",
                        "version": "2025.2",
                        "filename": pytz_wheel.name,
                        "sha256": hashlib.sha256(pytz_wheel.read_bytes()).hexdigest(),
                        "dependency_names": [],
                        "requires_dist": [],
                    },
                ],
            }
            lock_path = root / "runtime-lock.json"
            lock_path.write_text(
                json.dumps(lock, sort_keys=True) + "\n", encoding="utf-8"
            )
            python_hash = collector_module._file_sha256(
                Path(sys.executable).resolve(), "test Python executable"
            )

            snapshot, manifest = collector_module._create_locked_runtime_snapshot(
                private,
                lock_path=lock_path,
                wheel_directory=wheels,
                python_executable_hash=python_hash,
                require_protected_wheels=False,
            )
            self.assertEqual(
                [row["name"] for row in manifest["distributions"]],
                ["neo4j", "pytz"],
            )
            self.assertEqual(
                collector_module.verify_immutable_runtime_snapshot(
                    snapshot, manifest
                ),
                manifest,
            )

            (wheels / "rogue-1.0-py3-none-any.whl").write_bytes(b"rogue")
            with self.assertRaisesRegex(CollectorError, "unregistered wheel"):
                collector_module._create_locked_runtime_snapshot(
                    private,
                    lock_path=lock_path,
                    wheel_directory=wheels,
                    python_executable_hash=python_hash,
                    require_protected_wheels=False,
                )

    def test_collector_nonce_reader_requires_exact_private_modes(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            project = root / "project"
            project.mkdir()
            private = root / "private"
            private.mkdir(mode=0o700)
            nonce = private / "authorization.nonce"
            nonce.write_bytes(self.AUTHORIZATION_NONCE)
            nonce.chmod(0o400)

            with self.assertRaisesRegex(CollectorError, "mode 0600"):
                collector_module._read_private_authorization_nonce(
                    nonce, project
                )


if __name__ == "__main__":
    unittest.main()
