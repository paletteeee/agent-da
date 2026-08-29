import copy
import enum
import errno
import hashlib
import inspect
import json
import os
import signal
import socket
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


def _copy_locked_runner_import_closure(destination):
    repository = Path(__file__).resolve().parents[1]
    source_root = repository / "src"
    destination.mkdir(mode=0o755)
    for name, mode in (
        ("txnmem_provenance_runner.py", 0o555),
        ("txnmem_provenance_contract.py", 0o444),
    ):
        copied = destination / name
        copied.write_bytes((source_root / name).read_bytes())
        copied.chmod(mode)
    destination.chmod(0o555)
    return destination / "txnmem_provenance_runner.py"


def _isolated_contract_resolves_inside_locked_source(locked_source):
    repository = Path(__file__).resolve().parents[1]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(repository / "src")
    probe = "\n".join(
        (
            "import pathlib, sys",
            "root = pathlib.Path(sys.argv[1]).resolve()",
            "sys.path.insert(0, str(root))",
            "import txnmem_provenance_contract as contract",
            "actual = pathlib.Path(contract.__file__).resolve().parent",
            "raise SystemExit(0 if actual == root else 86)",
        )
    )
    completed = subprocess.run(
        (
            sys.executable,
            "-I",
            "-S",
            "-B",
            "-c",
            probe,
            str(locked_source),
        ),
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=5.0,
        check=False,
    )
    return completed.returncode == 0


class ProvenanceExecutionCollectorTests(unittest.TestCase):
    AUTHORIZATION_NONCE = b"collector-fixture-authorization-nonce-0001"

    def _assert_exact_bridge_tcp_reset_policy(self, batch):
        host_tcp_deny = (
            "tcp dport { 6333, 6334, 7474, 7687, 8474, 19000, 19001 } "
            "ip daddr { 172.19.0.0/16, 172.20.0.0/16 } "
            "reject with tcp reset "
            'comment "txnmem-host-bridge-tcp-deny"'
        )
        forward_tcp_deny = (
            'iifname != { "br-aaaaaaaaaaaa", "br-bbbbbbbbbbbb" } '
            "tcp dport { 6333, 6334, 7474, 7687, 8474, 19000, 19001 } "
            "ip daddr { 172.19.0.0/16, 172.20.0.0/16 } "
            "reject with tcp reset "
            'comment "txnmem-forward-bridge-tcp-deny"'
        )
        bridge_tcp_reset_rules = tuple(
            line.strip()
            for line in batch.splitlines()
            if "bridge-tcp-deny" in line
        )

        self.assertEqual(
            bridge_tcp_reset_rules,
            (host_tcp_deny, forward_tcp_deny),
        )
        self.assertEqual(
            batch.count(
                "tcp dport { 6333, 6334, 7474, 7687, 8474, 19000, 19001 }"
            ),
            2,
        )
        for rule in bridge_tcp_reset_rules:
            self.assertNotIn("meta l4proto tcp", rule)

        return host_tcp_deny, forward_tcp_deny

    def test_toxiproxy_version_parser_accepts_exact_registered_forms(self):
        cases = (
            (b"2.5.0", "2.5.0"),
            (b'"2.5.0"', "2.5.0"),
            (b'{"version":"2.5.0"}', "2.5.0"),
        )
        for payload, expected in cases:
            with self.subTest(payload=payload):
                self.assertEqual(
                    collector_module._parse_toxiproxy_version(payload), expected
                )

    def test_toxiproxy_version_parser_rejects_nonexact_or_unregistered_payloads(self):
        cases = (
            b"2.5.0\n",
            b" 2.5.0",
            b"2.5.0 ",
            b"v2.5.0",
            b"2.5.0-release",
            b"\xff",
            b'"2.5.0" trailing',
            b'{"version":',
            b"not-a-version",
            b"null",
            b"250",
            b"true",
            b"[]",
            b'{"version":250}',
            b'{"status":"ok"}',
            b"9.9.9",
        )
        for payload in cases:
            with self.subTest(payload=payload):
                with self.assertRaises(CollectorError) as raised:
                    collector_module._parse_toxiproxy_version(payload)
                self.assertNotIn(repr(payload), str(raised.exception))

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
                        if comment
                        in {
                            "txnmem-forward-bridge-tcp-deny",
                            "txnmem-forward-bridge-deny",
                        }
                        else "output",
                        "comment": comment,
                    }
                }
                for comment in (
                    "txnmem-proxy-allow",
                    "txnmem-proxy-established-allow",
                    "txnmem-management-allow",
                    "txnmem-docker-proxy-ingress-allow",
                    "txnmem-runner-deny",
                    "txnmem-management-deny",
                    "txnmem-attribution-deny",
                    "txnmem-host-bridge-reset-allow",
                    "txnmem-host-bridge-tcp-deny",
                    "txnmem-host-bridge-deny",
                    "txnmem-forward-bridge-tcp-deny",
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
            "schema": "txnmem-provenance-command-manifest-v3",
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
            "progress_environment_variable": "TXNMEM_PROVENANCE_PROGRESS_FD",
            "progress_binding_environment_variable": "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256",
            "progress_channel_required": True,
            "backend_timeout_policy": {
                "qdrant_request_seconds": 30.0,
                "neo4j_connection_seconds": 30.0,
                "neo4j_connection_acquisition_seconds": 30.0,
                "neo4j_transaction_query_seconds": 30.0,
            },
            "runtime_environment_variable": "TXNMEM_PROVENANCE_RUNTIME_SITE",
            "inherited_environment": False,
        }
        binding = {
            "schema": "txnmem-provenance-progress-binding-v1",
            "source_manifest_sha256": manifest["source_manifest_sha256"],
            "argv_sha256": manifest["argv_sha256"],
            "config_file_sha256": manifest["config_file_sha256"],
            "run_id_sha256": manifest["run_id_sha256"],
            "candidate_root_sha256": manifest["candidate_root_sha256"],
        }
        manifest["progress_binding_sha256"] = hashlib.sha256(
            json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
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
        parameters = inspect.signature(_collect_execution_evidence).parameters
        self.assertIn("interruption_check", parameters)
        if "interruption_check" not in parameters:
            return
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
            interruption_boundaries = []
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
                self.assertEqual(candidate_events, ["complete"])
                candidate_events.append("seal")
                self.assertEqual(candidate, candidate_root)
                self.assertEqual(receipt, material)
                return candidate_seal

            def progress_completer():
                candidate_events.append("complete")
                return {"status": "completed"}

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

            def interruption_check():
                interruption_boundaries.append(
                    (tuple(events), tuple(candidate_events))
                )

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
                    progress_completer=progress_completer,
                    interruption_check=interruption_check,
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
        self.assertEqual(candidate_events, ["complete", "seal", "material"])
        self.assertIn(((), ()), interruption_boundaries)
        self.assertTrue(
            any(
                "topology_before" in observed_events
                and "guard_activate" not in observed_events
                for observed_events, _candidate_events in interruption_boundaries
            )
        )
        self.assertTrue(
            any(
                "child_exit" in observed_events
                and "monitor_finalize" not in observed_events
                for observed_events, _candidate_events in interruption_boundaries
            )
        )
        self.assertTrue(
            any(
                "guard_deactivate" in observed_events
                and "completion_write" not in observed_events
                for observed_events, _candidate_events in interruption_boundaries
            )
        )
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

    def test_sealer_failure_preserves_completed_execution_without_publication(self):
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
            run_id = "sealer-failure-fixture"
            run_hash = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
            config_hash = "1" * 64
            candidate_id = (
                "diagnostic-vector_graph-"
                + config_hash[:16]
                + "-"
                + run_hash[:16]
            )
            source_identity = self._source_identity()
            command_manifest = self._command_manifest(
                run_hash=run_hash,
                config_file_hash="2" * 64,
                environment_hash="4" * 64,
                source_identity=source_identity,
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
                "operation_sample_count": 14400,
                "observed_service_versions": {
                    "qdrant": "1.15.4",
                    "neo4j": "5.26.0",
                    "toxiproxy": "2.9.0",
                },
                "candidate_operation_samples_sha256": "6" * 64,
                "candidate_repetitions_sha256": "7" * 64,
            }
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

            class Store:
                def __init__(self):
                    self.snapshot = (
                        ProvenanceExecutionCollectorTests._final_running_progress_snapshot()
                    )

                def read_view(self):
                    return dict(self.snapshot)

                def write_terminal(self, status, reason):
                    self.snapshot["status"] = status
                    self.snapshot["terminal_reason_class"] = reason

            store = Store()
            child = self._candidate_with_progress_store(store)

            def collect_until_sealer_failure():
                try:
                    _collect_execution_evidence(
                        project_root=project,
                        candidate_root=candidate,
                        launch_path=launch,
                        completion_path=completion,
                        run_id=run_id,
                        transport="local_loopback",
                        config_sha256=config_hash,
                        config_file_sha256="2" * 64,
                        workload_sha256="3" * 64,
                        environment_attestation_sha256="4" * 64,
                        command_manifest=command_manifest,
                        child_process=self._child_process(command_manifest),
                        authorization_nonce=self.AUTHORIZATION_NONCE,
                        network_guard_activate=lambda: {
                            "network_guard": self._network_guard(),
                            "proxy_routes": copy.deepcopy(PROXY_ROUTES),
                            "proxy_counters": baseline_b,
                            "route_rearmed": True,
                        },
                        network_guard_finalize=lambda: {
                            "network_guard": self._network_guard(),
                            "proxy_routes": copy.deepcopy(PROXY_ROUTES),
                            "proxy_counters": final,
                        },
                        network_guard_deactivate=lambda: None,
                        execution_monitor_start=lambda: None,
                        execution_monitor_finalize=lambda: self._execution_monitor(),
                        run_candidate=lambda: (0, material),
                        candidate_sealer=lambda *_args: (_ for _ in ()).throw(
                            CollectorError("sanitized sealer failure")
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
                            "candidate material must not be loaded after sealer failure"
                        ),
                        progress_blocker=child.block_progress,
                        progress_completer=child.complete_progress,
                    )
                except BaseException:
                    child.block_progress()
                    raise

            with patch.dict(
                FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
                {
                    run_hash: hashlib.sha256(
                        self.AUTHORIZATION_NONCE
                    ).hexdigest()
                },
                clear=True,
            ), self.assertRaisesRegex(CollectorError, "sealer"):
                collect_until_sealer_failure()

            terminal = store.read_view()
            launch_exists = launch.is_file()
            completion_exists = completion.exists()

        self.assertEqual(terminal["status"], "completed")
        self.assertTrue(launch_exists)
        self.assertFalse(completion_exists)

    def test_attribution_boundary_failure_defers_guard_to_outer_cleanup(self):
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

        self.assertEqual(events, [])
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

    def test_signal_latch_interrupts_receipt_wait_and_cleanup_is_idempotent(self):
        events = []

        class Drainer:
            def __init__(self):
                self.aborted = False

            def abort(self):
                if not self.aborted:
                    self.aborted = True
                    events.append("progress")

        class Monitor:
            def abort(self):
                events.append("monitor")

        class Guard:
            def __init__(self):
                self.active = True

            def deactivate(self):
                self.active = False
                events.append("guard")

        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            script = root / "blocked_after_gate.py"
            script.write_text(
                "\n".join(
                    (
                        "import os, time",
                        "ready = int(os.environ.pop('TXNMEM_PROVENANCE_READY_FD'))",
                        "gate = int(os.environ.pop('TXNMEM_PROVENANCE_START_GATE_FD'))",
                        "receipt = int(os.environ.pop('TXNMEM_PROVENANCE_COMPLETION_FD'))",
                        "os.write(ready, b'R')",
                        "os.close(ready)",
                        "os.read(gate, 1)",
                        "os.close(gate)",
                        "time.sleep(0.5)",
                        "os.close(receipt)",
                    )
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
            real_killpg = os.killpg

            def emergency_cleanup():
                if child.process.poll() is None:
                    real_killpg(child.process.pid, signal.SIGKILL)
                    child.process.wait(timeout=2.0)

            self.addCleanup(emergency_cleanup)
            child._progress_drainer = Drainer()
            start_identity = f"candidate:{child.process.pid}:99"
            child.bind_process_identity(start_identity)
            guard = Guard()
            latch = collector_module._SignalLatch()
            identity = {
                "pid": child.process.pid,
                "start_identity": start_identity,
                "pgid": child.process.pid,
                "sid": child.process.pid,
            }
            child.release()
            latch.trigger()
            latch.trigger()
            try:
                with self.assertRaises(collector_module._CollectorInterruption):
                    child.wait_with_receipt(
                        timeout=2.0, interrupt_fd=latch.read_fd
                    )
            finally:
                with patch.object(
                    collector_module,
                    "_read_process_group_identity",
                    return_value=identity,
                ), patch.object(
                    collector_module.os,
                    "killpg",
                    side_effect=lambda pgid, sig: (
                        events.append(("signal", sig)),
                        real_killpg(pgid, sig),
                    )[-1],
                ):
                    first = collector_module._cleanup_formal_execution_resources(
                        execution_monitor=Monitor(),
                        network_guard=guard,
                        child=child,
                    )
                    second = collector_module._cleanup_formal_execution_resources(
                        execution_monitor=None,
                        network_guard=guard,
                        child=child,
                    )
                latch.close()
                latch.close()

        self.assertEqual(first, [])
        self.assertEqual(second, [])
        self.assertEqual(
            events,
            ["monitor", ("signal", signal.SIGTERM), "progress", "guard"],
        )

    def test_signal_latch_handler_only_latches_when_pipe_write_fails(self):
        latch = collector_module._SignalLatch()
        try:
            self.assertIs(type(latch._interrupted), bool)
            with patch.object(
                collector_module.os,
                "write",
                side_effect=OSError("private-self-pipe-failure"),
            ):
                latch.trigger(signal.SIGTERM, None)
            self.assertIs(latch.interrupted, True)
        finally:
            latch.close()

    def test_failed_signal_pipe_write_still_interrupts_receipt_wait_boundedly(self):
        parameters = inspect.signature(
            collector_module._GatedCandidate.wait_with_receipt
        ).parameters
        self.assertIn("interrupt_latch", parameters)
        if "interrupt_latch" not in parameters:
            return
        receipt_read, receipt_write = os.pipe()

        class Process:
            args = ("python", "runner.py")

        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=receipt_read,
            ready_observed=True,
        )
        latch = collector_module._SignalLatch()
        try:
            with patch.object(
                collector_module.os,
                "write",
                side_effect=OSError("private-self-pipe-failure"),
            ):
                latch.trigger(signal.SIGTERM, None)
            started = time.monotonic()
            with self.assertRaises(collector_module._CollectorInterruption):
                child.wait_with_receipt(
                    timeout=0.5, interrupt_latch=latch
                )
            self.assertLess(time.monotonic() - started, 0.3)
        finally:
            os.close(receipt_write)
            latch.close()

    def test_receipt_wait_retries_eintr_and_rejects_signal_latch_eof(self):
        parameters = inspect.signature(
            collector_module._GatedCandidate.wait_with_receipt
        ).parameters
        self.assertIn("interrupt_latch", parameters)
        if "interrupt_latch" not in parameters:
            return

        class Process:
            args = ("python", "runner.py")

            def wait(self, timeout=None):
                return 0

        receipt_read, receipt_write = os.pipe()
        os.write(receipt_write, b"{}")
        os.close(receipt_write)
        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=receipt_read,
            ready_observed=True,
        )
        latch = collector_module._SignalLatch()
        with patch.object(
            collector_module.select,
            "select",
            side_effect=[
                InterruptedError(),
                ([receipt_read], [], []),
                ([receipt_read], [], []),
            ],
        ):
            self.assertEqual(
                child.wait_with_receipt(
                    timeout=0.5, interrupt_latch=latch
                ),
                (0, {}),
            )
        latch.close()

        receipt_read, receipt_write = os.pipe()
        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=receipt_read,
            ready_observed=True,
        )
        latch = collector_module._SignalLatch()
        latch_read = latch.read_fd
        real_read = os.read

        def read_descriptor(descriptor, size):
            if descriptor == latch_read:
                return b""
            return real_read(descriptor, size)

        try:
            with patch.object(
                collector_module.select,
                "select",
                return_value=([latch_read], [], []),
            ), patch.object(
                collector_module.os,
                "read",
                side_effect=read_descriptor,
            ), self.assertRaisesRegex(CollectorError, "signal latch"):
                child.wait_with_receipt(
                    timeout=0.5, interrupt_latch=latch
                )
        finally:
            os.close(receipt_write)
            latch.close()

    def test_signal_latch_close_attempts_every_restore_and_fd_once(self):
        latch = collector_module._SignalLatch()
        latch._previous_handlers = {
            signal.SIGTERM: signal.SIG_DFL,
            signal.SIGINT: signal.SIG_DFL,
        }
        restore_calls = []
        close_calls = []

        def restore(signal_number, handler):
            restore_calls.append((signal_number, handler))
            if signal_number == signal.SIGTERM:
                raise OSError("private-restore-failure")

        def close_descriptor(descriptor):
            close_calls.append(descriptor)
            if descriptor == latch._read_fd:
                raise OSError("private-close-failure")

        with patch.object(
            collector_module.signal, "signal", side_effect=restore
        ), patch.object(
            collector_module.os, "close", side_effect=close_descriptor
        ):
            try:
                failures = latch.close()
            except BaseException as exc:
                self.fail(f"latch close masked cleanup aggregation: {type(exc).__name__}")
            repeated = latch.close()

        self.assertEqual(
            restore_calls,
            [
                (signal.SIGTERM, signal.SIG_DFL),
                (signal.SIGINT, signal.SIG_DFL),
            ],
        )
        self.assertEqual(close_calls, [latch._read_fd, latch._write_fd])
        self.assertEqual(len(failures), 2)
        self.assertEqual(repeated, [])

    def test_candidate_close_attempts_all_resources_once(self):
        cleanup_calls = []

        class Drainer:
            def abort(self):
                cleanup_calls.append("progress")
                raise OSError("private-progress-close-failure")

        child = collector_module._GatedCandidate(
            process=SimpleNamespace(pid=4242),
            _release_fd=10,
            _receipt_fd=11,
            _progress_drainer=Drainer(),
            ready_observed=True,
        )

        def close_descriptor(descriptor):
            cleanup_calls.append(("close", descriptor))
            raise OSError("private-candidate-close-failure")

        with patch.object(
            collector_module.os, "close", side_effect=close_descriptor
        ):
            with self.assertRaisesRegex(CollectorError, "cleanup"):
                child.close()
            child.close()

        self.assertEqual(
            cleanup_calls,
            [("close", 10), ("close", 11), "progress"],
        )

    def test_receipt_fd_close_failure_does_not_mask_interruption(self):
        child = collector_module._GatedCandidate(
            process=SimpleNamespace(pid=4242, args=("runner",)),
            _release_fd=None,
            _receipt_fd=10,
            ready_observed=True,
        )
        close_calls = []

        def read_descriptor(descriptor, _size):
            if descriptor == 99:
                return b"I"
            self.fail("receipt must not be read after interruption")

        def close_descriptor(descriptor):
            close_calls.append(descriptor)
            raise OSError("private-receipt-close-failure")

        with patch.object(
            collector_module.select, "select", return_value=([99], [], [])
        ), patch.object(
            collector_module.os, "read", side_effect=read_descriptor
        ), patch.object(
            collector_module.os,
            "close",
            side_effect=close_descriptor,
        ):
            with self.assertRaises(collector_module._CollectorInterruption):
                child.wait_with_receipt(timeout=0.5, interrupt_fd=99)

        self.assertEqual(close_calls, [10])

    def test_ambiguous_interruption_channel_retains_receipt_ownership(self):
        receipt_read, receipt_write = os.pipe()
        latch = collector_module._SignalLatch()
        child = collector_module._GatedCandidate(
            process=SimpleNamespace(pid=4242, args=("runner",)),
            _release_fd=None,
            _receipt_fd=receipt_read,
            ready_observed=True,
        )
        try:
            with self.assertRaisesRegex(CollectorError, "ambiguous"):
                child.wait_with_receipt(
                    timeout=0.5,
                    interrupt_latch=latch,
                    interrupt_fd=latch.read_fd,
                )
            self.assertEqual(child._receipt_fd, receipt_read)
        finally:
            os.close(receipt_write)
            child.close()
            latch.close()

    def test_release_cleanup_failures_do_not_mask_gate_write_failure(self):
        child = collector_module._GatedCandidate(
            process=SimpleNamespace(pid=4242),
            _release_fd=10,
            _receipt_fd=None,
            ready_observed=True,
        )
        close_calls = []

        def close_descriptor(descriptor):
            close_calls.append(descriptor)
            raise OSError("private-gate-close-failure")

        with patch.object(
            collector_module.os,
            "write",
            side_effect=RuntimeError("primary-gate-write-failure"),
        ), patch.object(
            child,
            "block_progress",
            side_effect=OSError("private-progress-block-failure"),
        ), patch.object(
            collector_module.os,
            "close",
            side_effect=close_descriptor,
        ):
            with self.assertRaises(RuntimeError) as raised:
                child.release()

        self.assertEqual(str(raised.exception), "primary-gate-write-failure")
        self.assertEqual(close_calls, [10])

    def test_interrupted_progress_terminal_is_idempotent(self):
        class Store:
            def __init__(self):
                self.status = "running"
                self.reason = None
                self.writes = []

            def read_view(self):
                view = {"status": self.status}
                if self.reason is not None:
                    view["terminal_reason_class"] = self.reason
                return view

            def write_terminal(self, status, reason):
                self.writes.append((status, reason))
                self.status = status
                self.reason = reason

        store = Store()
        child = collector_module._GatedCandidate(
            process=SimpleNamespace(pid=4242),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
            _progress_store=store,
        )

        child.interrupt_progress()
        child.interrupt_progress()

        self.assertEqual(
            store.writes, [("interrupted", "collector_interrupted")]
        )

    def test_diagnostic_progress_terminals_preserve_exact_reason_and_view(self):
        from txnmem_provenance_progress import ProgressSnapshotStore

        binding = "a" * 64
        config_hash = "b" * 64
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            root.chmod(0o700)

            def candidate_for(name):
                scenario_root = root / name
                scenario_root.mkdir(mode=0o700)
                store = ProgressSnapshotStore(
                    scenario_root / "progress.json",
                    expected_uid=os.getuid(),
                    expected_gid=os.getgid(),
                )
                store.write_starting(binding, config_hash)
                child = collector_module._GatedCandidate(
                    process=SimpleNamespace(pid=4242),
                    _release_fd=None,
                    _receipt_fd=None,
                    ready_observed=True,
                    _progress_store=store,
                )
                return child, store

            for reason in ("formal_eligibility_failed", "backend_timeout"):
                child, store = candidate_for(reason)
                terminal = child.block_progress(reason)
                self.assertEqual(terminal, store.read_view())
                self.assertEqual(terminal["status"], "blocked")
                self.assertEqual(terminal["terminal_reason_class"], reason)
                self.assertEqual(terminal["completed_repetitions"], 0)
                self.assertEqual(terminal["completed_samples"], 0)
                self.assertEqual(child.block_progress(reason), terminal)

            interrupted, interrupted_store = candidate_for("interrupted")
            terminal = interrupted.interrupt_progress()
            self.assertEqual(terminal, interrupted_store.read_view())
            self.assertEqual(terminal["status"], "interrupted")
            self.assertEqual(
                terminal["terminal_reason_class"], "collector_interrupted"
            )
            self.assertEqual(interrupted.interrupt_progress(), terminal)

            invalid, invalid_store = candidate_for("invalid")
            before = invalid_store.read_view()
            with self.assertRaisesRegex(CollectorError, "reason"):
                invalid.block_progress("resource_cleanup_failed")
            self.assertEqual(invalid_store.read_view(), before)

    def test_gated_child_reads_the_collector_owned_starting_snapshot(self):
        from txnmem_provenance_progress import ProgressSnapshotStore

        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            store = ProgressSnapshotStore(
                root / "progress.json",
                expected_uid=os.getuid(),
                expected_gid=os.getgid(),
            )
            store.write_starting("a" * 64, "b" * 64)
            child = collector_module._GatedCandidate(
                process=SimpleNamespace(pid=4242),
                _release_fd=None,
                _receipt_fd=None,
                ready_observed=True,
                _progress_store=store,
            )

            observed = child.read_progress()

        self.assertEqual(observed["status"], "starting")
        self.assertEqual(observed["completed_repetitions"], 0)
        self.assertEqual(observed["completed_samples"], 0)
        self.assertEqual(observed["update_sequence"], 0)

    def test_gated_child_progress_pipe_is_collector_owned_and_drained(self):
        from txnmem_provenance_progress import canonical_progress_line

        binding = "a" * 64
        config_hash = "b" * 64
        event = {
            "schema": "txnmem-provenance-progress-event-v1",
            "run_binding_sha256": binding,
            "config_sha256": config_hash,
            "phase": "measurement",
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
            "status": "running",
        }
        line = canonical_progress_line(event)
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            candidate = root / "candidate"
            candidate.mkdir()
            progress_path = root / "progress.json"
            script = root / "progress_fixture.py"
            script.write_text(
                "\n".join(
                    [
                        "import os, sys",
                        "ready = int(os.environ.pop('TXNMEM_PROVENANCE_READY_FD'))",
                        "gate = int(os.environ.pop('TXNMEM_PROVENANCE_START_GATE_FD'))",
                        "progress = int(os.environ.pop('TXNMEM_PROVENANCE_PROGRESS_FD'))",
                        "os.environ.pop('TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256')",
                        "os.write(ready, b'R')",
                        "os.close(ready)",
                        "token = os.read(gate, 1)",
                        "os.close(gate)",
                        "os.write(progress, bytes.fromhex(sys.argv[1]))",
                        "os.close(progress)",
                        "raise SystemExit(0 if token == b'G' else 9)",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            child = collector_module._start_gated_candidate(
                command=(sys.executable, "-I", "-B", str(script), line.hex()),
                cwd=root,
                environment={},
                require_progress=True,
                progress_binding_sha256=binding,
                progress_config_sha256=config_hash,
                progress_snapshot_path=progress_path,
                progress_expected_uid=os.getuid(),
                progress_expected_gid=os.getgid(),
            )
            try:
                child.release()
                self.assertEqual(child.process.wait(timeout=5), 0)
                snapshot = child.finish_progress(2.0)
            finally:
                child.close()

            self.assertEqual(snapshot["status"], "running")
            self.assertEqual(snapshot["update_sequence"], 1)
            self.assertEqual(progress_path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(progress_path, root / "progress.json")
            self.assertFalse(progress_path.is_relative_to(candidate))

            (candidate / "result.json").write_text("{}\n", encoding="utf-8")
            receipt = {
                "schema": "txnmem-provenance-candidate-attestation-material-v1",
                "candidate_bundle_id": (
                    "diagnostic-vector_graph-" + "1" * 16 + "-" + "2" * 16
                ),
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
            seal = collector_module._seal_candidate_tree(
                candidate,
                expected_owner_uid=os.getuid(),
                sealed_owner_uid=os.getuid(),
                sealed_owner_gid=os.getgid(),
                completion_receipt=receipt,
            )
            self.assertEqual(seal["file_count"], 1)

    def test_gated_child_can_explicitly_close_an_empty_diagnostic_progress_pipe(self):
        binding = "a" * 64
        config_hash = "b" * 64
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            progress_path = root / "progress.json"
            script = root / "empty_progress_fixture.py"
            script.write_text(
                "\n".join(
                    [
                        "import os",
                        "ready = int(os.environ.pop('TXNMEM_PROVENANCE_READY_FD'))",
                        "gate = int(os.environ.pop('TXNMEM_PROVENANCE_START_GATE_FD'))",
                        "progress = int(os.environ.pop('TXNMEM_PROVENANCE_PROGRESS_FD'))",
                        "os.environ.pop('TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256')",
                        "os.write(ready, b'R')",
                        "os.close(ready)",
                        "token = os.read(gate, 1)",
                        "os.close(gate)",
                        "os.close(progress)",
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
                require_progress=True,
                progress_allow_empty=True,
                progress_binding_sha256=binding,
                progress_config_sha256=config_hash,
                progress_snapshot_path=progress_path,
                progress_expected_uid=os.getuid(),
                progress_expected_gid=os.getgid(),
            )
            try:
                child.release()
                self.assertEqual(child.process.wait(timeout=5), 0)
                self.assertIsNone(
                    child.finish_progress(2.0, allow_empty=True)
                )
                terminal = child.block_progress("formal_eligibility_failed")
            finally:
                child.close()

            self.assertEqual(terminal["status"], "blocked")
            self.assertEqual(
                terminal["terminal_reason_class"],
                "formal_eligibility_failed",
            )
            self.assertEqual(terminal["completed_repetitions"], 0)

    def test_gated_child_rejects_caller_supplied_progress_environment(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            for name in (
                "TXNMEM_PROVENANCE_PROGRESS_FD",
                "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256",
            ):
                with self.subTest(name=name), self.assertRaisesRegex(
                    CollectorError, "reserved"
                ):
                    collector_module._start_gated_candidate(
                        command=(sys.executable, "-I", "-B", "unused.py"),
                        cwd=root,
                        environment={name: "13"},
                    )

    def test_gated_child_closes_every_pipe_if_progress_store_setup_fails(self):
        from txnmem_provenance_progress import ProgressProtocolError

        descriptors = iter(((10, 11), (12, 13), (14, 15)))
        closed = []
        with TemporaryDirectory() as tmp, patch.object(
            collector_module.os, "pipe", side_effect=lambda: next(descriptors)
        ), patch.object(
            collector_module.os, "close", side_effect=closed.append
        ), patch.object(
            collector_module,
            "ProgressSnapshotStore",
            side_effect=ProgressProtocolError("fixture setup failure"),
        ):
            root = Path(tmp).resolve()
            with self.assertRaises(ProgressProtocolError):
                collector_module._start_gated_candidate(
                    command=(sys.executable, "-I", "-B", "unused.py"),
                    cwd=root,
                    environment={},
                    require_progress=True,
                    progress_binding_sha256="a" * 64,
                    progress_config_sha256="b" * 64,
                    progress_snapshot_path=root / "progress.json",
                    progress_expected_uid=os.getuid(),
                    progress_expected_gid=os.getgid(),
                )

        self.assertEqual(sorted(closed), [10, 11, 12, 13, 14, 15])

    def test_gated_child_never_retries_an_ambiguous_owned_fd_close(self):
        descriptors = iter(((10, 11), (12, 13)))
        close_attempts = []
        reused_descriptor_closed = False

        class Process:
            def poll(self):
                return 0

            def wait(self, timeout=None):
                return 0

        def close_descriptor(descriptor):
            nonlocal reused_descriptor_closed
            close_attempts.append(descriptor)
            if descriptor == 10 and close_attempts.count(10) == 1:
                raise InterruptedError("password=private-ambiguous-close")
            if descriptor == 10:
                reused_descriptor_closed = True

        with TemporaryDirectory() as tmp, patch.object(
            collector_module.os, "pipe", side_effect=lambda: next(descriptors)
        ), patch.object(
            collector_module.os, "close", side_effect=close_descriptor
        ), patch.object(
            collector_module.subprocess, "Popen", return_value=Process()
        ):
            root = Path(tmp).resolve()
            with self.assertRaises(BaseException) as raised:
                collector_module._start_gated_candidate(
                    command=(sys.executable, "-I", "-B", "unused.py"),
                    cwd=root,
                    environment={},
                )

        self.assertIsInstance(raised.exception, CollectorError)
        self.assertIn("startup cleanup failed", str(raised.exception))
        self.assertNotIn("private", str(raised.exception))
        self.assertEqual(close_attempts.count(10), 1)
        self.assertFalse(reused_descriptor_closed)
        self.assertEqual(sorted(close_attempts), [10, 11, 12, 13])

    def test_gated_child_popen_failure_terminalizes_durable_starting_snapshot(self):
        with TemporaryDirectory() as tmp, patch.object(
            collector_module.subprocess,
            "Popen",
            side_effect=OSError("password=private-popen"),
        ):
            root = Path(tmp).resolve()
            progress_path = root / "progress.json"
            with self.assertRaises(OSError):
                collector_module._start_gated_candidate(
                    command=(sys.executable, "-I", "-B", "unused.py"),
                    cwd=root,
                    environment={},
                    require_completion_receipt=True,
                    require_progress=True,
                    progress_binding_sha256="a" * 64,
                    progress_config_sha256="b" * 64,
                    progress_snapshot_path=progress_path,
                    progress_expected_uid=os.getuid(),
                    progress_expected_gid=os.getgid(),
                )

            snapshot = json.loads(progress_path.read_text(encoding="utf-8"))

        self.assertEqual(snapshot["status"], "blocked")
        self.assertEqual(snapshot["terminal_reason_class"], "progress_protocol_failed")

    def test_gated_child_fd_cleanup_does_not_mask_startup_failure(self):
        descriptors = iter(((10, 11), (12, 13)))
        close_attempts = []

        def close_descriptor(descriptor):
            close_attempts.append(descriptor)
            if descriptor == 10:
                raise OSError("private-fd-cleanup-failure")

        with TemporaryDirectory() as tmp, patch.object(
            collector_module.os, "pipe", side_effect=lambda: next(descriptors)
        ), patch.object(
            collector_module.os, "close", side_effect=close_descriptor
        ), patch.object(
            collector_module.subprocess,
            "Popen",
            side_effect=RuntimeError("primary-startup-failure"),
        ):
            root = Path(tmp).resolve()
            with self.assertRaises(RuntimeError) as raised:
                collector_module._start_gated_candidate(
                    command=(sys.executable, "-I", "-B", "unused.py"),
                    cwd=root,
                    environment={},
                )

        self.assertEqual(str(raised.exception), "primary-startup-failure")
        self.assertEqual(sorted(close_attempts), [10, 11, 12, 13])
        self.assertEqual(len(close_attempts), len(set(close_attempts)))

    def _assert_startup_process_cleanup_failure(self, failing_operation):
        descriptors = iter(((10, 11), (12, 13), (14, 15), (16, 17)))
        closed = []
        process_calls = []

        class Store:
            def __init__(self, *_args, **_kwargs):
                self.status = None
                self.write_attempts = 0

            def write_starting(self, _binding, _config):
                self.status = "starting"

            def read_view(self):
                view = {"status": self.status}
                if hasattr(self, "reason"):
                    view["terminal_reason_class"] = self.reason
                return view

            def write_terminal(self, status, reason):
                self.write_attempts += 1
                self.status = status
                self.reason = reason

        class Drainer:
            def __init__(self, descriptor, _state, _store):
                self.descriptor = descriptor
                self.aborted = False

            def start(self):
                return None

            def abort(self):
                if not self.aborted:
                    self.aborted = True
                    process_calls.append("progress")
                    os.close(self.descriptor)

        class Process:
            def __init__(self):
                self.wait_count = 0

            def poll(self):
                process_calls.append("poll")
                return None

            def terminate(self):
                process_calls.append("terminate")
                if failing_operation == "terminate":
                    raise RuntimeError("password=private-terminate")

            def wait(self, timeout=None):
                self.wait_count += 1
                process_calls.append(f"wait-{self.wait_count}")
                if self.wait_count == 1 and failing_operation == "wait":
                    raise RuntimeError("password=private-wait")
                if self.wait_count == 1 and failing_operation in {"kill", "second_wait"}:
                    raise subprocess.TimeoutExpired("private-child", timeout)
                if self.wait_count == 2 and failing_operation == "second_wait":
                    raise RuntimeError("password=private-second-wait")
                return 0

            def kill(self):
                process_calls.append("kill")
                if failing_operation == "kill":
                    raise RuntimeError("password=private-kill")

        store = Store()
        process = Process()
        with TemporaryDirectory() as tmp, patch.object(
            collector_module.os, "pipe", side_effect=lambda: next(descriptors)
        ), patch.object(
            collector_module.os, "close", side_effect=closed.append
        ), patch.object(
            collector_module, "ProgressSnapshotStore", return_value=store
        ), patch.object(
            collector_module, "ProgressPipeDrainer", Drainer
        ), patch.object(
            collector_module.subprocess, "Popen", return_value=process
        ), patch.object(
            collector_module.select, "select", return_value=([], [], [])
        ):
            root = Path(tmp).resolve()
            with self.assertRaises(CollectorError) as raised:
                collector_module._start_gated_candidate(
                    command=(sys.executable, "-I", "-B", "unused.py"),
                    cwd=root,
                    environment={},
                    require_completion_receipt=True,
                    require_progress=True,
                    progress_binding_sha256="a" * 64,
                    progress_config_sha256="b" * 64,
                    progress_snapshot_path=root / "progress.json",
                    progress_expected_uid=os.getuid(),
                    progress_expected_gid=os.getgid(),
                )

        self.assertEqual(store.status, "blocked")
        self.assertEqual(store.reason, "progress_protocol_failed")
        self.assertEqual(store.write_attempts, 1)
        self.assertEqual(sorted(closed), list(range(10, 18)))
        self.assertEqual(len(closed), len(set(closed)))
        self.assertIn("startup cleanup failed", str(raised.exception))
        self.assertNotIn("private", str(raised.exception))
        return process_calls

    def test_startup_terminate_failure_cannot_preempt_blocked_persistence(self):
        calls = self._assert_startup_process_cleanup_failure("terminate")
        self.assertEqual(calls, ["poll", "terminate", "wait-1", "progress"])

    def test_startup_wait_failure_cannot_preempt_blocked_persistence(self):
        calls = self._assert_startup_process_cleanup_failure("wait")
        self.assertEqual(
            calls,
            ["poll", "terminate", "wait-1", "kill", "wait-2", "progress"],
        )

    def test_startup_kill_failure_cannot_preempt_blocked_persistence(self):
        calls = self._assert_startup_process_cleanup_failure("kill")
        self.assertEqual(
            calls,
            ["poll", "terminate", "wait-1", "kill", "wait-2", "progress"],
        )

    def test_startup_second_wait_failure_cannot_preempt_blocked_persistence(self):
        calls = self._assert_startup_process_cleanup_failure("second_wait")
        self.assertEqual(
            calls,
            ["poll", "terminate", "wait-1", "kill", "wait-2", "progress"],
        )

    def test_startup_block_failure_dominates_process_cleanup_failure(self):
        descriptors = iter(((20, 21), (22, 23), (24, 25)))
        closed = []

        class Store:
            def __init__(self, *_args, **_kwargs):
                self.status = None
                self.write_attempts = 0

            def write_starting(self, _binding, _config):
                self.status = "starting"

            def read_view(self):
                return {"status": self.status}

            def write_terminal(self, _status, _reason):
                self.write_attempts += 1
                raise RuntimeError("password=private-block-write")

        class Process:
            def poll(self):
                return None

            def terminate(self):
                raise RuntimeError("password=private-terminate")

            def wait(self, timeout=None):
                return 0

        class Drainer:
            def __init__(self, descriptor, _state, _store):
                self.descriptor = descriptor
                self.aborted = False

            def start(self):
                return None

            def abort(self):
                if not self.aborted:
                    self.aborted = True
                    os.close(self.descriptor)

        store = Store()
        with TemporaryDirectory() as tmp, patch.object(
            collector_module.os, "pipe", side_effect=lambda: next(descriptors)
        ), patch.object(
            collector_module.os, "close", side_effect=closed.append
        ), patch.object(
            collector_module, "ProgressSnapshotStore", return_value=store
        ), patch.object(
            collector_module, "ProgressPipeDrainer", Drainer
        ), patch.object(
            collector_module.subprocess, "Popen", return_value=Process()
        ), patch.object(
            collector_module.select, "select", return_value=([], [], [])
        ):
            root = Path(tmp).resolve()
            with self.assertRaises(CollectorError) as raised:
                collector_module._start_gated_candidate(
                    command=(sys.executable, "-I", "-B", "unused.py"),
                    cwd=root,
                    environment={},
                    require_progress=True,
                    progress_binding_sha256="a" * 64,
                    progress_config_sha256="b" * 64,
                    progress_snapshot_path=root / "progress.json",
                    progress_expected_uid=os.getuid(),
                    progress_expected_gid=os.getgid(),
                )

            self.assertEqual(store.write_attempts, 1)
        self.assertEqual(sorted(closed), list(range(20, 26)))
        self.assertIn("progress blocking failed", str(raised.exception))
        self.assertNotIn("private", str(raised.exception))

    def test_startup_identity_unavailable_closes_gate_then_proves_uid_empty(self):
        descriptors = iter(((10, 11), (12, 13), (14, 15), (16, 17)))
        events = []

        class Store:
            def __init__(self, *_args, **_kwargs):
                self.status = None

            def write_starting(self, _binding, _config):
                self.status = "starting"

            def read_view(self):
                return {"status": self.status}

            def write_terminal(self, status, _reason):
                events.append("blocked-progress")
                self.status = status

        class Drainer:
            def __init__(self, descriptor, _state, _store):
                self.descriptor = descriptor

            def start(self):
                raise AssertionError("identity failure must precede drainer start")

        class Process:
            pid = 4242
            args = (sys.executable, "-I", "-B", "unused.py")

            def wait(self, timeout=None):
                events.append(("wait", timeout))
                return 70

            def poll(self):
                raise AssertionError("identity-unavailable cleanup must bounded-wait")

        def close_descriptor(descriptor):
            events.append(("close", descriptor))

        def prove_uid_empty(_uid, *, expected):
            events.append(("uid", expected))
            return {}

        with TemporaryDirectory() as tmp, patch.object(
            collector_module.os, "pipe", side_effect=lambda: next(descriptors)
        ), patch.object(
            collector_module.os, "close", side_effect=close_descriptor
        ), patch.object(
            collector_module.os, "geteuid", return_value=0
        ), patch.object(
            collector_module, "ProgressSnapshotStore", Store
        ), patch.object(
            collector_module, "ProgressPipeDrainer", Drainer
        ), patch.object(
            collector_module.subprocess, "Popen", return_value=Process()
        ), patch.object(
            collector_module,
            "_read_process_group_identity",
            side_effect=CollectorError("startup identity unavailable"),
        ), patch.object(
            collector_module,
            "_require_formal_uid_processes",
            side_effect=prove_uid_empty,
        ), patch.object(
            collector_module,
            "_require_pidfd_support",
            return_value=None,
            create=True,
        ), patch.object(
            collector_module,
            "_pidfd_open",
            return_value=91,
            create=True,
        ), patch.object(
            collector_module,
            "_pidfd_close",
            return_value=None,
            create=True,
        ):
            with self.assertRaises(CollectorError):
                collector_module._start_gated_candidate(
                    command=Process.args,
                    cwd=Path(tmp).resolve(),
                    environment={},
                    formal_uid=65532,
                    formal_gid=65532,
                    require_completion_receipt=True,
                    require_progress=True,
                    progress_binding_sha256="a" * 64,
                    progress_config_sha256="b" * 64,
                    progress_snapshot_path=Path(tmp).resolve() / "progress.json",
                    progress_expected_uid=0,
                    progress_expected_gid=0,
                )

        self.assertIn(("wait", 5.0), events)
        self.assertLess(events.index(("close", 11)), events.index(("wait", 5.0)))
        self.assertIn(("uid", {}), events)
        uid_index = events.index(("uid", {}))
        self.assertLess(events.index(("wait", 5.0)), uid_index)
        self.assertLess(uid_index, events.index("blocked-progress"))
        self.assertLess(uid_index, events.index(("close", 16)))

    def test_startup_identity_timeout_kills_only_bound_leader_pidfd(self):
        descriptors = iter(((10, 11), (12, 13), (14, 15), (16, 17)))
        events = []

        class Store:
            def __init__(self, *_args, **_kwargs):
                self.status = None

            def write_starting(self, _binding, _config):
                self.status = "starting"

            def read_view(self):
                return {"status": self.status}

            def write_terminal(self, status, _reason):
                events.append("blocked-progress")
                self.status = status

        class Drainer:
            def __init__(self, descriptor, _state, _store):
                self.descriptor = descriptor

            def start(self):
                raise AssertionError("identity failure must precede drainer start")

        class Process:
            pid = 4242
            args = (sys.executable, "-I", "-B", "unused.py")

            def __init__(self):
                self.wait_calls = 0

            def wait(self, timeout=None):
                self.wait_calls += 1
                events.append(("wait", timeout))
                if self.wait_calls == 1:
                    raise subprocess.TimeoutExpired(self.args, timeout)
                return -signal.SIGKILL

            def poll(self):
                raise AssertionError("identity-unavailable cleanup must use wait")

        process = Process()

        def open_pidfd(pid):
            events.append(("pidfd-open", pid))
            return 91

        def read_identity(*_args, **_kwargs):
            events.append("identity-read")
            raise CollectorError("startup identity unavailable")

        def close_descriptor(descriptor):
            events.append(("fd-close", descriptor))

        def send_pidfd(descriptor, sig):
            events.append(("pidfd-signal", descriptor, sig))

        def close_pidfd(descriptor):
            events.append(("pidfd-close", descriptor))

        def prove_uid_empty(_uid, *, expected):
            events.append(("uid", expected))
            return {}

        with TemporaryDirectory() as tmp, patch.object(
            collector_module.os, "pipe", side_effect=lambda: next(descriptors)
        ), patch.object(
            collector_module.os, "close", side_effect=close_descriptor
        ), patch.object(
            collector_module.os, "geteuid", return_value=0
        ), patch.object(
            collector_module, "ProgressSnapshotStore", Store
        ), patch.object(
            collector_module, "ProgressPipeDrainer", Drainer
        ), patch.object(
            collector_module.subprocess, "Popen", return_value=process
        ), patch.object(
            collector_module,
            "_read_process_group_identity",
            side_effect=read_identity,
        ), patch.object(
            collector_module,
            "_require_formal_uid_processes",
            side_effect=prove_uid_empty,
        ), patch.object(
            collector_module,
            "_require_pidfd_support",
            return_value=None,
            create=True,
        ), patch.object(
            collector_module,
            "_pidfd_open",
            side_effect=open_pidfd,
            create=True,
        ), patch.object(
            collector_module,
            "_pidfd_send_signal",
            side_effect=send_pidfd,
            create=True,
        ), patch.object(
            collector_module,
            "_pidfd_close",
            side_effect=close_pidfd,
            create=True,
        ):
            with self.assertRaises(CollectorError):
                collector_module._start_gated_candidate(
                    command=Process.args,
                    cwd=Path(tmp).resolve(),
                    environment={},
                    formal_uid=65532,
                    formal_gid=65532,
                    require_completion_receipt=True,
                    require_progress=True,
                    progress_binding_sha256="a" * 64,
                    progress_config_sha256="b" * 64,
                    progress_snapshot_path=Path(tmp).resolve() / "progress.json",
                    progress_expected_uid=0,
                    progress_expected_gid=0,
                )

        self.assertLess(events.index(("pidfd-open", 4242)), events.index("identity-read"))
        self.assertLess(events.index(("fd-close", 11)), events.index(("wait", 5.0)))
        first_wait = events.index(("wait", 5.0))
        signal_index = events.index(("pidfd-signal", 91, signal.SIGKILL))
        second_wait = events.index(("wait", 5.0), first_wait + 1)
        uid_index = events.index(("uid", {}))
        self.assertLess(first_wait, signal_index)
        self.assertLess(signal_index, second_wait)
        self.assertLess(second_wait, uid_index)
        self.assertLess(uid_index, events.index("blocked-progress"))
        self.assertLess(uid_index, events.index(("fd-close", 16)))
        self.assertEqual(events.count(("pidfd-close", 91)), 1)

    def test_startup_identity_absence_proof_failure_hands_off_without_cleanup(self):
        class SupervisorHandoff(BaseException):
            pass

        descriptors = iter(((10, 11), (12, 13), (14, 15), (16, 17)))
        events = []

        class Store:
            def __init__(self, *_args, **_kwargs):
                self.status = None

            def write_starting(self, _binding, _config):
                self.status = "starting"

            def read_view(self):
                return {"status": self.status}

            def write_terminal(self, status, _reason):
                events.append("blocked-progress")
                self.status = status

        class Drainer:
            def __init__(self, _descriptor, _state, _store):
                pass

            def start(self):
                raise AssertionError("identity failure must precede drainer start")

            def abort(self):
                events.append("drainer-abort")

        class Process:
            pid = 4242
            args = (sys.executable, "-I", "-B", "unused.py")

            def wait(self, timeout=None):
                events.append(("wait", timeout))
                return 70

            def poll(self):
                raise AssertionError("identity-unavailable cleanup must bounded-wait")

        def prove_uid_empty(_uid, *, expected):
            events.append(("uid-proof", expected))
            raise CollectorError("dedicated UID absence is unproven")

        with TemporaryDirectory() as tmp, patch.object(
            collector_module.os, "pipe", side_effect=lambda: next(descriptors)
        ), patch.object(
            collector_module.os,
            "close",
            side_effect=lambda descriptor: events.append(("fd-close", descriptor)),
        ), patch.object(
            collector_module.os, "geteuid", return_value=0
        ), patch.object(
            collector_module, "ProgressSnapshotStore", Store
        ), patch.object(
            collector_module, "ProgressPipeDrainer", Drainer
        ), patch.object(
            collector_module.subprocess, "Popen", return_value=Process()
        ), patch.object(
            collector_module,
            "_read_process_group_identity",
            side_effect=CollectorError("startup identity unavailable"),
        ), patch.object(
            collector_module,
            "_require_formal_uid_processes",
            side_effect=prove_uid_empty,
        ), patch.object(
            collector_module,
            "_require_pidfd_support",
            return_value=None,
        ), patch.object(
            collector_module,
            "_pidfd_open",
            return_value=91,
        ), patch.object(
            collector_module,
            "_pidfd_close",
            side_effect=lambda descriptor: events.append(
                ("pidfd-close", descriptor)
            ),
        ), patch.object(
            collector_module,
            "_formal_startup_fail_stop",
            side_effect=SupervisorHandoff("supervisor-handoff"),
        ) as fail_stop:
            with self.assertRaises(SupervisorHandoff):
                collector_module._start_gated_candidate(
                    command=Process.args,
                    cwd=Path(tmp).resolve(),
                    environment={},
                    formal_uid=65532,
                    formal_gid=65532,
                    require_completion_receipt=True,
                    require_progress=True,
                    progress_binding_sha256="a" * 64,
                    progress_config_sha256="b" * 64,
                    progress_snapshot_path=Path(tmp).resolve() / "progress.json",
                    progress_expected_uid=0,
                    progress_expected_gid=0,
                )

        fail_stop.assert_called_once_with()

        self.assertEqual(
            events[:3],
            [("fd-close", 11), ("wait", 5.0), ("uid-proof", {})],
        )
        self.assertNotIn("blocked-progress", events)
        self.assertNotIn("drainer-abort", events)
        self.assertEqual(
            sorted(event[1] for event in events if event[0] == "fd-close"),
            list(range(10, 18)),
        )
        self.assertNotIn(("pidfd-close", 91), events)

    def test_formal_pidfd_preflight_call_contract_uses_self_and_closes_once(self):
        close_calls = []
        with patch.object(
            collector_module.sys, "platform", "linux"
        ), patch.object(
            collector_module.os, "getpid", return_value=7001
        ), patch.object(
            collector_module.os, "pidfd_open", return_value=73, create=True
        ) as pidfd_open, patch.object(
            collector_module.signal,
            "pidfd_send_signal",
            return_value=None,
            create=True,
        ) as pidfd_send, patch.object(
            collector_module.os,
            "close",
            side_effect=lambda descriptor: close_calls.append(descriptor),
        ):
            collector_module._require_pidfd_support()

        pidfd_open.assert_called_once_with(7001, 0)
        pidfd_send.assert_called_once_with(73, 0, None, 0)
        self.assertEqual(close_calls, [73])

    def test_formal_pidfd_preflight_call_contract_failures_block_popen_and_close_once(self):
        cases = (
            ("open-enosys", OSError(errno.ENOSYS, "pidfd_open unavailable")),
            ("open-policy", OSError(errno.EPERM, "pidfd_open denied")),
            ("send-einval", OSError(errno.EINVAL, "pidfd signal unavailable")),
            ("send-policy", OSError(errno.EPERM, "pidfd signal denied")),
        )
        for case, failure in cases:
            with self.subTest(case=case), TemporaryDirectory() as tmp:
                close_calls = []
                real_close = os.close

                def observed_close(descriptor):
                    close_calls.append(descriptor)
                    if descriptor != 73:
                        real_close(descriptor)

                open_failure = failure if case.startswith("open-") else None
                send_failure = failure if case.startswith("send-") else None
                with patch.object(
                    collector_module.sys, "platform", "linux"
                ), patch.object(
                    collector_module.os, "geteuid", return_value=0
                ), patch.object(
                    collector_module.os, "getpid", return_value=7001
                ), patch.object(
                    collector_module.os,
                    "pidfd_open",
                    side_effect=open_failure,
                    return_value=73,
                    create=True,
                ) as pidfd_open, patch.object(
                    collector_module.signal,
                    "pidfd_send_signal",
                    side_effect=send_failure,
                    return_value=None,
                    create=True,
                ) as pidfd_send, patch.object(
                    collector_module.os, "close", side_effect=observed_close
                ), patch.object(
                    collector_module.subprocess,
                    "Popen",
                    side_effect=AssertionError("Popen must not run"),
                ) as popen:
                    with self.assertRaisesRegex(CollectorError, "pidfd"):
                        collector_module._start_gated_candidate(
                            command=(sys.executable, "-I", "-B", "unused.py"),
                            cwd=Path(tmp).resolve(),
                            environment={},
                            formal_uid=65532,
                            formal_gid=65532,
                        )

                popen.assert_not_called()
                pidfd_open.assert_called_once_with(7001, 0)
                if case.startswith("open-"):
                    pidfd_send.assert_not_called()
                    self.assertEqual(close_calls, [])
                else:
                    pidfd_send.assert_called_once_with(73, 0, None, 0)
                    self.assertEqual(close_calls, [73])

    def test_formal_pidfd_preflight_call_contract_rejects_malformed_results_before_popen(self):
        cases = (("open", True, None), ("send", 73, 1))
        for case, open_result, send_result in cases:
            with self.subTest(case=case), TemporaryDirectory() as tmp:
                close_calls = []
                with patch.object(
                    collector_module.sys, "platform", "linux"
                ), patch.object(
                    collector_module.os, "geteuid", return_value=0
                ), patch.object(
                    collector_module.os, "getpid", return_value=7001
                ), patch.object(
                    collector_module.os,
                    "pidfd_open",
                    return_value=open_result,
                    create=True,
                ), patch.object(
                    collector_module.signal,
                    "pidfd_send_signal",
                    return_value=send_result,
                    create=True,
                ), patch.object(
                    collector_module.os,
                    "close",
                    side_effect=lambda descriptor: close_calls.append(descriptor),
                ), patch.object(
                    collector_module.subprocess,
                    "Popen",
                    side_effect=AssertionError("Popen must not run"),
                ) as popen:
                    with self.assertRaisesRegex(CollectorError, "pidfd"):
                        collector_module._start_gated_candidate(
                            command=(sys.executable, "-I", "-B", "unused.py"),
                            cwd=Path(tmp).resolve(),
                            environment={},
                            formal_uid=65532,
                            formal_gid=65532,
                        )

                popen.assert_not_called()
                self.assertEqual(close_calls, [] if case == "open" else [73])

    @unittest.skipUnless(
        sys.platform.startswith("linux")
        and callable(getattr(os, "pidfd_open", None))
        and callable(getattr(signal, "pidfd_send_signal", None)),
        "protected Linux real pidfd syscalls only",
    )
    def test_protected_linux_pidfd_preflight_uses_real_kernel_syscalls(self):
        def pidfd_inventory():
            observed = []
            for entry in (Path("/proc") / "self" / "fd").iterdir():
                try:
                    target = os.readlink(entry)
                except FileNotFoundError:
                    continue
                if "pidfd" in target:
                    observed.append((entry.name, target))
            return tuple(sorted(observed))

        before = pidfd_inventory()
        collector_module._require_pidfd_support()
        after = pidfd_inventory()

        self.assertTrue(
            after == before,
            "protected Linux pidfd probe changed the process inventory",
        )

    def test_post_popen_leader_pidfd_failure_requires_exit_and_exact_uid_empty(self):
        descriptors = iter(((10, 11), (12, 13)))
        events = []

        class Process:
            pid = 4242
            args = (sys.executable, "-I", "-B", "unused.py")

            def wait(self, timeout=None):
                events.append(("wait", timeout))
                return 70

            def terminate(self):
                raise AssertionError("raw PID terminate is forbidden")

            def kill(self):
                raise AssertionError("raw PID kill is forbidden")

        def close_descriptor(descriptor):
            events.append(("close", descriptor))

        def prove_uid_empty(_uid, *, expected):
            events.append(("uid", expected))
            return {}

        with TemporaryDirectory() as tmp, patch.object(
            collector_module.os, "pipe", side_effect=lambda: next(descriptors)
        ), patch.object(
            collector_module.os, "close", side_effect=close_descriptor
        ), patch.object(
            collector_module.os, "geteuid", return_value=0
        ), patch.object(
            collector_module, "_require_pidfd_support", return_value=None
        ), patch.object(
            collector_module.subprocess, "Popen", return_value=Process()
        ) as popen, patch.object(
            collector_module,
            "_pidfd_open",
            side_effect=OSError("leader-pidfd-acquisition-failed"),
        ), patch.object(
            collector_module,
            "_require_formal_uid_processes",
            side_effect=prove_uid_empty,
        ), patch.object(
            collector_module.os, "kill"
        ) as raw_kill, patch.object(
            collector_module.os, "killpg"
        ) as raw_killpg:
            with self.assertRaisesRegex(OSError, "leader-pidfd"):
                collector_module._start_gated_candidate(
                    command=Process.args,
                    cwd=Path(tmp).resolve(),
                    environment={},
                    formal_uid=65532,
                    formal_gid=65532,
                )

        popen.assert_called_once()
        self.assertLess(events.index(("close", 11)), events.index(("wait", 5.0)))
        self.assertLess(events.index(("wait", 5.0)), events.index(("uid", {})))
        raw_kill.assert_not_called()
        raw_killpg.assert_not_called()

    def test_post_popen_leader_pidfd_failure_fail_stops_when_absence_is_unproven(self):
        class SupervisorHandoff(BaseException):
            pass

        for case in ("wait-unproven", "uid-unproven"):
            with self.subTest(case=case), TemporaryDirectory() as tmp:
                descriptors = iter(((10, 11), (12, 13)))
                events = []

                class Process:
                    pid = 4242
                    args = (sys.executable, "-I", "-B", "unused.py")

                    def wait(self, timeout=None):
                        events.append(("wait", timeout))
                        if case == "wait-unproven":
                            raise RuntimeError("child exit is unproven")
                        return 70

                    def terminate(self):
                        raise AssertionError("raw PID terminate is forbidden")

                    def kill(self):
                        raise AssertionError("raw PID kill is forbidden")

                def prove_uid(_uid, *, expected):
                    events.append(("uid", expected))
                    if case == "uid-unproven":
                        raise CollectorError("dedicated UID absence is unproven")
                    return {}

                with patch.object(
                    collector_module.os,
                    "pipe",
                    side_effect=lambda: next(descriptors),
                ), patch.object(
                    collector_module.os,
                    "close",
                    side_effect=lambda descriptor: events.append(
                        ("close", descriptor)
                    ),
                ), patch.object(
                    collector_module.os, "geteuid", return_value=0
                ), patch.object(
                    collector_module, "_require_pidfd_support", return_value=None
                ), patch.object(
                    collector_module.subprocess, "Popen", return_value=Process()
                ), patch.object(
                    collector_module,
                    "_pidfd_open",
                    side_effect=OSError("leader-pidfd-acquisition-failed"),
                ), patch.object(
                    collector_module,
                    "_require_formal_uid_processes",
                    side_effect=prove_uid,
                ), patch.object(
                    collector_module,
                    "_formal_startup_fail_stop",
                    side_effect=SupervisorHandoff("supervisor-handoff"),
                    create=True,
                ) as fail_stop, patch.object(
                    collector_module.os, "kill"
                ) as raw_kill, patch.object(
                    collector_module.os, "killpg"
                ) as raw_killpg:
                    with self.assertRaises(SupervisorHandoff):
                        collector_module._start_gated_candidate(
                            command=Process.args,
                            cwd=Path(tmp).resolve(),
                            environment={},
                            formal_uid=65532,
                            formal_gid=65532,
                        )

                fail_stop.assert_called_once_with()
                self.assertLess(
                    events.index(("close", 11)), events.index(("wait", 5.0))
                )
                raw_kill.assert_not_called()
                raw_killpg.assert_not_called()

    def test_formal_startup_fails_before_popen_without_pidfd_primitives(self):
        with TemporaryDirectory() as tmp, patch.object(
            collector_module.os, "geteuid", return_value=0
        ), patch.object(
            collector_module,
            "_require_pidfd_support",
            side_effect=CollectorError("formal pidfd support is unavailable"),
            create=True,
        ), patch.object(
            collector_module.subprocess,
            "Popen",
            side_effect=AssertionError("Popen must not run"),
        ) as popen:
            with self.assertRaisesRegex(CollectorError, "pidfd"):
                collector_module._start_gated_candidate(
                    command=(sys.executable, "-I", "-B", "unused.py"),
                    cwd=Path(tmp).resolve(),
                    environment={},
                    formal_uid=65532,
                    formal_gid=65532,
                )

        popen.assert_not_called()

    def test_gated_child_drainer_setup_failure_terminalizes_starting_snapshot(self):
        with TemporaryDirectory() as tmp, patch.object(
            collector_module,
            "ProgressPipeDrainer",
            side_effect=RuntimeError("token=private-setup"),
        ):
            root = Path(tmp).resolve()
            progress_path = root / "progress.json"
            with self.assertRaises(RuntimeError):
                collector_module._start_gated_candidate(
                    command=(sys.executable, "-I", "-B", "unused.py"),
                    cwd=root,
                    environment={},
                    require_progress=True,
                    progress_binding_sha256="a" * 64,
                    progress_config_sha256="b" * 64,
                    progress_snapshot_path=progress_path,
                    progress_expected_uid=os.getuid(),
                    progress_expected_gid=os.getgid(),
                )

            snapshot = json.loads(progress_path.read_text(encoding="utf-8"))

        self.assertEqual(snapshot["status"], "blocked")

    def test_gated_child_drainer_start_failure_terminalizes_starting_snapshot(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            script = root / "ready_then_wait.py"
            script.write_text(
                "\n".join(
                    (
                        "import os",
                        "ready = int(os.environ.pop('TXNMEM_PROVENANCE_READY_FD'))",
                        "gate = int(os.environ.pop('TXNMEM_PROVENANCE_START_GATE_FD'))",
                        "os.write(ready, b'R')",
                        "os.close(ready)",
                        "os.read(gate, 1)",
                        "os.close(gate)",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            progress_path = root / "progress.json"
            with patch.object(
                collector_module.ProgressPipeDrainer,
                "start",
                side_effect=RuntimeError("secret=private-drainer"),
            ), self.assertRaises(RuntimeError):
                collector_module._start_gated_candidate(
                    command=(sys.executable, "-I", "-B", str(script)),
                    cwd=root,
                    environment={},
                    require_progress=True,
                    progress_binding_sha256="a" * 64,
                    progress_config_sha256="b" * 64,
                    progress_snapshot_path=progress_path,
                    progress_expected_uid=os.getuid(),
                    progress_expected_gid=os.getgid(),
                )

            snapshot = json.loads(progress_path.read_text(encoding="utf-8"))

        self.assertEqual(snapshot["status"], "blocked")

    def test_gated_child_readiness_failure_terminalizes_starting_snapshot(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            script = root / "never_ready.py"
            script.write_text("raise SystemExit(0)\n", encoding="utf-8")
            progress_path = root / "progress.json"
            with self.assertRaisesRegex(CollectorError, "readiness"):
                collector_module._start_gated_candidate(
                    command=(sys.executable, "-I", "-B", str(script)),
                    cwd=root,
                    environment={},
                    require_progress=True,
                    progress_binding_sha256="a" * 64,
                    progress_config_sha256="b" * 64,
                    progress_snapshot_path=progress_path,
                    progress_expected_uid=os.getuid(),
                    progress_expected_gid=os.getgid(),
                )

            snapshot = json.loads(progress_path.read_text(encoding="utf-8"))

        self.assertEqual(snapshot["status"], "blocked")

    def test_gated_child_gate_write_failure_terminalizes_progress(self):
        for failure in ("epipe", "short"):
            with self.subTest(failure=failure), TemporaryDirectory() as tmp:
                root = Path(tmp).resolve()
                script = root / "ready_gate.py"
                script.write_text(
                    "\n".join(
                        (
                            "import os",
                            "ready = int(os.environ.pop('TXNMEM_PROVENANCE_READY_FD'))",
                            "gate = int(os.environ.pop('TXNMEM_PROVENANCE_START_GATE_FD'))",
                            "os.write(ready, b'R')",
                            "os.close(ready)",
                            "os.read(gate, 1)",
                            "os.close(gate)",
                        )
                    )
                    + "\n",
                    encoding="utf-8",
                )
                progress_path = root / "progress.json"
                child = collector_module._start_gated_candidate(
                    command=(sys.executable, "-I", "-B", str(script)),
                    cwd=root,
                    environment={},
                    require_progress=True,
                    progress_binding_sha256="a" * 64,
                    progress_config_sha256="b" * 64,
                    progress_snapshot_path=progress_path,
                    progress_expected_uid=os.getuid(),
                    progress_expected_gid=os.getgid(),
                )
                release_fd = child._release_fd
                real_write = os.write

                def gate_write(descriptor, payload):
                    if descriptor == release_fd:
                        if failure == "epipe":
                            raise BrokenPipeError("password=private-gate")
                        return 0
                    return real_write(descriptor, payload)

                try:
                    with patch.object(
                        collector_module.os, "write", side_effect=gate_write
                    ), self.assertRaises((BrokenPipeError, CollectorError)):
                        child.release()
                finally:
                    child.close()
                    child.process.wait(timeout=5)

                snapshot = json.loads(progress_path.read_text(encoding="utf-8"))

            self.assertEqual(snapshot["status"], "blocked")

    def test_semantically_invalid_canonical_receipt_blocks_final_progress(self):
        from txnmem_provenance_progress import (
            ProgressSnapshotStore,
            build_progress_event,
        )

        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            progress_path = root / "progress.json"
            store = ProgressSnapshotStore(
                progress_path,
                expected_uid=os.getuid(),
                expected_gid=os.getgid(),
            )
            store.write_starting("a" * 64, "b" * 64)
            store.write_running(
                build_progress_event(
                    run_binding_sha256="a" * 64,
                    config_sha256="b" * 64,
                    cell_index=15,
                    graph_size=10000,
                    concurrency=16,
                    repetition_index=30,
                    completed_repetitions=450,
                    completed_samples=14400,
                    update_sequence=450,
                )
            )
            child = collector_module._GatedCandidate(
                process=SimpleNamespace(),
                _release_fd=None,
                _receipt_fd=None,
                ready_observed=True,
                _progress_store=store,
            )
            invalid_receipt = {
                "schema": "txnmem-provenance-candidate-attestation-material-v1",
                "candidate_bundle_id": "diagnostic-vector_graph-" + "1" * 16 + "-" + "2" * 16,
                "run_id_sha256": "2" * 64,
                "config_sha256": "f" * 64,
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
            with self.assertRaises(CollectorError):
                collector_module._validate_candidate_receipt_for_sealing(
                    invalid_receipt,
                    expected_candidate_id=invalid_receipt["candidate_bundle_id"],
                    expected_run_hash="2" * 64,
                    expected_config_hash="1" * 64,
                    expected_config_file_hash="3" * 64,
                    expected_workload_hash="4" * 64,
                    expected_environment_hash="5" * 64,
                    progress_blocker=child.block_progress,
                )
            snapshot = store.read_view()

        self.assertEqual(snapshot["status"], "blocked")
        self.assertEqual(snapshot["update_sequence"], 450)

    def test_receipt_failure_survives_progress_blocker_failure(self):
        caught = None
        try:
            collector_module._validate_candidate_receipt_for_sealing(
                {},
                expected_candidate_id="diagnostic-vector_graph-" + "1" * 16 + "-" + "2" * 16,
                expected_run_hash="2" * 64,
                expected_config_hash="1" * 64,
                expected_config_file_hash="3" * 64,
                expected_workload_hash="4" * 64,
                expected_environment_hash="5" * 64,
                progress_blocker=lambda: (_ for _ in ()).throw(
                    OSError("private-progress-blocker-failure")
                ),
            )
        except BaseException as exc:
            caught = exc

        self.assertIsInstance(caught, CollectorError)
        self.assertNotIn("private", str(caught))

    def test_collector_interruption_survives_progress_blocker_failure(self):
        primary = collector_module._CollectorInterruption(
            "collector interruption requested"
        )
        caught = None
        with patch.object(
            collector_module,
            "_validate_candidate_material",
            side_effect=primary,
        ):
            try:
                collector_module._validate_candidate_receipt_for_sealing(
                    {},
                    expected_candidate_id="diagnostic-vector_graph-" + "1" * 16 + "-" + "2" * 16,
                    expected_run_hash="2" * 64,
                    expected_config_hash="1" * 64,
                    expected_config_file_hash="3" * 64,
                    expected_workload_hash="4" * 64,
                    expected_environment_hash="5" * 64,
                    progress_blocker=lambda: (_ for _ in ()).throw(
                        OSError("private-progress-blocker-failure")
                    ),
                )
            except BaseException as exc:
                caught = exc

        self.assertIs(caught, primary)

    def test_progress_completion_preserves_baseexception_primary_and_traceback(self):
        class CompletionInterruption(BaseException):
            pass

        primary = CompletionInterruption("completion-interruption")
        origin = []
        blocker_calls = []

        def complete_progress():
            try:
                raise primary
            except BaseException as exc:
                origin.append(exc.__traceback__)
                raise

        def fail_blocker():
            blocker_calls.append("block")
            raise RuntimeError("secondary-blocker-failure")

        caught = None
        try:
            collector_module._complete_progress_terminal(
                complete_progress,
                fail_blocker,
            )
        except BaseException as exc:
            caught = exc

        self.assertIs(caught, primary)
        self.assertEqual(blocker_calls, ["block"])
        traceback_nodes = []
        current = caught.__traceback__ if caught is not None else None
        while current is not None:
            traceback_nodes.append(current)
            current = current.tb_next
        self.assertIn(origin[0], traceback_nodes)

    def test_progress_completion_preserves_ordinary_primary_when_blocker_fails(self):
        primary = RuntimeError("ordinary-completion-failure")
        origin = []

        def complete_progress():
            try:
                raise primary
            except Exception as exc:
                origin.append(exc.__traceback__)
                raise

        caught = None
        try:
            collector_module._complete_progress_terminal(
                complete_progress,
                lambda: (_ for _ in ()).throw(
                    OSError("secondary-progress-blocker-failure")
                ),
            )
        except BaseException as exc:
            caught = exc

        self.assertIs(caught, primary)
        traceback_nodes = []
        current = caught.__traceback__ if caught is not None else None
        while current is not None:
            traceback_nodes.append(current)
            current = current.tb_next
        self.assertIn(origin[0], traceback_nodes)

    def test_blocked_progress_persistence_failure_is_sanitized_and_observable(self):
        class FailingStore:
            def read_view(self):
                return {"status": "running"}

            def write_terminal(self, _status, _reason):
                raise RuntimeError("password=private-block-write")

        child = collector_module._GatedCandidate(
            process=SimpleNamespace(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
            _progress_store=FailingStore(),
        )

        with self.assertRaises(CollectorError) as raised:
            child.block_progress()

        self.assertIn("progress blocking failed", str(raised.exception))
        self.assertNotIn("private", str(raised.exception))

    @staticmethod
    def _final_running_progress_snapshot(
        *, run_binding_sha256="a" * 64, config_sha256="b" * 64
    ):
        return {
            "schema": "txnmem-provenance-progress-snapshot-v1",
            "run_binding_sha256": run_binding_sha256,
            "config_sha256": config_sha256,
            "phase": "measurement",
            "cell_index": 15,
            "cell_count": 15,
            "graph_size": 10000,
            "concurrency": 16,
            "repetition_index": 30,
            "repetition_count": 30,
            "total_repetitions": 450,
            "status": "running",
            "update_sequence": 450,
            "completed_repetitions": 450,
            "completed_samples": 14400,
            "total_samples": 14400,
            "last_update_age_seconds": 0,
        }

    @staticmethod
    def _candidate_with_progress_store(
        store, *, run_binding_sha256="a" * 64, config_sha256="b" * 64
    ):
        child = collector_module._GatedCandidate(
            process=SimpleNamespace(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
            _progress_store=store,
        )
        child._progress_state = collector_module.FormalProgressState(
            run_binding_sha256, config_sha256
        )
        return child

    def test_completed_write_is_the_final_normal_path_store_operation(self):
        class Store:
            def __init__(self):
                self.read_count = 0
                self.write_count = 0

            def read_view(self):
                self.read_count += 1
                if self.read_count > 1:
                    raise RuntimeError("password=private-post-completion-read")
                return self._snapshot

            def write_terminal(self, status, reason):
                self.write_count += 1
                self.written = (status, reason)

        store = Store()
        store._snapshot = self._final_running_progress_snapshot()
        child = self._candidate_with_progress_store(store)

        terminal = child.complete_progress()

        self.assertEqual(terminal["status"], "completed")
        self.assertEqual(store.read_count, 1)
        self.assertEqual(store.write_count, 1)
        self.assertEqual(store.written, ("completed", "completed"))

    def test_ambiguous_completed_write_is_verified_as_committed(self):
        class Store:
            def __init__(self):
                self.status = "running"
                self.read_count = 0

            def read_view(self):
                self.read_count += 1
                snapshot = self._snapshot()
                snapshot["status"] = self.status
                if self.status == "completed":
                    snapshot["terminal_reason_class"] = "completed"
                return snapshot

            def write_terminal(self, _status, _reason):
                self.status = "completed"
                raise RuntimeError("password=private-ambiguous-write")

            def _snapshot(self):
                return ProvenanceExecutionCollectorTests._final_running_progress_snapshot()

        store = Store()
        child = self._candidate_with_progress_store(store)

        terminal = child.complete_progress()

        self.assertEqual(terminal["status"], "completed")
        self.assertEqual(store.read_count, 2)

    def test_ambiguous_completed_write_rejects_observed_nonterminal(self):
        class Store:
            def __init__(self):
                self.read_count = 0

            def read_view(self):
                self.read_count += 1
                return ProvenanceExecutionCollectorTests._final_running_progress_snapshot()

            def write_terminal(self, _status, _reason):
                raise RuntimeError("password=private-uncommitted-write")

        store = Store()
        child = self._candidate_with_progress_store(store)

        with self.assertRaises(CollectorError) as raised:
            child.complete_progress()

        self.assertEqual(store.read_count, 2)
        self.assertIn("progress completion failed", str(raised.exception))
        self.assertNotIn("private", str(raised.exception))

    def test_ambiguous_completed_write_rejects_different_execution_snapshot(self):
        class Store:
            def __init__(self):
                self.read_count = 0

            def read_view(self):
                self.read_count += 1
                snapshot = (
                    ProvenanceExecutionCollectorTests._final_running_progress_snapshot()
                )
                if self.read_count == 2:
                    snapshot["status"] = "completed"
                    snapshot["terminal_reason_class"] = "completed"
                    snapshot["run_binding_sha256"] = "c" * 64
                return snapshot

            def write_terminal(self, _status, _reason):
                raise RuntimeError("password=private-ambiguous-write")

        child = self._candidate_with_progress_store(Store())

        with self.assertRaises(CollectorError) as raised:
            child.complete_progress()

        self.assertIn("progress completion failed", str(raised.exception))
        self.assertNotIn("private", str(raised.exception))

    def test_completion_rejects_inexact_counts_before_terminal_write(self):
        for field, value in (
            ("status", "starting"),
            ("update_sequence", 449),
            ("completed_repetitions", 449),
            ("completed_samples", 14399),
        ):
            with self.subTest(field=field):
                snapshot = self._final_running_progress_snapshot()
                snapshot[field] = value
                writes = []
                store = SimpleNamespace(
                    read_view=lambda: snapshot,
                    write_terminal=lambda *args: writes.append(args),
                )
                child = self._candidate_with_progress_store(store)

                with self.assertRaisesRegex(CollectorError, "completion"):
                    child.complete_progress()

                self.assertEqual(writes, [])

    def test_completion_requires_exact_trusted_running_snapshot_closure(self):
        mutations = (
            ("truncated", lambda view: view.pop("schema")),
            ("extra", lambda view: view.__setitem__("run_id", "private-run")),
            (
                "wrong_binding",
                lambda view: view.__setitem__("run_binding_sha256", "c" * 64),
            ),
            (
                "wrong_config",
                lambda view: view.__setitem__("config_sha256", "d" * 64),
            ),
            (
                "wrong_schema",
                lambda view: view.__setitem__("schema", "progress-snapshot-v0"),
            ),
            ("wrong_phase", lambda view: view.__setitem__("phase", "setup")),
            ("wrong_cell", lambda view: view.__setitem__("cell_index", 14)),
            ("wrong_cell_count", lambda view: view.__setitem__("cell_count", 14)),
            ("wrong_graph", lambda view: view.__setitem__("graph_size", 1000)),
            ("wrong_concurrency", lambda view: view.__setitem__("concurrency", 8)),
            (
                "wrong_repetition",
                lambda view: view.__setitem__("repetition_index", 29),
            ),
            (
                "wrong_repetition_count",
                lambda view: view.__setitem__("repetition_count", 29),
            ),
            (
                "wrong_total_repetitions",
                lambda view: view.__setitem__("total_repetitions", 449),
            ),
            (
                "wrong_total_samples",
                lambda view: view.__setitem__("total_samples", 14399),
            ),
            (
                "terminal_reason_on_running",
                lambda view: view.__setitem__("terminal_reason_class", "completed"),
            ),
            (
                "negative_age",
                lambda view: view.__setitem__("last_update_age_seconds", -1),
            ),
            (
                "boolean_age",
                lambda view: view.__setitem__("last_update_age_seconds", True),
            ),
        )
        for label, mutate in mutations:
            with self.subTest(label=label):
                snapshot = self._final_running_progress_snapshot()
                mutate(snapshot)
                writes = []
                store = SimpleNamespace(
                    read_view=lambda: snapshot,
                    write_terminal=lambda *args: writes.append(args),
                )
                child = self._candidate_with_progress_store(store)

                with self.assertRaisesRegex(CollectorError, "completion"):
                    child.complete_progress()

                self.assertEqual(writes, [])

    def test_completion_rejects_type_confused_numeric_running_fields(self):
        numeric_values = {
            "cell_index": 15,
            "cell_count": 15,
            "graph_size": 10000,
            "concurrency": 16,
            "repetition_index": 30,
            "repetition_count": 30,
            "completed_repetitions": 450,
            "total_repetitions": 450,
            "completed_samples": 14400,
            "total_samples": 14400,
            "update_sequence": 450,
            "last_update_age_seconds": 0,
        }
        substitutions = [
            (field, float(value)) for field, value in numeric_values.items()
        ] + [
            ("cell_index", True),
            ("last_update_age_seconds", False),
        ]
        for field, substitution in substitutions:
            with self.subTest(field=field, substitution=repr(substitution)):
                snapshot = self._final_running_progress_snapshot()
                snapshot[field] = substitution
                writes = []
                store = SimpleNamespace(
                    read_view=lambda: snapshot,
                    write_terminal=lambda *args: writes.append(args),
                )
                child = self._candidate_with_progress_store(store)

                with self.assertRaisesRegex(CollectorError, "completion"):
                    child.complete_progress()

                self.assertEqual(writes, [])

    def test_completion_requires_candidate_trusted_progress_state(self):
        writes = []
        store = SimpleNamespace(
            read_view=self._final_running_progress_snapshot,
            write_terminal=lambda *args: writes.append(args),
        )
        child = collector_module._GatedCandidate(
            process=SimpleNamespace(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
            _progress_store=store,
        )

        with self.assertRaisesRegex(CollectorError, "completion"):
            child.complete_progress()

        self.assertEqual(writes, [])

    def test_ambiguous_completed_write_requires_exact_terminal_closure(self):
        mutations = (
            ("extra", lambda view: view.__setitem__("extra", "private-extra")),
            ("missing", lambda view: view.pop("last_update_age_seconds")),
            ("changed", lambda view: view.__setitem__("graph_size", 1000)),
            (
                "negative_age",
                lambda view: view.__setitem__("last_update_age_seconds", -1),
            ),
            (
                "boolean_age",
                lambda view: view.__setitem__("last_update_age_seconds", True),
            ),
            (
                "float_age",
                lambda view: view.__setitem__("last_update_age_seconds", 0.0),
            ),
        )
        for label, mutate in mutations:
            with self.subTest(label=label):
                running = self._final_running_progress_snapshot()
                terminal = dict(running)
                terminal["status"] = "completed"
                terminal["terminal_reason_class"] = "completed"
                mutate(terminal)

                class Store:
                    def __init__(self):
                        self.read_count = 0

                    def read_view(self):
                        self.read_count += 1
                        return dict(running if self.read_count == 1 else terminal)

                    def write_terminal(self, _status, _reason):
                        raise RuntimeError("password=private-ambiguous-write")

                child = self._candidate_with_progress_store(Store())

                with self.assertRaises(CollectorError) as raised:
                    child.complete_progress()

                self.assertIn("completion failed", str(raised.exception))
                self.assertNotIn("private", str(raised.exception))

    def test_ambiguous_completed_write_rejects_type_confused_numeric_fields(self):
        numeric_values = {
            "cell_index": 15,
            "cell_count": 15,
            "graph_size": 10000,
            "concurrency": 16,
            "repetition_index": 30,
            "repetition_count": 30,
            "completed_repetitions": 450,
            "total_repetitions": 450,
            "completed_samples": 14400,
            "total_samples": 14400,
            "update_sequence": 450,
            "last_update_age_seconds": 0,
        }
        substitutions = [
            (field, float(value)) for field, value in numeric_values.items()
        ] + [
            ("cell_index", True),
            ("last_update_age_seconds", False),
        ]
        for field, substitution in substitutions:
            with self.subTest(field=field, substitution=repr(substitution)):
                running = self._final_running_progress_snapshot()
                terminal = dict(running)
                terminal["status"] = "completed"
                terminal["terminal_reason_class"] = "completed"
                terminal[field] = substitution

                class Store:
                    def __init__(self):
                        self.read_count = 0

                    def read_view(self):
                        self.read_count += 1
                        return dict(running if self.read_count == 1 else terminal)

                    def write_terminal(self, _status, _reason):
                        raise RuntimeError("password=private-ambiguous-write")

                child = self._candidate_with_progress_store(Store())

                with self.assertRaises(CollectorError) as raised:
                    child.complete_progress()

                self.assertIn("completion failed", str(raised.exception))
                self.assertNotIn("private", str(raised.exception))

    def test_repeated_block_and_close_preserve_earlier_blocked_terminal(self):
        class BlockedStore:
            def __init__(self):
                self.write_count = 0

            def read_view(self):
                return {
                    "status": "blocked",
                    "terminal_reason_class": "progress_protocol_failed",
                }

            def write_terminal(self, _status, _reason):
                self.write_count += 1
                raise AssertionError("illegal second terminal transition")

        store = BlockedStore()
        child = collector_module._GatedCandidate(
            process=SimpleNamespace(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
            _progress_store=store,
        )

        child.block_progress()
        child.block_progress()
        child.close()

        self.assertEqual(store.write_count, 0)

    def test_pipe_allocation_failure_closes_every_earlier_descriptor_once(self):
        outcomes = iter(((10, 11), (12, 13), (14, 15)))
        closed = []

        def allocate_pipe():
            try:
                return next(outcomes)
            except StopIteration:
                raise OSError("EMFILE password=private-allocation") from None

        with TemporaryDirectory() as tmp, patch.object(
            collector_module.os, "pipe", side_effect=allocate_pipe
        ), patch.object(
            collector_module.os, "close", side_effect=closed.append
        ):
            root = Path(tmp).resolve()
            with self.assertRaises(OSError):
                collector_module._start_gated_candidate(
                    command=(sys.executable, "-I", "-B", "unused.py"),
                    cwd=root,
                    environment={},
                    require_completion_receipt=True,
                    require_progress=True,
                    progress_binding_sha256="a" * 64,
                    progress_config_sha256="b" * 64,
                    progress_snapshot_path=root / "progress.json",
                    progress_expected_uid=os.getuid(),
                    progress_expected_gid=os.getgid(),
                )

        self.assertEqual(sorted(closed), [10, 11, 12, 13, 14, 15])
        self.assertEqual(len(closed), len(set(closed)))

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

    def test_parent_death_signal_is_set_then_parent_identity_is_rechecked(self):
        calls = []

        def prctl(option, arg2, arg3, arg4, arg5):
            if option == 2:
                calls.append(("prctl-get", option, arg3, arg4, arg5))
                collector_module.ctypes.c_int.from_address(arg2).value = (
                    signal.SIGKILL
                )
                return 0
            calls.append(("prctl-set", option, arg2, arg3, arg4, arg5))
            return 0

        def getppid():
            calls.append(("getppid",))
            return 7001

        collector_module._set_parent_death_signal(
            7001, prctl=prctl, getppid=getppid
        )

        self.assertEqual(
            calls,
            [
                ("prctl-set", 1, signal.SIGKILL, 0, 0, 0),
                ("getppid",),
                ("prctl-get", 2, 0, 0, 0),
            ],
        )

    def test_parent_death_signal_fails_closed_on_get_or_exact_signal_mismatch(self):
        def prctl_with_get(result, observed_signal):
            def operation(option, arg2, _arg3, _arg4, _arg5):
                if option == 1:
                    return 0
                if option == 2:
                    collector_module.ctypes.c_int.from_address(arg2).value = (
                        observed_signal
                    )
                    return result
                raise AssertionError("unexpected prctl option")

            return operation

        for name, operation in (
            ("get-failed", prctl_with_get(-1, signal.SIGKILL)),
            ("wrong-signal", prctl_with_get(0, signal.SIGTERM)),
            ("cleared-signal", prctl_with_get(0, 0)),
        ):
            with self.subTest(name=name), self.assertRaisesRegex(
                CollectorError, "parent-death"
            ):
                collector_module._set_parent_death_signal(
                    7001, prctl=operation, getppid=lambda: 7001
                )

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux kernel only")
    def test_parent_death_signal_real_kernel_set_and_query(self):
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import os; "
                    "from txnmem_provenance_execution_collector import "
                    "_set_parent_death_signal; "
                    "_set_parent_death_signal(os.getppid())"
                ),
            ],
            cwd=Path(__file__).resolve().parents[1],
            env={**os.environ, "PYTHONPATH": "src"},
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        self.assertEqual(result.returncode, 0, result.stderr.decode("utf-8"))

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux kernel only")
    def test_parent_death_sigkill_terminates_child_after_actual_parent_exit(self):
        child_code = (
            "import os,time; "
            "from txnmem_provenance_execution_collector import "
            "_set_parent_death_signal; "
            "_set_parent_death_signal(os.getppid()); "
            "print(os.getpid(), flush=True); "
            "time.sleep(60)"
        )
        parent_code = (
            "import os,subprocess,sys; "
            f"code={child_code!r}; "
            "child=subprocess.Popen([sys.executable,'-c',code], "
            "stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=dict(os.environ)); "
            "line=child.stdout.readline(); "
            "sys.stdout.buffer.write(line); sys.stdout.buffer.flush(); "
            "os._exit(0)"
        )
        parent = subprocess.Popen(
            [sys.executable, "-c", parent_code],
            cwd=Path(__file__).resolve().parents[1],
            env={**os.environ, "PYTHONPATH": "src"},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        raw_pid = parent.stdout.readline()
        parent.wait(timeout=5.0)
        self.assertRegex(raw_pid, rb"^[1-9][0-9]*\n$")
        child_pid = int(raw_pid)
        proc_entry = Path("/proc") / str(child_pid)
        deadline = time.monotonic() + 5.0
        while proc_entry.exists() and time.monotonic() < deadline:
            time.sleep(0.01)

        self.assertFalse(proc_entry.exists())

    def test_parent_death_signal_fails_closed_for_invalid_or_changed_parent(self):
        for parent_pid in (0, -1, True, "7001"):
            with self.subTest(parent_pid=parent_pid), self.assertRaisesRegex(
                CollectorError, "parent"
            ):
                collector_module._set_parent_death_signal(
                    parent_pid,
                    prctl=lambda *_arguments: 0,
                    getppid=lambda: 7001,
                )

        with self.assertRaisesRegex(CollectorError, "parent"):
            collector_module._set_parent_death_signal(
                7001,
                prctl=lambda *_arguments: 0,
                getppid=lambda: 7002,
            )
        with self.assertRaisesRegex(CollectorError, "parent-death"):
            collector_module._set_parent_death_signal(
                7001,
                prctl=lambda *_arguments: -1,
                getppid=lambda: 7001,
            )

    def test_parent_death_signal_requires_available_linux_prctl(self):
        with patch.object(
            collector_module.platform, "system", return_value="Darwin"
        ), self.assertRaisesRegex(CollectorError, "Linux"):
            collector_module._set_parent_death_signal(7001)

        with patch.object(
            collector_module.platform, "system", return_value="Linux"
        ), patch.object(
            collector_module.ctypes,
            "CDLL",
            side_effect=OSError("private-unavailable"),
        ), self.assertRaisesRegex(CollectorError, "unavailable") as raised:
            collector_module._set_parent_death_signal(7001)
        self.assertNotIn("private", str(raised.exception))

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
        runner_deny = (
            'meta skuid 65532 reject comment "txnmem-runner-deny"'
        )
        management_deny = (
            "ip daddr 127.0.0.1 tcp dport 8474 reject with tcp reset "
            'comment "txnmem-management-deny"'
        )
        attribution_deny = (
            "ip daddr 127.0.0.1 tcp dport { 19000, 19001 } "
            "reject with tcp reset "
            'comment "txnmem-attribution-deny"'
        )
        host_tcp_deny, forward_tcp_deny = (
            self._assert_exact_bridge_tcp_reset_policy(batch)
        )
        host_fallback_deny = (
            "ip daddr { 172.19.0.0/16, 172.20.0.0/16 } "
            'reject comment "txnmem-host-bridge-deny"'
        )
        forward_prefix = (
            'iifname != { "br-aaaaaaaaaaaa", "br-bbbbbbbbbbbb" } '
        )
        forward_fallback_deny = (
            forward_prefix
            + "ip daddr { 172.19.0.0/16, 172.20.0.0/16 } "
            + 'reject comment "txnmem-forward-bridge-deny"'
        )
        for rule in (
            runner_deny,
            management_deny,
            attribution_deny,
            host_tcp_deny,
            host_fallback_deny,
            forward_tcp_deny,
            forward_fallback_deny,
        ):
            self.assertIn(rule, batch)
        self.assertLess(
            batch.index(host_tcp_deny), batch.index(host_fallback_deny)
        )
        self.assertLess(
            batch.index(forward_tcp_deny), batch.index(forward_fallback_deny)
        )
        self.assertIn("chain forward", batch)
        self.assertIn(
            'iifname != { "br-aaaaaaaaaaaa", "br-bbbbbbbbbbbb" }',
            batch,
        )
        self.assertEqual(batch.count(" accept comment"), 5)
        self.assertEqual(batch.count(" reject with tcp reset comment"), 4)
        self.assertEqual(batch.count(" reject comment"), 3)

    def test_nft_network_guard_preserves_established_proxy_flow_attribution(self):
        batch = collector_module._nft_guard_batch(
            "txnmem_" + "5" * 16,
            runner_uid=65532,
            backend_ipv4_subnet="172.19.0.0/16",
            ingress_ipv4_subnet="172.20.0.0/16",
            backend_bridge_interface="br-aaaaaaaaaaaa",
            ingress_bridge_interface="br-bbbbbbbbbbbb",
            toxiproxy_ingress_ipv4="172.20.0.2",
        )
        established_proxy_allow = (
            "ct state established ip daddr 127.0.0.1 "
            "tcp dport { 19000, 19001 } accept "
            'comment "txnmem-proxy-established-allow"'
        )

        self.assertIn(established_proxy_allow, batch)
        self.assertEqual(batch.count("txnmem-proxy-established-allow"), 1)
        self.assertLess(
            batch.index("txnmem-proxy-allow"),
            batch.index(established_proxy_allow),
        )
        self.assertLess(
            batch.index(established_proxy_allow),
            batch.index("txnmem-runner-deny"),
        )
        self.assertLess(
            batch.index(established_proxy_allow),
            batch.index("txnmem-attribution-deny"),
        )
        self.assertNotIn(
            "ct state established ip daddr 127.0.0.1 tcp dport 8474",
            batch,
        )
        self.assertNotIn("ct state established,related", batch)
        self.assertNotIn(
            "ct state established ip daddr {",
            batch,
        )

    def test_nft_network_guard_preserves_host_reset_before_bridge_fallback(self):
        batch = collector_module._nft_guard_batch(
            "txnmem_" + "5" * 16,
            runner_uid=65532,
            backend_ipv4_subnet="172.19.0.0/16",
            ingress_ipv4_subnet="172.20.0.0/16",
            backend_bridge_interface="br-aaaaaaaaaaaa",
            ingress_bridge_interface="br-bbbbbbbbbbbb",
            toxiproxy_ingress_ipv4="172.20.0.2",
        )
        subnets = "ip daddr { 172.19.0.0/16, 172.20.0.0/16 }"
        interfaces = 'iifname != { "br-aaaaaaaaaaaa", "br-bbbbbbbbbbbb" }'
        host_reset_allow = (
            f"{subnets} tcp flags & rst == rst accept "
            'comment "txnmem-host-bridge-reset-allow"'
        )
        host_tcp_deny, forward_tcp_deny = (
            self._assert_exact_bridge_tcp_reset_policy(batch)
        )
        host_fallback_deny = (
            f"{subnets} reject comment \"txnmem-host-bridge-deny\""
        )
        forward_fallback_deny = (
            f"{interfaces} {subnets} reject "
            'comment "txnmem-forward-bridge-deny"'
        )

        self.assertIn(host_reset_allow, batch)
        self.assertNotIn("txnmem-forward-bridge-reset-allow", batch)
        self.assertEqual(batch.count("tcp flags & rst == rst accept"), 1)
        self.assertLess(batch.index("txnmem-runner-deny"), batch.index(host_reset_allow))
        self.assertLess(batch.index(host_reset_allow), batch.index(host_tcp_deny))
        self.assertLess(batch.index(host_tcp_deny), batch.index(host_fallback_deny))
        self.assertLess(
            batch.index(forward_tcp_deny), batch.index(forward_fallback_deny)
        )

    def test_nft_bridge_tcp_reset_policy_rejects_noncanonical_port_predicates(self):
        batch = collector_module._nft_guard_batch(
            "txnmem_" + "5" * 16,
            runner_uid=65532,
            backend_ipv4_subnet="172.19.0.0/16",
            ingress_ipv4_subnet="172.20.0.0/16",
            backend_bridge_interface="br-aaaaaaaaaaaa",
            ingress_bridge_interface="br-bbbbbbbbbbbb",
            toxiproxy_ingress_ipv4="172.20.0.2",
        )
        exact_ports = (
            "tcp dport { 6333, 6334, 7474, 7687, 8474, 19000, 19001 }"
        )
        port_mutations = (
            "tcp dport { 6333, 6334, 7474, 7687, 8474, 19000 }",
            "tcp dport { 6334, 6333, 7474, 7687, 8474, 19000, 19001 }",
            "tcp dport { 6333, 6333, 6334, 7474, 7687, 8474, 19000, 19001 }",
            "tcp dport { 6333-6334, 7474, 7687, 8474, 19000, 19001 }",
            "tcp dport { 6333, 6334, 7474, 7687, 8474, 19000, 19001, 19002 }",
        )
        for scope, occurrence in (("host", 1), ("forward", 2)):
            for predicate in (*port_mutations, "meta l4proto tcp"):
                with self.subTest(scope=scope, predicate=predicate):
                    parts = batch.split(exact_ports)
                    self.assertEqual(len(parts), 3)
                    mutated = exact_ports.join(parts[:occurrence])
                    mutated += predicate
                    mutated += exact_ports.join(parts[occurrence:])
                    with self.assertRaises(AssertionError):
                        self._assert_exact_bridge_tcp_reset_policy(mutated)

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

        try:
            normalized = collector_module._normalize_nft_snapshot(
                document, table_name=table_name
            )
        except CollectorError as exc:
            self.fail(f"expected exact nft rule closure to normalize: {exc}")
        self.assertEqual(len(normalized["nftables"]), 15)

        for missing_comment in (
            "txnmem-proxy-established-allow",
            "txnmem-host-bridge-reset-allow",
            "txnmem-host-bridge-tcp-deny",
            "txnmem-forward-bridge-tcp-deny",
        ):
            with self.subTest(missing_comment=missing_comment):
                missing = copy.deepcopy(document)
                missing["nftables"] = [
                    item
                    for item in missing["nftables"]
                    if item.get("rule", {}).get("comment") != missing_comment
                ]
                with self.assertRaisesRegex(CollectorError, "closure"):
                    collector_module._normalize_nft_snapshot(
                        missing, table_name=table_name
                    )

        duplicate = copy.deepcopy(document)
        duplicate["nftables"].append(
            next(
                copy.deepcopy(item)
                for item in document["nftables"]
                if item.get("rule", {}).get("comment")
                == "txnmem-host-bridge-tcp-deny"
            )
        )
        with self.assertRaisesRegex(CollectorError, "closure"):
            collector_module._normalize_nft_snapshot(
                duplicate, table_name=table_name
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
        uid_proofs: list[tuple[int, dict[int, str]]] = []
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

        def require_uid_empty(uid, *, expected):
            uid_proofs.append((uid, dict(expected)))
            return {}

        with patch.object(guard, "_table_names", side_effect=table_names), patch.object(
            guard, "_run", side_effect=run
        ), patch.object(
            collector_module,
            "_require_formal_uid_processes",
            side_effect=require_uid_empty,
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
        self.assertEqual(uid_proofs, [(collector_module.FORMAL_RUNNER_UID, {})])
        self.assertNotIn(("delete", "table", "inet", table_name), calls)

    def test_nft_guard_deactivate_pre_inventory_failure_preserves_active_without_delete(self):
        table_name = "txnmem_" + "7" * 16
        guard = collector_module._NftNetworkGuard(
            table_name,
            backend_ipv4_subnet="172.19.0.0/16",
            ingress_ipv4_subnet="172.20.0.0/16",
            backend_bridge_interface="br-aaaaaaaaaaaa",
            ingress_bridge_interface="br-bbbbbbbbbbbb",
            toxiproxy_ingress_ipv4="172.20.0.2",
        )
        guard.active = True
        calls: list[tuple[str, ...]] = []

        def table_names():
            raise CollectorError("pre-delete inventory unavailable")

        def run(arguments, *, stdin=None, check=True):
            calls.append(tuple(arguments))
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with patch.object(guard, "_table_names", side_effect=table_names), patch.object(
            guard, "_run", side_effect=run
        ):
            with self.assertRaisesRegex(
                CollectorError, "^formal nftables guard cleanup failed$"
            ) as raised:
                guard.deactivate()

        self.assertIsInstance(raised.exception.__cause__, CollectorError)
        self.assertIn("pre-delete inventory unavailable", str(raised.exception.__cause__))
        self.assertTrue(guard.active)
        self.assertEqual(calls, [])

    def test_nft_guard_deactivate_absent_table_clears_active_without_delete(self):
        table_name = "txnmem_" + "8" * 16
        guard = collector_module._NftNetworkGuard(
            table_name,
            backend_ipv4_subnet="172.19.0.0/16",
            ingress_ipv4_subnet="172.20.0.0/16",
            backend_bridge_interface="br-aaaaaaaaaaaa",
            ingress_bridge_interface="br-bbbbbbbbbbbb",
            toxiproxy_ingress_ipv4="172.20.0.2",
        )
        guard.active = True
        calls: list[tuple[str, ...]] = []

        def table_names():
            return set()

        def run(arguments, *, stdin=None, check=True):
            calls.append(tuple(arguments))
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with patch.object(guard, "_table_names", side_effect=table_names), patch.object(
            guard, "_run", side_effect=run
        ):
            guard.deactivate()

        self.assertFalse(guard.active)
        self.assertEqual(calls, [])

    def test_nft_guard_deactivate_table_present_deletes_once_and_clears_after_absence_proof(self):
        table_name = "txnmem_" + "9" * 16
        guard = collector_module._NftNetworkGuard(
            table_name,
            backend_ipv4_subnet="172.19.0.0/16",
            ingress_ipv4_subnet="172.20.0.0/16",
            backend_bridge_interface="br-aaaaaaaaaaaa",
            ingress_bridge_interface="br-bbbbbbbbbbbb",
            toxiproxy_ingress_ipv4="172.20.0.2",
        )
        guard.active = True
        calls: list[tuple[str, ...]] = []
        table_names = iter(({table_name}, set()))

        def run(arguments, *, stdin=None, check=True):
            calls.append(tuple(arguments))
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with patch.object(
            guard, "_table_names", side_effect=lambda: next(table_names)
        ), patch.object(guard, "_run", side_effect=run):
            guard.deactivate()

        self.assertFalse(guard.active)
        self.assertEqual(calls, [("delete", "table", "inet", table_name)])

    def test_nft_guard_deactivate_retry_proves_absence_without_second_delete_after_post_inventory_failure(self):
        table_name = "txnmem_" + "a" * 16
        guard = collector_module._NftNetworkGuard(
            table_name,
            backend_ipv4_subnet="172.19.0.0/16",
            ingress_ipv4_subnet="172.20.0.0/16",
            backend_bridge_interface="br-aaaaaaaaaaaa",
            ingress_bridge_interface="br-bbbbbbbbbbbb",
            toxiproxy_ingress_ipv4="172.20.0.2",
        )
        guard.active = True
        calls: list[tuple[str, ...]] = []
        inventory_observations: list[set[str] | str] = []
        table_present = True
        delete_calls = 0
        post_delete_inventory_failed = False

        def table_names():
            nonlocal post_delete_inventory_failed
            if (
                delete_calls == 1
                and not table_present
                and not post_delete_inventory_failed
            ):
                post_delete_inventory_failed = True
                inventory_observations.append("post-delete-error")
                raise CollectorError("post-delete inventory unavailable")
            observed = {table_name} if table_present else set()
            inventory_observations.append(set(observed))
            return observed

        def run(arguments, *, stdin=None, check=True):
            nonlocal table_present, delete_calls
            calls.append(tuple(arguments))
            if arguments == ("delete", "table", "inet", table_name):
                delete_calls += 1
                if not table_present:
                    raise CollectorError("delete failed: table does not exist")
                table_present = False
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with patch.object(guard, "_table_names", side_effect=table_names), patch.object(
            guard, "_run", side_effect=run
        ):
            with self.assertRaisesRegex(
                CollectorError, "^formal nftables guard cleanup failed$"
            ) as raised:
                guard.deactivate()
            self.assertIsInstance(raised.exception.__cause__, CollectorError)
            self.assertIn(
                "post-delete inventory unavailable", str(raised.exception.__cause__)
            )
            self.assertTrue(guard.active)
            guard.deactivate()

        self.assertFalse(guard.active)
        self.assertEqual(calls, [("delete", "table", "inet", table_name)])
        self.assertEqual(
            inventory_observations, [{table_name}, "post-delete-error", set()]
        )

    def test_nft_guard_deactivate_residual_table_preserves_active_until_retry_proves_absence(self):
        table_name = "txnmem_" + "b" * 16
        guard = collector_module._NftNetworkGuard(
            table_name,
            backend_ipv4_subnet="172.19.0.0/16",
            ingress_ipv4_subnet="172.20.0.0/16",
            backend_bridge_interface="br-aaaaaaaaaaaa",
            ingress_bridge_interface="br-bbbbbbbbbbbb",
            toxiproxy_ingress_ipv4="172.20.0.2",
        )
        guard.active = True
        calls: list[tuple[str, ...]] = []
        table_names = iter(({table_name}, {table_name}, {table_name}, set()))

        def run(arguments, *, stdin=None, check=True):
            calls.append(tuple(arguments))
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        with patch.object(
            guard, "_table_names", side_effect=lambda: next(table_names)
        ), patch.object(guard, "_run", side_effect=run):
            with self.assertRaisesRegex(
                CollectorError, "^formal nftables guard cleanup failed$"
            ) as raised:
                guard.deactivate()
            self.assertIsNone(raised.exception.__cause__)
            self.assertTrue(guard.active)
            guard.deactivate()

        self.assertFalse(guard.active)
        self.assertEqual(calls, [("delete", "table", "inet", table_name)] * 2)

    def test_nft_guard_deactivate_delete_failure_preserves_active_and_wraps_cleanup(self):
        table_name = "txnmem_" + "c" * 16
        guard = collector_module._NftNetworkGuard(
            table_name,
            backend_ipv4_subnet="172.19.0.0/16",
            ingress_ipv4_subnet="172.20.0.0/16",
            backend_bridge_interface="br-aaaaaaaaaaaa",
            ingress_bridge_interface="br-bbbbbbbbbbbb",
            toxiproxy_ingress_ipv4="172.20.0.2",
        )
        guard.active = True
        calls: list[tuple[str, ...]] = []

        def run(arguments, *, stdin=None, check=True):
            calls.append(tuple(arguments))
            raise CollectorError("delete denied")

        with patch.object(
            guard, "_table_names", return_value={table_name}
        ), patch.object(guard, "_run", side_effect=run):
            with self.assertRaisesRegex(
                CollectorError, "^formal nftables guard cleanup failed$"
            ) as raised:
                guard.deactivate()

        self.assertIsInstance(raised.exception.__cause__, CollectorError)
        self.assertIn("delete denied", str(raised.exception.__cause__))
        self.assertTrue(guard.active)
        self.assertEqual(calls, [("delete", "table", "inet", table_name)])

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

    def test_formal_child_preexec_resets_and_rearms_after_privilege_drop(self):
        calls = []
        with patch.object(
            collector_module.signal,
            "signal",
            side_effect=lambda number, disposition: calls.append(
                ("signal", number, disposition)
            ),
        ), patch.object(
            collector_module.signal,
            "pthread_sigmask",
            side_effect=lambda operation, mask: calls.append(
                ("mask", operation, mask)
            ),
        ), patch.object(
            collector_module,
            "_set_parent_death_signal",
            side_effect=lambda parent_pid: calls.append(("parent-death", parent_pid)),
        ), patch.object(
            collector_module,
            "_drop_formal_child_privileges",
            side_effect=lambda uid, gid: calls.append(("privileges", uid, gid)),
        ):
            collector_module._prepare_formal_child_process(7001, 65532, 65532)

        self.assertEqual(
            calls,
            [
                ("signal", signal.SIGTERM, signal.SIG_DFL),
                ("mask", signal.SIG_SETMASK, {signal.SIGTERM}),
                ("parent-death", 7001),
                ("privileges", 65532, 65532),
                ("parent-death", 7001),
            ],
        )

    def test_formal_child_preexec_fails_closed_when_exact_sigterm_mask_fails(self):
        with patch.object(
            collector_module.signal, "signal", return_value=signal.SIG_DFL
        ), patch.object(
            collector_module.signal,
            "pthread_sigmask",
            side_effect=OSError("private-mask-failure"),
        ), patch.object(
            collector_module, "_set_parent_death_signal"
        ) as parent_death, patch.object(
            collector_module, "_drop_formal_child_privileges"
        ) as privilege_drop, self.assertRaisesRegex(
            CollectorError, "signal mask"
        ) as raised:
            collector_module._prepare_formal_child_process(7001, 65532, 65532)

        parent_death.assert_not_called()
        privilege_drop.assert_not_called()
        self.assertNotIn("private", str(raised.exception))

    def test_formal_child_preexec_fails_closed_when_sigterm_reset_fails(self):
        with patch.object(
            collector_module.signal,
            "signal",
            side_effect=OSError("private-restore-failure"),
        ), patch.object(
            collector_module, "_set_parent_death_signal"
        ) as parent_death, patch.object(
            collector_module, "_drop_formal_child_privileges"
        ) as privilege_drop, self.assertRaisesRegex(
            CollectorError, "signal disposition"
        ) as raised:
            collector_module._prepare_formal_child_process(7001, 65532, 65532)

        parent_death.assert_not_called()
        privilege_drop.assert_not_called()
        self.assertNotIn("private", str(raised.exception))

    def test_formal_child_preexec_closes_both_parent_race_windows(self):
        with patch.object(
            collector_module.signal, "signal", return_value=signal.SIG_DFL
        ), patch.object(
            collector_module,
            "_set_parent_death_signal",
            side_effect=CollectorError("formal child parent identity changed"),
        ), patch.object(
            collector_module, "_drop_formal_child_privileges"
        ) as privilege_drop, self.assertRaisesRegex(
            CollectorError, "parent identity"
        ):
            collector_module._prepare_formal_child_process(7001, 65532, 65532)
        privilege_drop.assert_not_called()

        parent_checks = []

        def parent_check(parent_pid):
            parent_checks.append(parent_pid)
            if len(parent_checks) == 2:
                raise CollectorError("formal child parent identity changed")

        with patch.object(
            collector_module.signal, "signal", return_value=signal.SIG_DFL
        ), patch.object(
            collector_module,
            "_set_parent_death_signal",
            side_effect=parent_check,
        ), patch.object(
            collector_module, "_drop_formal_child_privileges"
        ) as privilege_drop, self.assertRaisesRegex(
            CollectorError, "parent identity"
        ):
            collector_module._prepare_formal_child_process(7001, 65532, 65532)
        privilege_drop.assert_called_once_with(65532, 65532)
        self.assertEqual(parent_checks, [7001, 7001])

    def test_formal_launch_uses_one_ordered_preexec_function(self):
        descriptors = iter(((10, 11), (12, 13)))
        popen_kwargs = {}

        class Process:
            pid = 4242
            args = (sys.executable, "-I", "-B", "unused.py")

            def poll(self):
                return None

        def popen(*_arguments, **kwargs):
            popen_kwargs.update(kwargs)
            return Process()

        identity = {
            "pid": 4242,
            "start_identity": "candidate:4242:99",
            "pgid": 4242,
            "sid": 4242,
        }
        with TemporaryDirectory() as tmp, patch.object(
            collector_module.os, "pipe", side_effect=lambda: next(descriptors)
        ), patch.object(
            collector_module.os, "close"
        ), patch.object(
            collector_module.os, "geteuid", return_value=0
        ), patch.object(
            collector_module.os, "getpid", return_value=7001
        ), patch.object(
            collector_module.subprocess, "Popen", side_effect=popen
        ), patch.object(
            collector_module.select, "select", return_value=([12], [], [])
        ), patch.object(
            collector_module.os, "read", return_value=b"R"
        ), patch.object(
            collector_module,
            "_read_process_group_identity",
            return_value=identity,
        ) as identity_reader, patch.object(
            collector_module,
            "_require_pidfd_support",
            return_value=None,
        ), patch.object(
            collector_module,
            "_pidfd_open",
            return_value=91,
        ), patch.object(
            collector_module,
            "_pidfd_close",
            return_value=None,
        ) as pidfd_close:
            child = collector_module._start_gated_candidate(
                command=Process.args,
                cwd=Path(tmp).resolve(),
                environment={},
                formal_uid=65532,
                formal_gid=65532,
            )
            child.close()

        preexec = popen_kwargs["preexec_fn"]
        self.assertIs(preexec.func, collector_module._prepare_formal_child_process)
        self.assertEqual(preexec.args, (7001, 65532, 65532))
        self.assertIs(popen_kwargs["start_new_session"], True)
        self.assertEqual(child._bound_start_identity, "candidate:4242:99")
        identity_reader.assert_called_once_with(4242, Process.args)
        pidfd_close.assert_called_once_with(91)

    def test_formal_startup_cleanup_uses_validated_process_group(self):
        descriptors = iter(((10, 11), (12, 13)))
        signals = []
        pidfds = iter((90, 91))
        closed_pidfds = []

        class Process:
            pid = 4242
            args = (sys.executable, "-I", "-B", "unused.py")

            def poll(self):
                return 0 if signals else None

            def wait(self, timeout=None):
                return -signal.SIGTERM

            def terminate(self):
                raise AssertionError("unvalidated PID termination is forbidden")

            def kill(self):
                raise AssertionError("unvalidated PID kill is forbidden")

        identity = {
            "pid": 4242,
            "start_identity": "candidate:4242:99",
            "pgid": 4242,
            "sid": 4242,
        }
        def inventory(*_arguments, **_kwargs):
            return {} if signals else {4242: "99"}

        with TemporaryDirectory() as tmp, patch.object(
            collector_module.os, "pipe", side_effect=lambda: next(descriptors)
        ), patch.object(
            collector_module.os, "close"
        ), patch.object(
            collector_module.os, "geteuid", return_value=0
        ), patch.object(
            collector_module.subprocess, "Popen", return_value=Process()
        ), patch.object(
            collector_module.select, "select", return_value=([], [], [])
        ), patch.object(
            collector_module,
            "_read_process_group_identity",
            return_value=identity,
        ), patch.object(
            collector_module,
            "_process_group_members",
            side_effect=inventory,
        ), patch.object(
            collector_module,
            "_formal_uid_processes",
            side_effect=inventory,
        ), patch.object(
            collector_module.os,
            "killpg",
        ) as killpg, patch.object(
            collector_module,
            "_require_pidfd_support",
            return_value=None,
        ), patch.object(
            collector_module,
            "_pidfd_open",
            side_effect=lambda _pid: next(pidfds),
        ), patch.object(
            collector_module,
            "_pidfd_send_signal",
            side_effect=lambda descriptor, sig: signals.append((descriptor, sig)),
        ), patch.object(
            collector_module,
            "_pidfd_close",
            side_effect=lambda descriptor: closed_pidfds.append(descriptor),
        ):
            with self.assertRaisesRegex(CollectorError, "readiness"):
                collector_module._start_gated_candidate(
                    command=Process.args,
                    cwd=Path(tmp).resolve(),
                    environment={},
                    formal_uid=65532,
                    formal_gid=65532,
                )

        self.assertEqual(signals, [(91, signal.SIGTERM)])
        self.assertEqual(closed_pidfds, [91, 90])
        killpg.assert_not_called()

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
            modes_before = (
                candidate.stat().st_mode & 0o777,
                first.stat().st_mode & 0o777,
            )

            with self.assertRaisesRegex(
                CollectorError, "completion receipt"
            ):
                collector_module._seal_candidate_tree(
                    candidate,
                    expected_owner_uid=os.getuid(),
                    sealed_owner_uid=os.getuid(),
                    sealed_owner_gid=os.getgid(),
                    completion_receipt={},
                )
            self.assertEqual(
                (
                    candidate.stat().st_mode & 0o777,
                    first.stat().st_mode & 0o777,
                ),
                modes_before,
            )

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
            ), patch.object(runner_module.os, "close"), patch.object(
                runner_module, "_harden_execd_formal_runner", return_value=None
            ):
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
            ), patch.object(runner_module.os, "close"), patch.object(
                runner_module, "_harden_execd_formal_runner", return_value=None
            ):
                self.assertEqual(runner_module.main(["invalid-command"]), 72)
                self.assertNotIn(
                    "TXNMEM_PROVENANCE_RUNTIME_SITE", os.environ
                )

    def test_immutable_runner_emits_one_canonical_safe_progress_line(self):
        import txnmem_experiment
        import txnmem_provenance_runner as runner_module
        from txnmem_provenance_progress import decode_progress_line

        gate_read, gate_write = os.pipe()
        ready_read, ready_write = os.pipe()
        completion_read, completion_write = os.pipe()
        progress_read, progress_write = os.pipe()
        os.write(gate_write, b"G")
        os.close(gate_write)
        binding = "a" * 64
        config_hash = "b" * 64
        observed_hooks = []

        def experiment_main(_arguments, **hooks):
            observed_hooks.append(hooks)
            hooks["_progress_callback"](
                {
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
            )
            return 0

        with TemporaryDirectory() as tmp, patch.dict(
            os.environ,
            {
                "TXNMEM_PROVENANCE_START_GATE_FD": str(gate_read),
                "TXNMEM_PROVENANCE_READY_FD": str(ready_write),
                "TXNMEM_PROVENANCE_COMPLETION_FD": str(completion_write),
                "TXNMEM_PROVENANCE_PROGRESS_FD": str(progress_write),
                "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256": binding,
                "TXNMEM_PROVENANCE_RUNTIME_SITE": str(Path(tmp).resolve()),
            },
            clear=True,
        ), patch.object(txnmem_experiment, "main", side_effect=experiment_main), patch(
            "txnmem_provenance_performance.formal_matrix_config_sha256",
            return_value=config_hash,
        ), patch(
            "txnmem_provenance_performance.candidate_attestation_material",
            return_value={"result": "sealed"},
        ), patch.object(
            runner_module, "_harden_execd_formal_runner", return_value=None
        ), patch.object(
            runner_module,
            "_require_credential_matched_publication_preflight",
            return_value=None,
        ):
            result = runner_module.main(
                [
                    "provenance-performance",
                    "--backend",
                    "vector-graph",
                    "--config",
                    "/immutable/config.json",
                    "--run-id",
                    "runner-progress-fixture",
                    "--out-dir",
                    "/candidate",
                    "--service-url",
                    "http://127.0.0.1:19000",
                ]
            )

        self.assertEqual(result, 0)
        self.assertEqual(os.read(ready_read, 2), b"R")
        event = decode_progress_line(os.read(progress_read, 4096))
        self.assertEqual(event["run_binding_sha256"], binding)
        self.assertEqual(event["config_sha256"], config_hash)
        self.assertEqual(event["completed_repetitions"], 1)
        self.assertEqual(
            set(event),
            {
                "schema", "run_binding_sha256", "config_sha256", "phase",
                "cell_index", "cell_count", "graph_size", "concurrency",
                "repetition_index", "repetition_count", "completed_repetitions",
                "total_repetitions", "completed_samples", "total_samples",
                "update_sequence", "status",
            },
        )
        self.assertEqual(observed_hooks[0]["_require_formal_eligibility"], True)
        self.assertTrue(os.read(completion_read, 65536))
        for descriptor in (ready_read, completion_read, progress_read):
            os.close(descriptor)

    def test_immutable_runner_progress_channel_fails_closed_before_receipt(self):
        import txnmem_experiment
        import txnmem_provenance_runner as runner_module

        def experiment_main(_arguments, **hooks):
            hooks["_progress_callback"](
                {
                    "cell_index": 1, "cell_count": 15, "graph_size": 100,
                    "concurrency": 1, "repetition_index": 1,
                    "repetition_count": 30, "completed_repetitions": 1,
                    "total_repetitions": 450, "completed_samples": 32,
                    "total_samples": 14400, "update_sequence": 1,
                }
            )
            return 0

        for failure in ("epipe", "short"):
            with self.subTest(failure=failure), TemporaryDirectory() as tmp:
                gate_read, gate_write = os.pipe()
                ready_read, ready_write = os.pipe()
                completion_read, completion_write = os.pipe()
                progress_read, progress_write = os.pipe()
                os.write(gate_write, b"G")
                os.close(gate_write)
                if failure == "epipe":
                    os.close(progress_read)
                real_write = os.write

                def write(descriptor, payload):
                    if failure == "short" and descriptor == progress_write:
                        return 0
                    return real_write(descriptor, payload)

                with patch.dict(
                    os.environ,
                    {
                        "TXNMEM_PROVENANCE_START_GATE_FD": str(gate_read),
                        "TXNMEM_PROVENANCE_READY_FD": str(ready_write),
                        "TXNMEM_PROVENANCE_COMPLETION_FD": str(completion_write),
                        "TXNMEM_PROVENANCE_PROGRESS_FD": str(progress_write),
                        "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256": "a" * 64,
                        "TXNMEM_PROVENANCE_RUNTIME_SITE": str(Path(tmp).resolve()),
                    },
                    clear=True,
                ), patch.object(txnmem_experiment, "main", side_effect=experiment_main), patch.object(
                    runner_module.os, "write", side_effect=write
                ), patch(
                    "txnmem_provenance_performance.formal_matrix_config_sha256",
                    return_value="b" * 64,
                ), patch.object(
                    runner_module, "_harden_execd_formal_runner", return_value=None
                ), patch.object(
                    runner_module,
                    "_require_credential_matched_publication_preflight",
                    return_value=None,
                ):
                    result = runner_module.main(
                        [
                            "provenance-performance", "--backend", "vector-graph",
                            "--config", "/immutable/config.json", "--run-id", "x",
                            "--out-dir", "/candidate", "--service-url",
                            "http://127.0.0.1:19000",
                        ]
                    )
                self.assertNotEqual(result, 0)
                self.assertEqual(os.read(completion_read, 1), b"")
                os.close(ready_read)
                os.close(completion_read)
                if failure != "epipe":
                    os.close(progress_read)

    def test_immutable_runner_rejects_missing_equal_or_malformed_progress_values(self):
        import txnmem_provenance_runner as runner_module

        cases = (
            ({}, "missing"),
            ({"TXNMEM_PROVENANCE_PROGRESS_FD": "13"}, "missing_binding"),
            ({
                "TXNMEM_PROVENANCE_PROGRESS_FD": "10",
                "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256": "a" * 64,
            }, "equal_fd"),
            ({
                "TXNMEM_PROVENANCE_PROGRESS_FD": "010",
                "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256": "a" * 64,
            }, "numeric_equal_fd"),
            ({
                "TXNMEM_PROVENANCE_PROGRESS_FD": "13",
                "TXNMEM_PROVENANCE_PROGRESS_BINDING_SHA256": "A" * 64,
            }, "malformed_binding"),
        )
        with TemporaryDirectory() as tmp:
            for extra, name in cases:
                with self.subTest(name=name), patch.dict(
                    os.environ,
                    {
                        "TXNMEM_PROVENANCE_START_GATE_FD": "10",
                        "TXNMEM_PROVENANCE_READY_FD": "11",
                        "TXNMEM_PROVENANCE_COMPLETION_FD": "12",
                        "TXNMEM_PROVENANCE_RUNTIME_SITE": str(Path(tmp).resolve()),
                        **extra,
                    },
                    clear=True,
                ), patch.object(runner_module.os, "close"):
                    self.assertEqual(
                        runner_module.main(["provenance-performance"]), 70
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
            config.write_bytes(
                (
                    Path(__file__).resolve().parents[1]
                    / "configs"
                    / "provenance_performance_matrix.json"
                ).read_bytes()
            )
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

    def test_validated_group_termination_stops_after_successful_term(self):
        events = []

        class Process:
            pid = 4242
            args = ("python", "runner.py")

            def poll(self):
                return None

            def wait(self, timeout=None):
                events.append(("wait", timeout))
                return -signal.SIGTERM

        identity = {
            "pid": 4242,
            "start_identity": "candidate:4242:99",
            "pgid": 4242,
            "sid": 4242,
        }
        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
        )
        child.bind_process_identity("candidate:4242:99")
        with patch.object(
            collector_module,
            "_read_process_group_identity",
            return_value=identity,
        ) as identity_reader, patch.object(
            collector_module.os,
            "killpg",
            side_effect=lambda pgid, sig: events.append(("signal", pgid, sig)),
        ):
            child.terminate_validated_group()

        self.assertEqual(
            events,
            [("signal", 4242, signal.SIGTERM), ("wait", 5.0)],
        )
        identity_reader.assert_called_once_with(4242, Process.args)

    def test_required_signal_rejects_an_already_exited_formal_child(self):
        class Process:
            pid = 4242
            args = ("python", "runner.py")

            def poll(self):
                return 0

        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
            _formal_uid=65532,
        )
        child.bind_process_identity("candidate:4242:99")
        with patch.object(
            child, "_formal_group_members", return_value={}
        ), patch.object(
            child, "_require_formal_quiescence"
        ) as quiescence, self.assertRaisesRegex(
            CollectorError, "signal delivery"
        ):
            child.terminate_validated_group(require_signal=True)

        quiescence.assert_called_once_with()

    def test_required_signal_returns_true_only_after_pidfd_signal_succeeds(self):
        class Process:
            pid = 4242
            args = ("python", "runner.py")

            def poll(self):
                return None

        inventory = {4242: "99"}
        sent = []
        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
            _formal_uid=65532,
        )
        child.bind_process_identity("candidate:4242:99")
        with patch.object(
            child, "_validate_bound_group"
        ), patch.object(
            child, "_formal_group_members", side_effect=[inventory, inventory]
        ), patch.object(
            child, "_wait_for_formal_quiescence", return_value=True
        ), patch.object(
            collector_module, "_pidfd_open", return_value=51
        ), patch.object(
            collector_module,
            "_pidfd_send_signal",
            side_effect=lambda descriptor, sig: sent.append((descriptor, sig)),
        ), patch.object(
            collector_module, "_pidfd_close", return_value=None
        ):
            delivered = child.terminate_validated_group(
                term_seconds=0.01,
                kill_seconds=0.01,
                require_signal=True,
            )

        self.assertIs(delivered, True)
        self.assertEqual(sent, [(51, signal.SIGTERM)])

    def test_formal_pidfds_open_all_then_revalidate_start_identity_before_signal(self):
        class Process:
            pid = 4242
            args = ("python", "runner.py")

            def poll(self):
                return None

        initial = {4242: "99", 4243: "100"}
        drifted = {4242: "99", 4243: "101"}
        opened = []
        sent = []
        closed = []
        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
            _formal_uid=65532,
        )
        child.bind_process_identity("candidate:4242:99")

        with patch.object(
            child, "_validate_bound_group"
        ), patch.object(
            child,
            "_formal_group_members",
            side_effect=[initial, drifted],
        ), patch.object(
            collector_module,
            "_pidfd_open",
            side_effect=lambda pid: opened.append(pid) or (pid + 1000),
            create=True,
        ), patch.object(
            collector_module,
            "_pidfd_send_signal",
            side_effect=lambda descriptor, sig: sent.append((descriptor, sig)),
            create=True,
        ), patch.object(
            collector_module,
            "_pidfd_close",
            side_effect=lambda descriptor: closed.append(descriptor),
            create=True,
        ), patch.object(
            collector_module.os, "killpg"
        ) as killpg, self.assertRaisesRegex(CollectorError, "identity"):
            child.terminate_validated_group(
                term_seconds=0.01, kill_seconds=0.01
            )

        self.assertEqual(opened, [4242, 4243])
        self.assertEqual(sent, [])
        self.assertEqual(closed, [5242, 5243])
        killpg.assert_not_called()

    def test_formal_pidfd_partial_open_failure_closes_once_without_signal(self):
        class Process:
            pid = 4242
            args = ("python", "runner.py")

            def poll(self):
                return None

        inventory = {4242: "99", 4243: "100"}
        opened = []
        sent = []
        closed = []
        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
            _formal_uid=65532,
        )
        child.bind_process_identity("candidate:4242:99")

        def open_pidfd(pid):
            opened.append(pid)
            if pid == 4243:
                raise OSError("partial-pidfd-open")
            return 51

        with patch.object(
            child, "_validate_bound_group"
        ), patch.object(
            child, "_formal_group_members", return_value=inventory
        ), patch.object(
            child, "_wait_for_formal_quiescence", side_effect=[False, True]
        ), patch.object(
            collector_module,
            "_pidfd_open",
            side_effect=open_pidfd,
            create=True,
        ), patch.object(
            collector_module,
            "_pidfd_send_signal",
            side_effect=lambda descriptor, sig: sent.append((descriptor, sig)),
            create=True,
        ), patch.object(
            collector_module,
            "_pidfd_close",
            side_effect=lambda descriptor: closed.append(descriptor),
            create=True,
        ), patch.object(
            collector_module.os, "killpg"
        ) as killpg, self.assertRaisesRegex(CollectorError, "pidfd"):
            child.terminate_validated_group(
                term_seconds=0.01, kill_seconds=0.01
            )

        self.assertEqual(opened, [4242, 4243])
        self.assertEqual(sent, [])
        self.assertEqual(closed, [51])
        killpg.assert_not_called()

    def test_formal_pidfd_esrch_reinventories_and_kills_new_survivor(self):
        class Process:
            pid = 4242
            args = ("python", "runner.py")

            def poll(self):
                return None

        term_inventory = {4242: "99", 4243: "100"}
        kill_inventory = {4244: "101"}
        inventories = iter(
            (term_inventory, term_inventory, kill_inventory, kill_inventory)
        )
        descriptor_by_pid = {4242: 51, 4243: 52, 4244: 53}
        opened = []
        sent = []
        closed = []
        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
            _formal_uid=65532,
        )
        child.bind_process_identity("candidate:4242:99")

        def send_pidfd(descriptor, sig):
            sent.append((descriptor, sig))
            if descriptor == 52 and sig == signal.SIGTERM:
                raise OSError(errno.ESRCH, "exited-before-signal")

        with patch.object(
            child, "_validate_bound_group"
        ), patch.object(
            child,
            "_formal_group_members",
            side_effect=lambda: next(inventories),
        ), patch.object(
            child, "_wait_for_formal_quiescence", side_effect=[False, True]
        ), patch.object(
            collector_module,
            "_pidfd_open",
            side_effect=lambda pid: opened.append(pid) or descriptor_by_pid[pid],
            create=True,
        ), patch.object(
            collector_module,
            "_pidfd_send_signal",
            side_effect=send_pidfd,
            create=True,
        ), patch.object(
            collector_module,
            "_pidfd_close",
            side_effect=lambda descriptor: closed.append(descriptor),
            create=True,
        ), patch.object(
            collector_module.os, "killpg"
        ) as killpg:
            child.terminate_validated_group(
                term_seconds=0.01, kill_seconds=0.1
            )

        self.assertEqual(opened, [4242, 4243, 4244])
        self.assertEqual(
            sent,
            [
                (51, signal.SIGTERM),
                (52, signal.SIGTERM),
                (53, signal.SIGKILL),
            ],
        )
        self.assertEqual(closed, [51, 52, 53])
        killpg.assert_not_called()

    def test_formal_pidfd_close_failure_never_broadens_target(self):
        class Process:
            pid = 4242
            args = ("python", "runner.py")

            def poll(self):
                return None

        inventory = {4242: "99", 4243: "100"}
        sent = []
        closed = []
        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
            _formal_uid=65532,
        )
        child.bind_process_identity("candidate:4242:99")

        def close_pidfd(descriptor):
            closed.append(descriptor)
            if descriptor == 51:
                raise OSError("pidfd-close-failure")

        with patch.object(
            child, "_validate_bound_group"
        ), patch.object(
            child, "_formal_group_members", side_effect=[inventory, inventory]
        ), patch.object(
            child, "_wait_for_formal_quiescence", return_value=True
        ), patch.object(
            collector_module,
            "_pidfd_open",
            side_effect=lambda pid: {4242: 51, 4243: 52}[pid],
            create=True,
        ), patch.object(
            collector_module,
            "_pidfd_send_signal",
            side_effect=lambda descriptor, sig: sent.append((descriptor, sig)),
            create=True,
        ), patch.object(
            collector_module,
            "_pidfd_close",
            side_effect=close_pidfd,
            create=True,
        ), patch.object(
            collector_module.os, "killpg"
        ) as killpg, self.assertRaisesRegex(CollectorError, "pidfd"):
            child.terminate_validated_group(
                term_seconds=0.01, kill_seconds=0.01
            )

        self.assertEqual(
            sent,
            [(51, signal.SIGTERM), (52, signal.SIGTERM)],
        )
        self.assertEqual(closed, [51, 52])
        killpg.assert_not_called()

    def test_validated_group_revalidates_before_bounded_kill(self):
        events = []

        class Process:
            pid = 4242
            args = ("python", "runner.py")

            def __init__(self):
                self.wait_count = 0

            def poll(self):
                return None

            def wait(self, timeout=None):
                self.wait_count += 1
                events.append(("wait", timeout))
                if self.wait_count == 1:
                    raise subprocess.TimeoutExpired(self.args, timeout)
                return -signal.SIGKILL

        identity = {
            "pid": 4242,
            "start_identity": "candidate:4242:99",
            "pgid": 4242,
            "sid": 4242,
        }
        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
        )
        child.bind_process_identity("candidate:4242:99")
        with patch.object(
            collector_module,
            "_read_process_group_identity",
            side_effect=[dict(identity), dict(identity)],
        ) as identity_reader, patch.object(
            collector_module.os,
            "killpg",
            side_effect=lambda pgid, sig: events.append(("signal", pgid, sig)),
        ):
            child.terminate_validated_group(term_seconds=5.0, kill_seconds=5.0)

        self.assertEqual(
            events,
            [
                ("signal", 4242, signal.SIGTERM),
                ("wait", 5.0),
                ("signal", 4242, signal.SIGKILL),
                ("wait", 5.0),
            ],
        )
        self.assertEqual(identity_reader.call_count, 2)

    def test_validated_group_kills_survivors_after_leader_exits(self):
        signals = []
        opened = []
        closed = []

        class Process:
            pid = 4242
            args = ("python", "runner.py")

            def poll(self):
                return 0 if signals else None

            def wait(self, timeout=None):
                return 0

        identity = {
            "pid": 4242,
            "start_identity": "candidate:4242:99",
            "pgid": 4242,
            "sid": 4242,
        }
        initial = {4242: "99", 4243: "100"}
        survivor = {4243: "100"}

        def inventory(*_arguments, **_kwargs):
            if any(sig == signal.SIGKILL for _, sig in signals):
                return {}
            if any(sig == signal.SIGTERM for _, sig in signals):
                return survivor
            return initial

        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
        )
        child._formal_uid = 65532
        child.bind_process_identity("candidate:4242:99")
        with patch.object(
            collector_module,
            "_read_process_group_identity",
            return_value=identity,
        ), patch.object(
            collector_module,
            "_process_group_members",
            side_effect=inventory,
            create=True,
        ), patch.object(
            collector_module,
            "_formal_uid_processes",
            side_effect=inventory,
        ), patch.object(
            collector_module,
            "_pidfd_open",
            side_effect=lambda pid: opened.append(pid) or (50 + len(opened)),
        ), patch.object(
            collector_module,
            "_pidfd_send_signal",
            side_effect=lambda descriptor, sig: signals.append((descriptor, sig)),
        ), patch.object(
            collector_module,
            "_pidfd_close",
            side_effect=lambda descriptor: closed.append(descriptor),
        ), patch.object(
            collector_module.os, "killpg"
        ) as killpg:
            child.terminate_validated_group(
                term_seconds=0.01, kill_seconds=0.1
            )

        self.assertEqual(
            signals,
            [
                (51, signal.SIGTERM),
                (52, signal.SIGTERM),
                (53, signal.SIGKILL),
            ],
        )
        self.assertEqual(opened, [4242, 4243, 4243])
        self.assertEqual(closed, [51, 52, 53])
        killpg.assert_not_called()

    def test_exited_leader_with_uid_residue_is_not_quiescent(self):
        class Process:
            pid = 4242
            args = ("python", "runner.py")

            def poll(self):
                return 0

            def wait(self, timeout=None):
                return 0

        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
        )
        child._formal_uid = 65532
        child.bind_process_identity("candidate:4242:99")
        with patch.object(
            collector_module,
            "_process_group_members",
            return_value={},
            create=True,
        ), patch.object(
            collector_module,
            "_formal_uid_processes",
            return_value={4243: "100"},
        ), patch.object(collector_module.os, "killpg") as killpg:
            with self.assertRaisesRegex(CollectorError, "quiescence"):
                child.terminate_validated_group(
                    term_seconds=0.01, kill_seconds=0.01
                )
        killpg.assert_not_called()

    def test_post_term_group_uid_mismatch_forbids_kill(self):
        signals = []
        closed = []

        class Process:
            pid = 4242
            args = ("python", "runner.py")

            def poll(self):
                return None

            def wait(self, timeout=None):
                raise subprocess.TimeoutExpired(self.args, timeout)

        identity = {
            "pid": 4242,
            "start_identity": "candidate:4242:99",
            "pgid": 4242,
            "sid": 4242,
        }
        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
        )
        child._formal_uid = 65532
        child.bind_process_identity("candidate:4242:99")
        with patch.object(
            collector_module,
            "_read_process_group_identity",
            return_value=identity,
        ), patch.object(
            collector_module,
            "_process_group_members",
            side_effect=[
                {4242: "99"},
                {4242: "99"},
                {4243: "100"},
            ],
            create=True,
        ), patch.object(
            collector_module,
            "_formal_uid_processes",
            side_effect=[
                {4242: "99"},
                {4242: "99"},
                {4244: "101"},
            ],
        ), patch.object(
            collector_module,
            "_pidfd_open",
            return_value=51,
        ), patch.object(
            collector_module,
            "_pidfd_send_signal",
            side_effect=lambda descriptor, sig: signals.append((descriptor, sig)),
        ), patch.object(
            collector_module,
            "_pidfd_close",
            side_effect=lambda descriptor: closed.append(descriptor),
        ), patch.object(
            collector_module.os, "killpg"
        ) as killpg:
            with self.assertRaisesRegex(CollectorError, "identity|quiescence"):
                child.terminate_validated_group(
                    term_seconds=0.01, kill_seconds=0.01
                )

        self.assertEqual(signals, [(51, signal.SIGTERM)])
        self.assertEqual(closed, [51])
        killpg.assert_not_called()

    def test_pre_term_group_uid_mismatch_sends_no_signal(self):
        signals = []

        class Process:
            pid = 4242
            args = ("python", "runner.py")

            def poll(self):
                return None

        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
        )
        child._formal_uid = 65532
        child.bind_process_identity("candidate:4242:99")
        with patch.object(
            collector_module,
            "_read_process_group_identity",
            return_value={
                "pid": 4242,
                "start_identity": "candidate:4242:99",
                "pgid": 4242,
                "sid": 4242,
            },
        ), patch.object(
            collector_module,
            "_process_group_members",
            return_value={4242: "99"},
        ), patch.object(
            collector_module,
            "_formal_uid_processes",
            return_value={4243: "100"},
        ), patch.object(
            collector_module.os,
            "killpg",
            side_effect=lambda pgid, sig: signals.append((pgid, sig)),
        ), self.assertRaisesRegex(CollectorError, "identity"):
            child.terminate_validated_group(term_seconds=0.01, kill_seconds=0.01)

        self.assertEqual(signals, [])

    def test_survivor_inventory_drift_sends_no_kill(self):
        cases = (
            ({4243: "100"}, {4244: "101"}),
            ({4243: "100"}, {4243: "101"}),
        )
        for first, second in cases:
            with self.subTest(first=first, second=second):
                signals = []
                closed = []
                next_pidfd = iter((51, 52))

                class Process:
                    pid = 4242
                    args = ("python", "runner.py")

                    def poll(self):
                        return None

                child = collector_module._GatedCandidate(
                    process=Process(),
                    _release_fd=None,
                    _receipt_fd=None,
                    ready_observed=True,
                )
                child._formal_uid = 65532
                child.bind_process_identity("candidate:4242:99")
                term_inventory = {4242: "99"}
                inventories = iter(
                    (term_inventory, term_inventory, first, second)
                )
                with patch.object(
                    child,
                    "_validate_bound_group",
                ), patch.object(
                    child,
                    "_formal_group_members",
                    side_effect=lambda: next(inventories),
                ), patch.object(
                    child,
                    "_wait_for_formal_quiescence",
                    side_effect=[False, True],
                ), patch.object(
                    collector_module,
                    "_pidfd_open",
                    side_effect=lambda _pid: next(next_pidfd),
                ), patch.object(
                    collector_module,
                    "_pidfd_send_signal",
                    side_effect=lambda descriptor, sig: signals.append(
                        (descriptor, sig)
                    ),
                ), patch.object(
                    collector_module,
                    "_pidfd_close",
                    side_effect=lambda descriptor: closed.append(descriptor),
                ), patch.object(
                    collector_module.os,
                    "killpg",
                ), self.assertRaises(CollectorError):
                    child.terminate_validated_group(
                        term_seconds=0.000001, kill_seconds=0.01
                    )

                self.assertEqual(signals, [(51, signal.SIGTERM)])
                self.assertEqual(closed, [51, 52])

    def test_receipt_success_requires_group_and_uid_quiescence(self):
        receipt_read, receipt_write = os.pipe()
        os.write(receipt_write, b"{}")
        os.close(receipt_write)

        class Process:
            pid = 4242
            args = ("python", "runner.py")

            def wait(self, timeout=None):
                return 0

            def poll(self):
                return 0

        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=receipt_read,
            ready_observed=True,
        )
        child._formal_uid = 65532
        child.bind_process_identity("candidate:4242:99")
        with patch.object(
            collector_module,
            "_process_group_members",
            return_value={},
            create=True,
        ), patch.object(
            collector_module,
            "_formal_uid_processes",
            return_value={4243: "100"},
        ), self.assertRaisesRegex(CollectorError, "quiescence"):
            child.wait_with_receipt(timeout=0.1)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux /proc only")
    def test_validated_group_kills_term_ignoring_descendant_fixture(self):
        group_reader = getattr(collector_module, "_process_group_members", None)
        self.assertIsNotNone(group_reader)
        if group_reader is None:
            return
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            script = root / "group_fixture.py"
            script.write_text(
                "\n".join(
                    (
                        "import signal, subprocess, sys, time",
                        "signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))",
                        "subprocess.Popen([sys.executable, '-c', "
                        "'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(60)'])",
                        "print('R', flush=True)",
                        "time.sleep(60)",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            process = subprocess.Popen(
                (sys.executable, "-I", "-B", str(script)),
                cwd=root,
                stdout=subprocess.PIPE,
                start_new_session=True,
            )
            self.addCleanup(
                lambda: process.poll() is None
                and os.killpg(process.pid, signal.SIGKILL)
            )
            self.assertEqual(process.stdout.readline(), b"R\n")
            observed = collector_module._read_process_group_identity(
                process.pid, process.args
            )
            child = collector_module._GatedCandidate(
                process=process,
                _release_fd=None,
                _receipt_fd=None,
                ready_observed=True,
            )
            child._formal_uid = os.getuid()
            child.bind_process_identity(observed["start_identity"])

            with patch.object(
                collector_module,
                "_formal_uid_processes",
                side_effect=lambda _uid: group_reader(
                    process.pid, process.pid
                ),
            ):
                child.terminate_validated_group(
                    term_seconds=0.1, kill_seconds=1.0
                )

            self.assertEqual(
                group_reader(process.pid, process.pid), {}
            )

    @unittest.skipUnless(
        sys.platform.startswith("linux")
        and hasattr(os, "geteuid")
        and os.geteuid() == 0,
        "protected Linux root post-Popen pidfd handoff only",
    )
    def test_protected_linux_post_popen_pidfd_failure_fail_stops_and_clears_uid(self):
        collector_module._require_pidfd_support()
        collector_module._require_formal_uid_processes(
            collector_module.FORMAL_RUNNER_UID,
            expected={},
        )

        def exact_pidfd_kill(pid):
            try:
                descriptor = collector_module._pidfd_open(pid)
            except ProcessLookupError:
                return
            try:
                collector_module._pidfd_send_signal(descriptor, signal.SIGKILL)
            except ProcessLookupError:
                pass
            finally:
                collector_module._pidfd_close(descriptor)

        repository = Path(__file__).resolve().parents[1]
        isolated_environment = {
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONPATH": str(repository / "src"),
        }
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            root.chmod(0o755)
            hung_child = root / "hung_before_gate.py"
            hung_child.write_text(
                "import ctypes\nctypes.PyDLL(None).sleep(60)\n",
                encoding="utf-8",
            )
            hung_child.chmod(0o555)
            parent_fixture = root / "leader_pidfd_failure_parent.py"
            parent_fixture.write_text(
                "\n".join(
                    (
                        "import os, sys",
                        "import txnmem_provenance_execution_collector as collector",
                        f"root = {str(root)!r}",
                        f"hung_child = {str(hung_child)!r}",
                        "def fail_leader_pidfd(pid):",
                        "    print(pid, flush=True)",
                        "    raise OSError('injected leader pidfd acquisition failure')",
                        "collector._pidfd_open = fail_leader_pidfd",
                        "collector._start_gated_candidate(",
                        "    command=(sys.executable, '-I', '-B', hung_child),",
                        "    cwd=collector.Path(root),",
                        "    environment={},",
                        "    formal_uid=collector.FORMAL_RUNNER_UID,",
                        "    formal_gid=collector.FORMAL_RUNNER_GID,",
                        ")",
                        "raise SystemExit(99)",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            parent_fixture.chmod(0o555)

            libc = collector_module.ctypes.CDLL(None, use_errno=True)
            prctl = libc.prctl
            prctl.argtypes = [
                collector_module.ctypes.c_int,
                collector_module.ctypes.c_ulong,
                collector_module.ctypes.c_ulong,
                collector_module.ctypes.c_ulong,
                collector_module.ctypes.c_ulong,
            ]
            prctl.restype = collector_module.ctypes.c_int
            prior_subreaper = collector_module.ctypes.c_int(0)
            self.assertEqual(
                prctl(37, collector_module.ctypes.addressof(prior_subreaper), 0, 0, 0),
                0,
            )
            self.assertEqual(prctl(36, 1, 0, 0, 0), 0)
            parent = None
            child_pid = None
            child_waited = False
            try:
                parent = subprocess.Popen(
                    (sys.executable, "-B", str(parent_fixture)),
                    cwd=root,
                    env=isolated_environment,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                raw_pid = parent.stdout.readline()
                if not raw_pid:
                    parent.wait(timeout=20.0)
                    self.fail(parent.stderr.read().decode("utf-8", errors="replace"))
                child_pid = int(raw_pid)
                self.assertEqual(parent.wait(timeout=20.0), os.EX_SOFTWARE)
                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline:
                    waited_pid, status = os.waitpid(child_pid, os.WNOHANG)
                    if waited_pid == child_pid:
                        child_waited = True
                        self.assertTrue(os.WIFSIGNALED(status))
                        self.assertEqual(os.WTERMSIG(status), signal.SIGKILL)
                        break
                    time.sleep(0.01)
                self.assertTrue(child_waited)
                collector_module._require_formal_uid_processes(
                    collector_module.FORMAL_RUNNER_UID,
                    expected={},
                )
            finally:
                if parent is not None and parent.poll() is None:
                    exact_pidfd_kill(parent.pid)
                    parent.wait(timeout=5.0)
                if child_pid is not None and not child_waited:
                    exact_pidfd_kill(child_pid)
                    deadline = time.monotonic() + 5.0
                    while time.monotonic() < deadline:
                        try:
                            waited_pid, _status = os.waitpid(child_pid, os.WNOHANG)
                        except ChildProcessError:
                            break
                        if waited_pid == child_pid:
                            break
                        time.sleep(0.01)
                self.assertEqual(
                    prctl(36, int(prior_subreaper.value), 0, 0, 0),
                    0,
                )

    def test_protected_runner_fixture_requires_complete_isolated_import_closure(self):
        repository = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as tmp:
            locked_source = Path(tmp).resolve() / "locked-source"
            runner_copy = _copy_locked_runner_import_closure(locked_source)
            contract_copy = locked_source / "txnmem_provenance_contract.py"
            self.assertTrue(
                runner_copy.read_bytes()
                == (repository / "src" / runner_copy.name).read_bytes(),
                "protected runner fixture runner bytes differ",
            )
            self.assertTrue(
                contract_copy.read_bytes()
                == (repository / "src" / contract_copy.name).read_bytes(),
                "protected runner fixture contract bytes differ",
            )
            self.assertEqual(locked_source.stat().st_mode & 0o222, 0)
            self.assertEqual(runner_copy.stat().st_mode & 0o222, 0)
            self.assertEqual(contract_copy.stat().st_mode & 0o222, 0)
            self.assertTrue(
                _isolated_contract_resolves_inside_locked_source(locked_source),
                "isolated contract provenance check failed",
            )

            runner_only = Path(tmp).resolve() / "runner-only"
            runner_only.mkdir(mode=0o755)
            runner_only_copy = runner_only / "txnmem_provenance_runner.py"
            runner_only_copy.write_bytes(
                (repository / "src" / runner_only_copy.name).read_bytes()
            )
            runner_only_copy.chmod(0o555)
            runner_only.chmod(0o555)
            self.assertFalse(
                _isolated_contract_resolves_inside_locked_source(runner_only),
                "checkout or installed contract unexpectedly satisfied isolation",
            )

    @unittest.skipUnless(
        sys.platform.startswith("linux")
        and hasattr(os, "geteuid")
        and os.geteuid() == 0,
        "protected Linux root only",
    )
    def test_protected_linux_preexec_mask_and_dedicated_uid_are_exact(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            root.chmod(0o755)
            runtime = root / "runtime"
            runtime.mkdir(mode=0o755)
            locked_source = root / "locked-source"
            runner_copy = _copy_locked_runner_import_closure(locked_source)
            self.assertTrue(
                _isolated_contract_resolves_inside_locked_source(locked_source),
                "isolated contract provenance check failed",
            )
            child = collector_module._start_gated_candidate(
                command=(
                    sys.executable,
                    "-I",
                    "-S",
                    "-B",
                    str(runner_copy),
                    "invalid-command",
                ),
                cwd=root,
                environment={
                    "TXNMEM_PROVENANCE_RUNTIME_SITE": str(runtime),
                },
                formal_uid=collector_module.FORMAL_RUNNER_UID,
                formal_gid=collector_module.FORMAL_RUNNER_GID,
                require_completion_receipt=True,
            )
            try:
                child.release()
                exit_code, receipt = child.wait_with_receipt(timeout=5.0)
                self.assertEqual(exit_code, 72)
                self.assertEqual(receipt, {})
                child.require_quiescence()
            finally:
                if child.process.poll() is None:
                    child.terminate_validated_group(
                        term_seconds=0.5, kill_seconds=2.0
                    )
                child.close()

    @unittest.skipUnless(
        sys.platform.startswith("linux")
        and hasattr(os, "geteuid")
        and os.geteuid() == 0,
        "protected Linux same-UID non-dumpable publisher only",
    )
    def test_protected_linux_same_uid_peer_cannot_reopen_commit_fd_for_writing(self):
        collector_module._require_formal_uid_processes(
            collector_module.FORMAL_RUNNER_UID,
            expected={},
        )
        repository = Path(__file__).resolve().parents[1]
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            root.chmod(0o755)
            candidate = root / "candidate"
            candidate.mkdir(mode=0o700)
            os.chown(
                candidate,
                collector_module.FORMAL_RUNNER_UID,
                collector_module.FORMAL_RUNNER_GID,
            )
            release = root / "release"
            control = candidate / "paused-commit.json"
            publisher_script = root / "publisher.py"
            publisher_script.write_text(
                "\n".join(
                    (
                        "import json, os, sys, time",
                        "from pathlib import Path",
                        f"sys.path.insert(0, {str(repository / 'src')!r})",
                        "import txnmem_formal_io as formal_io",
                        "import txnmem_provenance_runner as runner",
                        "candidate, control, release = map(Path, sys.argv[1:])",
                        "gate = int(os.environ.pop('TXNMEM_PROVENANCE_START_GATE_FD'))",
                        "ready = int(os.environ.pop('TXNMEM_PROVENANCE_READY_FD'))",
                        "completion = int(os.environ.pop('TXNMEM_PROVENANCE_COMPLETION_FD'))",
                        "runner._harden_execd_formal_runner()",
                        "store = formal_io.FormalStore(candidate)",
                        "store._require_fd_bound_publication_support()",
                        "os.write(ready, b'R')",
                        "os.close(ready)",
                        "if os.read(gate, 1) != b'G': raise SystemExit(71)",
                        "os.close(gate)",
                        "store.ensure_directory('bundles')",
                        "real_link = formal_io._link_anonymous_inode",
                        "def paused_link(descriptor, parent, name):",
                        "    control.write_text(json.dumps({'pid': os.getpid(), 'fd': descriptor}), encoding='utf-8')",
                        "    while not release.exists(): time.sleep(0.01)",
                        "    real_link(descriptor, parent, name)",
                        "formal_io._link_anonymous_inode = paused_link",
                        "store._publish_json_exclusive('bundles', 'pointer.json', payload={'publication_status': 'complete', 'schema': 'peer-fixture-v1'})",
                        "os.write(completion, b'{}')",
                        "os.close(completion)",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            publisher_script.chmod(0o555)
            child = collector_module._start_gated_candidate(
                command=(
                    sys.executable,
                    "-I",
                    "-B",
                    str(publisher_script),
                    str(candidate),
                    str(control),
                    str(release),
                ),
                cwd=root,
                environment={
                    "PYTHONPATH": str(repository / "src"),
                },
                formal_uid=collector_module.FORMAL_RUNNER_UID,
                formal_gid=collector_module.FORMAL_RUNNER_GID,
                require_completion_receipt=True,
            )
            try:
                child.release()
                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline and not control.is_file():
                    time.sleep(0.01)
                self.assertTrue(control.is_file())
                paused = json.loads(control.read_text(encoding="utf-8"))
                peer_read, peer_write = os.pipe()
                peer_pid = os.fork()
                if peer_pid == 0:
                    try:
                        os.close(peer_read)
                        collector_module._drop_formal_child_privileges(
                            collector_module.FORMAL_RUNNER_UID,
                            collector_module.FORMAL_RUNNER_GID,
                        )
                        try:
                            descriptor = os.open(
                                f"/proc/{paused['pid']}/fd/{paused['fd']}",
                                os.O_WRONLY,
                            )
                        except PermissionError:
                            os.write(peer_write, b"D")
                        else:
                            os.close(descriptor)
                            os.write(peer_write, b"W")
                    finally:
                        os._exit(0)
                os.close(peer_write)
                peer_result = os.read(peer_read, 1)
                os.close(peer_read)
                waited_pid, peer_status = os.waitpid(peer_pid, 0)
                self.assertEqual(waited_pid, peer_pid)
                self.assertTrue(os.WIFEXITED(peer_status))
                self.assertEqual(peer_result, b"D")
                release.write_bytes(b"release\n")
                exit_code, receipt = child.wait_with_receipt(timeout=5.0)
                self.assertEqual(exit_code, 0)
                self.assertEqual(receipt, {})
                expected = (
                    json.dumps(
                        {
                            "publication_status": "complete",
                            "schema": "peer-fixture-v1",
                        },
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                    )
                    + "\n"
                ).encode("utf-8")
                self.assertEqual(
                    (candidate / "bundles" / "pointer.json").read_bytes(),
                    expected,
                )
            finally:
                if child.process.poll() is None:
                    child.terminate_validated_group(
                        term_seconds=0.5,
                        kill_seconds=2.0,
                    )
                child.close()
            collector_module._require_formal_uid_processes(
                collector_module.FORMAL_RUNNER_UID,
                expected={},
            )

    @unittest.skipUnless(
        sys.platform.startswith("linux")
        and hasattr(os, "geteuid")
        and os.geteuid() == 0,
        "protected Linux root only",
    )
    def test_protected_linux_collector_kills_gil_holder_within_external_bound(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            root.chmod(0o755)
            script = root / "gil_holder.py"
            script.write_text(
                "\n".join(
                    (
                        "import ctypes, os, signal",
                        "ready = int(os.environ.pop('TXNMEM_PROVENANCE_READY_FD'))",
                        "gate = int(os.environ.pop('TXNMEM_PROVENANCE_START_GATE_FD'))",
                        "if signal.pthread_sigmask(signal.SIG_BLOCK, set()) != {signal.SIGTERM}: raise SystemExit(90)",
                        "os.write(ready, b'R')",
                        "os.close(ready)",
                        "if os.read(gate, 1) != b'G': raise SystemExit(91)",
                        "os.close(gate)",
                        "ctypes.PyDLL(None).sleep(60)",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            script.chmod(0o555)
            child = collector_module._start_gated_candidate(
                command=(sys.executable, "-I", "-B", str(script)),
                cwd=root,
                environment={},
                formal_uid=collector_module.FORMAL_RUNNER_UID,
                formal_gid=collector_module.FORMAL_RUNNER_GID,
            )

            class Guard:
                active = True

                def deactivate(self):
                    child.require_quiescence()
                    self.active = False

            guard = Guard()
            try:
                child.release()
                started = time.monotonic()
                failures = collector_module._cleanup_formal_execution_resources(
                    execution_monitor=None,
                    network_guard=guard,
                    child=child,
                )
                elapsed = time.monotonic() - started
                child.require_quiescence()
            finally:
                if child.process.poll() is None:
                    child.terminate_validated_group(
                        term_seconds=0.1, kill_seconds=2.0
                    )
                child.close()

        self.assertEqual(failures, [])
        self.assertIs(guard.active, False)
        self.assertLess(elapsed, 12.0)

    @unittest.skipUnless(
        sys.platform.startswith("linux")
        and hasattr(os, "geteuid")
        and os.geteuid() == 0,
        "protected Linux root component probes only",
    )
    def test_protected_linux_component_probes_root_drop_parent_death_pidfd_guard_pointer(self):
        collector_module._require_pidfd_support()
        nft_path = Path(collector_module._FORMAL_NFT_EXECUTABLE)
        self.assertTrue(nft_path.is_file(), "protected Linux nft is unavailable")
        collector_module._require_formal_uid_processes(
            collector_module.FORMAL_RUNNER_UID,
            expected={},
        )

        def exact_pidfd_kill(pid):
            try:
                descriptor = collector_module._pidfd_open(pid)
            except ProcessLookupError:
                return
            try:
                collector_module._pidfd_send_signal(descriptor, signal.SIGKILL)
            except ProcessLookupError:
                pass
            finally:
                collector_module._pidfd_close(descriptor)

        def current_pidfds():
            observed = {}
            for entry in (Path("/proc") / "self" / "fd").iterdir():
                try:
                    target = os.readlink(entry)
                except FileNotFoundError:
                    continue
                if "pidfd" in target:
                    observed[int(entry.name)] = target
            return observed

        repository = Path(__file__).resolve().parents[1]
        isolated_environment = {
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PYTHONPATH": str(repository / "src"),
        }
        with TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            root.chmod(0o755)
            runtime = root / "runtime"
            runtime.mkdir(mode=0o755)
            locked_source = root / "locked-source"
            runner_copy = _copy_locked_runner_import_closure(locked_source)
            self.assertTrue(
                _isolated_contract_resolves_inside_locked_source(locked_source),
                "isolated contract provenance check failed",
            )

            parent_fixture = root / "parent_fixture.py"
            parent_fixture.write_text(
                "\n".join(
                    (
                        "import json, os, sys",
                        "from pathlib import Path",
                        "from txnmem_provenance_execution_collector import "
                        "FORMAL_RUNNER_GID, FORMAL_RUNNER_UID, _start_gated_candidate",
                        f"root = Path({str(root)!r})",
                        f"runner = {str(runner_copy)!r}",
                        f"runtime = {str(runtime)!r}",
                        "child = _start_gated_candidate(",
                        "    command=(sys.executable, '-I', '-S', '-B', runner, "
                        "'invalid-command'),",
                        "    cwd=root,",
                        "    environment={'TXNMEM_PROVENANCE_RUNTIME_SITE': runtime},",
                        "    formal_uid=FORMAL_RUNNER_UID,",
                        "    formal_gid=FORMAL_RUNNER_GID,",
                        "    require_completion_receipt=True,",
                        ")",
                        "print(json.dumps({'pid': child.process.pid, "
                        "'args': list(child.process.args)}, sort_keys=True), flush=True)",
                        "if os.read(0, 1) != b'X': os._exit(91)",
                        "os._exit(0)",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            parent_fixture.chmod(0o555)

            libc = collector_module.ctypes.CDLL(None, use_errno=True)
            prctl = libc.prctl
            prctl.argtypes = [
                collector_module.ctypes.c_int,
                collector_module.ctypes.c_ulong,
                collector_module.ctypes.c_ulong,
                collector_module.ctypes.c_ulong,
                collector_module.ctypes.c_ulong,
            ]
            prctl.restype = collector_module.ctypes.c_int
            prior_subreaper = collector_module.ctypes.c_int(0)
            self.assertEqual(
                prctl(
                    37,
                    collector_module.ctypes.addressof(prior_subreaper),
                    0,
                    0,
                    0,
                ),
                0,
            )
            self.assertEqual(prctl(36, 1, 0, 0, 0), 0)
            parent = None
            runner_pid = None
            runner_wait_status = None
            try:
                parent = subprocess.Popen(
                    (sys.executable, "-B", str(parent_fixture)),
                    cwd=root,
                    env=isolated_environment,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                raw_identity = parent.stdout.readline()
                if not raw_identity:
                    parent.wait(timeout=7.0)
                    self.fail(parent.stderr.read().decode("utf-8", errors="replace"))
                identity = json.loads(raw_identity)
                runner_pid = int(identity["pid"])
                runner_args = tuple(identity["args"])
                status_rows = {}
                for line in (Path("/proc") / str(runner_pid) / "status").read_text(
                    encoding="utf-8"
                ).splitlines():
                    if ":" in line:
                        key, value = line.split(":", 1)
                        status_rows[key] = value.strip()
                self.assertEqual(
                    [int(value) for value in status_rows["Uid"].split()],
                    [collector_module.FORMAL_RUNNER_UID] * 4,
                )
                self.assertEqual(
                    [int(value) for value in status_rows["Gid"].split()],
                    [collector_module.FORMAL_RUNNER_GID] * 4,
                )
                self.assertEqual(status_rows["Groups"].split(), [])
                self.assertEqual(
                    int(status_rows["SigBlk"], 16),
                    1 << (signal.SIGTERM - 1),
                )
                observed_runner = collector_module._read_process_group_identity(
                    runner_pid,
                    runner_args,
                )
                runner_start = observed_runner["start_identity"].rsplit(":", 1)[-1]
                collector_module._require_formal_uid_processes(
                    collector_module.FORMAL_RUNNER_UID,
                    expected={runner_pid: runner_start},
                )

                parent.stdin.write(b"X")
                parent.stdin.flush()
                parent.stdin.close()
                self.assertEqual(parent.wait(timeout=5.0), 0)
                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline:
                    waited_pid, status = os.waitpid(runner_pid, os.WNOHANG)
                    if waited_pid == runner_pid:
                        runner_wait_status = status
                        break
                    time.sleep(0.01)
                self.assertIsNotNone(runner_wait_status)
                self.assertTrue(os.WIFSIGNALED(runner_wait_status))
                self.assertEqual(os.WTERMSIG(runner_wait_status), signal.SIGKILL)
                collector_module._require_formal_uid_processes(
                    collector_module.FORMAL_RUNNER_UID,
                    expected={},
                )
            finally:
                if parent is not None and parent.poll() is None:
                    if parent.stdin is not None and not parent.stdin.closed:
                        parent.stdin.close()
                    try:
                        parent.wait(timeout=1.0)
                    except subprocess.TimeoutExpired:
                        exact_pidfd_kill(parent.pid)
                        parent.wait(timeout=5.0)
                if runner_pid is not None and runner_wait_status is None:
                    exact_pidfd_kill(runner_pid)
                    deadline = time.monotonic() + 5.0
                    while time.monotonic() < deadline:
                        try:
                            waited_pid, _status = os.waitpid(
                                runner_pid, os.WNOHANG
                            )
                        except ChildProcessError:
                            break
                        if waited_pid == runner_pid:
                            break
                        time.sleep(0.01)
                self.assertEqual(
                    prctl(36, int(prior_subreaper.value), 0, 0, 0),
                    0,
                )

            gil_fixture = root / "gil_descendant.py"
            gil_fixture.write_text(
                "\n".join(
                    (
                        "import ctypes, os, signal",
                        "ready = int(os.environ.pop('TXNMEM_PROVENANCE_READY_FD'))",
                        "gate = int(os.environ.pop('TXNMEM_PROVENANCE_START_GATE_FD'))",
                        "if signal.pthread_sigmask(signal.SIG_BLOCK, set()) != "
                        "{signal.SIGTERM}: raise SystemExit(90)",
                        "descendant = os.fork()",
                        "if descendant == 0:",
                        "    os.close(ready)",
                        "    os.close(gate)",
                        "    signal.signal(signal.SIGTERM, signal.SIG_IGN)",
                        "    ctypes.PyDLL(None).sleep(60)",
                        "    os._exit(0)",
                        "os.write(ready, b'R')",
                        "os.close(ready)",
                        "if os.read(gate, 1) != b'G': raise SystemExit(91)",
                        "os.close(gate)",
                        "ctypes.PyDLL(None).sleep(60)",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            gil_fixture.chmod(0o555)
            pidfds_before = current_pidfds()
            child = None
            guard = collector_module._NftNetworkGuard(
                "txnmem_"
                + hashlib.sha256(
                    f"{os.getpid()}:{time.monotonic_ns()}".encode("ascii")
                ).hexdigest()[:16],
                backend_ipv4_subnet="10.253.0.0/24",
                ingress_ipv4_subnet="10.254.0.0/24",
                backend_bridge_interface="br-f3f3f3f3f3f3",
                ingress_bridge_interface="br-e3e3e3e3e3e3",
                toxiproxy_ingress_ipv4="10.254.0.2",
            )
            signal_targets = []
            real_pidfd_signal = collector_module._pidfd_send_signal
            cleanup_failures = []

            def observed_pidfd_signal(descriptor, signal_number):
                self.assertTrue(guard.active)
                fdinfo = (
                    Path("/proc") / "self" / "fdinfo" / str(descriptor)
                ).read_text(encoding="utf-8")
                pid_rows = [
                    line for line in fdinfo.splitlines() if line.startswith("Pid:")
                ]
                self.assertEqual(len(pid_rows), 1)
                target_pid = int(pid_rows[0].split()[1])
                inventory = collector_module._formal_uid_processes(
                    collector_module.FORMAL_RUNNER_UID
                )
                self.assertIn(target_pid, inventory)
                signal_targets.append((target_pid, inventory[target_pid], signal_number))
                real_pidfd_signal(descriptor, signal_number)

            try:
                child = collector_module._start_gated_candidate(
                    command=(sys.executable, "-I", "-B", str(gil_fixture)),
                    cwd=root,
                    environment={},
                    formal_uid=collector_module.FORMAL_RUNNER_UID,
                    formal_gid=collector_module.FORMAL_RUNNER_GID,
                )
                guard.activate()
                guard.verify()
                child.release()
                started = time.monotonic()
                with patch.object(
                    collector_module,
                    "_pidfd_send_signal",
                    side_effect=observed_pidfd_signal,
                ):
                    cleanup_failures = (
                        collector_module._cleanup_formal_execution_resources(
                            execution_monitor=None,
                            network_guard=guard,
                            child=child,
                        )
                    )
                elapsed = time.monotonic() - started
            finally:
                if child is not None and child.process.poll() is None:
                    child.terminate_validated_group(
                        term_seconds=0.1,
                        kill_seconds=2.0,
                    )
                if child is not None:
                    child.close()
                if guard.active:
                    collector_module._require_formal_uid_processes(
                        collector_module.FORMAL_RUNNER_UID,
                        expected={},
                    )
                    guard.deactivate()

            self.assertEqual(cleanup_failures, [])
            self.assertLess(elapsed, 12.0)
            self.assertFalse(guard.active)
            self.assertNotIn(guard.table_name, guard._table_names())
            self.assertIn(signal.SIGTERM, [row[2] for row in signal_targets])
            self.assertIn(signal.SIGKILL, [row[2] for row in signal_targets])
            collector_module._require_formal_uid_processes(
                collector_module.FORMAL_RUNNER_UID,
                expected={},
            )
            self.assertEqual(current_pidfds(), pidfds_before)

            pointer_fixture = root / "pointer_fixture.py"
            pointer_fixture.write_text(
                "\n".join(
                    (
                        "import json, os, signal, sys",
                        "from pathlib import Path",
                        "import txnmem_formal_io as formal_io",
                        "phase, raw_root = sys.argv[1:]",
                        "root = Path(raw_root)",
                        "store = formal_io.FormalStore(root)",
                        "store.ensure_directory('bundles')",
                        "payload = {'publication_status': 'complete', "
                        "'schema': 'txnmem-integrated-pointer-v1'}",
                        "def before_link():",
                        "    if phase == 'before-link': os.kill(os.getpid(), signal.SIGKILL)",
                        "real_link = formal_io._link_anonymous_inode",
                        "def link_then_kill(*args, **kwargs):",
                        "    real_link(*args, **kwargs)",
                        "    os.kill(os.getpid(), signal.SIGKILL)",
                        "if phase == 'after-link': formal_io._link_anonymous_inode = link_then_kill",
                        "store._publish_json_exclusive('bundles', 'pointer.json', "
                        "payload=payload, _precommit_check=before_link)",
                        "(root / 'receipt.json').write_bytes("
                        "(root / 'bundles' / 'pointer.json').read_bytes())",
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            pointer_fixture.chmod(0o555)
            expected_pointer = (
                json.dumps(
                    {
                        "publication_status": "complete",
                        "schema": "txnmem-integrated-pointer-v1",
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                    allow_nan=False,
                )
                + "\n"
            ).encode("utf-8")
            for phase in ("before-link", "after-link"):
                with self.subTest(pointer_phase=phase):
                    publication_root = root / phase
                    publication_root.mkdir(mode=0o700)
                    publisher = subprocess.run(
                        (
                            sys.executable,
                            "-B",
                            str(pointer_fixture),
                            phase,
                            str(publication_root),
                        ),
                        cwd=root,
                        env=isolated_environment,
                        check=False,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                    self.assertEqual(publisher.returncode, -signal.SIGKILL)
                    pointer = publication_root / "bundles" / "pointer.json"
                    receipt = publication_root / "receipt.json"
                    if phase == "before-link":
                        self.assertFalse(pointer.exists())
                    else:
                        self.assertEqual(pointer.read_bytes(), expected_pointer)
                    self.assertFalse(receipt.exists())
                    self.assertFalse(pointer.exists() and receipt.exists())
                    for path in (publication_root / "bundles").iterdir():
                        if path.name.startswith(".pointer.json."):
                            path.unlink()
                    directory_fd = os.open(
                        publication_root / "bundles",
                        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                    )
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
                    self.assertFalse(
                        any(
                            path.name.startswith(".pointer.json.")
                            for path in (publication_root / "bundles").iterdir()
                        )
                    )

            collector_module._require_formal_uid_processes(
                collector_module.FORMAL_RUNNER_UID,
                expected={},
            )
            self.assertEqual(current_pidfds(), pidfds_before)

    def test_protected_linux_integrated_root_drop_parent_death_pidfd_guard_pointer_zero_residue(self):
        import txnmem_formal_controller as controller_module
        import txnmem_formal_smoke as smoke_module
        import txnmem_provenance_performance as performance_module
        import txnmem_provenance_runner as runner_module
        from txnmem_formal_io import FormalIOError, FormalStore

        selector = (
            "tests.test_txnmem_provenance_execution_collector."
            "ProvenanceExecutionCollectorTests."
            "test_protected_linux_integrated_root_drop_parent_death_pidfd_guard_pointer_zero_residue"
        )
        controller_entry = getattr(
            controller_module,
            "_run_protected_linux_integrated_lifecycle",
            None,
        )
        collector_fault = getattr(
            collector_module, "_IntegratedLifecycleFault", None
        )
        collector_validator = getattr(
            collector_module, "_require_integrated_lifecycle_fault", None
        )
        runner_fault = getattr(runner_module, "_IntegratedLifecycleFault", None)
        runner_validator = getattr(
            runner_module, "_require_integrated_lifecycle_fault", None
        )
        publication_mode = getattr(
            performance_module, "_PrivatePublicationMode", None
        )
        publication_validator = getattr(
            performance_module, "_require_private_publication_mode", None
        )
        self.assertTrue(
            callable(controller_entry),
            "integrated lifecycle controller entry is unavailable",
        )
        for enum_type, validator, expected_name, expected_value in (
            (
                collector_fault,
                collector_validator,
                "POINTER_WITHOUT_RECEIPT",
                "pointer_without_receipt",
            ),
            (
                runner_fault,
                runner_validator,
                "POINTER_WITHOUT_RECEIPT",
                "pointer_without_receipt",
            ),
            (
                publication_mode,
                publication_validator,
                "INTEGRATED_POINTER_WITHOUT_RECEIPT",
                "integrated_pointer_without_receipt",
            ),
        ):
            self.assertIsNotNone(enum_type)
            self.assertTrue(callable(validator))
            self.assertEqual(
                [(member.name, member.value) for member in enum_type],
                [(expected_name, expected_value)],
            )
            with self.assertRaises((CollectorError, TypeError, ValueError)):
                validator(expected_value)
            self.assertIs(
                validator(next(iter(enum_type))),
                next(iter(enum_type)),
            )
            class StringSubclass(str):
                pass

            class UnknownEnum(enum.Enum):
                UNKNOWN = expected_value

            for invalid in (
                True,
                StringSubclass(expected_value),
                UnknownEnum.UNKNOWN,
            ):
                with self.assertRaises((CollectorError, TypeError, ValueError)):
                    validator(invalid)
        self.assertIsNone(
            inspect.signature(
                collector_module._run_protected_linux_integrated_lifecycle
            ).parameters["fault"].default
        )
        self.assertIsNone(
            inspect.signature(
                runner_module._run_protected_linux_integrated_lifecycle_probe
            ).parameters["fault"].default
        )
        self.assertIsNone(
            inspect.signature(
                performance_module.publish_provenance_bundle
            ).parameters["_private_publication_mode"].default
        )
        self.assertEqual(
            smoke_module._REPORT_SCHEMA,
            "txnmem-formal-provenance-smoke-v2",
        )
        with self.assertRaisesRegex(
            controller_module.FormalControllerError, "unsupported"
        ):
            controller_module._dispatch(
                "integrated-lifecycle",
                (),
                Path("."),
                object(),
            )

        with self.subTest(correction="recorded-pidfd-identities-only"):
            signal_recorded = getattr(
                collector_module,
                "_signal_integrated_lifecycle_identities",
                None,
            )
            self.assertTrue(callable(signal_recorded))
            expected = {101: "11", 102: "12"}
            opened = iter((71, 72))
            delivered = []
            closed = []
            with patch.object(
                collector_module,
                "_integrated_lifecycle_start_ticks",
                side_effect=lambda pid: expected[pid],
            ), patch.object(
                collector_module,
                "_pidfd_open",
                side_effect=lambda _pid: next(opened),
            ), patch.object(
                collector_module,
                "_pidfd_send_signal",
                side_effect=lambda descriptor, sig: delivered.append(
                    (descriptor, sig)
                ),
            ), patch.object(
                collector_module,
                "_pidfd_close",
                side_effect=lambda descriptor: closed.append(descriptor),
            ), patch.object(
                collector_module,
                "_formal_uid_processes",
                side_effect=AssertionError("UID inventory is assertion-only"),
            ):
                self.assertEqual(
                    signal_recorded(expected, signal.SIGKILL),
                    2,
                )
            self.assertEqual(
                delivered,
                [(71, signal.SIGKILL), (72, signal.SIGKILL)],
            )
            self.assertEqual(closed, [71, 72])

            opened = iter((81, 82))
            delivered = []
            closed = []
            with patch.object(
                collector_module,
                "_integrated_lifecycle_start_ticks",
                side_effect=lambda pid: "13" if pid == 102 else expected[pid],
            ), patch.object(
                collector_module,
                "_pidfd_open",
                side_effect=lambda _pid: next(opened),
            ), patch.object(
                collector_module,
                "_pidfd_send_signal",
                side_effect=lambda descriptor, sig: delivered.append(
                    (descriptor, sig)
                ),
            ), patch.object(
                collector_module,
                "_pidfd_close",
                side_effect=lambda descriptor: closed.append(descriptor),
            ), patch.object(
                collector_module,
                "_formal_uid_processes",
                side_effect=AssertionError("UID inventory is assertion-only"),
            ):
                with self.assertRaisesRegex(CollectorError, "identity"):
                    signal_recorded(expected, signal.SIGKILL)
            self.assertEqual(delivered, [])
            self.assertEqual(closed, [81, 82])
            lifecycle_source = inspect.getsource(
                collector_module._run_protected_linux_integrated_lifecycle
            ) + inspect.getsource(
                collector_module._run_protected_linux_integrated_lifecycle_body
            )
            finalizer_source = inspect.getsource(
                collector_module._finalize_integrated_lifecycle_resources
            )
            self.assertEqual(
                lifecycle_source.count(
                    "_signal_integrated_lifecycle_identities("
                )
                + finalizer_source.count(
                    "_signal_integrated_lifecycle_identities("
                ),
                2,
            )
            self.assertNotIn(
                "_pidfd_send_signal(", lifecycle_source + finalizer_source
            )

        with self.subTest(correction="surviving-parent-owns-guard-removal"):
            guard_owner_type = getattr(
                collector_module,
                "_IntegratedLifecycleGuardOwner",
                None,
            )
            self.assertTrue(callable(guard_owner_type))
            events = []
            fake_guard = SimpleNamespace(
                deactivate=lambda: events.append("deactivate")
            )
            owner = guard_owner_type(
                guard=fake_guard,
                owner_pid=os.getpid(),
            )
            with patch.object(
                collector_module.os,
                "getpid",
                return_value=os.getpid() + 1,
            ), self.assertRaisesRegex(CollectorError, "owner"):
                owner.deactivate_after_quiescence(
                    lambda: events.append("quiescence")
                )
            self.assertEqual(events, [])
            with self.assertRaisesRegex(CollectorError, "drifted"):
                owner.deactivate_after_quiescence(
                    lambda: (_ for _ in ()).throw(
                        CollectorError("recorded identity drifted")
                    )
                )
            self.assertEqual(events, [])
            owner.deactivate_after_quiescence(
                lambda: events.append("quiescence")
            )
            self.assertEqual(events, ["quiescence", "deactivate"])
            worker_start = lifecycle_source.index("def worker_main()")
            worker_end = lifecycle_source.index(
                "\n        worker_pid, worker_start = _fork_integrated_lifecycle_worker(",
                worker_start,
            )
            worker_source = lifecycle_source[worker_start:worker_end]
            self.assertNotIn("guard.deactivate(", worker_source)
            self.assertNotIn(".deactivate_after_quiescence(", worker_source)

        with self.subTest(correction="actual-receipt-and-completion-boundary"):
            send_state = getattr(
                collector_module,
                "_send_integrated_lifecycle_worker_state",
                None,
            )
            receive_state = getattr(
                collector_module,
                "_receive_integrated_lifecycle_worker_state",
                None,
            )
            receipt_owner_type = getattr(
                collector_module,
                "_IntegratedLifecycleReceiptOwner",
                None,
            )
            read_absent = getattr(
                collector_module,
                "_read_integrated_lifecycle_absent_receipt",
                None,
            )
            completion_writer = getattr(
                collector_module,
                "_write_collector_completion_record",
                None,
            )
            self.assertTrue(
                all(
                    callable(value)
                    for value in (
                        send_state,
                        receive_state,
                        read_absent,
                        completion_writer,
                        receipt_owner_type,
                    )
                )
            )
            state = {
                "schema": "txnmem-integrated-lifecycle-worker-state-v1",
                "runner_pid": 101,
                "runner_start_ticks": "11",
                "descendant_pid": 102,
                "descendant_start_ticks": "12",
            }

            def make_receive_resources():
                parent = Path(
                    collector_module.tempfile.gettempdir()
                ).resolve()
                parent_metadata = parent.stat()
                resources = collector_module._IntegratedLifecycleResources(
                    lifecycle_root=parent / "txnmem-receive-fixture",
                    lifecycle_parent_device=int(parent_metadata.st_dev),
                    lifecycle_parent_inode=int(parent_metadata.st_ino),
                    lifecycle_device=int(parent_metadata.st_dev),
                    lifecycle_inode=int(parent_metadata.st_ino),
                    controller_uid=os.getuid(),
                    runner_uid=os.getuid(),
                    runner_owned_relative=Path("candidate"),
                    pidfds_before={},
                )
                collector_module._adopt_integrated_lifecycle_worker(
                    resources,
                    100,
                    "10",
                )
                return resources

            sender, receiver = socket.socketpair()
            receipt_read, receipt_write = os.pipe()
            resources = make_receive_resources()
            try:
                send_state(sender, state, receipt_read)
                with patch.object(
                    collector_module,
                    "_require_formal_uid_processes",
                    return_value={101: "11", 102: "12"},
                ):
                    observed_state = receive_state(
                        receiver,
                        timeout=1.0,
                        resources=resources,
                    )
                transferred_fd = resources.receipt_owner.borrow()
                self.assertEqual(observed_state, state)
                os.close(receipt_read)
                receipt_read = -1
                os.close(receipt_write)
                receipt_write = -1
                absent_receipt = read_absent(
                    resources.receipt_owner,
                    timeout=1.0,
                )
            finally:
                sender.close()
                receiver.close()
                resources.fd_owners.close_retained()
                for descriptor in (receipt_read, receipt_write):
                    if descriptor is not None and descriptor >= 0:
                        os.close(descriptor)
            self.assertIsNone(absent_receipt)
            collection_source = inspect.getsource(
                collector_module._collect_execution_evidence
            )
            self.assertIn(
                "_write_collector_completion_record(",
                collection_source,
            )
            receipt_read = lifecycle_source.index(
                "_read_integrated_lifecycle_absent_receipt("
            )
            reap_proof = lifecycle_source.index(
                "set(reaped_statuses) != expected_pids"
            )
            self.assertGreater(receipt_read, reap_proof)
            self.assertIn(
                "completion_receipt=absent_receipt",
                lifecycle_source,
            )
            self.assertNotIn("completion_receipt={}", lifecycle_source)
            with TemporaryDirectory() as tmp:
                boundary_root = Path(tmp).resolve()
                boundary_store = FormalStore(boundary_root)
                with self.assertRaisesRegex(
                    CollectorError, "completion evidence"
                ):
                    completion_writer(
                        boundary_store,
                        "completion.json",
                        absent_receipt,
                    )
                self.assertEqual(
                    boundary_store.entry_kind("completion.json"),
                    "missing",
                )
                candidate = boundary_root / "candidate"
                candidate.mkdir(mode=0o700)
                candidate_mode = candidate.stat().st_mode & 0o777
                with self.assertRaisesRegex(
                    CollectorError, "completion receipt"
                ):
                    collector_module._seal_candidate_tree(
                        candidate,
                        expected_owner_uid=os.getuid(),
                        sealed_owner_uid=os.getuid(),
                        sealed_owner_gid=os.getgid(),
                        completion_receipt=absent_receipt,
                    )
                self.assertEqual(
                    candidate.stat().st_mode & 0o777,
                    candidate_mode,
                )

        with self.subTest(correction="descriptor-relative-tree-removal"):
            remove_tree = collector_module._remove_integrated_lifecycle_tree
            parameters = inspect.signature(remove_tree).parameters
            self.assertTrue(
                {
                    "expected_parent_device",
                    "expected_parent_inode",
                    "controller_uid",
                    "runner_uid",
                    "runner_owned_relative",
                }.issubset(parameters)
            )
            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                lifecycle = parent / "lifecycle"
                lifecycle.mkdir(mode=0o700)
                candidate = lifecycle / "candidate"
                candidate.mkdir(mode=0o700)
                payload = candidate / "payload.json"
                payload.write_text("{}\n", encoding="utf-8")
                linked = candidate / "linked"
                linked.symlink_to(payload.name)
                parent_metadata = parent.stat()
                lifecycle_metadata = lifecycle.stat()
                with self.assertRaisesRegex(CollectorError, "special file"):
                    remove_tree(
                        lifecycle,
                        expected_parent_device=int(parent_metadata.st_dev),
                        expected_parent_inode=int(parent_metadata.st_ino),
                        expected_device=int(lifecycle_metadata.st_dev),
                        expected_inode=int(lifecycle_metadata.st_ino),
                        controller_uid=os.getuid(),
                        runner_uid=os.getuid(),
                        runner_owned_relative=Path("candidate"),
                    )
                self.assertTrue(lifecycle.is_dir())
                self.assertTrue(linked.is_symlink())

            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                lifecycle = parent / "lifecycle"
                lifecycle.symlink_to("missing-lifecycle")
                parent_metadata = parent.stat()
                with self.assertRaisesRegex(CollectorError, "identity"):
                    remove_tree(
                        lifecycle,
                        expected_parent_device=int(parent_metadata.st_dev),
                        expected_parent_inode=int(parent_metadata.st_ino),
                        expected_device=int(parent_metadata.st_dev),
                        expected_inode=int(parent_metadata.st_ino),
                        controller_uid=os.getuid(),
                        runner_uid=os.getuid(),
                        runner_owned_relative=Path("candidate"),
                    )
                self.assertTrue(lifecycle.is_symlink())

            remove_source = inspect.getsource(remove_tree)
            inspect_source = inspect.getsource(
                collector_module._inspect_integrated_lifecycle_tree_fd
            )
            delete_source = inspect.getsource(
                collector_module._delete_integrated_lifecycle_tree_fd
            )
            cleanup_source = remove_source + inspect_source + delete_source
            self.assertNotIn("shutil", cleanup_source)
            self.assertNotIn("rglob", cleanup_source)
            self.assertNotIn(".exists()", remove_source)
            self.assertIn("O_NOFOLLOW", cleanup_source)
            self.assertIn("dir_fd=", cleanup_source)

        with self.subTest(
            correction="round2-parent-owned-guard-activation"
        ):
            table_name = "txnmem_" + "d" * 16

            def activation_probe():
                table_present = False
                calls = []
                guard = collector_module._NftNetworkGuard(
                    table_name,
                    backend_ipv4_subnet="172.19.0.0/16",
                    ingress_ipv4_subnet="172.20.0.0/16",
                    backend_bridge_interface="br-aaaaaaaaaaaa",
                    ingress_bridge_interface="br-bbbbbbbbbbbb",
                    toxiproxy_ingress_ipv4="172.20.0.2",
                )

                def table_names():
                    return {table_name} if table_present else set()

                def run(arguments, *, stdin=None, check=True):
                    nonlocal table_present
                    calls.append(tuple(arguments))
                    if arguments == ("-f", "-"):
                        table_present = True
                    elif arguments == (
                        "delete",
                        "table",
                        "inet",
                        table_name,
                    ):
                        table_present = False
                    return SimpleNamespace(
                        returncode=0,
                        stdout="",
                        stderr="",
                    )

                return guard, calls, table_names, run, lambda: table_present

            (
                rollback_guard,
                rollback_calls,
                rollback_tables,
                rollback_run,
                rollback_present,
            ) = activation_probe()
            with patch.object(
                rollback_guard,
                "_table_names",
                side_effect=rollback_tables,
            ), patch.object(
                rollback_guard,
                "_run",
                side_effect=rollback_run,
            ), patch.object(
                rollback_guard,
                "snapshot",
                side_effect=CollectorError("post-apply snapshot failed"),
            ):
                with self.assertRaisesRegex(
                    CollectorError, "post-apply snapshot"
                ):
                    rollback_guard.activate()
            self.assertIn(
                ("delete", "table", "inet", table_name),
                rollback_calls,
            )
            self.assertFalse(rollback_present())

            (
                retained_guard,
                retained_calls,
                retained_tables,
                retained_run,
                retained_present,
            ) = activation_probe()
            retained_owner = guard_owner_type(
                guard=retained_guard,
                owner_pid=os.getpid(),
            )
            self.assertTrue(
                callable(
                    getattr(
                        retained_owner,
                        "activate_retaining_table",
                        None,
                    )
                )
            )
            with patch.object(
                collector_module.os,
                "getpid",
                return_value=os.getpid() + 1,
            ), patch.object(
                retained_guard,
                "_table_names",
                side_effect=retained_tables,
            ), patch.object(
                retained_guard,
                "_run",
                side_effect=retained_run,
            ), self.assertRaisesRegex(CollectorError, "owner"):
                retained_owner.activate_retaining_table()
            self.assertEqual(retained_calls, [])

            with patch.object(
                retained_guard,
                "_table_names",
                side_effect=retained_tables,
            ), patch.object(
                retained_guard,
                "_run",
                side_effect=retained_run,
            ), patch.object(
                retained_guard,
                "snapshot",
                side_effect=CollectorError("post-apply snapshot failed"),
            ):
                with self.assertRaisesRegex(
                    CollectorError, "post-apply snapshot"
                ):
                    retained_owner.activate_retaining_table()
            self.assertTrue(retained_guard.active)
            self.assertTrue(retained_present())
            self.assertNotIn(
                ("delete", "table", "inet", table_name),
                retained_calls,
            )
            with self.assertRaisesRegex(CollectorError, "not quiescent"):
                retained_owner.deactivate_after_quiescence(
                    lambda: (_ for _ in ()).throw(
                        CollectorError("lineage not quiescent")
                    )
                )
            self.assertTrue(retained_present())

        with self.subTest(correction="round2-atomic-quarantine-deletion"):
            real_rename = collector_module.os.rename
            real_unlink = collector_module.os.unlink
            real_rmdir = collector_module.os.rmdir

            def inode_survives(parent, expected_inode):
                for directory, names, files in os.walk(parent):
                    for name in [*names, *files]:
                        path = Path(directory) / name
                        try:
                            if path.lstat().st_ino == expected_inode:
                                return True
                        except FileNotFoundError:
                            continue
                return False

            for node_kind in ("file", "directory", "root"):
                with self.subTest(node_kind=node_kind):
                    with TemporaryDirectory() as tmp:
                        parent = Path(tmp).resolve()
                        lifecycle = parent / "lifecycle"
                        lifecycle.mkdir(mode=0o700)
                        candidate = lifecycle / "candidate"
                        candidate.mkdir(mode=0o700)
                        if node_kind == "file":
                            target = candidate / "swap-file"
                            target.write_bytes(b"bound")
                        elif node_kind == "directory":
                            target = candidate / "swap-directory"
                            target.mkdir(mode=0o700)
                        else:
                            target = lifecycle
                        original_inode = target.lstat().st_ino
                        target_name = target.name
                        survivor_name = target_name + ".bound-survivor"
                        replacement_inode = None
                        swapped = False

                        def install_swap(directory_fd):
                            nonlocal replacement_inode, swapped
                            if swapped:
                                return
                            swapped = True
                            real_rename(
                                target_name,
                                survivor_name,
                                src_dir_fd=directory_fd,
                                dst_dir_fd=directory_fd,
                            )
                            if node_kind == "file":
                                descriptor = os.open(
                                    target_name,
                                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                                    0o600,
                                    dir_fd=directory_fd,
                                )
                                try:
                                    os.write(descriptor, b"replacement")
                                finally:
                                    os.close(descriptor)
                            else:
                                os.mkdir(
                                    target_name,
                                    mode=0o700,
                                    dir_fd=directory_fd,
                                )
                            replacement_inode = os.stat(
                                target_name,
                                dir_fd=directory_fd,
                                follow_symlinks=False,
                            ).st_ino

                        def swapping_rename(
                            source,
                            destination,
                            *args,
                            **kwargs,
                        ):
                            if source == target_name and not swapped:
                                install_swap(kwargs.get("src_dir_fd"))
                            return real_rename(
                                source,
                                destination,
                                *args,
                                **kwargs,
                            )

                        def swapping_unlink(name, *args, **kwargs):
                            if name == target_name and not swapped:
                                install_swap(kwargs.get("dir_fd"))
                            return real_unlink(name, *args, **kwargs)

                        def swapping_rmdir(name, *args, **kwargs):
                            if name == target_name and not swapped:
                                install_swap(kwargs.get("dir_fd"))
                            return real_rmdir(name, *args, **kwargs)

                        parent_metadata = parent.stat()
                        lifecycle_metadata = lifecycle.stat()
                        removal_failure = None
                        with patch.object(
                            collector_module.os,
                            "rename",
                            side_effect=swapping_rename,
                        ), patch.object(
                            collector_module.os,
                            "unlink",
                            side_effect=swapping_unlink,
                        ), patch.object(
                            collector_module.os,
                            "rmdir",
                            side_effect=swapping_rmdir,
                        ):
                            try:
                                remove_tree(
                                    lifecycle,
                                    expected_parent_device=int(
                                        parent_metadata.st_dev
                                    ),
                                    expected_parent_inode=int(
                                        parent_metadata.st_ino
                                    ),
                                    expected_device=int(
                                        lifecycle_metadata.st_dev
                                    ),
                                    expected_inode=int(
                                        lifecycle_metadata.st_ino
                                    ),
                                    controller_uid=os.getuid(),
                                    runner_uid=os.getuid(),
                                    runner_owned_relative=Path("candidate"),
                                )
                            except BaseException as exc:
                                removal_failure = exc
                        self.assertTrue(swapped)
                        self.assertIsNotNone(removal_failure)
                        self.assertIsNotNone(replacement_inode)
                        self.assertTrue(
                            inode_survives(parent, original_inode)
                        )
                        self.assertTrue(
                            inode_survives(parent, replacement_inode)
                        )
                        guard_events = []
                        cleanup_owner = guard_owner_type(
                            guard=SimpleNamespace(
                                deactivate=lambda: guard_events.append(
                                    "deactivate"
                                )
                            ),
                            owner_pid=os.getpid(),
                        )
                        with self.assertRaises(type(removal_failure)):
                            cleanup_owner.deactivate_after_quiescence(
                                lambda: (_ for _ in ()).throw(
                                    removal_failure
                                )
                            )
                        self.assertEqual(guard_events, [])

        with self.subTest(correction="round2-exact-scm-rights-receive"):
            int_size = collector_module.array.array("i").itemsize

            class MutatingReceiveSocket(socket.socket):
                def __init__(
                    self,
                    *,
                    fileno,
                    mutation,
                    restore_failure,
                    fd_capacity,
                ):
                    super().__init__(fileno=fileno)
                    self.mutation = mutation
                    self.restore_failure = restore_failure
                    self.fd_capacity = fd_capacity
                    self.timeout_writes = 0
                    self.received_descriptors = []

                def settimeout(self, value):
                    self.timeout_writes += 1
                    if self.restore_failure and self.timeout_writes == 2:
                        raise OSError("timeout restore failed")
                    return super().settimeout(value)

                def recvmsg(self, bufsize, ancbufsize=0, flags=0):
                    if self.fd_capacity > 1:
                        ancbufsize = socket.CMSG_SPACE(
                            int_size * self.fd_capacity
                        )
                    payload, ancillary, message_flags, address = (
                        super().recvmsg(
                            bufsize,
                            ancbufsize,
                            flags,
                        )
                    )
                    for level, kind, data in ancillary:
                        if (
                            level == socket.SOL_SOCKET
                            and kind == socket.SCM_RIGHTS
                        ):
                            values = collector_module.array.array("i")
                            usable = len(data) - (len(data) % int_size)
                            values.frombytes(data[:usable])
                            self.received_descriptors.extend(values)
                    if self.mutation == "unexpected":
                        ancillary = [
                            *ancillary,
                            (socket.SOL_SOCKET, socket.SCM_RIGHTS + 97, b""),
                        ]
                    elif self.mutation == "non-integral":
                        level, kind, data = ancillary[0]
                        ancillary = [(level, kind, data + b"x")]
                    elif self.mutation == "message-truncated":
                        message_flags |= getattr(socket, "MSG_TRUNC", 0x20)
                    elif self.mutation == "control-truncated":
                        message_flags |= getattr(socket, "MSG_CTRUNC", 0x08)
                    return payload, ancillary, message_flags, address

            receive_cases = (
                (
                    "restore",
                    None,
                    True,
                    1,
                    False,
                    "timeout restoration",
                ),
                (
                    "unexpected-primary",
                    "unexpected",
                    True,
                    1,
                    False,
                    "worker state is invalid",
                ),
                (
                    "unexpected",
                    "unexpected",
                    False,
                    1,
                    False,
                    "worker state is invalid",
                ),
                (
                    "non-integral",
                    "non-integral",
                    False,
                    1,
                    False,
                    "worker state is invalid",
                ),
                (
                    "multiple",
                    None,
                    False,
                    2,
                    False,
                    "worker state is invalid",
                ),
                (
                    "message-truncated",
                    "message-truncated",
                    False,
                    1,
                    False,
                    "worker state is invalid",
                ),
                (
                    "control-truncated-close-secondary",
                    "control-truncated",
                    False,
                    1,
                    True,
                    "worker state is invalid",
                ),
            )
            state_payload = collector_module.canonical_json_bytes(state)
            real_close = collector_module.os.close
            real_object_close = (
                collector_module._close_integrated_lifecycle_fd_object
            )
            for (
                case_name,
                mutation,
                restore_failure,
                fd_count,
                close_secondary,
                expected_error,
            ) in receive_cases:
                with self.subTest(protocol_case=case_name):
                    sender, base_receiver = socket.socketpair()
                    receiver = MutatingReceiveSocket(
                        fileno=base_receiver.detach(),
                        mutation=mutation,
                        restore_failure=restore_failure,
                        fd_capacity=fd_count,
                    )
                    pipes = [os.pipe() for _ in range(fd_count)]
                    sent_descriptors = [pair[0] for pair in pipes]
                    descriptor_bytes = collector_module.array.array(
                        "i", sent_descriptors
                    ).tobytes()
                    sender.sendmsg(
                        [state_payload],
                        [
                            (
                                socket.SOL_SOCKET,
                                socket.SCM_RIGHTS,
                                descriptor_bytes,
                            )
                        ],
                    )
                    close_counts = {}

                    def tracked_close(file_object):
                        descriptor = file_object.fileno()
                        close_counts[descriptor] = (
                            close_counts.get(descriptor, 0) + 1
                        )
                        result = real_object_close(file_object)
                        if (
                            close_secondary
                            and descriptor in receiver.received_descriptors
                        ):
                            raise OSError("received FD close secondary")
                        return result

                    caught = None
                    resources = make_receive_resources()
                    try:
                        with patch.object(
                            collector_module,
                            "_close_integrated_lifecycle_fd_object",
                            side_effect=tracked_close,
                        ), patch.object(
                            collector_module,
                            "_require_formal_uid_processes",
                            return_value={101: "11", 102: "12"},
                        ):
                            try:
                                receive_state(
                                    receiver,
                                    timeout=1.0,
                                    resources=resources,
                                )
                            except BaseException as exc:
                                caught = exc
                    finally:
                        sender.close()
                        receiver.close()
                        resources.fd_owners.close_retained()
                        for read_descriptor, write_descriptor in pipes:
                            real_close(read_descriptor)
                            real_close(write_descriptor)
                    try:
                        self.assertIsInstance(caught, CollectorError)
                        self.assertRegex(str(caught), expected_error)
                        self.assertEqual(
                            len(receiver.received_descriptors),
                            fd_count,
                        )
                        for descriptor in receiver.received_descriptors:
                            self.assertEqual(close_counts.get(descriptor), 1)
                            with self.assertRaises(OSError):
                                os.fstat(descriptor)
                    finally:
                        for descriptor in receiver.received_descriptors:
                            try:
                                real_close(descriptor)
                            except OSError:
                                pass

            sender, receiver = socket.socketpair()
            receipt_read, receipt_write = os.pipe()
            transferred_descriptor = None
            success_resources = make_receive_resources()
            success_close_counts = {}
            try:
                sender.sendmsg(
                    [state_payload],
                    [
                        (
                            socket.SOL_SOCKET,
                            socket.SCM_RIGHTS,
                            collector_module.array.array(
                                "i", [receipt_read]
                            ).tobytes(),
                        )
                    ],
                )

                def track_success_close(file_object):
                    descriptor = file_object.fileno()
                    success_close_counts[descriptor] = (
                        success_close_counts.get(descriptor, 0) + 1
                    )
                    return real_object_close(file_object)

                with patch.object(
                    collector_module,
                    "_close_integrated_lifecycle_fd_object",
                    side_effect=track_success_close,
                ), patch.object(
                    collector_module,
                    "_require_formal_uid_processes",
                    return_value={101: "11", 102: "12"},
                ):
                    observed = receive_state(
                        receiver,
                        timeout=1.0,
                        resources=success_resources,
                    )
                    transferred_descriptor = (
                        success_resources.receipt_owner.borrow()
                    )
                self.assertEqual(observed, state)
                self.assertFalse(os.get_inheritable(transferred_descriptor))
                self.assertNotIn(
                    transferred_descriptor,
                    success_close_counts,
                )
                os.fstat(transferred_descriptor)
            finally:
                sender.close()
                receiver.close()
                success_resources.fd_owners.close_retained()
                for descriptor in (receipt_read, receipt_write):
                    if descriptor is not None:
                        try:
                            real_close(descriptor)
                        except OSError:
                            pass

        with self.subTest(
            correction="round2-failure-accumulating-cleanup"
        ):
            class PrimaryUnlinkFailure(BaseException):
                pass

            class SecondaryCloseFailure(BaseException):
                pass

            primary = PrimaryUnlinkFailure("first-unlink-primary")
            origin = []
            unlink_calls = []
            unlink_failed = False
            close_failed = False
            close_attempts = {}
            real_unlink = collector_module.os.unlink
            real_close = collector_module.os.close

            def accumulating_unlink(name, *args, **kwargs):
                nonlocal unlink_failed
                unlink_calls.append(name)
                if not unlink_failed:
                    unlink_failed = True
                    try:
                        raise primary
                    except BaseException as exc:
                        origin.append(exc.__traceback__)
                        raise
                return real_unlink(name, *args, **kwargs)

            def accumulating_close(descriptor):
                nonlocal close_failed
                is_regular = False
                inode = None
                try:
                    metadata = os.fstat(descriptor)
                    inode = metadata.st_ino
                    is_regular = collector_module.stat.S_ISREG(
                        metadata.st_mode
                    )
                except OSError:
                    pass
                if inode is not None:
                    close_attempts[inode] = close_attempts.get(inode, 0) + 1
                if unlink_failed and is_regular and not close_failed:
                    close_failed = True
                    real_close(descriptor)
                    raise SecondaryCloseFailure("file-close-secondary")
                return real_close(descriptor)

            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                lifecycle = parent / "lifecycle"
                lifecycle.mkdir(mode=0o700)
                candidate = lifecycle / "candidate"
                candidate.mkdir(mode=0o700)
                (candidate / "a").write_bytes(b"a")
                (candidate / "b").write_bytes(b"b")
                nested = candidate / "nested"
                nested.mkdir(mode=0o700)
                (nested / "c").write_bytes(b"c")
                expected_close_inodes = {
                    (candidate / "a").stat().st_ino,
                    (candidate / "b").stat().st_ino,
                    nested.stat().st_ino,
                    (nested / "c").stat().st_ino,
                }
                parent_metadata = parent.stat()
                lifecycle_metadata = lifecycle.stat()
                caught = None
                caught_traceback = None
                with patch.object(
                    collector_module.os,
                    "unlink",
                    side_effect=accumulating_unlink,
                ), patch.object(
                    collector_module.os,
                    "close",
                    side_effect=accumulating_close,
                ):
                    try:
                        remove_tree(
                            lifecycle,
                            expected_parent_device=int(
                                parent_metadata.st_dev
                            ),
                            expected_parent_inode=int(
                                parent_metadata.st_ino
                            ),
                            expected_device=int(lifecycle_metadata.st_dev),
                            expected_inode=int(lifecycle_metadata.st_ino),
                            controller_uid=os.getuid(),
                            runner_uid=os.getuid(),
                            runner_owned_relative=Path("candidate"),
                        )
                    except BaseException as exc:
                        caught = exc
                        caught_traceback = exc.__traceback__
                self.assertIs(caught, primary)
                self.assertTrue(close_failed)
                self.assertGreaterEqual(len(unlink_calls), 3)
                for inode in expected_close_inodes:
                    self.assertEqual(close_attempts.get(inode), 2, inode)
                traceback_nodes = []
                while caught_traceback is not None:
                    traceback_nodes.append(caught_traceback)
                    caught_traceback = caught_traceback.tb_next
                self.assertIn(origin[0], traceback_nodes)

        with self.subTest(
            correction="round3-persistent-final-cleanup-veto"
        ):
            resources_type = getattr(
                collector_module,
                "_IntegratedLifecycleResources",
                None,
            )
            finalize_resources = getattr(
                collector_module,
                "_finalize_integrated_lifecycle_resources",
                None,
            )
            self.assertTrue(callable(resources_type))
            self.assertTrue(callable(finalize_resources))

            class FinalCleanupPrimary(BaseException):
                pass

            primary = FinalCleanupPrimary("persistent-tree-cleanup-primary")
            primary_origins = []
            guard_events = []
            unlink_failed = False
            real_unlink = collector_module.os.unlink

            def fail_first_quarantine_unlink(name, *args, **kwargs):
                nonlocal unlink_failed
                if not unlink_failed:
                    unlink_failed = True
                    try:
                        raise primary
                    except BaseException as exc:
                        primary_origins.append(exc.__traceback__)
                        raise
                return real_unlink(name, *args, **kwargs)

            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                lifecycle = parent / "lifecycle"
                lifecycle.mkdir(mode=0o700)
                candidate = lifecycle / "candidate"
                candidate.mkdir(mode=0o700)
                (candidate / "a").write_bytes(b"a")
                (candidate / "b").write_bytes(b"b")
                parent_metadata = parent.stat()
                lifecycle_metadata = lifecycle.stat()
                guard = SimpleNamespace(
                    active=True,
                    table_name="txnmem_" + "e" * 16,
                    _expected_snapshot=None,
                    _table_names=lambda: {"txnmem_" + "e" * 16},
                    snapshot=lambda: {"table": "retained"},
                    deactivate=lambda: guard_events.append("deactivate"),
                )
                resources = resources_type(
                    lifecycle_root=lifecycle,
                    lifecycle_parent_device=int(parent_metadata.st_dev),
                    lifecycle_parent_inode=int(parent_metadata.st_ino),
                    lifecycle_device=int(lifecycle_metadata.st_dev),
                    lifecycle_inode=int(lifecycle_metadata.st_ino),
                    controller_uid=os.getuid(),
                    runner_uid=os.getuid(),
                    runner_owned_relative=Path("candidate"),
                    pidfds_before={},
                )
                resources.guard = guard
                resources.guard_owner = guard_owner_type(
                    guard=guard,
                    owner_pid=os.getpid(),
                )
                caught = None
                caught_traceback = None
                with patch.object(
                    collector_module.os,
                    "unlink",
                    side_effect=fail_first_quarantine_unlink,
                ), patch.object(
                    collector_module,
                    "_remove_integrated_lifecycle_tree",
                    wraps=remove_tree,
                ) as remove_spy, patch.object(
                    collector_module,
                    "_require_formal_uid_processes",
                    return_value=None,
                ), patch.object(
                    collector_module,
                    "_integrated_lifecycle_pidfds",
                    return_value={},
                ):
                    try:
                        finalize_resources(
                            resources,
                            primary_failure=None,
                            primary_traceback=None,
                        )
                    except BaseException as exc:
                        caught = exc
                        caught_traceback = exc.__traceback__
                self.assertIs(caught, primary)
                self.assertEqual(remove_spy.call_count, 1)
                self.assertTrue(lifecycle.is_dir())
                quarantine_residue = [
                    entry
                    for entry in parent.iterdir()
                    if entry.name.startswith(".txnmem-cleanup-")
                ]
                self.assertEqual(len(quarantine_residue), 1)
                self.assertEqual(guard_events, [])
                self.assertFalse(resources.tree_removed)
                self.assertTrue(
                    resources.tree_cleanup.has_unresolved_quarantine
                )
                self.assertIs(resources.cleanup_primary.exception, primary)
                traceback_nodes = []
                while caught_traceback is not None:
                    traceback_nodes.append(caught_traceback)
                    caught_traceback = caught_traceback.tb_next
                self.assertIn(primary_origins[0], traceback_nodes)

            finalizer_source = inspect.getsource(finalize_resources)
            lifecycle_source = inspect.getsource(
                collector_module._run_protected_linux_integrated_lifecycle
            ) + inspect.getsource(
                collector_module._run_protected_linux_integrated_lifecycle_body
            )
            self.assertEqual(
                lifecycle_source.count(
                    "_finalize_integrated_lifecycle_resources("
                ),
                1,
            )
            self.assertNotIn(
                "_remove_integrated_lifecycle_tree(",
                lifecycle_source,
            )
            self.assertEqual(
                finalizer_source.count(
                    "guard_owner.deactivate_after_quiescence("
                ),
                1,
            )

        with self.subTest(correction="round3-setup-resource-ownership"):
            prepare_setup = getattr(
                collector_module,
                "_prepare_integrated_lifecycle_setup",
                None,
            )

            class SetupPrimary(BaseException):
                pass

            class FakePrctl:
                def __init__(self):
                    self.argtypes = None
                    self.restype = None
                    self.calls = []
                    self.current = 0
                    self.set_returned = False

                def __call__(self, option, value, arg3, arg4, arg5):
                    self.calls.append((option, int(value)))
                    if option == 37:
                        pointer = collector_module.ctypes.cast(
                            value,
                            collector_module.ctypes.POINTER(
                                collector_module.ctypes.c_int
                            ),
                        )
                        pointer.contents.value = self.current
                    elif option == 36:
                        self.current = int(value)
                        if int(value) == 1:
                            self.set_returned = True
                    return 0

            class TrackingSocket:
                def __init__(self, owned):
                    self.owned = owned
                    self.close_count = 0

                def fileno(self):
                    return self.owned.fileno()

                def close(self):
                    self.close_count += 1
                    return self.owned.close()

            setup_cases = (
                "root-return-pre-record",
                "post-root-chown",
                "workspace-construction",
                "export-construction",
                "subreaper-return-pre-owned",
                "post-subreaper-pre-socket",
                "socketpair-return-pre-slots",
                "socketpair-partial-adopt",
                "socket-endpoint-setup",
            )
            real_mkdir = collector_module.os.mkdir
            real_set_inheritable = collector_module.os.set_inheritable
            real_socketpair = socket.socketpair
            repository = Path(__file__).resolve().parents[1]
            source_identity = {"runner_sha256": "a" * 64}

            def invoke_setup_with_trace(boundary_trace):
                caught = None
                caught_traceback = None
                run_hash = hashlib.sha256(
                    b"txnmem-integrated-lifecycle-private-run-v1"
                ).hexdigest()
                scope_hash = hashlib.sha256(
                    b"txnmem-integrated-lifecycle-private-scope-v1"
                ).hexdigest()
                resources = (
                    collector_module._new_integrated_lifecycle_resources(
                        run_hash=run_hash,
                        scope_hash=scope_hash,
                        pidfds_before={},
                        controller_uid=os.getuid(),
                        runner_uid=os.getuid(),
                    )
                )
                setup_failure = None
                setup_traceback = None
                previous_trace = sys.gettrace()
                if boundary_trace is not None:
                    sys.settrace(boundary_trace)
                try:
                    prepare_setup(
                        project_root=repository,
                        source_identity=source_identity,
                        pidfds_before={},
                        resources=resources,
                        controller_uid=os.getuid(),
                        runner_uid=os.getuid(),
                        runner_gid=os.getgid(),
                        require_root=False,
                    )
                except BaseException as exc:
                    setup_failure = exc
                    setup_traceback = exc.__traceback__
                finally:
                    sys.settrace(previous_trace)
                try:
                    finalize_resources(
                        resources,
                        primary_failure=setup_failure,
                        primary_traceback=setup_traceback,
                    )
                except BaseException as exc:
                    caught = exc
                    caught_traceback = exc.__traceback__
                return caught, caught_traceback

            for setup_case in setup_cases:
                with self.subTest(setup_fault=setup_case):
                    self.assertTrue(callable(prepare_setup))
                    primary = SetupPrimary(setup_case)
                    primary_origins = []
                    created_roots = []
                    tracked_sockets = []
                    inheritable_calls = []
                    fake_prctl = FakePrctl()
                    boundary_state = {
                        "root_returned": False,
                        "socketpair_returned": False,
                        "raised": False,
                    }

                    def raise_primary():
                        try:
                            raise primary
                        except BaseException as exc:
                            primary_origins.append(exc.__traceback__)
                            raise

                    def tracked_root_mkdir(path, *args, **kwargs):
                        result = real_mkdir(path, *args, **kwargs)
                        name = os.fspath(path)
                        if (
                            isinstance(name, str)
                            and name.startswith("txnmem-integrated-lifecycle-")
                        ):
                            created = setup_parent / name
                            if created not in created_roots:
                                created_roots.append(created)
                            if setup_case == "root-return-pre-record":
                                boundary_state["root_returned"] = True
                        return result

                    def fail_or_accept_chown(*_args, **_kwargs):
                        if setup_case == "post-root-chown":
                            raise_primary()
                        return None

                    def fail_workspace(
                        _run_hash,
                        _scope_hash,
                        *,
                        runs_root,
                        **_kwargs,
                    ):
                        (runs_root / "workspace-partial").mkdir(mode=0o700)
                        raise_primary()

                    def successful_export(
                        _project_root,
                        private_parent,
                        _source_identity,
                    ):
                        immutable_source = private_parent / "immutable-source"
                        source_directory = immutable_source / "src"
                        source_directory.mkdir(parents=True, mode=0o700)
                        (source_directory / "txnmem_provenance_runner.py").write_text(
                            "# setup fixture\n",
                            encoding="utf-8",
                        )
                        return immutable_source

                    def fail_export(
                        _project_root,
                        private_parent,
                        _source_identity,
                    ):
                        (private_parent / "export-partial").write_bytes(b"partial")
                        raise_primary()

                    def fail_socketpair():
                        raise_primary()

                    def tracked_socketpair():
                        first, second = real_socketpair()
                        tracked_sockets.extend(
                            (TrackingSocket(first), TrackingSocket(second))
                        )
                        if setup_case == "socketpair-return-pre-slots":
                            boundary_state["socketpair_returned"] = True
                        return tuple(tracked_sockets)

                    def fail_second_inheritable(descriptor, inheritable):
                        inheritable_calls.append(descriptor)
                        real_set_inheritable(descriptor, inheritable)
                        if len(inheritable_calls) == 2:
                            raise_primary()

                    workspace_context = (
                        patch.object(
                            collector_module,
                            "_prepare_formal_run_workspace",
                            side_effect=fail_workspace,
                        )
                        if setup_case == "workspace-construction"
                        else nullcontext()
                    )
                    export_context = patch.object(
                        collector_module,
                        "create_immutable_source_export",
                        side_effect=(
                            fail_export
                            if setup_case == "export-construction"
                            else successful_export
                        ),
                    )
                    socket_context = (
                        patch.object(
                            collector_module.socket,
                            "socketpair",
                            side_effect=fail_socketpair,
                        )
                        if setup_case == "post-subreaper-pre-socket"
                        else patch.object(
                            collector_module.socket,
                            "socketpair",
                            side_effect=tracked_socketpair,
                        )
                        if setup_case in {
                            "socketpair-return-pre-slots",
                            "socketpair-partial-adopt",
                            "socket-endpoint-setup",
                        }
                        else nullcontext()
                    )
                    inheritable_context = (
                        patch.object(
                            collector_module.os,
                            "set_inheritable",
                            side_effect=fail_second_inheritable,
                        )
                        if setup_case == "socket-endpoint-setup"
                        else nullcontext()
                    )
                    mkdir_context = patch.object(
                        collector_module.os,
                        "mkdir",
                        side_effect=tracked_root_mkdir,
                    )
                    caught = None
                    caught_traceback = None
                    with TemporaryDirectory() as tmp:
                        setup_parent = Path(tmp).resolve()

                        def setup_boundary_trace(frame, event, _arg):
                            if (
                                event != "line"
                                or frame.f_globals is not collector_module.__dict__
                                or boundary_state["raised"]
                            ):
                                return setup_boundary_trace
                            should_raise = bool(
                                setup_case == "root-return-pre-record"
                                and boundary_state["root_returned"]
                            ) or bool(
                                setup_case == "subreaper-return-pre-owned"
                                and fake_prctl.set_returned
                            ) or bool(
                                setup_case == "socketpair-return-pre-slots"
                                and boundary_state["socketpair_returned"]
                            ) or bool(
                                setup_case == "socketpair-partial-adopt"
                                and frame.f_locals.get("resources") is not None
                                and frame.f_locals[
                                    "resources"
                                ].parent_channel
                                is not None
                                and frame.f_locals[
                                    "resources"
                                ].worker_channel
                                is None
                            )
                            if should_raise:
                                boundary_state["raised"] = True
                                sys.settrace(None)
                                raise_primary()
                            return setup_boundary_trace

                        with patch.object(
                            collector_module.tempfile,
                            "gettempdir",
                            return_value=str(setup_parent),
                        ), patch.object(
                            collector_module.os,
                            "chown",
                            side_effect=fail_or_accept_chown,
                        ), workspace_context, export_context, patch.object(
                            collector_module,
                            "_publish_formal_input_tree",
                            return_value=None,
                        ), patch.object(
                            collector_module,
                            "_file_sha256",
                            return_value="a" * 64,
                        ), patch.object(
                            collector_module,
                            "_preflight_external_outputs",
                            return_value=(
                                SimpleNamespace(),
                                "launch.json",
                                SimpleNamespace(),
                                "completion.json",
                            ),
                        ), patch.object(
                            collector_module.ctypes,
                            "CDLL",
                            return_value=SimpleNamespace(prctl=fake_prctl),
                        ), socket_context, inheritable_context, mkdir_context, patch.object(
                            collector_module,
                            "_require_formal_uid_processes",
                            return_value=None,
                        ), patch.object(
                            collector_module,
                            "_integrated_lifecycle_pidfds",
                            return_value={},
                        ), patch.object(
                            collector_module,
                            "_remove_integrated_lifecycle_tree",
                            wraps=remove_tree,
                        ) as remove_spy:
                            boundary_trace = (
                                setup_boundary_trace
                                if setup_case in {
                                    "root-return-pre-record",
                                    "subreaper-return-pre-owned",
                                    "socketpair-return-pre-slots",
                                    "socketpair-partial-adopt",
                                }
                                else None
                            )
                            caught, caught_traceback = invoke_setup_with_trace(
                                boundary_trace
                            )
                        self.assertIs(caught, primary)
                        self.assertEqual(len(created_roots), 1)
                        self.assertEqual(remove_spy.call_count, 1)
                        self.assertFalse(created_roots[0].exists())
                        self.assertEqual(
                            [
                                entry
                                for entry in setup_parent.iterdir()
                                if entry.name.startswith(".txnmem-cleanup-")
                            ],
                            [],
                        )
                    traceback_nodes = []
                    while caught_traceback is not None:
                        traceback_nodes.append(caught_traceback)
                        caught_traceback = caught_traceback.tb_next
                    self.assertIn(primary_origins[0], traceback_nodes)
                    subreaper_values = [
                        value
                        for option, value in fake_prctl.calls
                        if option == 36
                    ]
                    if setup_case in {
                        "subreaper-return-pre-owned",
                        "post-subreaper-pre-socket",
                        "socketpair-return-pre-slots",
                        "socketpair-partial-adopt",
                        "socket-endpoint-setup",
                    }:
                        self.assertEqual(subreaper_values, [1, 0])
                    else:
                        self.assertEqual(subreaper_values, [])
                    if setup_case in {
                        "socketpair-return-pre-slots",
                        "socketpair-partial-adopt",
                        "socket-endpoint-setup",
                    }:
                        observed_close_counts = [
                            endpoint.close_count for endpoint in tracked_sockets
                        ]
                        for endpoint in tracked_sockets:
                            try:
                                endpoint.owned.close()
                            except OSError:
                                pass
                        self.assertEqual(
                            observed_close_counts,
                            [1, 1],
                        )
                    else:
                        self.assertEqual(tracked_sockets, [])

        with self.subTest(correction="round4-lifecycle-handoff-ownership"):
            resources_type = collector_module._IntegratedLifecycleResources
            run_lifecycle = (
                collector_module._run_protected_linux_integrated_lifecycle
            )
            finalize_resources = (
                collector_module._finalize_integrated_lifecycle_resources
            )

            class Round4Primary(BaseException):
                pass

            class Round4Channel:
                def __init__(
                    self,
                    owned,
                    *,
                    fail_before_close=False,
                    fail_after_close=False,
                ):
                    self.owned = owned
                    self.fail_before_close = fail_before_close
                    self.fail_after_close = fail_after_close
                    self.close_count = 0

                def __getattr__(self, name):
                    return getattr(self.owned, name)

                def close(self):
                    self.close_count += 1
                    if self.fail_before_close and self.close_count == 1:
                        raise OSError("channel close failed before effect")
                    result = self.owned.close()
                    if self.fail_after_close and self.close_count == 1:
                        raise OSError("channel close failed after effect")
                    return result

            def make_round4_resources(parent, *, channels=True):
                lifecycle = parent / "lifecycle"
                lifecycle.mkdir(mode=0o700)
                candidate = lifecycle / "candidate"
                candidate.mkdir(mode=0o700)
                parent_metadata = parent.stat()
                lifecycle_metadata = lifecycle.stat()
                resources = resources_type(
                    lifecycle_root=lifecycle,
                    lifecycle_parent_device=int(parent_metadata.st_dev),
                    lifecycle_parent_inode=int(parent_metadata.st_ino),
                    lifecycle_device=int(lifecycle_metadata.st_dev),
                    lifecycle_inode=int(lifecycle_metadata.st_ino),
                    controller_uid=os.getuid(),
                    runner_uid=os.getuid(),
                    runner_owned_relative=Path("candidate"),
                    pidfds_before={},
                )
                endpoints = ()
                if channels:
                    parent_channel, worker_channel = socket.socketpair()
                    endpoints = (
                        Round4Channel(parent_channel),
                        Round4Channel(worker_channel),
                    )
                    resources.parent_channel = endpoints[0]
                    resources.worker_channel = endpoints[1]
                return resources, lifecycle, endpoints

            def make_round4_setup(resources, lifecycle):
                guard = SimpleNamespace(
                    active=False,
                    table_name="txnmem_" + "f" * 16,
                    _table_names=lambda: set(),
                )
                return SimpleNamespace(
                    resources=resources,
                    workspace=SimpleNamespace(candidate=lifecycle / "candidate"),
                    immutable_source=lifecycle,
                    runner_path=lifecycle / "runner.py",
                    bundle_id="0" * 64,
                    pointer_path=lifecycle / "pointer.json",
                    precommit_marker=lifecycle / "precommit",
                    external_completion=lifecycle / "completion.json",
                    external_completion_store=SimpleNamespace(),
                    external_completion_name="completion.json",
                    guard=guard,
                    guard_owner=None,
                    command=(sys.executable,),
                    environment={},
                )

            def finalize_round4(resources, primary):
                caught = None
                with patch.object(
                    collector_module,
                    "_require_formal_uid_processes",
                    return_value=None,
                ), patch.object(
                    collector_module,
                    "_integrated_lifecycle_pidfds",
                    return_value={},
                ), patch.object(
                    collector_module,
                    "_signal_integrated_lifecycle_identities",
                    side_effect=lambda identities, _signal: len(identities),
                ), patch.object(
                    collector_module,
                    "_integrated_lifecycle_observed_start_ticks",
                    return_value=None,
                ), patch.object(
                    collector_module.os,
                    "waitpid",
                    side_effect=lambda pid, _options: (
                        pid,
                        signal.SIGKILL,
                    ),
                ):
                    try:
                        finalize_resources(
                            resources,
                            primary_failure=primary,
                            primary_traceback=primary.__traceback__,
                        )
                    except BaseException as exc:
                        caught = exc
                return caught

            def run_with_local_preflight(setup, **extra_patches):
                patches = (
                    patch.object(collector_module.sys, "platform", "linux"),
                    patch.object(collector_module.os, "geteuid", return_value=0),
                    patch.object(collector_module.Path, "is_file", return_value=True),
                    patch.object(
                        collector_module,
                        "_require_pidfd_support",
                        return_value=None,
                    ),
                    patch.object(
                        collector_module,
                        "_require_formal_uid_processes",
                        return_value=None,
                    ),
                    patch.object(
                        collector_module,
                        "_validate_formal_controller_context",
                        return_value={
                            "source_commit": "0" * 40,
                            "source_manifest": {},
                        },
                    ),
                    patch.object(
                        collector_module,
                        "attest_committed_source",
                        return_value={"runner_sha256": "a" * 64},
                    ),
                    patch.object(
                        collector_module,
                        "_integrated_lifecycle_pidfds",
                        return_value={},
                    ),
                    patch.object(
                        collector_module,
                        "_new_integrated_lifecycle_resources",
                        create=True,
                        return_value=setup.resources,
                    ),
                    patch.object(
                        collector_module,
                        "_prepare_integrated_lifecycle_setup",
                        side_effect=lambda **_kwargs: setup,
                    ),
                )
                entered = []
                try:
                    for context in patches:
                        entered.append(context)
                        context.__enter__()
                    for context in extra_patches.values():
                        entered.append(context)
                        context.__enter__()
                    return run_lifecycle(
                        project_root=Path(__file__).resolve().parents[1],
                        _controller_context={},
                        fault=collector_module._IntegratedLifecycleFault.POINTER_WITHOUT_RECEIPT,
                    )
                finally:
                    for context in reversed(entered):
                        context.__exit__(None, None, None)

            with self.subTest(ownership_gap="root-known-name-collision"):
                caught = None
                acquisition_failure = None
                with TemporaryDirectory() as tmp:
                    parent = Path(tmp).resolve()
                    with patch.object(
                        collector_module.tempfile,
                        "gettempdir",
                        return_value=str(parent),
                    ), patch.object(
                        collector_module.secrets,
                        "token_hex",
                        return_value="c" * 32,
                    ):
                        resources = (
                            collector_module._new_integrated_lifecycle_resources(
                                run_hash="1" * 64,
                                scope_hash="2" * 64,
                                pidfds_before={},
                                controller_uid=os.getuid(),
                                runner_uid=os.getuid(),
                            )
                        )
                    lifecycle = resources.lifecycle_root
                    lifecycle.mkdir(mode=0o700)
                    marker = lifecycle / "preexisting-marker"
                    marker.write_text("not lifecycle-owned\n", encoding="utf-8")
                    try:
                        collector_module._acquire_integrated_lifecycle_root(
                            resources
                        )
                    except BaseException as exc:
                        acquisition_failure = exc
                        caught = finalize_round4(resources, exc)
                    self.assertIs(caught, acquisition_failure)
                    self.assertTrue(resources.root_name_collision)
                    self.assertTrue(lifecycle.is_dir())
                    self.assertEqual(
                        marker.read_text(encoding="utf-8"),
                        "not lifecycle-owned\n",
                    )
                    self.assertFalse(resources.tree_cleanup.root_removed)

            with self.subTest(ownership_gap="setup-return-caller-handoff"):
                primary = Round4Primary("setup-return-caller-handoff")
                origin = []
                caught = None
                with TemporaryDirectory() as tmp:
                    parent = Path(tmp).resolve()
                    resources, lifecycle, endpoints = make_round4_resources(parent)
                    setup = make_round4_setup(resources, lifecycle)
                    raised = False

                    def handoff_trace(frame, event, _arg):
                        nonlocal raised
                        if (
                            not raised
                            and event == "line"
                            and frame.f_globals is collector_module.__dict__
                            and frame.f_locals.get("setup") is setup
                        ):
                            raised = True
                            sys.settrace(None)
                            try:
                                raise primary
                            except BaseException as exc:
                                origin.append(exc.__traceback__)
                                raise
                        return handoff_trace

                    previous_trace = sys.gettrace()
                    try:
                        sys.settrace(handoff_trace)
                        try:
                            run_with_local_preflight(setup)
                        except BaseException as exc:
                            caught = exc
                    finally:
                        sys.settrace(previous_trace)
                    observed_finalized = resources.finalized
                    observed_root_exists = lifecycle.exists()
                    observed_close_counts = tuple(
                        endpoint.close_count for endpoint in endpoints
                    )
                    for endpoint in endpoints:
                        try:
                            endpoint.close()
                        except OSError:
                            pass
                    self.assertIs(caught, primary)
                    self.assertTrue(observed_finalized)
                    self.assertFalse(observed_root_exists)
                    self.assertEqual(observed_close_counts, (1, 1))
                    self.assertTrue(origin)

            with self.subTest(ownership_gap="fork-return-pid-start-record"):
                primary = Round4Primary("fork-return-pid-start-record")
                caught = None
                signal_calls = []
                synthetic_pid = 4242
                synthetic_start = "424200"
                with TemporaryDirectory() as tmp:
                    parent = Path(tmp).resolve()
                    resources, lifecycle, endpoints = make_round4_resources(parent)
                    setup = make_round4_setup(resources, lifecycle)
                    endpoints[1].sendall(
                        f"H{synthetic_pid}:{synthetic_start}\n".encode("ascii")
                    )
                    raised = False

                    def fork_trace(frame, event, _arg):
                        nonlocal raised
                        if (
                            not raised
                            and event == "line"
                            and frame.f_globals is collector_module.__dict__
                            and synthetic_pid
                            in {
                                frame.f_locals.get("worker_pid"),
                                frame.f_locals.get("fork_pid"),
                            }
                            and not resources.recorded_identities
                        ):
                            raised = True
                            sys.settrace(None)
                            raise primary
                        return fork_trace

                    def record_signal(identities, signal_number):
                        signal_calls.append((dict(identities), signal_number))
                        return len(identities)

                    previous_trace = sys.gettrace()
                    try:
                        sys.settrace(fork_trace)
                        try:
                            run_with_local_preflight(
                                setup,
                                fork=patch.object(
                                    collector_module.os,
                                    "fork",
                                    return_value=synthetic_pid,
                                ),
                                start=patch.object(
                                    collector_module,
                                    "_integrated_lifecycle_start_ticks",
                                    return_value=synthetic_start,
                                ),
                                observed=patch.object(
                                    collector_module,
                                    "_integrated_lifecycle_observed_start_ticks",
                                    return_value=None,
                                ),
                                signal=patch.object(
                                    collector_module,
                                    "_signal_integrated_lifecycle_identities",
                                    side_effect=record_signal,
                                ),
                                waitpid=patch.object(
                                    collector_module.os,
                                    "waitpid",
                                    return_value=(synthetic_pid, signal.SIGKILL),
                                ),
                            )
                        except BaseException as exc:
                            caught = exc
                    finally:
                        sys.settrace(previous_trace)
                    for endpoint in endpoints:
                        try:
                            endpoint.close()
                        except OSError:
                            pass
                    self.assertIs(caught, primary)
                    self.assertEqual(
                        resources.recorded_identities,
                        {synthetic_pid: synthetic_start},
                    )
                    self.assertEqual(
                        signal_calls,
                        [({synthetic_pid: synthetic_start}, signal.SIGKILL)],
                    )
                    self.assertIn(synthetic_pid, resources.reaped_statuses)
                    self.assertFalse(lifecycle.exists())

            with self.subTest(ownership_gap="scm-return-direct-adopt"):
                primary = Round4Primary("scm-return-direct-adopt")
                caught = None
                received_descriptors = []

                class RecordingReceiveSocket(socket.socket):
                    def recvmsg(self, bufsize, ancbufsize=0, flags=0):
                        result = super().recvmsg(bufsize, ancbufsize, flags)
                        for level, kind, data in result[1]:
                            if (
                                level == socket.SOL_SOCKET
                                and kind == socket.SCM_RIGHTS
                            ):
                                values = collector_module.array.array("i")
                                values.frombytes(data[: values.itemsize])
                                received_descriptors.extend(values)
                        return result

                with TemporaryDirectory() as tmp:
                    parent = Path(tmp).resolve()
                    resources, lifecycle, _endpoints = make_round4_resources(
                        parent,
                        channels=False,
                    )
                    collector_module._adopt_integrated_lifecycle_worker(
                        resources,
                        100,
                        "10",
                    )
                    sender, base_receiver = socket.socketpair()
                    receiver = RecordingReceiveSocket(
                        fileno=base_receiver.detach()
                    )
                    receipt_read, receipt_write = os.pipe()
                    state_payload = collector_module.canonical_json_bytes(state)
                    sender.sendmsg(
                        [state_payload],
                        [
                            (
                                socket.SOL_SOCKET,
                                socket.SCM_RIGHTS,
                                collector_module.array.array(
                                    "i", [receipt_read]
                                ).tobytes(),
                            )
                        ],
                    )
                    try:
                        with patch.object(
                            collector_module,
                            "_require_formal_uid_processes",
                            return_value={101: "11", 102: "12"},
                        ):
                            receive_state(
                                receiver,
                                timeout=1.0,
                                resources=resources,
                            )
                        raise primary
                    except BaseException as exc:
                        caught = finalize_round4(resources, exc)
                    finally:
                        sender.close()
                        receiver.close()
                        os.close(receipt_read)
                        os.close(receipt_write)
                    self.assertIs(caught, primary)
                    self.assertEqual(len(received_descriptors), 1)
                    with self.assertRaises(OSError):
                        os.fstat(received_descriptors[0])
                    self.assertFalse(lifecycle.exists())

            with self.subTest(ownership_gap="scm-recvmsg-return-pre-harvest"):
                primary = Round4Primary("scm-recvmsg-return-pre-harvest")
                caught = None
                received_descriptors = []
                with TemporaryDirectory() as tmp:
                    parent = Path(tmp).resolve()
                    resources, lifecycle, _endpoints = make_round4_resources(
                        parent,
                        channels=False,
                    )
                    collector_module._adopt_integrated_lifecycle_worker(
                        resources,
                        100,
                        "10",
                    )
                    sender, base_receiver = socket.socketpair()
                    receiver = RecordingReceiveSocket(
                        fileno=base_receiver.detach()
                    )
                    receipt_read, receipt_write = os.pipe()
                    state_payload = collector_module.canonical_json_bytes(state)
                    sender.sendmsg(
                        [state_payload],
                        [
                            (
                                socket.SOL_SOCKET,
                                socket.SCM_RIGHTS,
                                collector_module.array.array(
                                    "i", [receipt_read]
                                ).tobytes(),
                            )
                        ],
                    )
                    raised = False

                    def recvmsg_trace(frame, event, _arg):
                        nonlocal raised
                        if (
                            not raised
                            and event == "line"
                            and frame.f_globals is collector_module.__dict__
                            and (
                                frame.f_locals.get("ancillary")
                                or frame.f_locals.get("recv_result")
                            )
                            and not frame.f_locals.get("received_owners")
                        ):
                            raised = True
                            sys.settrace(None)
                            raise primary
                        return recvmsg_trace

                    previous_trace = sys.gettrace()
                    try:
                        sys.settrace(recvmsg_trace)
                        try:
                            receive_state(
                                receiver,
                                timeout=1.0,
                                resources=resources,
                            )
                        except BaseException as exc:
                            caught = finalize_round4(resources, exc)
                    finally:
                        sys.settrace(previous_trace)
                        sender.close()
                        receiver.close()
                        os.close(receipt_read)
                        os.close(receipt_write)
                    self.assertIs(caught, primary)
                    self.assertTrue(raised)
                    self.assertEqual(
                        resources.recorded_identities,
                        {100: "10", 101: "11", 102: "12"},
                    )
                    self.assertEqual(len(received_descriptors), 1)
                    descriptor_open = True
                    try:
                        os.fstat(received_descriptors[0])
                    except OSError:
                        descriptor_open = False
                    if descriptor_open:
                        os.close(received_descriptors[0])
                    self.assertFalse(descriptor_open)
                    self.assertFalse(lifecycle.exists())

            with self.subTest(ownership_gap="channel-close-retained"):
                for close_fault, expected_count in (
                    ("before", 2),
                    ("after", 1),
                ):
                    with self.subTest(close_fault=close_fault):
                        primary = Round4Primary(
                            f"channel-close-retained-{close_fault}"
                        )
                        caught = None
                        with TemporaryDirectory() as tmp:
                            parent = Path(tmp).resolve()
                            resources, lifecycle, _endpoints = (
                                make_round4_resources(
                                    parent,
                                    channels=False,
                                )
                            )
                            owned, peer = socket.socketpair()
                            channel = Round4Channel(
                                owned,
                                fail_before_close=close_fault == "before",
                                fail_after_close=close_fault == "after",
                            )
                            descriptor = owned.fileno()
                            resources.parent_channel = channel
                            caught = finalize_round4(resources, primary)
                            descriptor_open = True
                            try:
                                os.fstat(descriptor)
                            except OSError:
                                descriptor_open = False
                            if descriptor_open:
                                owned.close()
                            peer.close()
                            self.assertIs(caught, primary)
                            self.assertEqual(
                                channel.close_count,
                                expected_count,
                            )
                            self.assertFalse(descriptor_open)
                            self.assertFalse(lifecycle.exists())

            with self.subTest(ownership_gap="receipt-consumer-entry"):
                primary = Round4Primary("receipt-consumer-entry")
                caught = None
                with TemporaryDirectory() as tmp:
                    parent = Path(tmp).resolve()
                    resources, lifecycle, _endpoints = make_round4_resources(
                        parent,
                        channels=False,
                    )
                    receipt_read, receipt_write = os.pipe()
                    receipt_fd_owner = (
                        resources.fd_owners.adopt_descriptor(receipt_read)
                    )
                    resources.receipt_owner.adopt_owner(receipt_fd_owner)
                    try:
                        raise primary
                    except BaseException as exc:
                        caught = finalize_round4(resources, exc)
                    descriptor_open = True
                    try:
                        os.fstat(receipt_read)
                    except OSError:
                        descriptor_open = False
                    if descriptor_open:
                        os.close(receipt_read)
                    os.close(receipt_write)
                    self.assertIs(caught, primary)
                    self.assertFalse(descriptor_open)
                    self.assertFalse(lifecycle.exists())

        class Round5Primary(BaseException):
            pass

        class Round5CloseBefore(OSError):
            pass

        class Round5CloseAfter(OSError):
            pass

        class Round5RecordingReceiveSocket(socket.socket):
            def __init__(self, *, fileno):
                super().__init__(fileno=fileno)
                self.received_descriptors = []

            def recvmsg(self, bufsize, ancbufsize=0, flags=0):
                result = super().recvmsg(bufsize, ancbufsize, flags)
                integer_size = collector_module.array.array("i").itemsize
                for level, kind, data in result[1]:
                    if (
                        level == socket.SOL_SOCKET
                        and kind == socket.SCM_RIGHTS
                    ):
                        values = collector_module.array.array("i")
                        usable = len(data) - (len(data) % integer_size)
                        values.frombytes(data[:usable])
                        self.received_descriptors.extend(values)
                return result

        def make_round5_named_resources(parent, token):
            with patch.object(
                collector_module.tempfile,
                "gettempdir",
                return_value=str(parent),
            ), patch.object(
                collector_module.secrets,
                "token_hex",
                return_value=token,
            ):
                return collector_module._new_integrated_lifecycle_resources(
                    run_hash="1" * 64,
                    scope_hash="2" * 64,
                    pidfds_before={},
                    controller_uid=os.getuid(),
                    runner_uid=os.getuid(),
                )

        def attach_round5_guard(resources, suffix):
            events = []
            table_name = "txnmem_" + suffix * 16
            guard = SimpleNamespace(
                active=True,
                table_name=table_name,
                _expected_snapshot=None,
                _table_names=lambda: {table_name},
                snapshot=lambda: {"table": "retained"},
                deactivate=lambda: events.append("deactivate"),
            )
            resources.guard = guard
            resources.guard_owner = guard_owner_type(
                guard=guard,
                owner_pid=os.getpid(),
            )
            return events

        def receive_round5(channel, resources, *, timeout=1.0):
            return receive_state(
                channel,
                timeout=timeout,
                resources=resources,
            )

        def finalize_round5(resources, primary):
            signal_calls = []
            uid_checks = []

            def signal_exact(identities, signal_number):
                signal_calls.append((dict(identities), signal_number))
                return len(identities)

            def require_uid(uid, *, expected, **_kwargs):
                uid_checks.append((uid, dict(expected)))
                return dict(expected)

            caught = None
            with patch.object(
                collector_module,
                "_signal_integrated_lifecycle_identities",
                side_effect=signal_exact,
            ), patch.object(
                collector_module,
                "_integrated_lifecycle_observed_start_ticks",
                return_value=None,
            ), patch.object(
                collector_module.os,
                "waitpid",
                side_effect=lambda pid, _options: (pid, signal.SIGKILL),
            ), patch.object(
                collector_module,
                "_require_formal_uid_processes",
                side_effect=require_uid,
            ), patch.object(
                collector_module,
                "_integrated_lifecycle_pidfds",
                return_value={},
            ), patch.object(
                collector_module.os,
                "killpg",
                side_effect=AssertionError(
                    "integrated lifecycle must not use killpg fallback"
                ),
            ) as killpg_spy:
                try:
                    finalize_resources(
                        resources,
                        primary_failure=primary,
                        primary_traceback=(
                            None if primary is None else primary.__traceback__
                        ),
                    )
                except BaseException as exc:
                    caught = exc
            return caught, signal_calls, uid_checks, killpg_spy.call_count

        def traceback_contains(traceback_value, expected):
            while traceback_value is not None:
                if traceback_value is expected:
                    return True
                traceback_value = traceback_value.tb_next
            return False

        with self.subTest(round5_gap="genuine-collision-preflight"):
            real_mkdir = collector_module.os.mkdir
            mkdir_calls = []
            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                resources = make_round5_named_resources(parent, "7" * 32)
                lifecycle = resources.lifecycle_root
                lifecycle.mkdir(mode=0o700)
                marker = lifecycle / "foreign-marker"
                marker.write_text("foreign\n", encoding="utf-8")
                guard_events = attach_round5_guard(resources, "7")

                def collision_mkdir(*args, **kwargs):
                    mkdir_calls.append((args, kwargs))
                    return real_mkdir(*args, **kwargs)

                acquisition_failure = None
                with patch.object(
                    collector_module.os,
                    "mkdir",
                    side_effect=collision_mkdir,
                ):
                    try:
                        collector_module._acquire_integrated_lifecycle_root(
                            resources
                        )
                    except BaseException as exc:
                        acquisition_failure = exc
                caught, _signals, _uids, killpg_calls = finalize_round5(
                    resources,
                    acquisition_failure,
                )
                self.assertIs(caught, acquisition_failure)
                self.assertTrue(resources.root_name_collision)
                self.assertTrue(lifecycle.is_dir())
                self.assertEqual(
                    marker.read_text(encoding="utf-8"),
                    "foreign\n",
                )
                self.assertFalse(resources.tree_cleanup.root_removed)
                self.assertEqual(guard_events, [])
                self.assertEqual(killpg_calls, 0)
                self.assertEqual(
                    mkdir_calls,
                    [],
                    "a genuine collision must be identified before mkdir",
                )

        with self.subTest(round5_gap="post-success-file-exists-provenance"):
            real_mkdir = collector_module.os.mkdir
            primary = FileExistsError("post-success-file-exists")
            primary_origins = []
            boundary = {"mkdir_succeeded": False, "raised": False}
            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                resources = make_round5_named_resources(parent, "8" * 32)
                lifecycle = resources.lifecycle_root
                marker = lifecycle / "owned-marker"
                guard_events = attach_round5_guard(resources, "8")

                def successful_root_mkdir(path, *args, **kwargs):
                    result = real_mkdir(path, *args, **kwargs)
                    if os.fspath(path) == resources.lifecycle_name:
                        marker.write_text("owned\n", encoding="utf-8")
                        boundary["mkdir_succeeded"] = True
                    return result

                def post_success_trace(frame, event, _arg):
                    if (
                        event == "line"
                        and frame.f_code
                        is collector_module._acquire_integrated_lifecycle_root.__code__
                        and boundary["mkdir_succeeded"]
                        and not boundary["raised"]
                    ):
                        boundary["raised"] = True
                        sys.settrace(None)
                        try:
                            raise primary
                        except BaseException as exc:
                            primary_origins.append(exc.__traceback__)
                            raise
                    return post_success_trace

                acquisition_failure = None
                previous_trace = sys.gettrace()
                try:
                    with patch.object(
                        collector_module.os,
                        "mkdir",
                        side_effect=successful_root_mkdir,
                    ):
                        sys.settrace(post_success_trace)
                        try:
                            collector_module._acquire_integrated_lifecycle_root(
                                resources
                            )
                        except BaseException as exc:
                            acquisition_failure = exc
                finally:
                    sys.settrace(previous_trace)
                caught, _signals, _uids, killpg_calls = finalize_round5(
                    resources,
                    acquisition_failure,
                )
                self.assertTrue(boundary["raised"])
                self.assertIs(acquisition_failure, primary)
                self.assertIs(caught, primary)
                self.assertFalse(resources.root_name_collision)
                self.assertTrue(
                    getattr(resources, "root_absent_before_attempt", False)
                )
                self.assertTrue(resources.tree_cleanup.root_removed)
                self.assertFalse(lifecycle.exists())
                self.assertFalse(marker.exists())
                self.assertEqual(guard_events, [])
                self.assertEqual(killpg_calls, 0)
                self.assertTrue(
                    traceback_contains(caught.__traceback__, primary_origins[0])
                )

        with self.subTest(round5_gap="directory-close-no-numeric-retry"):
            real_open = collector_module.os.open
            real_close = collector_module.os.close
            primary = Round5CloseAfter("directory-close-after-effect")
            parent_descriptors = []
            close_attempts = []
            replacement_installed = False
            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                resources = make_round5_named_resources(parent, "9" * 32)
                lifecycle = resources.lifecycle_root
                guard_events = attach_round5_guard(resources, "9")
                replacement_source = real_open(
                    parent,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                )

                def tracked_open(path, *args, **kwargs):
                    descriptor = real_open(path, *args, **kwargs)
                    if (
                        os.fspath(path) == os.fspath(parent)
                        and kwargs.get("dir_fd") is None
                    ):
                        parent_descriptors.append(descriptor)
                    return descriptor

                def close_after_effect(descriptor):
                    nonlocal replacement_installed
                    if descriptor in parent_descriptors:
                        close_attempts.append(descriptor)
                        if not replacement_installed:
                            real_close(descriptor)
                            os.dup2(replacement_source, descriptor)
                            replacement_installed = True
                            raise primary
                    return real_close(descriptor)

                acquisition_failure = None
                with patch.object(
                    collector_module.os,
                    "open",
                    side_effect=tracked_open,
                ), patch.object(
                    collector_module.os,
                    "close",
                    side_effect=close_after_effect,
                ):
                    try:
                        collector_module._acquire_integrated_lifecycle_root(
                            resources
                        )
                    except BaseException as exc:
                        acquisition_failure = exc
                caught, _signals, _uids, killpg_calls = finalize_round5(
                    resources,
                    acquisition_failure,
                )
                replacement_alive = False
                if parent_descriptors:
                    try:
                        os.fstat(parent_descriptors[0])
                    except OSError:
                        pass
                    else:
                        replacement_alive = True
                        real_close(parent_descriptors[0])
                real_close(replacement_source)
                self.assertEqual(len(parent_descriptors), 1)
                self.assertTrue(replacement_installed)
                self.assertIs(acquisition_failure, primary)
                self.assertIs(caught, primary)
                self.assertEqual(len(close_attempts), 1)
                self.assertTrue(replacement_alive)
                self.assertTrue(resources.tree_cleanup.root_removed)
                self.assertFalse(lifecycle.exists())
                self.assertEqual(guard_events, [])
                self.assertEqual(killpg_calls, 0)

        with self.subTest(round5_gap="receiver-return-full-state-adoption"):
            worker_pid = 100
            worker_start = "10"
            primary = Round5Primary("receiver-return-full-state-adoption")
            primary_origins = []
            receive_boundary = {"returned": False, "raised": False}
            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                resources, lifecycle, _endpoints = make_round4_resources(
                    parent,
                    channels=False,
                )
                sender, base_receiver = socket.socketpair()
                receiver = Round5RecordingReceiveSocket(
                    fileno=base_receiver.detach()
                )
                resources.parent_channel = receiver
                setup = make_round4_setup(resources, lifecycle)
                guard_events = attach_round5_guard(resources, "a")
                setup.guard = resources.guard
                setup.guard_owner = resources.guard_owner
                receipt_read, receipt_write = os.pipe()
                sender.sendmsg(
                    [collector_module.canonical_json_bytes(state)],
                    [
                        (
                            socket.SOL_SOCKET,
                            socket.SCM_RIGHTS,
                            collector_module.array.array(
                                "i", [receipt_read]
                            ).tobytes(),
                        )
                    ],
                )

                def adopt_worker(_resources, _worker_main):
                    collector_module._adopt_integrated_lifecycle_worker(
                        _resources,
                        worker_pid,
                        worker_start,
                    )
                    return worker_pid, worker_start

                def receive_then_return(channel, **_kwargs):
                    observed = receive_round5(channel, resources)
                    receive_boundary["returned"] = True
                    return observed

                def caller_boundary_trace(frame, event, _arg):
                    if (
                        event == "line"
                        and frame.f_code
                        is collector_module._run_protected_linux_integrated_lifecycle_body.__code__
                        and receive_boundary["returned"]
                        and not receive_boundary["raised"]
                    ):
                        receive_boundary["raised"] = True
                        sys.settrace(None)
                        try:
                            raise primary
                        except BaseException as exc:
                            primary_origins.append(exc.__traceback__)
                            raise
                    return caller_boundary_trace

                body_failure = None
                previous_trace = sys.gettrace()
                try:
                    with patch.object(
                        collector_module,
                        "_prepare_integrated_lifecycle_setup",
                        return_value=setup,
                    ), patch.object(
                        collector_module,
                        "_fork_integrated_lifecycle_worker",
                        side_effect=adopt_worker,
                    ), patch.object(
                        collector_module,
                        "_receive_integrated_lifecycle_worker_state",
                        side_effect=receive_then_return,
                    ), patch.object(
                        collector_module,
                        "_require_formal_uid_processes",
                        return_value={},
                    ):
                        sys.settrace(caller_boundary_trace)
                        try:
                            collector_module._run_protected_linux_integrated_lifecycle_body(
                                root=Path(__file__).resolve().parents[1],
                                source_identity={},
                                pidfds_before={},
                                resources=resources,
                            )
                        except BaseException as exc:
                            body_failure = exc
                finally:
                    sys.settrace(previous_trace)
                caught, signal_calls, _uids, killpg_calls = finalize_round5(
                    resources,
                    body_failure,
                )
                transferred_descriptor = receiver.received_descriptors[0]
                transferred_closed = False
                try:
                    os.fstat(transferred_descriptor)
                except OSError:
                    transferred_closed = True
                sender.close()
                os.close(receipt_read)
                os.close(receipt_write)
                if not transferred_closed:
                    os.close(transferred_descriptor)
                expected_identities = {
                    worker_pid: worker_start,
                    101: "11",
                    102: "12",
                }
                self.assertTrue(receive_boundary["returned"])
                self.assertTrue(receive_boundary["raised"])
                self.assertIs(body_failure, primary)
                self.assertIs(caught, primary)
                self.assertEqual(
                    resources.recorded_identities,
                    expected_identities,
                )
                self.assertEqual(
                    signal_calls,
                    [(expected_identities, signal.SIGKILL)],
                )
                self.assertEqual(
                    set(resources.reaped_statuses),
                    set(expected_identities),
                )
                self.assertTrue(transferred_closed)
                self.assertTrue(resources.tree_cleanup.root_removed)
                self.assertFalse(lifecycle.exists())
                self.assertEqual(guard_events, [])
                self.assertEqual(killpg_calls, 0)
                self.assertTrue(
                    traceback_contains(caught.__traceback__, primary_origins[0])
                )

        with self.subTest(round5_gap="invalid-scm-close-before-effect"):
            real_close = collector_module.os.close
            worker_pid = 200
            worker_start = "20"
            close_primary = Round5CloseBefore(
                "invalid-scm-close-before-effect"
            )
            close_attempts = []
            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                resources, lifecycle, _endpoints = make_round4_resources(
                    parent,
                    channels=False,
                )
                collector_module._adopt_integrated_lifecycle_worker(
                    resources,
                    worker_pid,
                    worker_start,
                )
                guard_events = attach_round5_guard(resources, "b")
                sender, base_receiver = socket.socketpair()
                receiver = MutatingReceiveSocket(
                    fileno=base_receiver.detach(),
                    mutation="unexpected",
                    restore_failure=False,
                    fd_capacity=1,
                )
                resources.parent_channel = receiver
                receipt_read, receipt_write = os.pipe()
                sender.sendmsg(
                    [collector_module.canonical_json_bytes(state)],
                    [
                        (
                            socket.SOL_SOCKET,
                            socket.SCM_RIGHTS,
                            collector_module.array.array(
                                "i", [receipt_read]
                            ).tobytes(),
                        )
                    ],
                )
                object_close = (
                    collector_module._close_integrated_lifecycle_fd_object
                )
                object_failed = False

                def fail_object_close(file_object):
                    nonlocal object_failed
                    descriptor = file_object.fileno()
                    if (
                        descriptor in receiver.received_descriptors
                        and not object_failed
                    ):
                        object_failed = True
                        close_attempts.append(descriptor)
                        raise close_primary
                    return object_close(file_object)

                close_context = patch.object(
                    collector_module,
                    "_close_integrated_lifecycle_fd_object",
                    side_effect=fail_object_close,
                )
                receive_failure = None
                with close_context, patch.object(
                    collector_module,
                    "_require_formal_uid_processes",
                    return_value={},
                ):
                    try:
                        receive_round5(receiver, resources)
                    except BaseException as exc:
                        receive_failure = exc
                caught, _signals, _uids, killpg_calls = finalize_round5(
                    resources,
                    receive_failure,
                )
                self.assertEqual(len(receiver.received_descriptors), 1)
                received_descriptor = receiver.received_descriptors[0]
                descriptor_open = True
                try:
                    os.fstat(received_descriptor)
                except OSError:
                    descriptor_open = False
                if descriptor_open:
                    real_close(received_descriptor)
                sender.close()
                real_close(receipt_read)
                real_close(receipt_write)
                self.assertIs(caught, receive_failure)
                self.assertIsInstance(receive_failure, CollectorError)
                self.assertRegex(str(receive_failure), "worker state is invalid")
                self.assertEqual(len(close_attempts), 1)
                self.assertFalse(descriptor_open)
                self.assertTrue(resources.tree_cleanup.root_removed)
                self.assertFalse(lifecycle.exists())
                self.assertEqual(guard_events, [])
                self.assertEqual(killpg_calls, 0)

        with self.subTest(round5_gap="close-after-effect-same-pipe-reuse"):
            real_close = collector_module.os.close
            worker_pid = 300
            worker_start = "30"
            close_primary = Round5CloseAfter(
                "close-after-effect-same-pipe-reuse"
            )
            close_attempts = []
            replacement_installed = False
            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                resources, lifecycle, _endpoints = make_round4_resources(
                    parent,
                    channels=False,
                )
                collector_module._adopt_integrated_lifecycle_worker(
                    resources,
                    worker_pid,
                    worker_start,
                )
                guard_events = attach_round5_guard(resources, "c")
                sender, base_receiver = socket.socketpair()
                receiver = Round5RecordingReceiveSocket(
                    fileno=base_receiver.detach()
                )
                resources.parent_channel = receiver
                receipt_read, receipt_write = os.pipe()
                sender.sendmsg(
                    [collector_module.canonical_json_bytes(state)],
                    [
                        (
                            socket.SOL_SOCKET,
                            socket.SCM_RIGHTS,
                            collector_module.array.array(
                                "i", [receipt_read]
                            ).tobytes(),
                        )
                    ],
                )
                with patch.object(
                    collector_module,
                    "_require_formal_uid_processes",
                    return_value={},
                ):
                    receive_round5(receiver, resources)
                self.assertEqual(len(receiver.received_descriptors), 1)
                transferred_descriptor = receiver.received_descriptors[0]
                object_close = (
                    collector_module._close_integrated_lifecycle_fd_object
                )

                def close_after_object(file_object):
                    nonlocal replacement_installed
                    descriptor = file_object.fileno()
                    close_attempts.append(descriptor)
                    object_close(file_object)
                    os.dup2(receipt_read, transferred_descriptor)
                    replacement_installed = True
                    raise close_primary

                close_context = patch.object(
                    collector_module,
                    "_close_integrated_lifecycle_fd_object",
                    side_effect=close_after_object,
                )
                receipt_close_failure = None
                with close_context:
                    try:
                        resources.receipt_owner.close_retained()
                    except BaseException as exc:
                        receipt_close_failure = exc
                caught, _signals, _uids, killpg_calls = finalize_round5(
                    resources,
                    receipt_close_failure,
                )
                replacement_alive = True
                try:
                    os.fstat(transferred_descriptor)
                except OSError:
                    replacement_alive = False
                if replacement_alive:
                    real_close(transferred_descriptor)
                sender.close()
                real_close(receipt_read)
                real_close(receipt_write)
                self.assertTrue(replacement_installed)
                self.assertIs(receipt_close_failure, close_primary)
                self.assertIs(caught, close_primary)
                self.assertEqual(len(close_attempts), 1)
                self.assertTrue(replacement_alive)
                self.assertTrue(resources.tree_cleanup.root_removed)
                self.assertFalse(lifecycle.exists())
                self.assertEqual(guard_events, [])
                self.assertEqual(killpg_calls, 0)

        with self.subTest(round5_gap="recvmsg-return-oserror-provenance"):
            worker_pid = 400
            worker_start = "40"
            primary = OSError("recvmsg-return-oserror-provenance")
            primary_origins = []
            boundary_raised = False
            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                resources, lifecycle, _endpoints = make_round4_resources(
                    parent,
                    channels=False,
                )
                collector_module._adopt_integrated_lifecycle_worker(
                    resources,
                    worker_pid,
                    worker_start,
                )
                guard_events = attach_round5_guard(resources, "d")
                sender, base_receiver = socket.socketpair()
                receiver = Round5RecordingReceiveSocket(
                    fileno=base_receiver.detach()
                )
                resources.parent_channel = receiver
                receipt_read, receipt_write = os.pipe()
                sender.sendmsg(
                    [collector_module.canonical_json_bytes(state)],
                    [
                        (
                            socket.SOL_SOCKET,
                            socket.SCM_RIGHTS,
                            collector_module.array.array(
                                "i", [receipt_read]
                            ).tobytes(),
                        )
                    ],
                )

                def recvmsg_return_trace(frame, event, _arg):
                    nonlocal boundary_raised
                    if (
                        event == "line"
                        and frame.f_code
                        is collector_module._recv_integrated_lifecycle_message_owned.__code__
                        and frame.f_locals.get("recv_result") is not None
                        and not boundary_raised
                    ):
                        boundary_raised = True
                        sys.settrace(None)
                        try:
                            raise primary
                        except BaseException as exc:
                            primary_origins.append(exc.__traceback__)
                            raise
                    return recvmsg_return_trace

                receive_failure = None
                previous_trace = sys.gettrace()
                try:
                    with patch.object(
                        collector_module,
                        "_require_formal_uid_processes",
                        return_value={},
                    ):
                        sys.settrace(recvmsg_return_trace)
                        try:
                            receive_round5(receiver, resources)
                        except BaseException as exc:
                            receive_failure = exc
                finally:
                    sys.settrace(previous_trace)
                caught, _signals, _uids, killpg_calls = finalize_round5(
                    resources,
                    receive_failure,
                )
                received_descriptor = receiver.received_descriptors[0]
                received_closed = False
                try:
                    os.fstat(received_descriptor)
                except OSError:
                    received_closed = True
                sender.close()
                os.close(receipt_read)
                os.close(receipt_write)
                if not received_closed:
                    os.close(received_descriptor)
                self.assertTrue(boundary_raised)
                self.assertIs(receive_failure, primary)
                self.assertIs(caught, primary)
                self.assertTrue(received_closed)
                self.assertTrue(resources.tree_cleanup.root_removed)
                self.assertFalse(lifecycle.exists())
                self.assertEqual(guard_events, [])
                self.assertEqual(killpg_calls, 0)
                self.assertTrue(
                    traceback_contains(caught.__traceback__, primary_origins[0])
                )

        with self.subTest(round5_gap="signal-mask-restore-first-failure"):
            worker_pid = 500
            worker_start = "50"
            primary = Round5CloseBefore(
                "signal-mask-restore-first-failure"
            )
            restore_calls = []
            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                resources, lifecycle, _endpoints = make_round4_resources(
                    parent,
                    channels=False,
                )
                collector_module._adopt_integrated_lifecycle_worker(
                    resources,
                    worker_pid,
                    worker_start,
                )
                guard_events = attach_round5_guard(resources, "e")
                sender, base_receiver = socket.socketpair()
                receiver = Round5RecordingReceiveSocket(
                    fileno=base_receiver.detach()
                )
                resources.parent_channel = receiver
                receipt_read, receipt_write = os.pipe()
                sender.sendmsg(
                    [collector_module.canonical_json_bytes(state)],
                    [
                        (
                            socket.SOL_SOCKET,
                            socket.SCM_RIGHTS,
                            collector_module.array.array(
                                "i", [receipt_read]
                            ).tobytes(),
                        )
                    ],
                )

                def restore_mask(operation, previous):
                    restore_calls.append((operation, previous))
                    if len(restore_calls) == 1:
                        raise primary
                    return set()

                receive_failure = None
                with patch.object(
                    collector_module,
                    "_block_integrated_lifecycle_signals",
                    return_value=set(),
                ), patch.object(
                    collector_module.signal,
                    "pthread_sigmask",
                    side_effect=restore_mask,
                    create=True,
                ), patch.object(
                    collector_module,
                    "_require_formal_uid_processes",
                    return_value={},
                ):
                    try:
                        receive_round5(receiver, resources)
                    except BaseException as exc:
                        receive_failure = exc
                caught, _signals, _uids, killpg_calls = finalize_round5(
                    resources,
                    receive_failure,
                )
                received_descriptor = receiver.received_descriptors[0]
                received_closed = False
                try:
                    os.fstat(received_descriptor)
                except OSError:
                    received_closed = True
                sender.close()
                os.close(receipt_read)
                os.close(receipt_write)
                if not received_closed:
                    os.close(received_descriptor)
                self.assertIs(receive_failure, primary)
                self.assertIs(caught, primary)
                self.assertGreaterEqual(len(restore_calls), 2)
                self.assertTrue(received_closed)
                self.assertTrue(resources.tree_cleanup.root_removed)
                self.assertFalse(lifecycle.exists())
                self.assertEqual(guard_events, [])
                self.assertEqual(killpg_calls, 0)

        with self.subTest(
            round5_review_gap="collision-marker-line-event"
        ):
            primary = Round5Primary("collision-marker-line-event")
            primary_origins = []
            acquisition = (
                collector_module._acquire_integrated_lifecycle_root
            )
            acquisition_lines, acquisition_start = inspect.getsourcelines(
                acquisition
            )
            collision_else_offset = next(
                offset
                for offset, source_line in enumerate(acquisition_lines)
                if source_line.strip() == "else:"
            )
            collision_line = next(
                acquisition_start + offset
                for offset, source_line in enumerate(acquisition_lines)
                if offset > collision_else_offset
                and source_line.strip() == "raise CollectorError("
            )
            trace_raised = False
            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                resources = make_round5_named_resources(parent, "f" * 32)
                lifecycle = resources.lifecycle_root
                lifecycle.mkdir(mode=0o700)
                marker = lifecycle / "foreign-marker"
                marker.write_text("foreign\n", encoding="utf-8")
                guard_events = attach_round5_guard(resources, "f")

                def collision_marker_trace(frame, event, _arg):
                    nonlocal trace_raised
                    if (
                        event == "line"
                        and frame.f_code is acquisition.__code__
                        and frame.f_lineno == collision_line
                        and not trace_raised
                    ):
                        trace_raised = True
                        sys.settrace(None)
                        try:
                            raise primary
                        except BaseException as exc:
                            primary_origins.append(exc.__traceback__)
                            raise
                    return collision_marker_trace

                acquisition_failure = None
                previous_trace = sys.gettrace()
                try:
                    sys.settrace(collision_marker_trace)
                    try:
                        acquisition(resources)
                    except BaseException as exc:
                        acquisition_failure = exc
                finally:
                    sys.settrace(previous_trace)
                caught, _signals, _uids, killpg_calls = finalize_round5(
                    resources,
                    acquisition_failure,
                )
                self.assertTrue(trace_raised)
                self.assertIs(acquisition_failure, primary)
                self.assertIs(caught, primary)
                self.assertTrue(resources.root_name_collision)
                self.assertTrue(lifecycle.is_dir())
                self.assertEqual(
                    marker.read_text(encoding="utf-8"),
                    "foreign\n",
                )
                self.assertFalse(resources.tree_cleanup.root_removed)
                self.assertEqual(guard_events, [])
                self.assertEqual(killpg_calls, 0)
                self.assertTrue(
                    traceback_contains(caught.__traceback__, primary_origins[0])
                )

        with self.subTest(
            round5_review_gap="mkdir-race-collision-line-event"
        ):
            real_mkdir = collector_module.os.mkdir
            primary = Round5Primary("mkdir-race-collision-line-event")
            primary_origins = []
            acquisition = (
                collector_module._acquire_integrated_lifecycle_root
            )
            acquisition_lines, acquisition_start = inspect.getsourcelines(
                acquisition
            )
            race_collision_line = max(
                acquisition_start + offset
                for offset, source_line in enumerate(acquisition_lines)
                if "resources.root_name_collision = True" in source_line
            )
            collision_created = False
            trace_raised = False
            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                resources = make_round5_named_resources(parent, "3" * 32)
                lifecycle = resources.lifecycle_root
                marker = lifecycle / "racing-foreign-marker"
                guard_events = attach_round5_guard(resources, "3")

                def racing_mkdir(path, *args, **kwargs):
                    nonlocal collision_created
                    if not collision_created:
                        collision_created = True
                        lifecycle.mkdir(mode=0o700)
                        marker.write_text("foreign\n", encoding="utf-8")
                    return real_mkdir(path, *args, **kwargs)

                def race_collision_trace(frame, event, _arg):
                    nonlocal trace_raised
                    if (
                        event == "line"
                        and frame.f_code is acquisition.__code__
                        and frame.f_lineno == race_collision_line
                        and not trace_raised
                    ):
                        trace_raised = True
                        sys.settrace(None)
                        try:
                            raise primary
                        except BaseException as exc:
                            primary_origins.append(exc.__traceback__)
                            raise
                    return race_collision_trace

                acquisition_failure = None
                previous_trace = sys.gettrace()
                try:
                    with patch.object(
                        collector_module.os,
                        "mkdir",
                        side_effect=racing_mkdir,
                    ):
                        sys.settrace(race_collision_trace)
                        try:
                            acquisition(resources)
                        except BaseException as exc:
                            acquisition_failure = exc
                finally:
                    sys.settrace(previous_trace)
                caught, _signals, _uids, killpg_calls = finalize_round5(
                    resources,
                    acquisition_failure,
                )
                self.assertTrue(collision_created)
                self.assertTrue(trace_raised)
                self.assertIs(acquisition_failure, primary)
                self.assertIs(caught, primary)
                self.assertTrue(resources.root_name_collision)
                self.assertTrue(lifecycle.is_dir())
                self.assertEqual(
                    marker.read_text(encoding="utf-8"),
                    "foreign\n",
                )
                self.assertFalse(resources.tree_cleanup.root_removed)
                self.assertEqual(guard_events, [])
                self.assertEqual(killpg_calls, 0)
                self.assertTrue(
                    traceback_contains(caught.__traceback__, primary_origins[0])
                )

        with self.subTest(
            round5_review_gap="receiver-pre-ledger-line-event"
        ):
            primary = Round5Primary("receiver-pre-ledger-line-event")
            primary_origins = []
            adoption_function = (
                collector_module._adopt_integrated_lifecycle_worker_state_inventory
            )
            adoption_lines, adoption_start = inspect.getsourcelines(
                adoption_function
            )
            ledger_line = next(
                adoption_start + offset
                for offset, source_line in enumerate(adoption_lines)
                if "resources.recorded_identities.update" in source_line
            )
            trace_raised = False
            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                resources, lifecycle, _endpoints = make_round4_resources(
                    parent,
                    channels=False,
                )
                collector_module._adopt_integrated_lifecycle_worker(
                    resources,
                    100,
                    "10",
                )
                guard_events = attach_round5_guard(resources, "1")
                sender, base_receiver = socket.socketpair()
                receiver = Round5RecordingReceiveSocket(
                    fileno=base_receiver.detach()
                )
                resources.parent_channel = receiver
                receipt_read, receipt_write = os.pipe()
                sender.sendmsg(
                    [collector_module.canonical_json_bytes(state)],
                    [
                        (
                            socket.SOL_SOCKET,
                            socket.SCM_RIGHTS,
                            collector_module.array.array(
                                "i", [receipt_read]
                            ).tobytes(),
                        )
                    ],
                )

                def pre_ledger_trace(frame, event, _arg):
                    nonlocal trace_raised
                    if (
                        event == "line"
                        and frame.f_code is adoption_function.__code__
                        and frame.f_lineno == ledger_line
                        and not trace_raised
                    ):
                        trace_raised = True
                        sys.settrace(None)
                        try:
                            raise primary
                        except BaseException as exc:
                            primary_origins.append(exc.__traceback__)
                            raise
                    return pre_ledger_trace

                receive_failure = None
                previous_trace = sys.gettrace()
                try:
                    sys.settrace(pre_ledger_trace)
                    try:
                        receive_round5(receiver, resources)
                    except BaseException as exc:
                        receive_failure = exc
                finally:
                    sys.settrace(previous_trace)
                expected_identities = {
                    100: "10",
                    101: "11",
                    102: "12",
                }
                identities_before_finalize = dict(
                    resources.recorded_identities
                )
                caught, signal_calls, _uids, killpg_calls = finalize_round5(
                    resources,
                    receive_failure,
                )
                transferred_descriptor = receiver.received_descriptors[0]
                transferred_closed = False
                try:
                    os.fstat(transferred_descriptor)
                except OSError:
                    transferred_closed = True
                sender.close()
                os.close(receipt_read)
                os.close(receipt_write)
                if not transferred_closed:
                    os.close(transferred_descriptor)
                self.assertTrue(trace_raised)
                self.assertIs(receive_failure, primary)
                self.assertIs(caught, primary)
                self.assertEqual(
                    identities_before_finalize,
                    expected_identities,
                )
                self.assertEqual(
                    signal_calls,
                    [(expected_identities, signal.SIGKILL)],
                )
                self.assertTrue(transferred_closed)
                self.assertTrue(resources.tree_cleanup.root_removed)
                self.assertFalse(lifecycle.exists())
                self.assertEqual(guard_events, [])
                self.assertEqual(killpg_calls, 0)
                self.assertTrue(
                    traceback_contains(caught.__traceback__, primary_origins[0])
                )

        with self.subTest(
            round5_review_gap="finalizer-first-close-before-effect"
        ):
            primary = Round5Primary("finalizer-first-close-before-effect")
            close_primary = Round5CloseBefore(
                "finalizer-first-close-before-effect"
            )
            close_attempts = []
            real_object_close = (
                collector_module._close_integrated_lifecycle_fd_object
            )
            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                resources, lifecycle, _endpoints = make_round4_resources(
                    parent,
                    channels=False,
                )
                guard_events = attach_round5_guard(resources, "2")
                receipt_read, receipt_write = os.pipe()
                receipt_fd_owner = (
                    resources.fd_owners.adopt_descriptor(receipt_read)
                )
                resources.receipt_owner.adopt_owner(receipt_fd_owner)

                def fail_first_object_close(file_object):
                    close_attempts.append(file_object.fileno())
                    if len(close_attempts) == 1:
                        raise close_primary
                    return real_object_close(file_object)

                with patch.object(
                    collector_module,
                    "_close_integrated_lifecycle_fd_object",
                    side_effect=fail_first_object_close,
                ):
                    caught, _signals, _uids, killpg_calls = finalize_round5(
                        resources,
                        primary,
                    )
                descriptor_open = True
                try:
                    os.fstat(receipt_read)
                except OSError:
                    descriptor_open = False
                if descriptor_open:
                    os.close(receipt_read)
                os.close(receipt_write)
                self.assertIs(caught, primary)
                self.assertEqual(len(close_attempts), 2)
                self.assertFalse(descriptor_open)
                self.assertTrue(
                    any(
                        captured.exception is close_primary
                        for captured in resources.lifecycle_failures
                    )
                )
                self.assertTrue(resources.tree_cleanup.root_removed)
                self.assertFalse(lifecycle.exists())
                self.assertEqual(guard_events, [])
                self.assertEqual(killpg_calls, 0)

        with self.subTest(
            round5_review_gap="owner-slot-precedes-file-object"
        ):
            owner_type = collector_module._IntegratedLifecycleFdOwner
            owner_group_type = (
                collector_module._IntegratedLifecycleFdOwnerGroup
            )
            ensure_file_object = getattr(
                owner_type,
                "ensure_file_object",
                None,
            )
            self.assertTrue(callable(ensure_file_object))
            adoption_source = inspect.getsource(
                owner_group_type.adopt_descriptor
            )
            self.assertLess(
                adoption_source.index("self.owners.append(owner)"),
                adoption_source.index("owner.ensure_file_object()"),
            )
            ensure_lines, ensure_start = inspect.getsourcelines(
                ensure_file_object
            )
            materialize_offsets = [
                offset
                for offset, source_line in enumerate(ensure_lines)
                if "self.file_object = io.FileIO" in source_line
            ]
            self.assertEqual(len(materialize_offsets), 1)
            materialize_line = ensure_start + materialize_offsets[0]
            primary = Round5Primary("owner-slot-precedes-file-object")
            primary_origins = []
            trace_raised = False
            with TemporaryDirectory() as tmp:
                parent = Path(tmp).resolve()
                resources, lifecycle, _endpoints = make_round4_resources(
                    parent,
                    channels=False,
                )
                collector_module._adopt_integrated_lifecycle_worker(
                    resources,
                    100,
                    "10",
                )
                guard_events = attach_round5_guard(resources, "4")
                sender, base_receiver = socket.socketpair()
                receiver = Round5RecordingReceiveSocket(
                    fileno=base_receiver.detach()
                )
                resources.parent_channel = receiver
                receipt_read, receipt_write = os.pipe()
                sender.sendmsg(
                    [collector_module.canonical_json_bytes(state)],
                    [
                        (
                            socket.SOL_SOCKET,
                            socket.SCM_RIGHTS,
                            collector_module.array.array(
                                "i", [receipt_read]
                            ).tobytes(),
                        )
                    ],
                )

                def owner_materialization_trace(frame, event, _arg):
                    nonlocal trace_raised
                    if (
                        event == "line"
                        and frame.f_code is ensure_file_object.__code__
                        and frame.f_lineno == materialize_line
                        and not trace_raised
                    ):
                        trace_raised = True
                        sys.settrace(None)
                        try:
                            raise primary
                        except BaseException as exc:
                            primary_origins.append(exc.__traceback__)
                            raise
                    return owner_materialization_trace

                receive_failure = None
                previous_trace = sys.gettrace()
                try:
                    sys.settrace(owner_materialization_trace)
                    try:
                        receive_round5(receiver, resources)
                    except BaseException as exc:
                        receive_failure = exc
                finally:
                    sys.settrace(previous_trace)
                expected_identities = {
                    100: "10",
                    101: "11",
                    102: "12",
                }
                identities_before_finalize = dict(
                    resources.recorded_identities
                )
                caught, signal_calls, _uids, killpg_calls = finalize_round5(
                    resources,
                    receive_failure,
                )
                transferred_descriptor = receiver.received_descriptors[0]
                transferred_closed = False
                try:
                    os.fstat(transferred_descriptor)
                except OSError:
                    transferred_closed = True
                sender.close()
                os.close(receipt_read)
                os.close(receipt_write)
                if not transferred_closed:
                    os.close(transferred_descriptor)
                self.assertTrue(trace_raised)
                self.assertIs(receive_failure, primary)
                self.assertIs(caught, primary)
                self.assertEqual(len(resources.fd_owners.owners), 1)
                self.assertEqual(
                    identities_before_finalize,
                    expected_identities,
                )
                self.assertEqual(
                    signal_calls,
                    [(expected_identities, signal.SIGKILL)],
                )
                self.assertTrue(transferred_closed)
                self.assertTrue(
                    all(owner.closed for owner in resources.fd_owners.owners)
                )
                self.assertTrue(resources.tree_cleanup.root_removed)
                self.assertFalse(lifecycle.exists())
                self.assertEqual(guard_events, [])
                self.assertEqual(killpg_calls, 0)
                self.assertTrue(
                    traceback_contains(caught.__traceback__, primary_origins[0])
                )

        protected_primitives = bool(
            sys.platform.startswith("linux")
            and hasattr(os, "geteuid")
            and os.geteuid() == 0
            and Path("/proc/self/status").is_file()
            and hasattr(os, "O_TMPFILE")
            and callable(getattr(os, "pidfd_open", None))
            and callable(getattr(signal, "pidfd_send_signal", None))
            and Path(collector_module._FORMAL_NFT_EXECUTABLE).is_file()
            and controller_module.CONTROLLER_INSTALL_PATH.is_file()
            and controller_module.APPROVAL_MANIFEST_PATH.is_file()
        )
        if not protected_primitives:
            self.skipTest("protected Linux lifecycle primitives unavailable")
        try:
            collector_module._require_pidfd_support()
            with TemporaryDirectory() as probe_tmp:
                FormalStore(probe_tmp)._require_fd_bound_publication_support(
                    _require_credential_match=False
                )
        except (CollectorError, FormalIOError, OSError):
            self.skipTest("protected Linux lifecycle primitives unavailable")

        def current_pidfds():
            observed = {}
            for entry in (Path("/proc") / "self" / "fd").iterdir():
                try:
                    target = os.readlink(entry)
                except FileNotFoundError:
                    continue
                if "pidfd" in target:
                    observed[int(entry.name)] = target
            return observed

        guard_probe = collector_module._NftNetworkGuard(
            "txnmem_0000000000000000",
            backend_ipv4_subnet="10.253.0.0/24",
            ingress_ipv4_subnet="10.254.0.0/24",
            backend_bridge_interface="br-f3f3f3f3f3f3",
            ingress_bridge_interface="br-e3e3e3e3e3e3",
            toxiproxy_ingress_ipv4="10.254.0.2",
        )
        pidfds_before = current_pidfds()
        bootstrap_before = (
            set(controller_module.BOOTSTRAP_ROOT.iterdir())
            if controller_module.BOOTSTRAP_ROOT.is_dir()
            else set()
        )
        nft_tables_before = guard_probe._table_names()
        collector_module._require_formal_uid_processes(
            collector_module.FORMAL_RUNNER_UID,
            expected={},
        )

        repository = Path(__file__).resolve().parents[1]
        invocation = "\n".join(
            (
                "import importlib.util, json, pathlib, sys",
                "controller_path = pathlib.Path(sys.argv[1]).resolve(strict=True)",
                "project_root = pathlib.Path(sys.argv[2]).resolve(strict=True)",
                "spec = importlib.util.spec_from_file_location("
                "'_txnmem_installed_controller', controller_path)",
                "if spec is None or spec.loader is None: raise SystemExit(90)",
                "module = importlib.util.module_from_spec(spec)",
                "sys.modules[spec.name] = module",
                "spec.loader.exec_module(module)",
                "result = module._run_protected_linux_integrated_lifecycle("
                "project_root)",
                "print(json.dumps(result, sort_keys=True, separators=(',', ':')), "
                "flush=True)",
            )
        )
        with TemporaryDirectory() as output_tmp:
            output_root = Path(output_tmp).resolve()
            stdout_path = output_root / "stdout.txt"
            stderr_path = output_root / "stderr.txt"
            with stdout_path.open("wb") as stdout_stream, stderr_path.open(
                "wb"
            ) as stderr_stream:
                controller_process = subprocess.Popen(
                    (
                        sys.executable,
                        "-I",
                        "-S",
                        "-B",
                        "-c",
                        invocation,
                        str(controller_module.CONTROLLER_INSTALL_PATH),
                        str(repository),
                    ),
                    cwd=repository,
                    env={"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"},
                    stdin=subprocess.DEVNULL,
                    stdout=stdout_stream,
                    stderr=stderr_stream,
                )
                controller_pid = controller_process.pid
                try:
                    controller_status = controller_process.wait(timeout=30.0)
                except subprocess.TimeoutExpired:
                    controller_pidfd = os.pidfd_open(controller_pid)
                    try:
                        signal.pidfd_send_signal(
                            controller_pidfd, signal.SIGKILL
                        )
                    finally:
                        os.close(controller_pidfd)
                    controller_process.wait(timeout=5.0)
                    self.fail(f"{selector} controller timed out")
            self.assertEqual(
                controller_status,
                0,
                f"{selector} controller status={controller_status}",
            )
            self.assertFalse((Path("/proc") / str(controller_pid)).exists())
            raw_result = stdout_path.read_bytes()
            self.assertLessEqual(len(raw_result), 4096)
            self.assertEqual(raw_result.count(b"\n"), 1)
            result = json.loads(raw_result)

        true_fields = {
            "installed_controller_proven",
            "committed_export_proven",
            "collector_worker_proven",
            "immutable_runner_proven",
            "credential_drop_proven",
            "post_drop_parent_death_sigkill_proven",
            "actual_parent_death_proven",
            "resistant_descendant_proven",
            "pidfd_identity_drift_rejected",
            "pidfd_wrong_target_avoided",
            "guard_removed_last",
            "anonymous_pointer_visible",
            "completion_receipt_absent",
            "candidate_seal_invoked",
            "seal_rejected",
            "promotion_validator_invoked",
            "promotion_rejected",
        }
        count_fields = {
            "controller_process_count",
            "runner_process_count",
            "descendant_process_count",
            "dedicated_uid_process_count",
            "owned_pidfd_count",
            "anonymous_pointer_residue_count",
            "named_pointer_residue_count",
            "candidate_residue_count",
            "committed_export_residue_count",
            "nft_table_count",
        }
        self.assertEqual(
            set(result),
            {"schema", "selector", *true_fields, *count_fields},
        )
        self.assertEqual(
            result["schema"],
            "txnmem-protected-linux-integrated-lifecycle-v1",
        )
        self.assertEqual(result["selector"], selector)
        for field in true_fields:
            self.assertIs(result[field], True, field)
        for field in count_fields:
            self.assertIs(type(result[field]), int, field)
            self.assertEqual(result[field], 0, field)

        collector_module._require_formal_uid_processes(
            collector_module.FORMAL_RUNNER_UID,
            expected={},
        )
        self.assertEqual(current_pidfds(), pidfds_before)
        self.assertEqual(guard_probe._table_names(), nft_tables_before)
        bootstrap_after = (
            set(controller_module.BOOTSTRAP_ROOT.iterdir())
            if controller_module.BOOTSTRAP_ROOT.is_dir()
            else set()
        )
        self.assertEqual(bootstrap_after, bootstrap_before)

    def test_validated_group_identity_mismatch_sends_no_signal(self):
        valid = {
            "pid": 4242,
            "start_identity": "candidate:4242:99",
            "pgid": 4242,
            "sid": 4242,
        }
        cases = {
            "pid": {**valid, "pid": 4243},
            "start": {**valid, "start_identity": "candidate:4242:100"},
            "pgid": {**valid, "pgid": 4000},
            "sid": {**valid, "sid": 4000},
        }

        class Process:
            pid = 4242
            args = ("python", "runner.py")

            def poll(self):
                return None

        for name, observed in cases.items():
            with self.subTest(name=name):
                child = collector_module._GatedCandidate(
                    process=Process(),
                    _release_fd=None,
                    _receipt_fd=None,
                    ready_observed=True,
                )
                child.bind_process_identity("candidate:4242:99")
                with patch.object(
                    collector_module,
                    "_read_process_group_identity",
                    return_value=observed,
                ), patch.object(collector_module.os, "killpg") as killpg:
                    with self.assertRaisesRegex(CollectorError, "identity"):
                        child.terminate_validated_group()
                killpg.assert_not_called()

        child = collector_module._GatedCandidate(
            process=Process(),
            _release_fd=None,
            _receipt_fd=None,
            ready_observed=True,
        )
        child.bind_process_identity("candidate:4242:99")
        with patch.object(
            collector_module,
            "_read_process_group_identity",
            side_effect=CollectorError("candidate command line mismatch"),
        ), patch.object(collector_module.os, "killpg") as killpg:
            with self.assertRaisesRegex(CollectorError, "command line"):
                child.terminate_validated_group()
        killpg.assert_not_called()

    def test_formal_cleanup_is_monitor_child_progress_guard_order(self):
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

        class Child:
            def terminate_validated_group(self, *, term_seconds, kill_seconds):
                if (term_seconds, kill_seconds) != (5.0, 5.0):
                    raise AssertionError("cleanup grace periods changed")
                events.append("child")

            def require_quiescence(self):
                events.append("quiescence")

            def close(self):
                events.append("progress")

        failures = collector_module._cleanup_formal_execution_resources(
            execution_monitor=Monitor(), network_guard=Guard(), child=Child()
        )

        self.assertEqual(
            events,
            [
                "monitor",
                "child",
                "quiescence",
                "progress",
                "quiescence",
                "guard",
            ],
        )
        self.assertEqual(len(failures), 2)

    def test_guard_removal_rechecks_immediate_quiescence_and_retains_on_drift(self):
        events = []

        class Guard:
            active = True

            def deactivate(self):
                events.append("guard")

        class Child:
            def __init__(self):
                self.quiescence_checks = 0

            def terminate_validated_group(self, *, term_seconds, kill_seconds):
                events.append("child")

            def require_quiescence(self):
                self.quiescence_checks += 1
                events.append(f"quiescence-{self.quiescence_checks}")
                if self.quiescence_checks == 2:
                    raise CollectorError("candidate quiescence identity changed")

            def close(self):
                events.append("progress")

        guard = Guard()
        failures = collector_module._cleanup_formal_execution_resources(
            execution_monitor=None,
            network_guard=guard,
            child=Child(),
        )

        self.assertEqual(
            events,
            ["child", "quiescence-1", "progress", "quiescence-2"],
        )
        self.assertEqual(len(failures), 1)
        self.assertIs(guard.active, True)

    def test_cleanup_identity_failure_preserves_guard_and_is_hard_failure(self):
        events = []

        class Guard:
            active = True

            def deactivate(self):
                events.append("guard")

        class Child:
            def terminate_validated_group(self, *, term_seconds, kill_seconds):
                if (term_seconds, kill_seconds) != (5.0, 5.0):
                    raise AssertionError("cleanup grace periods changed")
                events.append("child")
                raise CollectorError("candidate process identity changed")

            def close(self):
                events.append("progress")

        guard = Guard()
        failures = collector_module._cleanup_formal_execution_resources(
            execution_monitor=None, network_guard=guard, child=Child()
        )

        self.assertEqual(events, ["child", "progress"])
        self.assertEqual(len(failures), 1)
        self.assertIs(guard.active, True)

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

    def test_topology_snapshot_accepts_exact_plain_text_version_and_strict_metrics(self):
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
                return b"2.5.0", 0.0
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
        self.assertIn(
            "src/txnmem_provenance_progress.py",
            collector_module._REQUIRED_SOURCE_PATHS,
        )
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
