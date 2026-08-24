import copy
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import txnmem_topology_attestation as topology_module

from txnmem_topology_attestation import (
    FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
    FORMAL_PROVENANCE_TOPOLOGY_ATTESTATION_SHA256_BY_RUN,
    TopologyAttestationError,
    _read_private_authorization_nonce,
    _validate_network_guard_attestation,
    _validate_sanitized_backend_isolation,
    execution_authorization_proof,
    sanitize_topology_attestation,
    _validate_runtime_manifest,
    validate_registered_topology_attestation,
)
from txnmem_provenance_contract import FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS
from txnmem_toxiproxy_metrics import (
    derive_proxy_counter_deltas,
    proxy_counter_payload_sha256,
)


def proxy_snapshot(phase, *, qdrant, neo4j):
    identities = (
        ("qdrant", "txnmem-qdrant", "0.0.0.0:19000", "qdrant:6333"),
        ("neo4j", "txnmem-neo4j", "0.0.0.0:19001", "neo4j:7687"),
    )
    routes = []
    for identity, values in zip(identities, (qdrant, neo4j)):
        routes.append(
            {
                "role": identity[0],
                "proxy_name": identity[1],
                "listener": identity[2],
                "upstream": identity[3],
                "received_upstream_bytes": values[0],
                "sent_upstream_bytes": values[1],
                "received_downstream_bytes": values[2],
                "sent_downstream_bytes": values[3],
                "total_bytes": sum(values),
            }
        )
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


def rehash_proxy_snapshot(document):
    payload = {
        key: copy.deepcopy(value)
        for key, value in document.items()
        if key != "snapshot_sha256"
    }
    document["snapshot_sha256"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


class TopologyAttestationTests(unittest.TestCase):
    AUTHORIZATION_NONCE = b"topology-fixture-authorization-nonce-0001"

    def test_formal_20260822_run_nonce_is_pre_registered(self):
        self.assertEqual(
            FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN.get(
                "68632b903c7feff62c1996b0b36d238eadaee0bfcdf00b6e60c65ff4d021e5ee"
            ),
            "520e62eb73f9293fe539c2a227e0297eed67969453c0d4213656e8319b3f40cd",
        )

    def test_formal_20260824_v2_run_nonce_is_pre_registered(self):
        self.assertEqual(
            FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN.get(
                "7651f39b4f8681ef7ce609f9b0f7e47b5d917867088194da9929c7d1581b4af0"
            ),
            "c3bf87f31151a021b142589048fc1b0be32bfaa9bd69662c59cbccb5a0851ef7",
        )

    def test_formal_20260824_v3_run_nonce_is_pre_registered(self):
        self.assertEqual(
            FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN.get(
                "4a2d2f6394325c21bb19432ae0b7a4c3b2de525fb19fdd2c55c5ee607e855c85"
            ),
            "e07bbf42cf32a71c834ecaf40128215ab066f826f35359f8c8ac2e77b2e84362",
        )

    def test_formal_20260825_v4_run_nonce_is_pre_registered(self):
        self.assertEqual(
            FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN.get(
                "2ba04699c01d587aa226360de75497c0e3b4ea8c42dae9283cb9d14178d30eda"
            ),
            "c2facfda6697bd2cd2a4c988145873e76c476815b3e158b1a7d3727f226c43bd",
        )

    def test_runtime_manifest_accepts_registered_system_python_3_10_12(self):
        launch, _completion = self._documents()
        runtime_manifest = copy.deepcopy(
            launch["command_manifest"]["runtime_manifest"]
        )
        runtime_manifest["python"]["version"] = "3.10.12"

        validated = _validate_runtime_manifest(runtime_manifest)

        self.assertEqual(validated["python"]["version"], "3.10.12")

    def test_network_guard_v3_requires_exact_schema_closure(self):
        launch, _completion = self._documents()
        guard = launch["network_guard"]

        validated = _validate_network_guard_attestation(guard)
        self.assertEqual(validated, guard)

        for name, mutation in (
            (
                "legacy_schema",
                lambda value: value.update(
                    {"schema": "txnmem-provenance-network-guard-v2"}
                ),
            ),
            (
                "missing_exact_destination",
                lambda value: value.pop("root_ingress_destination_exact"),
            ),
            ("extra_field", lambda value: value.update({"extra": True})),
        ):
            with self.subTest(name=name):
                drifted = copy.deepcopy(guard)
                mutation(drifted)
                with self.assertRaisesRegex(
                    TopologyAttestationError, "formal network guard"
                ):
                    _validate_network_guard_attestation(drifted)

    def test_network_guard_v3_rejects_bool_and_float_uids_and_ports(self):
        launch, _completion = self._documents()
        guard = launch["network_guard"]

        for name, field, replacement in (
            ("runner_uid_bool", "runner_uid", False),
            ("runner_uid_float", "runner_uid", 65532.0),
            ("controller_uid_bool", "controller_uid", False),
            ("controller_uid_float", "controller_uid", 0.0),
            (
                "loopback_first_float",
                "allowed_ipv4_loopback_ports",
                [19000.0, 19001],
            ),
            (
                "loopback_second_float",
                "allowed_ipv4_loopback_ports",
                [19000, 19001.0],
            ),
            (
                "root_ingress_first_float",
                "allowed_root_ingress_ports",
                [8474.0, 19000, 19001],
            ),
            (
                "root_ingress_second_float",
                "allowed_root_ingress_ports",
                [8474, 19000.0, 19001],
            ),
            (
                "root_ingress_third_float",
                "allowed_root_ingress_ports",
                [8474, 19000, 19001.0],
            ),
        ):
            with self.subTest(name=name):
                drifted = copy.deepcopy(guard)
                drifted[field] = replacement
                with self.assertRaisesRegex(
                    TopologyAttestationError, "formal network guard"
                ):
                    _validate_network_guard_attestation(drifted)

    def test_raw_launch_v4_and_completion_v5_require_exact_counter_fields(self):
        launch, completion = self._documents()
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
        launch["schema"] = "txnmem-provenance-execution-launch-raw-v4"
        launch["proxy_counter_baseline_a"] = baseline_a
        launch["proxy_counter_baseline_b"] = baseline_b
        launch["proxy_route_rearm_verified"] = True
        completion["schema"] = "txnmem-provenance-execution-completion-raw-v5"
        completion["proxy_counter_baseline_b_sha256"] = hashlib.sha256(
            json.dumps(
                {
                    "routes": baseline_b["routes"],
                    "toxiproxy_total_bytes": baseline_b["toxiproxy_total_bytes"],
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        completion["proxy_counter_final"] = final
        completion["proxy_counter_deltas"] = {
            "schema": "txnmem-provenance-proxy-counter-deltas-v1",
            "routes": [
                {
                    "role": "qdrant",
                    "proxy_name": "txnmem-qdrant",
                    "listener": "0.0.0.0:19000",
                    "upstream": "qdrant:6333",
                    "received_upstream_bytes": 10,
                    "sent_upstream_bytes": 20,
                    "received_downstream_bytes": 30,
                    "sent_downstream_bytes": 50,
                    "total_bytes": 110,
                },
                {
                    "role": "neo4j",
                    "proxy_name": "txnmem-neo4j",
                    "listener": "0.0.0.0:19001",
                    "upstream": "neo4j:7687",
                    "received_upstream_bytes": 50,
                    "sent_upstream_bytes": 60,
                    "received_downstream_bytes": 70,
                    "sent_downstream_bytes": 90,
                    "total_bytes": 270,
                },
            ],
            "toxiproxy_total_bytes": 380,
        }

        topology_module._validate_shared(
            launch,
            expected_schema="txnmem-provenance-execution-launch-raw-v4",
        )
        topology_module._validate_shared(
            completion,
            expected_schema="txnmem-provenance-execution-completion-raw-v5",
        )
        topology_module._validate_snapshot(launch["roles"])

        legacy_launch = copy.deepcopy(launch)
        legacy_launch["schema"] = "txnmem-provenance-execution-launch-raw-v3"
        with self.assertRaises(TopologyAttestationError):
            topology_module._validate_shared(
                legacy_launch,
                expected_schema="txnmem-provenance-execution-launch-raw-v4",
            )
        legacy_completion = copy.deepcopy(completion)
        legacy_completion["schema"] = (
            "txnmem-provenance-execution-completion-raw-v4"
        )
        with self.assertRaises(TopologyAttestationError):
            topology_module._validate_shared(
                legacy_completion,
                expected_schema="txnmem-provenance-execution-completion-raw-v5",
            )
        role_with_legacy_counter = copy.deepcopy(launch["roles"])
        role_with_legacy_counter[0]["proxy_counter_bytes"] = 0
        with self.assertRaises(TopologyAttestationError):
            topology_module._validate_snapshot(role_with_legacy_counter)

        for document, schema, field in (
            (
                launch,
                "txnmem-provenance-execution-launch-raw-v4",
                "proxy_counter_baseline_a",
            ),
            (
                completion,
                "txnmem-provenance-execution-completion-raw-v5",
                "proxy_counter_final",
            ),
        ):
            missing = copy.deepcopy(document)
            missing.pop(field)
            with self.assertRaises(TopologyAttestationError):
                topology_module._validate_shared(missing, expected_schema=schema)

        for name, document, schema in (
            (
                "launch",
                launch,
                "txnmem-provenance-execution-launch-raw-v4",
            ),
            (
                "completion",
                completion,
                "txnmem-provenance-execution-completion-raw-v5",
            ),
        ):
            with self.subTest(name=name):
                extra = copy.deepcopy(document)
                extra["extra"] = True
                with self.assertRaises(TopologyAttestationError):
                    topology_module._validate_shared(extra, expected_schema=schema)

    @staticmethod
    def _file_bytes(value):
        return (
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        ).encode("utf-8")

    def _documents(self):
        source_manifest = {
            "schema": "txnmem-provenance-source-manifest-v1",
            "source_commit": "a" * 40,
            "files": [
                {
                    "path": "src/txnmem_experiment.py",
                    "blob_sha256": "6" * 64,
                }
            ],
        }
        command_manifest = {
            "schema": "txnmem-provenance-command-manifest-v2",
            "transport": "container_bridge",
            "argv_sha256": "9" * 64,
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
            "python_executable_path_sha256": "a" * 64,
            "python_executable_sha256": "b" * 64,
            "python_implementation": "CPython",
            "python_version": "3.11.9",
            "runtime_manifest": {
                "schema": "txnmem-provenance-runtime-manifest-v1",
                "python": {
                    "implementation": "CPython",
                    "version": "3.11.9",
                    "executable_sha256": "b" * 64,
                    "build_sha256": "3" * 64,
                    "compiler_sha256": "4" * 64,
                    "platform_sha256": "5" * 64,
                },
                "distributions": [
                    {
                        "name": "neo4j",
                        "version": "5.28.1",
                        "files": [
                            {
                                "path": "neo4j/__init__.py",
                                "sha256": "6" * 64,
                            }
                        ],
                        "files_sha256": hashlib.sha256(
                            json.dumps(
                                [
                                    {
                                        "path": "neo4j/__init__.py",
                                        "sha256": "6" * 64,
                                    }
                                ],
                                sort_keys=True,
                                separators=(",", ":"),
                            ).encode("utf-8")
                        ).hexdigest(),
                        "declared_requirements_sha256": "7" * 64,
                    }
                ],
            },
            "working_directory_sha256": "c" * 64,
            "source_manifest_sha256": hashlib.sha256(
                json.dumps(
                    source_manifest, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")
            ).hexdigest(),
            "runner_sha256": "8" * 64,
            "config_file_sha256": "3" * 64,
            "run_id_sha256": "1" * 64,
            "candidate_root_sha256": "d" * 64,
            "environment_attestation_sha256": "5" * 64,
            "environment_attestation_file_sha256": "8" * 64,
            "qdrant_endpoint_sha256": "e" * 64,
            "qdrant_endpoint_port": 19000,
            "neo4j_endpoint_sha256": "f" * 64,
            "neo4j_endpoint_port": 19001,
            "toxiproxy_endpoint_sha256": "2" * 64,
            "literal_environment": {
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "PYTHONDONTWRITEBYTECODE": "1",
            },
            "hashed_environment": {
                "TXNMEM_NEO4J_URI": "0" * 64,
                "TXNMEM_NEO4J_USER": "1" * 64,
                "TXNMEM_PROVENANCE_RUNTIME_SITE": "4" * 64,
            },
            "secret_environment_variables": ["TXNMEM_NEO4J_PASSWORD"],
            "gate_environment_variable": "TXNMEM_PROVENANCE_START_GATE_FD",
            "ready_environment_variable": "TXNMEM_PROVENANCE_READY_FD",
            "completion_environment_variable": "TXNMEM_PROVENANCE_COMPLETION_FD",
            "completion_receipt_required": True,
            "runtime_environment_variable": "TXNMEM_PROVENANCE_RUNTIME_SITE",
            "inherited_environment": False,
        }
        command_manifest["runtime_manifest_sha256"] = hashlib.sha256(
            json.dumps(
                command_manifest["runtime_manifest"],
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        command_manifest["runtime_lock_file_sha256"] = "a" * 64
        command_manifest["runtime_snapshot_path_sha256"] = "4" * 64
        command_manifest["external_tools"] = [
            {
                "role": role,
                "requested_path_sha256": value * 64,
                "resolved_path_sha256": value * 64,
                "executable_sha256": "b" * 64 if role == "python" else value * 64,
                "owner_uid": 0,
                "mode": 0o555,
            }
            for role, value in (
                ("docker", "6"),
                ("git", "7"),
                ("nft", "8"),
                ("python", "9"),
            )
        ]
        child_process = {
            "pid": 1234,
            "start_identity": "candidate-process:1234:fixture-start",
            "uid": 65532,
            "executable_sha256": command_manifest["python_executable_sha256"],
            "argv_sha256": command_manifest["argv_sha256"],
            "cmdline_sha256": "2" * 64,
        }
        proxy_routes = [
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
        shared = {
            "collector_id": "txnmem-provenance-execution-collector-v1",
            "formal_execution_requested": True,
            "run_id_sha256": "1" * 64,
            "config_sha256": "2" * 64,
            "config_file_sha256": "3" * 64,
            "workload_sha256": "4" * 64,
            "environment_attestation_sha256": "5" * 64,
            "source_commit": "a" * 40,
            "source_manifest": source_manifest,
            "source_manifest_sha256": hashlib.sha256(
                json.dumps(
                    source_manifest, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")
            ).hexdigest(),
            "collector_sha256": "7" * 64,
            "runner_sha256": "8" * 64,
            "command_manifest": command_manifest,
            "command_sha256": hashlib.sha256(
                json.dumps(
                    command_manifest, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")
            ).hexdigest(),
            "child_process": child_process,
            "network_guard": {
                "schema": "txnmem-provenance-network-guard-v3",
                "table_name_sha256": "b" * 64,
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
                "policy_sha256": "c" * 64,
                "ruleset_sha256": "d" * 64,
            },
            "backend_isolation": {
                "schema": "txnmem-provenance-backend-isolation-v3",
                "network_name_sha256": "e" * 64,
                "network_id_sha256": "f" * 64,
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
            },
            "transport": "container_bridge",
            "matrix_cell_count": 15,
            "repetition_count": 450,
            "operation_sample_count": 14_400,
        }
        role_data = {
            "client": (
                "private-client-host",
                child_process["start_identity"],
                "3.11.9",
            ),
            "qdrant": ("private-backend-host", "qdrant-process-222", "1.15.4"),
            "neo4j": ("private-backend-host", "neo4j-process-333", "5.26.0"),
            "toxiproxy": ("private-client-host", "toxiproxy-process-444", "2.9.0"),
        }
        launch_roles = [
            {
                "role": role,
                "host_identity": values[0],
                "listener_owner": values[1],
                "service_version": values[2],
                "rtt_ms": 0.1 + index,
            }
            for index, (role, values) in enumerate(role_data.items())
        ]
        completion_roles = copy.deepcopy(launch_roles)
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
        proxy_deltas = derive_proxy_counter_deltas(baseline_b, final)
        candidate_id = (
            "diagnostic-vector_graph-" + "2" * 16 + "-" + "1" * 16
        )
        receipt_material = {
            "schema": "txnmem-provenance-candidate-attestation-material-v1",
            "candidate_bundle_id": candidate_id,
            "run_id_sha256": shared["run_id_sha256"],
            "config_sha256": shared["config_sha256"],
            "config_file_sha256": shared["config_file_sha256"],
            "workload_sha256": shared["workload_sha256"],
            "environment_attestation_sha256": shared[
                "environment_attestation_sha256"
            ],
            "evidence_manifest_sha256": "b" * 64,
            "matrix_cell_count": shared["matrix_cell_count"],
            "repetition_count": shared["repetition_count"],
            "operation_sample_count": shared["operation_sample_count"],
            "observed_service_versions": {
                role: next(
                    row["service_version"]
                    for row in completion_roles
                    if row["role"] == role
                )
                for role in ("qdrant", "neo4j", "toxiproxy")
            },
            "candidate_operation_samples_sha256": "c" * 64,
            "candidate_repetitions_sha256": "d" * 64,
        }
        candidate_seal = {
            "schema": "txnmem-provenance-candidate-seal-v1",
            "root_device": 11,
            "root_inode": 22,
            "directory_count": 3,
            "file_count": 4,
            "tree_sha256": "e" * 64,
            "completion_receipt_sha256": hashlib.sha256(
                json.dumps(
                    receipt_material, sort_keys=True, separators=(",", ":")
                ).encode("utf-8")
            ).hexdigest(),
        }
        launch = {
            "schema": "txnmem-provenance-execution-launch-raw-v4",
            **shared,
            "roles": launch_roles,
            "proxy_routes": copy.deepcopy(proxy_routes),
            "proxy_counter_baseline_a": baseline_a,
            "proxy_counter_baseline_b": baseline_b,
            "proxy_route_rearm_verified": True,
            "authorization_nonce_sha256": hashlib.sha256(
                self.AUTHORIZATION_NONCE
            ).hexdigest(),
        }
        launch["authorization_proof_sha256"] = execution_authorization_proof(
            self.AUTHORIZATION_NONCE, launch
        )
        launch_hash = hashlib.sha256(self._file_bytes(launch)).hexdigest()
        completion = {
            "schema": "txnmem-provenance-execution-completion-raw-v5",
            **shared,
            "launch_file_sha256": launch_hash,
            "exit_code": 0,
            "candidate_bundle_id": candidate_id,
            "evidence_manifest_sha256": "b" * 64,
            "candidate_operation_samples_sha256": "c" * 64,
            "candidate_repetitions_sha256": "d" * 64,
            "candidate_seal": candidate_seal,
            "execution_monitor": {
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
            },
            "roles": completion_roles,
            "proxy_routes": copy.deepcopy(proxy_routes),
            "proxy_counter_baseline_b_sha256": proxy_counter_payload_sha256(
                baseline_b
            ),
            "proxy_counter_final": final,
            "proxy_counter_deltas": proxy_deltas,
            "authorization_nonce_sha256": hashlib.sha256(
                self.AUTHORIZATION_NONCE
            ).hexdigest(),
        }
        completion["authorization_proof_sha256"] = execution_authorization_proof(
            self.AUTHORIZATION_NONCE, completion
        )
        return copy.deepcopy(launch), copy.deepcopy(completion)

    def _sanitize(self, launch=None, completion=None):
        launch_default, completion_default = self._documents()
        launch = launch or launch_default
        completion = completion or completion_default
        launch_raw = self._file_bytes(launch)
        default_launch_hash = hashlib.sha256(
            self._file_bytes(launch_default)
        ).hexdigest()
        preserve_explicit_launch_hash = (
            completion.get("launch_file_sha256") != default_launch_hash
        )
        launch["authorization_proof_sha256"] = execution_authorization_proof(
            self.AUTHORIZATION_NONCE, launch
        )
        launch_raw = self._file_bytes(launch)
        if not preserve_explicit_launch_hash:
            completion["launch_file_sha256"] = hashlib.sha256(launch_raw).hexdigest()
        completion["authorization_proof_sha256"] = execution_authorization_proof(
            self.AUTHORIZATION_NONCE, completion
        )
        completion_raw = self._file_bytes(completion)
        with patch.dict(
            FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
            {
                launch["run_id_sha256"]: hashlib.sha256(
                    self.AUTHORIZATION_NONCE
                ).hexdigest()
            },
            clear=True,
        ):
            return sanitize_topology_attestation(
                launch,
                completion,
                launch_file_sha256=hashlib.sha256(launch_raw).hexdigest(),
                completion_file_sha256=hashlib.sha256(completion_raw).hexdigest(),
                authorization_nonce=self.AUTHORIZATION_NONCE,
            )

    @staticmethod
    def _validation_arguments():
        return {
            "expected_run_id_sha256": "1" * 64,
            "expected_config_sha256": "2" * 64,
            "expected_config_file_sha256": "3" * 64,
            "expected_workload_sha256": "4" * 64,
            "expected_environment_attestation_sha256": "5" * 64,
            "expected_evidence_manifest_sha256": "b" * 64,
            "expected_candidate_bundle_id": "diagnostic-vector_graph-"
            + "2" * 16
            + "-"
            + "1" * 16,
            "expected_candidate_operation_samples_sha256": "c" * 64,
            "expected_candidate_repetitions_sha256": "d" * 64,
        }

    @staticmethod
    def _rehash_sanitized(attestation):
        without_hash = copy.deepcopy(attestation)
        without_hash.pop("attestation_sha256", None)
        attestation["attestation_sha256"] = hashlib.sha256(
            json.dumps(
                without_hash,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    def test_sanitization_hashes_identity_and_binds_two_execution_phases(self):
        sanitized = self._sanitize()
        encoded = json.dumps(sanitized, sort_keys=True)
        roles = {row["role"]: row for row in sanitized["roles"]}

        self.assertEqual(sanitized["schema"], "txnmem-topology-attestation-v6")
        self.assertNotIn("private-client-host", encoded)
        self.assertNotIn("private-backend-host", encoded)
        self.assertNotIn("process-", encoded)
        self.assertEqual(sanitized["host_count"], 2)
        self.assertEqual(
            roles["qdrant"]["host_identity_sha256"],
            roles["neo4j"]["host_identity_sha256"],
        )
        self.assertTrue(sanitized["source_continuity_verified"])
        self.assertTrue(sanitized["listener_continuity_verified"])
        self.assertTrue(sanitized["host_continuity_verified"])
        self.assertTrue(sanitized["toxiproxy_route_observed"])
        self.assertEqual(sanitized["execution_monitor"]["duration_ns"], 500_000_000)
        self.assertEqual(
            sanitized["execution_monitor"]["execution_coverage_ns"],
            300_000_000,
        )
        self.assertEqual(
            sanitized["execution_monitor"]["terminal_coverage_gap_ns"],
            100_000_000,
        )
        self.assertTrue(
            sanitized["backend_isolation"]["backend_network_internal"]
        )
        self.assertTrue(
            sanitized["backend_isolation"]["ingress_network_external"]
        )
        self.assertTrue(
            sanitized["backend_isolation"]["ingress_proxy_only"]
        )
        self.assertEqual(
            sanitized["backend_isolation"]["schema"],
            "txnmem-provenance-backend-isolation-sanitized-v3",
        )
        self.assertNotIn(
            "toxiproxy_ingress_ipv4", sanitized["backend_isolation"]
        )
        self.assertNotIn("172.20.0.2", encoded)
        self.assertEqual(
            sanitized["backend_isolation"]["toxiproxy_ingress_ipv4_sha256"],
            hashlib.sha256(b"172.20.0.2").hexdigest(),
        )
        self.assertTrue(
            sanitized["backend_isolation"]["toxiproxy_ingress_membership_verified"]
        )
        self.assertTrue(
            sanitized["backend_isolation"][
                "ingress_unique_workload_container_verified"
            ]
        )
        self.assertEqual(
            [
                row["role"]
                for row in sanitized["backend_isolation"]["containers"]
            ],
            ["qdrant", "neo4j", "toxiproxy"],
        )
        self.assertEqual(
            sanitized["command_manifest"]["argv_sha256"], "9" * 64
        )
        self.assertEqual(
            sanitized["child_process"]["start_identity_sha256"],
            hashlib.sha256(
                b"candidate-process:1234:fixture-start"
            ).hexdigest(),
        )
        self.assertEqual(
            sanitized["proxy_routes"],
            [
                {
                    "role": "qdrant",
                    "proxy_name": "txnmem-qdrant",
                    "listen_port": 19000,
                    "upstream_service": "qdrant",
                    "upstream_port": 6333,
                    "enabled": True,
                    "toxics_count": 0,
                },
                {
                    "role": "neo4j",
                    "proxy_name": "txnmem-neo4j",
                    "listen_port": 19001,
                    "upstream_service": "neo4j",
                    "upstream_port": 7687,
                    "enabled": True,
                    "toxics_count": 0,
                },
            ],
        )
        launch, completion = self._documents()
        self.assertEqual(
            sanitized["proxy_counter_attribution"],
            {
                "schema": "txnmem-provenance-proxy-attribution-v1",
                "baseline_a_sha256": launch["proxy_counter_baseline_a"][
                    "snapshot_sha256"
                ],
                "baseline_b_sha256": launch["proxy_counter_baseline_b"][
                    "snapshot_sha256"
                ],
                "final_sha256": completion["proxy_counter_final"][
                    "snapshot_sha256"
                ],
                "boundary_values_equal": True,
                "route_rearmed": True,
                "qdrant_delta_bytes": 110,
                "neo4j_delta_bytes": 270,
                "toxiproxy_delta_bytes": 380,
                "component_deltas_sha256": hashlib.sha256(
                    json.dumps(
                        completion["proxy_counter_deltas"],
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest(),
            },
        )
        self.assertEqual(
            {
                role: (
                    row["proxy_counter_bytes_before"],
                    row["proxy_counter_bytes_after"],
                    row["proxy_counter_bytes_delta"],
                )
                for role, row in roles.items()
            },
            {
                "client": (0, 0, 0),
                "qdrant": (60, 170, 110),
                "neo4j": (120, 390, 270),
                "toxiproxy": (180, 560, 380),
            },
        )
        self.assertGreater(roles["qdrant"]["proxy_counter_bytes_delta"], 0)
        self.assertGreater(roles["neo4j"]["proxy_counter_bytes_delta"], 0)
        self.assertRegex(sanitized["attestation_sha256"], r"^[0-9a-f]{64}$")
        self.assertNotIn("root_device", sanitized["candidate_seal"])
        self.assertNotIn("root_inode", sanitized["candidate_seal"])
        self.assertRegex(
            sanitized["candidate_seal"]["tree_sha256"], r"^[0-9a-f]{64}$"
        )

    def test_sanitization_independently_recomputes_proxy_attribution_mutations(self):
        cases = []

        for field in (
            "proxy_counter_baseline_a",
            "proxy_counter_baseline_b",
        ):
            launch, completion = self._documents()
            launch[field]["snapshot_sha256"] = "f" * 64
            cases.append((f"{field}_hash", launch, completion))
        launch, completion = self._documents()
        completion["proxy_counter_final"]["snapshot_sha256"] = "f" * 64
        cases.append(("proxy_counter_final_hash", launch, completion))

        launch, completion = self._documents()
        baseline_b_route = launch["proxy_counter_baseline_b"]["routes"][0]
        baseline_b_route["received_upstream_bytes"] += 1
        baseline_b_route["total_bytes"] += 1
        launch["proxy_counter_baseline_b"]["toxiproxy_total_bytes"] += 1
        rehash_proxy_snapshot(launch["proxy_counter_baseline_b"])
        cases.append(("boundary_component", launch, completion))

        launch, completion = self._documents()
        final_route = completion["proxy_counter_final"]["routes"][0]
        final_route["received_upstream_bytes"] += 1
        final_route["total_bytes"] += 1
        completion["proxy_counter_final"]["toxiproxy_total_bytes"] += 1
        rehash_proxy_snapshot(completion["proxy_counter_final"])
        cases.append(("final_component", launch, completion))

        launch, completion = self._documents()
        stored_delta = completion["proxy_counter_deltas"]["routes"][0]
        stored_delta["received_upstream_bytes"] += 1
        stored_delta["total_bytes"] += 1
        completion["proxy_counter_deltas"]["toxiproxy_total_bytes"] += 1
        cases.append(("stored_delta", launch, completion))

        launch, completion = self._documents()
        completion["proxy_counter_baseline_b_sha256"] = "f" * 64
        cases.append(("completion_baseline_b_hash", launch, completion))

        launch, completion = self._documents()
        completion["proxy_routes"].reverse()
        cases.append(("route_order", launch, completion))

        launch, completion = self._documents()
        launch["proxy_route_rearm_verified"] = False
        cases.append(("stored_route_rearm_boolean", launch, completion))

        launch, completion = self._documents()
        for document in (launch, completion):
            document["network_guard"]["toxiproxy_ingress_ipv4_sha256"] = "f" * 64
        cases.append(("ingress_hash", launch, completion))

        launch, completion = self._documents()
        for document in (launch, completion):
            document["backend_isolation"][
                "toxiproxy_ingress_membership_verified"
            ] = False
        cases.append(("ingress_membership", launch, completion))

        for name, launch, completion in cases:
            with self.subTest(name=name):
                with self.assertRaises(TopologyAttestationError):
                    self._sanitize(launch, completion)

        for field in ("boundary_values_equal", "route_rearmed"):
            with self.subTest(name=f"sanitized_{field}"):
                sanitized = self._sanitize()
                sanitized["proxy_counter_attribution"][field] = False
                self._rehash_sanitized(sanitized)
                with self.assertRaises(TopologyAttestationError):
                    topology_module._validate_sanitized_shape(sanitized)

    def test_sanitization_rejects_negative_components_and_broken_delta_sums(self):
        launch, completion = self._documents()
        final_route = completion["proxy_counter_final"]["routes"][0]
        final_route["received_upstream_bytes"] = 10
        final_route["total_bytes"] = sum(
            final_route[field]
            for field in (
                "received_upstream_bytes",
                "sent_upstream_bytes",
                "received_downstream_bytes",
                "sent_downstream_bytes",
            )
        )
        completion["proxy_counter_final"]["toxiproxy_total_bytes"] = sum(
            row["total_bytes"]
            for row in completion["proxy_counter_final"]["routes"]
        )
        rehash_proxy_snapshot(completion["proxy_counter_final"])
        with self.assertRaises(TopologyAttestationError):
            self._sanitize(launch, completion)

        for name, mutate in (
            (
                "broken_role_sum",
                lambda deltas: deltas["routes"][0].update(
                    {"total_bytes": deltas["routes"][0]["total_bytes"] + 1}
                ),
            ),
            (
                "broken_global_sum",
                lambda deltas: deltas.update(
                    {
                        "toxiproxy_total_bytes": deltas[
                            "toxiproxy_total_bytes"
                        ]
                        + 1
                    }
                ),
            ),
        ):
            with self.subTest(name=name):
                launch, completion = self._documents()
                mutate(completion["proxy_counter_deltas"])
                with self.assertRaises(TopologyAttestationError):
                    self._sanitize(launch, completion)

    def test_task5_exact_key_closures_and_legacy_schemas_fail_closed(self):
        raw_cases = []
        for name, mutate in (
            (
                "launch_fields",
                lambda launch, completion: launch.update({"extra": True}),
            ),
            (
                "completion_fields",
                lambda launch, completion: completion.update({"extra": True}),
            ),
            (
                "snapshot_fields",
                lambda launch, completion: launch[
                    "proxy_counter_baseline_a"
                ].update({"extra": True}),
            ),
            (
                "snapshot_route_fields",
                lambda launch, completion: launch["proxy_counter_baseline_a"][
                    "routes"
                ][0].update({"extra": True}),
            ),
            (
                "delta_fields",
                lambda launch, completion: completion[
                    "proxy_counter_deltas"
                ].update({"extra": True}),
            ),
            (
                "delta_route_fields",
                lambda launch, completion: completion["proxy_counter_deltas"][
                    "routes"
                ][0].update({"extra": True}),
            ),
            (
                "legacy_launch_v3",
                lambda launch, completion: launch.update(
                    {"schema": "txnmem-provenance-execution-launch-raw-v3"}
                ),
            ),
            (
                "legacy_completion_v4",
                lambda launch, completion: completion.update(
                    {"schema": "txnmem-provenance-execution-completion-raw-v4"}
                ),
            ),
            (
                "legacy_backend_v2",
                lambda launch, completion: (
                    launch["backend_isolation"].update(
                        {"schema": "txnmem-provenance-backend-isolation-v2"}
                    ),
                    completion["backend_isolation"].update(
                        {"schema": "txnmem-provenance-backend-isolation-v2"}
                    ),
                ),
            ),
            (
                "legacy_guard_v2",
                lambda launch, completion: (
                    launch["network_guard"].update(
                        {"schema": "txnmem-provenance-network-guard-v2"}
                    ),
                    completion["network_guard"].update(
                        {"schema": "txnmem-provenance-network-guard-v2"}
                    ),
                ),
            ),
        ):
            launch, completion = self._documents()
            mutate(launch, completion)
            raw_cases.append((name, launch, completion))

        for name, launch, completion in raw_cases:
            with self.subTest(name=name):
                with self.assertRaises(TopologyAttestationError):
                    self._sanitize(launch, completion)

        for name, mutate in (
            (
                "sanitized_top_level",
                lambda value: value.update({"extra": True}),
            ),
            (
                "proxy_attribution",
                lambda value: value["proxy_counter_attribution"].update(
                    {"extra": True}
                ),
            ),
            (
                "sanitized_backend",
                lambda value: value["backend_isolation"].update({"extra": True}),
            ),
            (
                "legacy_sanitized_v5",
                lambda value: value.update(
                    {"schema": "txnmem-topology-attestation-v5"}
                ),
            ),
            (
                "raw_backend_in_sanitized_document",
                lambda value: value["backend_isolation"].update(
                    {
                        "schema": "txnmem-provenance-backend-isolation-v3",
                        "toxiproxy_ingress_ipv4": "172.20.0.2",
                    }
                ),
            ),
        ):
            with self.subTest(name=name):
                sanitized = self._sanitize()
                mutate(sanitized)
                self._rehash_sanitized(sanitized)
                with self.assertRaises(TopologyAttestationError):
                    topology_module._validate_sanitized_shape(sanitized)

    def test_sanitization_rejects_candidate_seal_receipt_substitution(self):
        launch, completion = self._documents()
        completion["candidate_seal"]["completion_receipt_sha256"] = "0" * 64

        with self.assertRaisesRegex(
            TopologyAttestationError, "receipt is not completion-bound"
        ):
            self._sanitize(launch, completion)

    def test_sanitization_rejects_backend_isolation_drift_and_wrong_image(self):
        launch, completion = self._documents()
        completion["backend_isolation"]["network_id_sha256"] = "0" * 64

        with self.assertRaisesRegex(
            TopologyAttestationError, "backend_isolation changed"
        ):
            self._sanitize(launch, completion)

        launch, completion = self._documents()
        launch["backend_isolation"]["ingress_proxy_only"] = False
        completion["backend_isolation"]["ingress_proxy_only"] = False
        with self.assertRaisesRegex(
            TopologyAttestationError, "formal backend isolation"
        ):
            self._sanitize(launch, completion)

        sanitized = self._sanitize()
        sanitized["backend_isolation"]["toxiproxy_ingress_ipv4_sha256"] = (
            "not-a-sha256"
        )
        with self.assertRaisesRegex(
            TopologyAttestationError, "proxy ingress IPv4"
        ):
            _validate_sanitized_backend_isolation(sanitized["backend_isolation"])

        launch, completion = self._documents()
        launch["network_guard"]["backend_ipv4_subnet_sha256"] = "2" * 64
        completion["network_guard"]["backend_ipv4_subnet_sha256"] = "2" * 64
        with self.assertRaisesRegex(
            TopologyAttestationError, "network guard.*backend isolation"
        ):
            self._sanitize(launch, completion)

        launch, completion = self._documents()
        completion["execution_monitor"]["max_observed_gap_ns"] = 2_000_000_001
        with self.assertRaisesRegex(
            TopologyAttestationError, "execution monitor"
        ):
            self._sanitize(launch, completion)

        launch, completion = self._documents()
        completion["execution_monitor"]["max_load1_milli"] = 8001
        with self.assertRaisesRegex(
            TopologyAttestationError, "execution monitor"
        ):
            self._sanitize(launch, completion)

        launch, completion = self._documents()
        completion["execution_monitor"]["child_exit_monotonic_ns"] = (
            completion["execution_monitor"]["last_sample_monotonic_ns"] + 1
        )
        with self.assertRaisesRegex(
            TopologyAttestationError, "execution monitor"
        ):
            self._sanitize(launch, completion)

        launch, completion = self._documents()
        launch["backend_isolation"]["containers"][0]["manifest_digest"] = "0" * 64
        completion["backend_isolation"]["containers"][0][
            "manifest_digest"
        ] = "0" * 64
        with self.assertRaisesRegex(
            TopologyAttestationError, "backend container identity"
        ):
            self._sanitize(launch, completion)

    def test_sanitization_rejects_unregistered_and_wrong_authorization_nonce(self):
        launch, completion = self._documents()
        launch_raw = self._file_bytes(launch)
        completion_raw = self._file_bytes(completion)
        arguments = {
            "launch_file_sha256": hashlib.sha256(launch_raw).hexdigest(),
            "completion_file_sha256": hashlib.sha256(completion_raw).hexdigest(),
        }

        with patch.dict(
            FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN, {}, clear=True
        ):
            with self.assertRaisesRegex(
                TopologyAttestationError, "nonce is not registered"
            ):
                sanitize_topology_attestation(
                    launch,
                    completion,
                    authorization_nonce=self.AUTHORIZATION_NONCE,
                    **arguments,
                )

        wrong_nonce = b"wrong-topology-authorization-nonce-0000001"
        with patch.dict(
            FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
            {
                launch["run_id_sha256"]: hashlib.sha256(
                    wrong_nonce
                ).hexdigest()
            },
            clear=True,
        ):
            with self.assertRaises(TopologyAttestationError):
                sanitize_topology_attestation(
                    launch,
                    completion,
                    authorization_nonce=wrong_nonce,
                    **arguments,
                )

    def test_authorization_nonce_file_must_be_private_and_outside_repository(self):
        repository_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            private_parent = Path(temporary) / "private"
            private_parent.mkdir(mode=0o700)
            nonce_path = private_parent / "authorization.nonce"
            nonce_path.write_bytes(self.AUTHORIZATION_NONCE)
            nonce_path.chmod(0o600)

            self.assertEqual(
                _read_private_authorization_nonce(
                    nonce_path, repository_root=repository_root
                ),
                self.AUTHORIZATION_NONCE,
            )

            nonce_path.chmod(0o644)
            with self.assertRaisesRegex(
                TopologyAttestationError, "mode 0600"
            ):
                _read_private_authorization_nonce(
                    nonce_path, repository_root=repository_root
                )
            nonce_path.chmod(0o600)

            private_parent.chmod(0o755)
            with self.assertRaisesRegex(
                TopologyAttestationError, "parent directory mode 0700"
            ):
                _read_private_authorization_nonce(
                    nonce_path, repository_root=repository_root
                )
            private_parent.chmod(0o700)

            symlink_path = private_parent / "authorization-link.nonce"
            symlink_path.symlink_to(nonce_path)
            with self.assertRaisesRegex(
                TopologyAttestationError, "regular file"
            ):
                _read_private_authorization_nonce(
                    symlink_path, repository_root=repository_root
                )

        with tempfile.TemporaryDirectory(dir=repository_root) as temporary:
            repository_nonce = Path(temporary) / "authorization.nonce"
            repository_nonce.write_bytes(self.AUTHORIZATION_NONCE)
            os.chmod(temporary, 0o700)
            repository_nonce.chmod(0o600)
            with self.assertRaisesRegex(
                TopologyAttestationError, "outside the repository"
            ):
                _read_private_authorization_nonce(
                    repository_nonce, repository_root=repository_root
                )

    def test_authorization_proof_rejects_post_launch_document_forgery(self):
        launch, completion = self._documents()
        launch["matrix_cell_count"] = 14
        launch_raw = self._file_bytes(launch)
        completion_raw = self._file_bytes(completion)

        with patch.dict(
            FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
            {
                launch["run_id_sha256"]: hashlib.sha256(
                    self.AUTHORIZATION_NONCE
                ).hexdigest()
            },
            clear=True,
        ):
            with self.assertRaisesRegex(
                TopologyAttestationError, "authorization proof mismatch"
            ):
                sanitize_topology_attestation(
                    launch,
                    completion,
                    launch_file_sha256=hashlib.sha256(launch_raw).hexdigest(),
                    completion_file_sha256=hashlib.sha256(
                        completion_raw
                    ).hexdigest(),
                    authorization_nonce=self.AUTHORIZATION_NONCE,
                )

    def test_closed_schema_rejects_secret_fields_ip_versions_and_nonfinite_rtt(self):
        mutations = []
        launch, completion = self._documents()
        launch["password"] = "do-not-commit"
        mutations.append((launch, completion))
        for unsafe_version in (
            "198.51.100.7",
            "203.0.113",
            "1.2-host.internal.example",
            "1.2-password_demoSecret",
        ):
            launch, completion = self._documents()
            launch["roles"][1]["service_version"] = unsafe_version
            mutations.append((launch, completion))
        launch, completion = self._documents()
        launch["roles"][1]["rtt_ms"] = float("nan")
        mutations.append((launch, completion))

        for launch, completion in mutations:
            with self.subTest(launch=launch):
                with self.assertRaises(TopologyAttestationError):
                    self._sanitize(launch, completion)

    def test_launch_binding_source_listener_and_proxy_drift_fail_closed(self):
        cases = []
        launch, completion = self._documents()
        completion["launch_file_sha256"] = "f" * 64
        cases.append((launch, completion, "launch"))
        launch, completion = self._documents()
        completion["source_manifest_sha256"] = "f" * 64
        cases.append((launch, completion, "source"))
        launch, completion = self._documents()
        completion["roles"][1]["listener_owner"] = "qdrant-process-other"
        cases.append((launch, completion, "listener"))
        launch, completion = self._documents()
        completion["proxy_counter_deltas"]["routes"][0]["total_bytes"] += 1
        cases.append((launch, completion, "proxy"))
        launch, completion = self._documents()
        completion["exit_code"] = 1
        cases.append((launch, completion, "exit"))
        launch, completion = self._documents()
        launch["command_manifest"]["config_file_sha256"] = "f" * 64
        cases.append((launch, completion, "command"))
        launch, completion = self._documents()
        completion["child_process"]["pid"] = 9999
        cases.append((launch, completion, "child"))
        launch, completion = self._documents()
        launch["roles"][0]["listener_owner"] = "collector-process"
        completion["roles"][0]["listener_owner"] = "collector-process"
        cases.append((launch, completion, "client-owner"))
        launch, completion = self._documents()
        completion["proxy_routes"][0]["upstream"] = "unrelated:6333"
        cases.append((launch, completion, "route"))
        launch, completion = self._documents()
        baseline_b_route = launch["proxy_counter_baseline_b"]["routes"][0]
        baseline_b_route["received_upstream_bytes"] += 1
        baseline_b_route["total_bytes"] += 1
        launch["proxy_counter_baseline_b"]["toxiproxy_total_bytes"] += 1
        rehash_proxy_snapshot(launch["proxy_counter_baseline_b"])
        cases.append((launch, completion, "baseline"))
        launch, completion = self._documents()
        for document in (launch, completion):
            document["command_manifest"]["qdrant_endpoint_port"] = 6333
            document["command_sha256"] = hashlib.sha256(
                json.dumps(
                    document["command_manifest"],
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
        cases.append((launch, completion, "direct-endpoint"))
        launch, completion = self._documents()
        for document in (launch, completion):
            document["command_manifest"]["runtime_manifest"]["distributions"][
                0
            ]["files"][0]["sha256"] = "f" * 64
            document["command_sha256"] = hashlib.sha256(
                json.dumps(
                    document["command_manifest"],
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
        cases.append((launch, completion, "runtime"))

        for launch, completion, name in cases:
            with self.subTest(name=name):
                if name == "listener":
                    sanitized = self._sanitize(launch, completion)
                    with patch.dict(
                        FORMAL_PROVENANCE_TOPOLOGY_ATTESTATION_SHA256_BY_RUN,
                        {"1" * 64: sanitized["attestation_sha256"]},
                        clear=True,
                    ), patch.dict(
                        FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
                        {
                            "1" * 64: hashlib.sha256(
                                self.AUTHORIZATION_NONCE
                            ).hexdigest()
                        },
                        clear=True,
                    ):
                        with self.assertRaises(TopologyAttestationError):
                            validate_registered_topology_attestation(
                                sanitized, **self._validation_arguments()
                            )
                else:
                    with self.assertRaises(TopologyAttestationError):
                        self._sanitize(launch, completion)

    def test_exact_registered_attestation_validates_and_tampering_fails(self):
        sanitized = self._sanitize()
        arguments = self._validation_arguments()

        with self.assertRaises(TopologyAttestationError):
            validate_registered_topology_attestation(sanitized, **arguments)

        with patch.dict(
            FORMAL_PROVENANCE_TOPOLOGY_ATTESTATION_SHA256_BY_RUN,
            {"1" * 64: sanitized["attestation_sha256"]},
            clear=True,
        ), patch.dict(
            FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
            {
                "1" * 64: hashlib.sha256(
                    self.AUTHORIZATION_NONCE
                ).hexdigest()
            },
            clear=True,
        ):
            validated = validate_registered_topology_attestation(
                sanitized, **arguments
            )
            tampered = copy.deepcopy(sanitized)
            tampered["workload_sha256"] = "f" * 64
            with self.assertRaises(TopologyAttestationError):
                validate_registered_topology_attestation(tampered, **arguments)

        self.assertEqual(validated, sanitized)

    def test_registered_sanitized_attestation_rejects_guard_backend_mismatch(self):
        for name, object_name, field in (
            (
                "guard_subnet",
                "network_guard",
                "backend_ipv4_subnet_sha256",
            ),
            (
                "guard_ingress_address",
                "network_guard",
                "toxiproxy_ingress_ipv4_sha256",
            ),
            (
                "backend_ingress_address",
                "backend_isolation",
                "toxiproxy_ingress_ipv4_sha256",
            ),
        ):
            with self.subTest(name=name):
                sanitized = self._sanitize()
                sanitized[object_name][field] = "2" * 64
                without_hash = dict(sanitized)
                without_hash.pop("attestation_sha256")
                sanitized["attestation_sha256"] = hashlib.sha256(
                    json.dumps(
                        without_hash,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()

                with patch.dict(
                    FORMAL_PROVENANCE_TOPOLOGY_ATTESTATION_SHA256_BY_RUN,
                    {"1" * 64: sanitized["attestation_sha256"]},
                    clear=True,
                ), patch.dict(
                    FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
                    {
                        "1" * 64: hashlib.sha256(
                            self.AUTHORIZATION_NONCE
                        ).hexdigest()
                    },
                    clear=True,
                ):
                    with self.assertRaisesRegex(
                        TopologyAttestationError,
                        "network guard.*backend isolation",
                    ):
                        validate_registered_topology_attestation(
                            sanitized, **self._validation_arguments()
                        )

    def test_cross_host_transport_cannot_be_promoted_without_remote_collector(self):
        launch, completion = self._documents()
        for document in (launch, completion):
            document["transport"] = "ssh_local_port_forward"
            document["command_manifest"]["transport"] = "ssh_local_port_forward"
            document["command_sha256"] = hashlib.sha256(
                json.dumps(
                    document["command_manifest"],
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
        sanitized = self._sanitize(launch, completion)
        with patch.dict(
            FORMAL_PROVENANCE_TOPOLOGY_ATTESTATION_SHA256_BY_RUN,
            {"1" * 64: sanitized["attestation_sha256"]},
            clear=True,
        ), patch.dict(
            FORMAL_PROVENANCE_LAUNCH_NONCE_SHA256_BY_RUN,
            {
                "1" * 64: hashlib.sha256(
                    self.AUTHORIZATION_NONCE
                ).hexdigest()
            },
            clear=True,
        ):
            with self.assertRaisesRegex(
                TopologyAttestationError, "cross-host transport"
            ):
                validate_registered_topology_attestation(
                    sanitized, **self._validation_arguments()
                )

    def test_repository_wrapper_uses_collector_then_two_file_sanitization(self):
        root = Path(__file__).resolve().parents[1]
        wrapper = root / "scripts" / "run_cross_host_provenance_performance.sh"
        text = wrapper.read_text(encoding="utf-8")
        delegated = (root / "scripts" / "run_provenance_performance.sh").read_text(
            encoding="utf-8"
        )

        sanitize_position = text.index('--project-root "$PWD" attest')
        promote_position = text.index('--project-root "$PWD" promote')
        self.assertLess(sanitize_position, promote_position)
        self.assertIn("/opt/txnmem-formal-controller", delegated)
        self.assertIn("/usr/bin/env -i", delegated)
        self.assertIn("-I -S -B", delegated)
        self.assertIn("run_provenance_performance.sh", text)
        self.assertIn("--launch", text)
        self.assertIn("--completion", text)
        self.assertIn("--authorization-nonce", text)
        self.assertIn("--authorization-nonce", delegated)
        self.assertNotIn("--environment-attestation", delegated)
        self.assertIn("register", text.lower())
        self.assertNotIn("--formal", text)
        self.assertNotIn("PYTHONPATH", text)
        self.assertNotIn("PYTHONPATH", delegated)
        self.assertNotIn("sshpass", text)
        self.assertNotIn("StrictHostKeyChecking=no", text)


if __name__ == "__main__":
    unittest.main()
