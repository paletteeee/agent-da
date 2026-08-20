import hashlib
import io
import json
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from txnmem_appworld_projection import (
    APPWORLD_FORMAL_SPLIT_SHA256,
    main as appworld_projection_main,
    regenerate_appworld_native_realism_projection,
    select_appworld_realism_families,
    validate_appworld_native_realism_bundle,
    validate_appworld_realism_bundle,
)
from txnmem_benchmark_manifests import _canonical_hash, shard_manifest
from txnmem_conditions import canonical_fingerprint
from txnmem_formal_io import bind_native_shard_report
from txnmem_trace_pipeline import load_trace_records


class AppWorldNativeRealismTests(unittest.TestCase):
    @staticmethod
    def _fixture_parent_identity(root: Path):
        parent = json.loads(
            (root / "manifests" / "appworld" / "parent.json").read_text(
                encoding="utf-8"
            )
        )
        return patch(
            "txnmem_appworld_projection.APPWORLD_FORMAL_PARENT_MANIFEST_SHA256",
            parent["manifest_hash"],
        )

    def _native_run(self, root: Path) -> dict[str, Path]:
        raw_task_ids = [
            f"family{family:03d}_{task_number}"
            for family in range(56)
            for task_number in (1, 2, 3)
        ]
        tasks = [
            {
                "task_id": f"appworld-{raw_task_id}",
                "raw_task_id": raw_task_id,
                "source_position": position,
                "source_index": position,
                "prompt": "private benchmark prompt",
            }
            for position, raw_task_id in enumerate(raw_task_ids)
        ]
        source_material = {
            "split_file": {
                "path": "datasets/test_normal.txt",
                "sha256": APPWORLD_FORMAL_SPLIT_SHA256,
            },
            "version_file": {"path": "version.txt", "sha256": "b" * 64},
        }
        ordered_raw_ids = json.dumps(
            raw_task_ids, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        parent = {
            "manifest_version": 1,
            "dataset_name": "appworld-test_normal",
            "benchmark": "appworld",
            "split": "test_normal",
            "seed": 17,
            "task_count": len(tasks),
            "source_task_count": len(tasks),
            "tasks": tasks,
            "source_identity": {
                **source_material,
                "fingerprint": _canonical_hash(source_material),
            },
            "public_split_identity": {
                "benchmark": "appworld",
                "split": "test_normal",
                "source_task_count": len(tasks),
                "selected_task_count": len(tasks),
                "ordered_raw_task_ids_sha256": hashlib.sha256(
                    ordered_raw_ids
                ).hexdigest(),
            },
        }
        parent["condition_fingerprint"] = _canonical_hash(
            {
                "benchmark": "appworld",
                "domain": None,
                "split": "test_normal",
                "seed": 17,
                "source_identity": parent["source_identity"],
                "public_split_identity": parent["public_split_identity"],
            }
        )
        parent["manifest_hash"] = _canonical_hash(parent)
        shard = shard_manifest(parent, 1)[0]

        manifest_dir = root / "manifests" / "appworld"
        run_dir = root / "runs" / "appworld" / "shard_000"
        trace_path = run_dir / "data" / "native_model_traces.jsonl"
        report_dir = run_dir / "results"
        manifest_dir.mkdir(parents=True)
        trace_path.parent.mkdir(parents=True)
        report_dir.mkdir(parents=True)
        (manifest_dir / "parent.json").write_text(
            json.dumps(parent), encoding="utf-8"
        )
        (manifest_dir / "shard_000.json").write_text(
            json.dumps(shard), encoding="utf-8"
        )

        trace_rows = []
        task_summaries = []
        for task in tasks:
            trace_rows.append(
                {
                    "task_id": task["task_id"],
                    "run": {
                        "status": "completed",
                        "prompt_profile": "baseline",
                        "trusted_preflight_enabled": False,
                        "app_tool_strategy": "all_public",
                        "events": [
                            {
                                "event_id": "event-1",
                                "kind": "memory_write",
                                "agent_id": "agent-private",
                                "step": 1,
                                "memory_id": f"private-{task['raw_task_id']}",
                                "value": "SENSITIVE_NATIVE_VALUE",
                            }
                        ],
                    },
                }
            )
            task_summaries.append(
                {
                    "task_id": task["task_id"],
                    "status": "completed",
                    "native_event_count": 1,
                    "prompt_profile": "baseline",
                    "trusted_preflight_enabled": False,
                    "app_tool_strategy": "all_public",
                    "official": {"status": "available", "success": False},
                }
            )
        trace_bytes = "".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
            for row in trace_rows
        ).encode("utf-8")
        trace_path.write_bytes(trace_bytes)

        runner_components = {
            "txnmem_experiment": "e" * 64,
            "txnmem_benchmark_bridge": "4" * 64,
            "txnmem_model_protocol": "5" * 64,
            "txnmem_real_experiment": "6" * 64,
            "appworld_environment": "f" * 64,
            "appworld_evaluator": "2" * 64,
            "appworld_common_evaluation": "3" * 64,
        }
        condition = {
            "benchmark": "appworld",
            "manifest_sha256": parent["manifest_hash"],
            "model_id": "qwen2.5-7b-instruct",
            "model_revision": "d" * 64,
            "model_revision_status": "sha256",
            "model_server_build": "vllm:test",
            "runner_evaluator_source_identity": {
                "component_sha256": runner_components,
                "fingerprint": canonical_fingerprint(runner_components),
            },
            "model_execution_mode": "remote_endpoint",
            "memory_backend": "sqlite",
            "repetitions": 1,
            "max_tokens": 512,
            "timeout_seconds": 300.0,
            "generation_parameters": "seed_temperature_and_max_steps_from_fixed_manifest",
            "official_evaluator": "appworld.TestTracker.success_and_task_completed",
            "runtime_version": "0.2.0",
            "appworld_model_tool_strategy": "all_public",
            "split": "test_normal",
        }
        raw_summary = {
            "schema_version": 1,
            "manifest_sha256": shard["manifest_hash"],
            "benchmark": "appworld",
            "split": "test_normal",
            "condition": condition,
            "condition_fingerprint": canonical_fingerprint(condition),
            "model_execution_mode": "remote_endpoint",
            "model_id": "qwen2.5-7b-instruct",
            "memory_backend": "sqlite",
            "prompt_profile": "baseline",
            "treatment": {
                "prompt_profile": "baseline",
                "trusted_preflight_enabled": False,
                "app_tool_strategy": "all_public",
            },
            "repetitions": 1,
            "task_count": len(tasks),
            "unique_task_count": len(tasks),
            "native_event_count": len(tasks),
            "evaluation_error_count": 0,
            "task_summaries": task_summaries,
            "native_trace_artifacts": [
                {
                    "relative_path": "data/native_model_traces.jsonl",
                    "sha256": hashlib.sha256(trace_bytes).hexdigest(),
                    "size_bytes": len(trace_bytes),
                    "line_count": len(tasks),
                }
            ],
        }
        (report_dir / "native_batch_summary.json").write_text(
            json.dumps(raw_summary), encoding="utf-8"
        )
        (run_dir / "shard_report.json").write_text(
            json.dumps(bind_native_shard_report(shard, raw_summary)),
            encoding="utf-8",
        )

        selection = select_appworld_realism_families(
            raw_task_ids,
            evaluation_family_count=50,
            calibration_family_count=None,
            seed=17,
            official_split="test_normal",
        )
        selection["dataset_file_sha256"] = APPWORLD_FORMAL_SPLIT_SHA256
        selection_path = root / "selection.json"
        selection_path.write_text(json.dumps(selection), encoding="utf-8")
        return {
            "selection": selection_path,
            "trace": trace_path,
        }

    @staticmethod
    def _rewrite_bound_report(root: Path) -> None:
        shard_path = root / "manifests" / "appworld" / "shard_000.json"
        summary_path = (
            root
            / "runs"
            / "appworld"
            / "shard_000"
            / "results"
            / "native_batch_summary.json"
        )
        bound_path = summary_path.parents[1] / "shard_report.json"
        shard = json.loads(shard_path.read_text(encoding="utf-8"))
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        bound_path.write_text(
            json.dumps(bind_native_shard_report(shard, summary)),
            encoding="utf-8",
        )

    @staticmethod
    def _task11_attestation(root: Path, output: Path) -> tuple[Path, str]:
        parent = json.loads(
            (root / "manifests" / "appworld" / "parent.json").read_text(
                encoding="utf-8"
            )
        )
        shard_path = root / "manifests" / "appworld" / "shard_000.json"
        summary_path = (
            root
            / "runs"
            / "appworld"
            / "shard_000"
            / "results"
            / "native_batch_summary.json"
        )
        bound_path = summary_path.parents[1] / "shard_report.json"
        shard = json.loads(shard_path.read_text(encoding="utf-8"))
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        attestation = {
            "schema": "txnmem-task11-appworld-execution-attestation-v1",
            "launch": {
                "launch_id_sha256": "9" * 64,
                "benchmark": "appworld",
                "split": "test_normal",
                "parent_manifest_hash": parent["manifest_hash"],
                "shard_count": 1,
                "repetitions": 1,
                "shard_manifest_hashes": [shard["manifest_hash"]],
                "execution_condition": summary["condition"],
                "execution_condition_fingerprint": summary[
                    "condition_fingerprint"
                ],
                "prompt_profile": summary["prompt_profile"],
                "treatment": summary["treatment"],
            },
            "completion": {
                "status": "completed",
                "shards": [
                    {
                        "shard_index": 0,
                        "manifest_hash": shard["manifest_hash"],
                        "raw_summary_sha256": hashlib.sha256(
                            summary_path.read_bytes()
                        ).hexdigest(),
                        "bound_report_sha256": hashlib.sha256(
                            bound_path.read_bytes()
                        ).hexdigest(),
                        "native_trace_artifacts": summary[
                            "native_trace_artifacts"
                        ],
                    }
                ],
            },
        }
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(attestation, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return output, hashlib.sha256(output.read_bytes()).hexdigest()

    @classmethod
    def _rewrite_trace_rows(cls, root: Path, rows: list[dict]) -> None:
        trace_path = (
            root
            / "runs"
            / "appworld"
            / "shard_000"
            / "data"
            / "native_model_traces.jsonl"
        )
        summary_path = (
            root
            / "runs"
            / "appworld"
            / "shard_000"
            / "results"
            / "native_batch_summary.json"
        )
        trace_bytes = "".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
            for row in rows
        ).encode("utf-8")
        trace_path.write_bytes(trace_bytes)
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        summary["native_trace_artifacts"][0].update(
            {
                "sha256": hashlib.sha256(trace_bytes).hexdigest(),
                "size_bytes": len(trace_bytes),
                "line_count": len(rows),
            }
        )
        summary_path.write_text(json.dumps(summary), encoding="utf-8")
        cls._rewrite_bound_report(root)

    def test_native_run_is_rederived_redacted_and_bound_to_execution_identity(self):
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            self._native_run(root)
            events_path = workspace / "export" / "native_realism_events.jsonl"
            inventory_path = workspace / "export" / "native_realism_inventory.json"

            with self._fixture_parent_identity(root):
                inventory = regenerate_appworld_native_realism_projection(
                    root, events_path
                )
                inventory_path.write_text(json.dumps(inventory), encoding="utf-8")
                binding = validate_appworld_native_realism_bundle(
                    events_path=events_path,
                    selection_path=root / "selection.json",
                    inventory_path=inventory_path,
                    native_run_root=root,
                )
            event_text = events_path.read_text(encoding="utf-8")
            events = load_trace_records(events_path)

        self.assertEqual(binding["task_count"], 168)
        self.assertEqual(binding["family_count"], 56)
        self.assertEqual(binding["event_count"], 168)
        self.assertEqual(binding["execution"]["model_id"], "qwen2.5-7b-instruct")
        self.assertEqual(binding["evidence_scope"], "candidate_native_bundle")
        self.assertEqual(binding["promotion_status"], "blocked")
        self.assertEqual(
            binding["blocking_reason"],
            "unregistered_task11_execution_attestation",
        )
        self.assertEqual(len(events), 168)
        self.assertTrue(all(event["family_id"].startswith("family") for event in events))
        self.assertNotIn("SENSITIVE_NATIVE_VALUE", event_text)
        self.assertNotIn("agent-private", event_text)
        self.assertNotIn("private-family", event_text)

    def test_registered_external_task11_attestation_promotes_native_bundle(self):
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            paths = self._native_run(root)
            attestation_path, attestation_sha256 = self._task11_attestation(
                root, workspace / "trusted" / "task11-attestation.json"
            )
            events_path = workspace / "export" / "events.jsonl"
            inventory_path = workspace / "export" / "inventory.json"
            treatment_fingerprint = canonical_fingerprint(
                {
                    "prompt_profile": "baseline",
                    "trusted_preflight_enabled": False,
                    "app_tool_strategy": "all_public",
                }
            )
            registry = {treatment_fingerprint: attestation_sha256}

            with self._fixture_parent_identity(root), patch(
                "txnmem_appworld_projection."
                "APPWORLD_FORMAL_TASK11_ATTESTATION_SHA256_BY_TREATMENT",
                registry,
            ):
                inventory = regenerate_appworld_native_realism_projection(
                    root,
                    events_path,
                    task11_attestation_path=attestation_path,
                )
                inventory_path.parent.mkdir(parents=True, exist_ok=True)
                inventory_path.write_text(json.dumps(inventory), encoding="utf-8")
                binding = validate_appworld_native_realism_bundle(
                    events_path=events_path,
                    selection_path=paths["selection"],
                    inventory_path=inventory_path,
                    native_run_root=root,
                    task11_attestation_path=attestation_path,
                )

        self.assertEqual(
            binding["evidence_scope"], "trace_grounded_native_agent_execution"
        )
        self.assertEqual(binding["promotion_status"], "eligible")
        self.assertEqual(
            binding["task11_execution_attestation_sha256"], attestation_sha256
        )

    def test_bound_shard_report_carries_trace_hash_and_execution_condition(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._native_run(root)
            bound = json.loads(
                (
                    root
                    / "runs"
                    / "appworld"
                    / "shard_000"
                    / "shard_report.json"
                ).read_text(encoding="utf-8")
            )

        self.assertEqual(
            bound["native_trace_artifacts"][0]["relative_path"],
            "data/native_model_traces.jsonl",
        )
        self.assertEqual(
            canonical_fingerprint(bound["execution_condition"]),
            bound["execution_condition_fingerprint"],
        )

    def test_native_bundle_rejects_trace_tampering_after_inventory_generation(self):
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            paths = self._native_run(root)
            events_path = workspace / "export" / "native_realism_events.jsonl"
            inventory_path = workspace / "export" / "native_realism_inventory.json"
            with self._fixture_parent_identity(root):
                inventory = regenerate_appworld_native_realism_projection(
                    root, events_path
                )
                inventory_path.write_text(json.dumps(inventory), encoding="utf-8")
                paths["trace"].write_text(
                    paths["trace"].read_text(encoding="utf-8") + "{}\n",
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(ValueError, "native trace.*hash|native trace.*size"):
                    validate_appworld_native_realism_bundle(
                        events_path=events_path,
                        selection_path=paths["selection"],
                        inventory_path=inventory_path,
                        native_run_root=root,
                    )

    def test_native_bundle_rejects_duplicate_inventory_keys(self):
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            paths = self._native_run(root)
            events_path = workspace / "export" / "native_realism_events.jsonl"
            inventory_path = workspace / "export" / "native_realism_inventory.json"
            with self._fixture_parent_identity(root):
                inventory = regenerate_appworld_native_realism_projection(
                    root, events_path
                )
                canonical = json.dumps(inventory)
                inventory_path.write_text(
                    '{"schema":"forged",' + canonical[1:], encoding="utf-8"
                )

                with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
                    validate_appworld_native_realism_bundle(
                        events_path=events_path,
                        selection_path=paths["selection"],
                        inventory_path=inventory_path,
                        native_run_root=root,
                    )

    def test_native_projection_rejects_unregistered_treatment_fields(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._native_run(root)
            shard_path = root / "manifests" / "appworld" / "shard_000.json"
            summary_path = (
                root
                / "runs"
                / "appworld"
                / "shard_000"
                / "results"
                / "native_batch_summary.json"
            )
            shard = json.loads(shard_path.read_text(encoding="utf-8"))
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary["treatment"]["password"] = "must-never-enter-aggregate"

            with self.assertRaisesRegex(ValueError, "treatment"):
                bind_native_shard_report(shard, summary)

    def test_native_projection_rejects_unregistered_source_identity_components(self):
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            self._native_run(root)
            shard_path = root / "manifests" / "appworld" / "shard_000.json"
            summary_path = (
                root
                / "runs"
                / "appworld"
                / "shard_000"
                / "results"
                / "native_batch_summary.json"
            )
            bound_path = summary_path.parents[1] / "shard_report.json"
            shard = json.loads(shard_path.read_text(encoding="utf-8"))
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            components = summary["condition"]["runner_evaluator_source_identity"][
                "component_sha256"
            ]
            components["password"] = "7" * 64
            summary["condition"]["runner_evaluator_source_identity"][
                "fingerprint"
            ] = canonical_fingerprint(components)
            summary["condition_fingerprint"] = canonical_fingerprint(
                summary["condition"]
            )
            summary_path.write_text(json.dumps(summary), encoding="utf-8")
            bound_path.write_text(
                json.dumps(bind_native_shard_report(shard, summary)),
                encoding="utf-8",
            )

            with self._fixture_parent_identity(root):
                with self.assertRaisesRegex(ValueError, "source identity"):
                    regenerate_appworld_native_realism_projection(
                        root, workspace / "native_realism_events.jsonl"
                    )

    def test_native_projection_rejects_rehashed_but_semantically_stale_parent_condition(self):
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            self._native_run(root)
            parent_path = root / "manifests" / "appworld" / "parent.json"
            shard_path = root / "manifests" / "appworld" / "shard_000.json"
            summary_path = (
                root
                / "runs"
                / "appworld"
                / "shard_000"
                / "results"
                / "native_batch_summary.json"
            )
            bound_path = summary_path.parents[1] / "shard_report.json"
            parent = json.loads(parent_path.read_text(encoding="utf-8"))
            parent["condition_fingerprint"] = "0" * 64
            parent["manifest_hash"] = _canonical_hash(parent)
            shard = shard_manifest(parent, 1)[0]
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary["manifest_sha256"] = shard["manifest_hash"]
            summary["condition"]["manifest_sha256"] = parent["manifest_hash"]
            summary["condition_fingerprint"] = canonical_fingerprint(
                summary["condition"]
            )
            parent_path.write_text(json.dumps(parent), encoding="utf-8")
            shard_path.write_text(json.dumps(shard), encoding="utf-8")
            summary_path.write_text(json.dumps(summary), encoding="utf-8")
            bound_path.write_text(
                json.dumps(bind_native_shard_report(shard, summary)),
                encoding="utf-8",
            )

            with self._fixture_parent_identity(root):
                with self.assertRaisesRegex(ValueError, "parent condition"):
                    regenerate_appworld_native_realism_projection(
                        root, workspace / "native_realism_events.jsonl"
                    )

    def test_native_projection_rejects_rehashed_fabricated_task_content(self):
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            self._native_run(root)
            parent_path = root / "manifests" / "appworld" / "parent.json"
            shard_path = root / "manifests" / "appworld" / "shard_000.json"
            summary_path = (
                root
                / "runs"
                / "appworld"
                / "shard_000"
                / "results"
                / "native_batch_summary.json"
            )
            bound_path = summary_path.parents[1] / "shard_report.json"
            parent = json.loads(parent_path.read_text(encoding="utf-8"))
            expected_parent_hash = parent["manifest_hash"]
            parent["tasks"][0]["prompt"] = "fabricated task content"
            parent["manifest_hash"] = _canonical_hash(parent)
            shard = shard_manifest(parent, 1)[0]
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            summary["manifest_sha256"] = shard["manifest_hash"]
            summary["condition"]["manifest_sha256"] = parent["manifest_hash"]
            summary["condition_fingerprint"] = canonical_fingerprint(
                summary["condition"]
            )
            parent_path.write_text(json.dumps(parent), encoding="utf-8")
            shard_path.write_text(json.dumps(shard), encoding="utf-8")
            summary_path.write_text(json.dumps(summary), encoding="utf-8")
            bound_path.write_text(
                json.dumps(bind_native_shard_report(shard, summary)),
                encoding="utf-8",
            )

            with patch(
                "txnmem_appworld_projection.APPWORLD_FORMAL_PARENT_MANIFEST_SHA256",
                expected_parent_hash,
            ):
                with self.assertRaisesRegex(ValueError, "official parent manifest"):
                    regenerate_appworld_native_realism_projection(
                        root, workspace / "native_realism_events.jsonl"
                    )

    def test_native_projection_rejects_symlinked_private_trace(self):
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            paths = self._native_run(root)
            private_copy = paths["trace"].with_name("private-copy.jsonl")
            paths["trace"].rename(private_copy)
            paths["trace"].symlink_to(private_copy.name)

            with self._fixture_parent_identity(root):
                with self.assertRaisesRegex(ValueError, "native trace.*regular file"):
                    regenerate_appworld_native_realism_projection(
                        root, workspace / "native_realism_events.jsonl"
                    )

    def test_trace_replay_cli_accepts_only_the_bound_native_bundle(self):
        from txnmem_experiment import main

        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            paths = self._native_run(root)
            events_path = workspace / "export" / "native_realism_events.jsonl"
            inventory_path = workspace / "export" / "native_realism_inventory.json"
            config_path = workspace / "realism.json"
            out_dir = workspace / "out"
            with self._fixture_parent_identity(root):
                inventory = regenerate_appworld_native_realism_projection(
                    root, events_path
                )
                inventory_path.write_text(json.dumps(inventory), encoding="utf-8")
            config_path.write_text(
                json.dumps(
                    {
                        "seed": 17,
                        "synthetic": {
                            "workloads": ["atomic_multi_write"],
                            "seeds": [0],
                            "parameter_ranges": {},
                        },
                        "statistics": {
                            "bootstrap_repetitions": 10,
                            "joint_test_permutations": 9,
                            "joint_test_dimensions": 4,
                            "cluster_bootstrap_repetitions": 10,
                        },
                    }
                ),
                encoding="utf-8",
            )

            with self._fixture_parent_identity(root):
                with redirect_stdout(io.StringIO()):
                    exit_code = main(
                        [
                    "trace-replay",
                    "--events",
                    str(events_path),
                    "--adapter",
                    "appworld",
                    "--source",
                    "appworld-native-agent",
                    "--group-key",
                    "family_id",
                    "--group-selection",
                    str(paths["selection"]),
                    "--realism-config",
                    str(config_path),
                    "--appworld-native-run-root",
                    str(root),
                    "--appworld-native-inventory",
                    str(inventory_path),
                    "--out-dir",
                    str(out_dir),
                        ]
                    )
            report = json.loads(
                (out_dir / "results" / "trace_realism.json").read_text(
                    encoding="utf-8"
                )
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            report["cross_fitted"]["appworld_formal_binding"]["evidence_scope"],
            "candidate_native_bundle",
        )
        self.assertEqual(
            report["cross_fitted"]["appworld_formal_binding"]["promotion_status"],
            "blocked",
        )
        self.assertEqual(report["cross_fitted"]["fold_count"], 50)

    def test_projection_cli_exports_native_events_and_inventory_together(self):
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            self._native_run(root)
            events_path = workspace / "export" / "events.jsonl"
            inventory_path = workspace / "export" / "inventory.json"

            with self._fixture_parent_identity(root):
                with redirect_stdout(io.StringIO()):
                    exit_code = appworld_projection_main(
                        [
                    "--native-run-root",
                    str(root),
                    "--native-output",
                    str(events_path),
                    "--native-inventory",
                    str(inventory_path),
                        ]
                    )
            inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
            events_sha256 = hashlib.sha256(events_path.read_bytes()).hexdigest()

        self.assertEqual(exit_code, 0)
        self.assertEqual(inventory["event_count"], 168)
        self.assertEqual(events_sha256, inventory["output_sha256"])

    def test_unregistered_attestation_cannot_promote_a_self_consistent_run(self):
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            self._native_run(root)
            attestation_path, _ = self._task11_attestation(
                root, workspace / "untrusted" / "attestation.json"
            )

            with self._fixture_parent_identity(root):
                with self.assertRaisesRegex(ValueError, "pre-registered"):
                    regenerate_appworld_native_realism_projection(
                        root,
                        workspace / "events.jsonl",
                        task11_attestation_path=attestation_path,
                    )

    def test_registered_attestation_rejects_run_drift_and_in_tree_anchor(self):
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            self._native_run(root)
            attestation_path, attestation_sha256 = self._task11_attestation(
                root, workspace / "trusted" / "attestation.json"
            )
            treatment_fingerprint = canonical_fingerprint(
                {
                    "prompt_profile": "baseline",
                    "trusted_preflight_enabled": False,
                    "app_tool_strategy": "all_public",
                }
            )
            registry = {treatment_fingerprint: attestation_sha256}
            summary_path = (
                root
                / "runs"
                / "appworld"
                / "shard_000"
                / "results"
                / "native_batch_summary.json"
            )
            summary_path.write_bytes(summary_path.read_bytes() + b"\n")

            with self._fixture_parent_identity(root), patch(
                "txnmem_appworld_projection."
                "APPWORLD_FORMAL_TASK11_ATTESTATION_SHA256_BY_TREATMENT",
                registry,
            ):
                with self.assertRaisesRegex(ValueError, "completion attestation"):
                    regenerate_appworld_native_realism_projection(
                        root,
                        workspace / "events.jsonl",
                        task11_attestation_path=attestation_path,
                    )

        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            self._native_run(root)
            in_tree_attestation, attestation_sha256 = self._task11_attestation(
                root, root / "attestations" / "task11.json"
            )
            treatment_fingerprint = canonical_fingerprint(
                {
                    "prompt_profile": "baseline",
                    "trusted_preflight_enabled": False,
                    "app_tool_strategy": "all_public",
                }
            )
            with self._fixture_parent_identity(root), patch(
                "txnmem_appworld_projection."
                "APPWORLD_FORMAL_TASK11_ATTESTATION_SHA256_BY_TREATMENT",
                {treatment_fingerprint: attestation_sha256},
            ):
                with self.assertRaisesRegex(ValueError, "outside.*run root"):
                    regenerate_appworld_native_realism_projection(
                        root,
                        workspace / "events.jsonl",
                        task11_attestation_path=in_tree_attestation,
                    )

    def test_native_selection_rejects_conflicting_alias_fields(self):
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            paths = self._native_run(root)
            events_path = workspace / "export" / "events.jsonl"
            inventory_path = workspace / "export" / "inventory.json"
            with self._fixture_parent_identity(root):
                inventory = regenerate_appworld_native_realism_projection(
                    root, events_path
                )
            inventory_path.write_text(json.dumps(inventory), encoding="utf-8")
            selection = json.loads(paths["selection"].read_text(encoding="utf-8"))
            selection["evaluation_groups"] = selection["calibration_family_ids"]
            selection["calibration_groups"] = selection["evaluation_family_ids"]
            paths["selection"].write_text(json.dumps(selection), encoding="utf-8")

            with self._fixture_parent_identity(root):
                with self.assertRaisesRegex(ValueError, "selection.*exact|unknown"):
                    validate_appworld_native_realism_bundle(
                        events_path=events_path,
                        selection_path=paths["selection"],
                        inventory_path=inventory_path,
                        native_run_root=root,
                    )

    def test_native_projection_enforces_profile_contract_per_task_and_trace(self):
        mutations = ("top_level", "summary_row", "trace_row")
        for mutation in mutations:
            with self.subTest(mutation=mutation), TemporaryDirectory() as tmp:
                workspace = Path(tmp)
                root = workspace / "run"
                self._native_run(root)
                summary_path = (
                    root
                    / "runs"
                    / "appworld"
                    / "shard_000"
                    / "results"
                    / "native_batch_summary.json"
                )
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
                if mutation == "top_level":
                    summary["treatment"]["trusted_preflight_enabled"] = True
                    summary_path.write_text(json.dumps(summary), encoding="utf-8")
                    self._rewrite_bound_report(root)
                elif mutation == "summary_row":
                    summary["task_summaries"][0]["prompt_profile"] = "tuned"
                    summary_path.write_text(json.dumps(summary), encoding="utf-8")
                    self._rewrite_bound_report(root)
                else:
                    trace_path = (
                        root
                        / "runs"
                        / "appworld"
                        / "shard_000"
                        / "data"
                        / "native_model_traces.jsonl"
                    )
                    rows = [
                        json.loads(line)
                        for line in trace_path.read_text(encoding="utf-8").splitlines()
                    ]
                    rows[0]["run"]["prompt_profile"] = "tuned"
                    self._rewrite_trace_rows(root, rows)

                with self._fixture_parent_identity(root):
                    with self.assertRaisesRegex(
                        ValueError, "profile|preflight|treatment"
                    ):
                        regenerate_appworld_native_realism_projection(
                            root, workspace / "events.jsonl"
                        )

    def test_formal_promotion_rejects_bool_integer_type_confusion(self):
        for mutation in ("preflight_zero", "event_count_true"):
            with self.subTest(mutation=mutation), TemporaryDirectory() as tmp:
                workspace = Path(tmp)
                root = workspace / "run"
                self._native_run(root)
                summary_path = (
                    root
                    / "runs"
                    / "appworld"
                    / "shard_000"
                    / "results"
                    / "native_batch_summary.json"
                )
                trace_path = (
                    root
                    / "runs"
                    / "appworld"
                    / "shard_000"
                    / "data"
                    / "native_model_traces.jsonl"
                )
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
                if mutation == "preflight_zero":
                    summary["task_summaries"][0][
                        "trusted_preflight_enabled"
                    ] = 0
                    summary_path.write_text(json.dumps(summary), encoding="utf-8")
                    rows = [
                        json.loads(line)
                        for line in trace_path.read_text(encoding="utf-8").splitlines()
                    ]
                    rows[0]["run"]["trusted_preflight_enabled"] = 0
                    self._rewrite_trace_rows(root, rows)
                    expected_error = "preflight.*boolean"
                else:
                    summary["task_summaries"][0]["native_event_count"] = True
                    summary_path.write_text(json.dumps(summary), encoding="utf-8")
                    self._rewrite_bound_report(root)
                    expected_error = "event count.*integer"

                attestation_path, attestation_sha256 = self._task11_attestation(
                    root, workspace / "trusted" / "attestation.json"
                )
                treatment_fingerprint = canonical_fingerprint(
                    {
                        "prompt_profile": "baseline",
                        "trusted_preflight_enabled": False,
                        "app_tool_strategy": "all_public",
                    }
                )
                with self._fixture_parent_identity(root), patch(
                    "txnmem_appworld_projection."
                    "APPWORLD_FORMAL_TASK11_ATTESTATION_SHA256_BY_TREATMENT",
                    {treatment_fingerprint: attestation_sha256},
                ):
                    with self.assertRaisesRegex(ValueError, expected_error):
                        regenerate_appworld_native_realism_projection(
                            root,
                            workspace / "events.jsonl",
                            task11_attestation_path=attestation_path,
                        )

    def test_native_projection_rejects_inexact_condition_repetitions_and_extra_dirs(self):
        for invalid in ("1", 1.9):
            with self.subTest(invalid=invalid), TemporaryDirectory() as tmp:
                workspace = Path(tmp)
                root = workspace / "run"
                self._native_run(root)
                summary_path = (
                    root
                    / "runs"
                    / "appworld"
                    / "shard_000"
                    / "results"
                    / "native_batch_summary.json"
                )
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
                summary["condition"]["repetitions"] = invalid
                summary["condition_fingerprint"] = canonical_fingerprint(
                    summary["condition"]
                )
                summary_path.write_text(json.dumps(summary), encoding="utf-8")
                self._rewrite_bound_report(root)

                with self._fixture_parent_identity(root):
                    with self.assertRaisesRegex(ValueError, "repetitions.*integer"):
                        regenerate_appworld_native_realism_projection(
                            root, workspace / "events.jsonl"
                        )

        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            self._native_run(root)
            (
                root
                / "runs"
                / "appworld"
                / "shard_000"
                / "rep_02"
                / "data"
            ).mkdir(parents=True)
            with self._fixture_parent_identity(root):
                with self.assertRaisesRegex(ValueError, "unexpected repetition"):
                    regenerate_appworld_native_realism_projection(
                        root, workspace / "events.jsonl"
                    )

    def test_native_export_is_external_distinct_exclusive_and_no_follow(self):
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            paths = self._native_run(root)
            original_trace = paths["trace"].read_bytes()

            with self._fixture_parent_identity(root):
                with self.assertRaisesRegex(ValueError, "outside.*run root"):
                    regenerate_appworld_native_realism_projection(
                        root, paths["trace"]
                    )
            self.assertEqual(paths["trace"].read_bytes(), original_trace)

            target = workspace / "must-not-change.txt"
            target.write_text("sentinel", encoding="utf-8")
            symlink_output = workspace / "events-link.jsonl"
            symlink_output.symlink_to(target)
            with self._fixture_parent_identity(root):
                with self.assertRaisesRegex(ValueError, "symlink|overwrite|existing"):
                    regenerate_appworld_native_realism_projection(
                        root, symlink_output
                    )
            self.assertEqual(target.read_text(encoding="utf-8"), "sentinel")

            events_path = workspace / "export" / "events.jsonl"
            with self._fixture_parent_identity(root):
                regenerate_appworld_native_realism_projection(root, events_path)
                with self.assertRaisesRegex(ValueError, "overwrite|existing"):
                    regenerate_appworld_native_realism_projection(root, events_path)

    def test_rejected_in_tree_output_does_not_create_missing_repetition_directory(self):
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            self._native_run(root)
            unexpected = (
                root
                / "runs"
                / "appworld"
                / "shard_000"
                / "rep_02"
            )
            with self._fixture_parent_identity(root):
                with self.assertRaisesRegex(ValueError, "outside.*run root"):
                    regenerate_appworld_native_realism_projection(
                        root, unexpected / "events.jsonl"
                    )
            self.assertFalse(unexpected.exists())
            with self._fixture_parent_identity(root):
                inventory = regenerate_appworld_native_realism_projection(
                    root, workspace / "safe-events.jsonl"
                )
            self.assertEqual(inventory["event_count"], 168)

    def test_native_export_cli_rejects_inventory_alias_and_symlink_before_writing(self):
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            root = workspace / "run"
            self._native_run(root)
            same_path = workspace / "same.json"
            with self._fixture_parent_identity(root):
                with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    appworld_projection_main(
                        [
                            "--native-run-root",
                            str(root),
                            "--native-output",
                            str(same_path),
                            "--native-inventory",
                            str(same_path),
                        ]
                    )
            self.assertFalse(same_path.exists())

            target = workspace / "inventory-target.json"
            target.write_text("sentinel", encoding="utf-8")
            inventory_link = workspace / "inventory-link.json"
            inventory_link.symlink_to(target)
            events_path = workspace / "events.jsonl"
            with self._fixture_parent_identity(root):
                with self.assertRaisesRegex(ValueError, "symlink"):
                    appworld_projection_main(
                        [
                            "--native-run-root",
                            str(root),
                            "--native-output",
                            str(events_path),
                            "--native-inventory",
                            str(inventory_link),
                        ]
                    )
            self.assertFalse(events_path.exists())
            self.assertEqual(target.read_text(encoding="utf-8"), "sentinel")

    def test_native_export_cli_rejects_ancestor_alias_without_partial_bundle(self):
        for event_suffix, inventory_suffix in (
            (("bundle", "events.jsonl"), ("bundle",)),
            (("bundle",), ("bundle", "inventory.json")),
        ):
            with self.subTest(
                event_suffix=event_suffix,
                inventory_suffix=inventory_suffix,
            ), TemporaryDirectory() as tmp:
                workspace = Path(tmp)
                root = workspace / "run"
                self._native_run(root)
                events_path = workspace.joinpath(*event_suffix)
                inventory_path = workspace.joinpath(*inventory_suffix)
                with self._fixture_parent_identity(root):
                    with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                        appworld_projection_main(
                            [
                                "--native-run-root",
                                str(root),
                                "--native-output",
                                str(events_path),
                                "--native-inventory",
                                str(inventory_path),
                            ]
                        )
                self.assertFalse((workspace / "bundle").exists())

    def test_reference_projection_cannot_be_promoted_to_formal_realism(self):
        with self.assertRaisesRegex(ValueError, "diagnostic.*native Agent trace"):
            validate_appworld_realism_bundle(
                events_path=Path("projection.jsonl"),
                selection_path=Path("selection.json"),
                projection_inventory_path=Path("inventory.json"),
                dataset_path=Path("test_normal.txt"),
                appworld_root=Path("appworld"),
            )


if __name__ == "__main__":
    unittest.main()
