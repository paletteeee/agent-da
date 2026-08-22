"""External baseline runner artifact and exclusion contracts."""

from __future__ import annotations

import csv
import hashlib
import json
import subprocess
import sys
import unittest
from unittest.mock import patch
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_adapter_contract import (  # noqa: E402
    CapabilitySupport,
    ReplayObservation,
    RuntimeAdapterError,
    UnsupportedMappingError,
)
from txnmem_external_experiment import (  # noqa: E402
    AdapterRegistration,
    CAPABILITY_DIMENSIONS,
    REGISTRY_ORDER,
    RESULTS_FIELDS,
    RunContext,
    _default_registry,
    run_external_experiment,
)
from txnmem_workloads import generate_instance  # noqa: E402


class _SuccessAdapter:
    capabilities = (CapabilitySupport("native_write", True, "fake native write"),)

    def run(self, instance):
        trace = [{"step": 1, "event": "capability_absent", "capability": "atomic_commit"}]
        return ReplayObservation(
            transaction_state="completed",
            final_memories={},
            committed_memory_ids=[],
            trace=trace,
            metrics={"operation_count": 1, "repair_count": 0},
        )


class _RuntimeAdapter:
    capabilities = ()

    def run(self, instance):
        raise RuntimeAdapterError("backend failed: secret://not-for-artifact")


class _UnsupportedAdapter:
    capabilities = ()

    def run(self, instance):
        raise UnsupportedMappingError("crash recovery is not mapped")


class _MutatingAdapter:
    capabilities = ()

    def run(self, instance):
        instance["operations"][0]["type"] = "tampered"
        return _SuccessAdapter().run(instance)


def _registration(name, adapter_type, mode="controlled"):
    return AdapterRegistration(
        name=name,
        adapter_version="fake-v1",
        backend_mode=mode,
        factory=lambda context: adapter_type(),
        capabilities=lambda: adapter_type.capabilities,
    )


def _instances():
    first = generate_instance("atomic_multi_write", 701, {"txn_size": 1})
    first["failure_schedule"] = []
    second = generate_instance("scope_bypass", 702)
    return [first, second]


class ExternalExperimentTests(unittest.TestCase):
    def test_runner_keeps_registry_order_and_writes_one_success_per_pair(self):
        """Would fail if requested ordering controls output or attempts are skipped/duplicated."""
        registry = (
            _registration("AppendOnly", _SuccessAdapter),
            _registration("LastWriteWins", _SuccessAdapter),
        )
        with TemporaryDirectory() as temporary:
            output = run_external_experiment(
                _instances(),
                Path(temporary),
                requested_adapters=("LastWriteWins", "AppendOnly"),
                registry=registry,
                context=RunContext(run_id="run-order"),
            )
            with (Path(temporary) / "results.csv").open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

        self.assertEqual([row["adapter"] for row in rows], ["AppendOnly", "AppendOnly", "LastWriteWins", "LastWriteWins"])
        self.assertEqual(len(rows), 4)
        self.assertEqual(output["counts"], {"attempted": 4, "successful": 4, "correctness_included": 4, "excluded": 0, "capability_absent_observed": 4})
        self.assertTrue(all(row["correctness_included"] == "true" for row in rows))
        self.assertTrue(all(float(row["latency_ms"]) >= 0 for row in rows))

    def test_runner_records_exclusions_without_oracle_metrics_or_error_secrets(self):
        """Would fail if failures are scored, not categorized, or error payloads leak messages."""
        registry = (
            _registration("Runtime", _RuntimeAdapter),
            _registration("Unsupported", _UnsupportedAdapter),
        )
        with TemporaryDirectory() as temporary:
            output = run_external_experiment(
                [_instances()[0]],
                Path(temporary),
                registry=registry,
                context=RunContext(run_id="run-errors"),
            )
            root = Path(temporary)
            with (root / "results.csv").open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            errors = [json.loads(line) for line in (root / "errors.jsonl").read_text(encoding="utf-8").splitlines()]

        self.assertEqual([row["error_category"] for row in rows], ["runtime_error", "unsupported_mapping"])
        self.assertTrue(all(row["correctness_included"] == "false" for row in rows))
        self.assertTrue(all(row["partial_update_rate"] == "" for row in rows))
        self.assertEqual(output["summary"]["oracle"]["groups"], {})
        self.assertEqual([error["error_category"] for error in errors], ["runtime_error", "unsupported_mapping"])
        self.assertNotIn("secret://", json.dumps(errors))
        self.assertTrue(all({"workload", "seed", "adapter_version", "backend_mode", "run_status", "latency_ms"} <= set(error) for error in errors))

    def test_artifacts_have_fixed_headers_and_hash_bound_manifest(self):
        """Would fail if reproducibility metadata omits an artifact, input identity, or fixed schema."""
        registry = (_registration("AppendOnly", _SuccessAdapter),)
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload = b'{"input":"raw bytes are retained"}\n'
            output = run_external_experiment(
                _instances(), root, registry=registry, context=RunContext(run_id="run-manifest"), input_bytes=payload
            )
            manifest = json.loads((root / "run_manifest.json").read_text(encoding="utf-8"))
            summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
            with (root / "results.csv").open(encoding="utf-8") as handle:
                header = next(csv.reader(handle))
            with (root / "capabilities.csv").open(encoding="utf-8") as handle:
                capabilities_header = next(csv.reader(handle))
            results_sha256 = hashlib.sha256((root / "results.csv").read_bytes()).hexdigest()

        self.assertEqual(header, list(RESULTS_FIELDS))
        self.assertEqual(capabilities_header, ["adapter", "adapter_version", "backend_mode", "capability", "supported", "detail"])
        self.assertEqual(manifest["run_id"], "run-manifest")
        self.assertEqual(manifest["input"]["count"], 2)
        self.assertEqual(manifest["input"]["sha256"], hashlib.sha256(payload).hexdigest())
        self.assertGreaterEqual(manifest["duration_ms"], 0)
        self.assertNotIn("run_manifest.json", manifest["artifacts"])
        self.assertEqual(set(manifest["artifacts"]), {"results.csv", "summary.json", "capabilities.csv", "capabilities.json", "environment.json", "errors.jsonl"})
        self.assertEqual(summary["schema_version"], "txnmem-external-runner-v1")
        self.assertEqual(summary["counts"], output["counts"])
        self.assertEqual(
            manifest["artifacts"]["results.csv"]["sha256"],
            results_sha256,
        )
        self.assertIn("head", manifest["git"])
        self.assertIn("dirty", manifest["git"])
        self.assertEqual(set(manifest["environment"]["package_versions"]), {"mem0ai", "langgraph", "langgraph-checkpoint-postgres", "psycopg-binary"})

    def test_controlled_only_run_does_not_import_optional_backends(self):
        """Would fail if merely selecting a controlled baseline imported Mem0 or LangGraph."""
        imported = []
        original_import = __import__

        def guarded_import(name, *args, **kwargs):
            if name.startswith(("mem0", "langgraph")):
                imported.append(name)
                raise AssertionError(f"optional backend imported: {name}")
            return original_import(name, *args, **kwargs)

        with TemporaryDirectory() as temporary, patch("builtins.__import__", side_effect=guarded_import):
            output = run_external_experiment(
                [_instances()[0]],
                Path(temporary),
                requested_adapters=("AppendOnly",),
                context=RunContext(run_id="controlled-only"),
            )

        self.assertEqual(imported, [])
        self.assertEqual(output["counts"]["attempted"], 1)

    def test_duplicate_instance_ids_are_rejected_before_artifacts_are_published(self):
        """Would fail if invalid input emitted a partial manifest or duplicate replay rows."""
        first = _instances()[0]
        duplicate = dict(first)
        with TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            with self.assertRaisesRegex(ValueError, "duplicate instance_id"):
                run_external_experiment(
                    [first, duplicate], output, registry=(_registration("AppendOnly", _SuccessAdapter),)
                )
            self.assertFalse(output.exists())

    def test_mem0_state_root_is_unique_for_a_run_id(self):
        """Would fail if a repeat run silently reused persistent Mem0 state."""
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = run_external_experiment(
                [_instances()[0]], root, requested_adapters=("Mem0",), context=RunContext(run_id="mem0-unique")
            )
            with self.assertRaisesRegex(ValueError, "Mem0 run root already exists"):
                run_external_experiment(
                    [_instances()[0]], root, requested_adapters=("Mem0",), context=RunContext(run_id="mem0-unique")
                )

        self.assertEqual(first["manifest"]["backend_state"]["mem0"]["mode"], "embedded_qdrant")

    def test_in_memory_fallback_excludes_crash_without_calling_adapter(self):
        """Would fail if an ephemeral recovery row reached the correctness oracle."""
        called = []
        registration = AdapterRegistration(
            name="LangGraphStore", adapter_version="fake-v1", backend_mode="in_memory_fallback",
            factory=lambda context: called.append(True), capabilities=lambda: (),
        )
        with TemporaryDirectory() as temporary:
            output = run_external_experiment(
                [generate_instance("crash_during_commit", 703)], Path(temporary), registry=(registration,)
            )

        self.assertEqual(called, [])
        self.assertEqual(output["counts"], {"attempted": 1, "successful": 0, "correctness_included": 0, "excluded": 1, "capability_absent_observed": 0})
        self.assertEqual(output["summary"]["exclusions_by_category"], {"unsupported_mapping": 1})

    def test_cli_binds_exact_input_bytes_and_publishes_required_artifacts(self):
        """Would fail if the public CLI skipped validation, selected extra adapters, or omitted artifacts."""
        instance = _instances()[0]
        payload = json.dumps(instance, sort_keys=True).encode("utf-8") + b"\n"
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "instances.jsonl"
            output = root / "output"
            source.write_bytes(payload)
            completed = subprocess.run(
                [
                    sys.executable, "src/txnmem_external_experiment.py", "run", "--instances", str(source),
                    "--out-dir", str(output), "--adapters", "AppendOnly", "--run-id", "cli-run",
                ], cwd=ROOT, check=True, capture_output=True, text=True,
            )
            manifest = json.loads((output / "run_manifest.json").read_text(encoding="utf-8"))
            artifacts = {path.name for path in output.iterdir() if path.is_file()}

        self.assertIn("wrote 1 attempts", completed.stdout)
        self.assertEqual(manifest["input"]["sha256"], hashlib.sha256(payload).hexdigest())
        self.assertEqual(manifest["input"]["path"], str(source.resolve()))
        self.assertEqual(artifacts, {"results.csv", "summary.json", "capabilities.csv", "capabilities.json", "environment.json", "errors.jsonl", "run_manifest.json"})

    def test_mutating_adapter_is_excluded_without_changing_canonical_input_or_hash(self):
        """Would fail if adapters can taint later attempts, rows, or the bound input digest."""
        instance = _instances()[0]
        payload = json.dumps(instance, sort_keys=True).encode("utf-8") + b"\n"
        original = json.loads(json.dumps(instance))
        with TemporaryDirectory() as temporary:
            output = run_external_experiment(
                [instance], Path(temporary), registry=(_registration("AppendOnly", _MutatingAdapter),), input_bytes=payload
            )
            with (Path(temporary) / "results.csv").open(newline="", encoding="utf-8") as handle:
                row = next(csv.DictReader(handle))

        self.assertEqual(instance, original)
        self.assertEqual(row["error_category"], "runtime_error")
        self.assertEqual(output["manifest"]["input"]["sha256"], hashlib.sha256(payload).hexdigest())

    def test_unavailable_langgraph_is_registered_and_emits_one_redacted_row_per_instance(self):
        """Would fail if an optional LangGraph import failure aborted registry setup instead of replay accounting."""
        original_import = __import__

        def unavailable_langgraph(name, *args, **kwargs):
            if name.startswith("langgraph"):
                raise ImportError("simulated missing optional dependency")
            return original_import(name, *args, **kwargs)

        with TemporaryDirectory() as temporary, patch("builtins.__import__", side_effect=unavailable_langgraph):
            run_external_experiment(
                _instances(), Path(temporary), requested_adapters=("LangGraphStore",), context=RunContext(run_id="missing-langgraph")
            )
            with (Path(temporary) / "results.csv").open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            errors = [json.loads(line) for line in (Path(temporary) / "errors.jsonl").read_text().splitlines()]

        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["error_category"] == "runtime_error" for row in rows))
        self.assertEqual(len(errors), 2)
        self.assertNotIn("simulated missing", json.dumps(errors))

    def test_mem0_base_root_and_run_id_are_safe_and_manifested(self):
        """Would fail if a user root was ignored or a run ID could escape the state base directory."""
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            state_base = root / "external-mem0"
            result = run_external_experiment(
                [_instances()[0]], root / "out", requested_adapters=("Mem0",),
                context=RunContext(run_id="safe_run-1", mem0_state_root=state_base),
            )
            with self.assertRaisesRegex(ValueError, "safe path component"):
                run_external_experiment(
                    [_instances()[0]], root / "unsafe", requested_adapters=("Mem0",),
                    context=RunContext(run_id="../escape", mem0_state_root=state_base),
                )

        self.assertEqual(result["manifest"]["backend_state"]["mem0"]["root"], str((state_base / "safe_run-1").resolve()))

    def test_summary_uses_variant_groups_and_retains_excluded_only_groups(self):
        """Would fail if grouped oracle stats use adapter names or omit a workload/variant with only exclusions."""
        registry = (
            _registration("AppendOnly", _SuccessAdapter),
            _registration("LangGraphStore", _RuntimeAdapter),
        )
        with TemporaryDirectory() as temporary:
            output = run_external_experiment([_instances()[0]], Path(temporary), registry=registry)

        groups = output["summary"]["groups"]
        self.assertEqual(output["summary"]["oracle"]["group_keys"], ["workload", "variant"])
        self.assertIn("atomic_multi_write/AppendOnly", groups)
        self.assertEqual(groups["atomic_multi_write/LangGraphStore"]["attempted"], 1)
        self.assertEqual(groups["atomic_multi_write/LangGraphStore"]["excluded"], 1)
        self.assertEqual(output["summary"]["adapter_counts"]["LangGraphStore"]["successful"], 0)
        self.assertEqual(output["summary"]["adapter_counts"]["LangGraphStore"]["exclusions_by_category"], {"runtime_error": 1})
        self.assertEqual(output["summary"]["workload_counts"]["atomic_multi_write"]["attempted"], 2)
        self.assertEqual(output["summary"]["workload_counts"]["atomic_multi_write"]["exclusions_by_category"], {"runtime_error": 1})

    def test_capabilities_json_has_schema_and_distinguishes_controlled_baselines(self):
        """Would fail if JSON diverged from CSV or flattened the three controlled baseline semantics."""
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_external_experiment(
                [_instances()[0]], root,
                requested_adapters=("MetadataFiltered", "LastWriteWins", "AppendOnly"),
            )
            capability_json = json.loads((root / "capabilities.json").read_text(encoding="utf-8"))
            with (root / "capabilities.csv").open(newline="", encoding="utf-8") as handle:
                csv_rows = list(csv.DictReader(handle))

        self.assertEqual(capability_json["schema_version"], "txnmem-external-runner-v1")
        flat = [row for adapter in capability_json["adapters"] for row in adapter["capabilities"]]
        self.assertEqual([(row["adapter"], row["capability"]) for row in flat], [(row["adapter"], row["capability"]) for row in csv_rows])
        supports = {(row["adapter"], row["capability"]): row["supported"] for row in flat}
        self.assertTrue(supports[("LastWriteWins", "version_supersession")])
        self.assertTrue(supports[("MetadataFiltered", "shared_scope_isolation")])
        self.assertFalse(supports[("AppendOnly", "atomic_multi_record_commit")])
        dimensions = {"single_record_read_write", "atomic_multi_record_commit", "commit_policy_revalidation", "shared_scope_isolation", "version_supersession", "provenance_propagation", "recursive_provenance_invalidation", "crash_recovery"}
        for adapter in ("AppendOnly", "LastWriteWins", "MetadataFiltered"):
            self.assertEqual({row["capability"] for row in flat if row["adapter"] == adapter}, dimensions)

    def test_production_registry_uses_one_ordered_capability_matrix_for_all_formal_adapters(self):
        """Would fail if a formal adapter exposes sparse or incomparable capability dimensions."""
        with TemporaryDirectory() as temporary:
            registry = _default_registry(RunContext(run_id="capability-matrix"), set(REGISTRY_ORDER), Path(temporary))
            capability_rows = {
                registration.name: tuple(registration.capabilities())
                for registration in registry
            }

        self.assertEqual(tuple(capability_rows), REGISTRY_ORDER)
        self.assertTrue(all(tuple(row.capability for row in rows) == CAPABILITY_DIMENSIONS for rows in capability_rows.values()))
        support = {
            (adapter, row.capability): row.supported
            for adapter, rows in capability_rows.items()
            for row in rows
        }
        for adapter in ("AppendOnly", "LastWriteWins", "MetadataFiltered"):
            self.assertTrue(support[(adapter, "single_record_read_write")])
        self.assertTrue(support[("MetadataFiltered", "shared_scope_isolation")])
        self.assertTrue(support[("LastWriteWins", "version_supersession")])
        self.assertTrue(support[("Mem0", "crash_recovery")])
        self.assertFalse(support[("LangGraphStore", "crash_recovery")])


if __name__ == "__main__":
    unittest.main()
