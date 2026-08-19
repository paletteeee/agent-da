"""Reproducible task-level manifests for the public benchmark batch runner."""

from __future__ import annotations

import json
import hashlib
import os
import re
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import txnmem_benchmark_manifests as benchmark_manifests
import txnmem_experiment as experiment_cli
from txnmem_benchmark_manifests import build_native_scale_manifest, write_manifest
from txnmem_formal_io import FormalIOError, FormalStore
from txnmem_real_experiment import load_task_manifest


class NativeScaleManifestTests(unittest.TestCase):
    def _appworld_source(self, root: Path) -> Path:
        data_root = root / "data"
        (data_root / "datasets").mkdir(parents=True)
        (data_root / "tasks").mkdir()
        (data_root / "version.txt").write_text("0.2.0\n", encoding="utf-8")
        ordered_ids = ["z-task", "a-task", "m-task"]
        (data_root / "datasets" / "test_normal.txt").write_text(
            "\n".join(ordered_ids) + "\n", encoding="utf-8"
        )
        for task_id in ordered_ids:
            task_dir = data_root / "tasks" / task_id
            task_dir.mkdir()
            (task_dir / "specs.json").write_text(
                json.dumps({"instruction": task_id}), encoding="utf-8"
            )
        return root

    def _parent_manifest(self, task_count: int = 7) -> dict:
        manifest = {
            "manifest_version": 1,
            "dataset_name": "tau-bench-retail-test",
            "benchmark": "tau-bench",
            "domain": "retail",
            "split": "test",
            "task_count": task_count,
            "source_identity": {
                "task_source": {
                    "path": "tau_bench/envs/retail/tasks_test.py",
                    "sha256": "6" * 64,
                },
                "package": {"distribution": "tau-bench", "version": "0.1.0"},
                "fingerprint": "7" * 64,
            },
            "condition_fingerprint": "8" * 64,
            "tasks": [
                {
                    "task_id": f"tau-retail-test-{index:04d}",
                    "raw_task_id": index,
                    "source_position": index,
                    "prompt": f"task {index}",
                }
                for index in range(task_count)
            ],
        }
        encoded = json.dumps(
            manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        manifest["manifest_hash"] = hashlib.sha256(encoded).hexdigest()
        return manifest

    def _locomo_source(self, directory: Path) -> Path:
        path = directory / "locomo.json"
        path.write_text(
            json.dumps(
                {
                    "samples": [
                        {"sample_id": f"sample-{index}", "conversation": {}}
                        for index in range(5)
                    ]
                }
            ),
            encoding="utf-8",
        )
        return path

    def _run_scale_script(
        self, arguments: list[str], *, environment: dict[str, str] | None = None
    ) -> subprocess.CompletedProcess[str]:
        script_environment = dict(os.environ)
        script_environment["TXNMEM_PYTHON"] = sys.executable
        if environment is not None:
            script_environment.update(environment)
        return subprocess.run(
            [str(ROOT / "scripts" / "run_native_scale.sh"), *arguments],
            cwd=ROOT,
            env=script_environment,
            text=True,
            capture_output=True,
            check=False,
        )

    def _write_merge_fixture(
        self, out_dir: Path, *, parent: dict | None = None, shard_count: int = 2
    ) -> dict:
        parent = self._parent_manifest(task_count=4) if parent is None else parent
        manifest_dir = out_dir / "manifests" / "tau_bench"
        manifest_dir.mkdir(parents=True, exist_ok=True)
        (manifest_dir / "parent.json").write_text(
            json.dumps(parent, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        shards = benchmark_manifests.shard_manifest(parent, shard_count)
        for shard in shards:
            shard_name = f"shard_{shard['shard_index']:03d}"
            (manifest_dir / f"{shard_name}.json").write_text(
                json.dumps(shard, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            rows = [
                {
                    "task_id": task["task_id"],
                    "source_position": task["source_position"],
                    "repetition": 1,
                    "status": "completed",
                    "official": {"status": "available", "success": True},
                }
                for task in shard["tasks"]
            ]
            report = {
                "parent_manifest_hash": parent["manifest_hash"],
                "execution_manifest_hash": shard["manifest_hash"],
                "shard_index": shard["shard_index"],
                "shard_count": shard["shard_count"],
                "benchmark": parent["benchmark"],
                "domain": parent["domain"],
                "split": parent["split"],
                "source_identity": parent["source_identity"],
                "condition_fingerprint": parent["condition_fingerprint"],
                "execution_condition_fingerprint": "e" * 64,
                "repetitions": 1,
                "task_summaries": rows,
            }
            report_dir = out_dir / "runs" / "tau_bench" / shard_name
            report_dir.mkdir(parents=True)
            raw_rows = [
                {
                    "task_id": row["task_id"],
                    "status": row["status"],
                    "official": row["official"],
                }
                for row in rows
            ]
            raw_path = report_dir / "results" / "native_batch_summary.json"
            raw_path.parent.mkdir()
            raw_path.write_text(
                json.dumps(
                    {
                        "manifest_sha256": shard["manifest_hash"],
                        "condition_fingerprint": "e" * 64,
                        "repetitions": 1,
                        "task_summaries": raw_rows,
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            (report_dir / "shard_report.json").write_text(
                json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        return parent

    def _complete_locomo_scale_run(
        self, root: Path, *, shard_count: int = 1
    ) -> tuple[Path, list[str], dict[str, str]]:
        source = self._locomo_source(root)
        out_dir = root / "out"
        model_log = root / "model-invocation.json"
        python_shim = self._native_batch_python_shim(root / "native-batch-python")
        environment = {
            "TXNMEM_PYTHON": str(python_shim),
            "TXNMEM_LOCOMO_SOURCE": str(source),
            "TXNMEM_LOCOMO_EVALUATOR_COMMAND": "",
            "TXNMEM_TEST_MODEL_LOG": str(model_log),
        }
        arguments = [
            "--endpoint",
            "no-model-call://local-shim",
            "--model",
            "local-shim",
            "--benchmarks",
            "locomo",
            "--locomo-tasks",
            "5",
            "--out-dir",
            str(out_dir),
            "--shard-count",
            str(shard_count),
        ]
        completed = self._run_scale_script(arguments, environment=environment)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertTrue(model_log.is_file())
        return out_dir, arguments, environment

    def _insert_duplicate_scalar(
        self,
        path: Path,
        *,
        key: str,
        duplicate_value: object,
        occurrence: int = 0,
    ) -> None:
        payload = json.loads(path.read_text(encoding="utf-8"))
        text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
        pattern = re.compile(
            rf"(?m)^(?P<indent>[ \t]*){re.escape(json.dumps(key))}[ \t]*:"
        )
        matches = list(pattern.finditer(text))
        self.assertGreater(len(matches), occurrence, f"missing JSON key {key} in {path}")
        match = matches[occurrence]
        insertion = (
            f"{match.group('indent')}{json.dumps(key)}: "
            f"{json.dumps(duplicate_value, ensure_ascii=False)},\n"
        )
        path.write_text(text[: match.start()] + insertion + text[match.start() :], encoding="utf-8")

    def _formal_tau_source(self, root: Path) -> Path:
        task_source = root / "tau_bench" / "envs" / "retail" / "tasks_test.py"
        task_source.parent.mkdir(parents=True)
        for package in (
            root / "tau_bench",
            root / "tau_bench" / "envs",
            root / "tau_bench" / "envs" / "retail",
        ):
            (package / "__init__.py").write_text("", encoding="utf-8")
        task_source.write_text(
            "TASKS_TEST = ["
            + ",".join(
                repr({"instruction": f"retail task {index}"}) for index in range(115)
            )
            + "]\n",
            encoding="utf-8",
        )
        dist_info = root / "tau_bench-0.1.0.dist-info"
        dist_info.mkdir()
        (dist_info / "METADATA").write_text(
            "Metadata-Version: 2.1\nName: tau-bench\nVersion: 0.1.0\n",
            encoding="utf-8",
        )
        return root

    def _no_model_python_shim(self, path: Path) -> Path:
        path.write_text(
            f"""#!{sys.executable}
import os
from pathlib import Path
import sys

if len(sys.argv) > 1 and Path(sys.argv[1]).name == "txnmem_experiment.py":
    raise SystemExit("model execution forbidden by native-scale script test")
os.execv({sys.executable!r}, [{sys.executable!r}, *sys.argv[1:]])
""",
            encoding="utf-8",
        )
        path.chmod(0o755)
        return path

    def _native_batch_python_shim(self, path: Path) -> Path:
        path.write_text(
            f"""#!{sys.executable}
import json
import os
from pathlib import Path
import sys

if len(sys.argv) > 1 and Path(sys.argv[1]).name == "txnmem_experiment.py":
    arguments = sys.argv[2:]

    def option(name):
        return arguments[arguments.index(name) + 1]

    manifest = json.loads(Path(option("--manifest")).read_text(encoding="utf-8"))
    repetitions = int(option("--repetitions"))
    rows = []
    for _repetition in range(repetitions):
        for task in manifest["tasks"]:
            rows.append({{
                "task_id": task["task_id"],
                "status": "completed",
                "official": {{"status": "available", "success": True}},
            }})
    output = Path(option("--out-dir")) / "results" / "native_batch_summary.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({{
        "manifest_sha256": manifest["manifest_hash"],
        "condition_fingerprint": "f" * 64,
        "repetitions": repetitions,
        "task_summaries": rows,
    }}), encoding="utf-8")
    Path(os.environ["TXNMEM_TEST_MODEL_LOG"]).write_text(
        json.dumps(arguments), encoding="utf-8"
    )
    raise SystemExit(0)
os.execv({sys.executable!r}, [{sys.executable!r}, *sys.argv[1:]])
""",
            encoding="utf-8",
        )
        path.chmod(0o755)
        return path

    def _actual_offline_batch_python_shim(self, path: Path) -> Path:
        path.write_text(
            f"""#!{sys.executable}
import os
from pathlib import Path
import sys

if len(sys.argv) > 1 and Path(sys.argv[1]).name == "txnmem_experiment.py":
    arguments = list(sys.argv[2:])

    def option(name):
        return arguments[arguments.index(name) + 1]

    planted = os.environ.get("TXNMEM_TEST_PLANTED_OUTPUT")
    if planted:
        victim = Path(option("--out-dir")).joinpath(*planted.split("/"))
        victim.parent.mkdir(parents=True, exist_ok=True)
        victim.symlink_to(Path(os.environ["TXNMEM_TEST_ESCAPED_TARGET"]))
    arguments.append("--offline-fixture")
    os.execv(
        {sys.executable!r},
        [{sys.executable!r}, sys.argv[1], *arguments],
    )
os.execv({sys.executable!r}, [{sys.executable!r}, *sys.argv[1:]])
""",
            encoding="utf-8",
        )
        path.chmod(0o755)
        return path

    def _model_side_effect_python_shim(self, path: Path) -> Path:
        path.write_text(
            f"""#!{sys.executable}
import os
from pathlib import Path
import sys

if len(sys.argv) > 1 and Path(sys.argv[1]).name == "txnmem_experiment.py":
    Path(os.environ["TXNMEM_TEST_MODEL_MARKER"]).write_text(
        "runner invoked\\n", encoding="utf-8"
    )
    raise SystemExit("model execution forbidden by protected-merge test")
os.execv({sys.executable!r}, [{sys.executable!r}, *sys.argv[1:]])
""",
            encoding="utf-8",
        )
        path.chmod(0o755)
        return path

    def test_manifest_is_deterministic_and_contains_hash_and_task_split(self):
        with TemporaryDirectory() as tmp:
            source = self._locomo_source(Path(tmp))
            first = build_native_scale_manifest("locomo", source, limit=5, seed=17, split="test")
            second = build_native_scale_manifest("locomo", source, limit=5, seed=17, split="test")
        self.assertEqual(first, second)
        self.assertEqual(first["task_count"], 5)
        self.assertEqual(len(first["manifest_hash"]), 64)
        split = first["task_level_split"]
        self.assertTrue(set(split["train_task_ids"]).isdisjoint(split["holdout_task_ids"]))
        self.assertEqual(set(split["train_task_ids"]) | set(split["holdout_task_ids"]),
                         {task["task_id"] for task in first["tasks"]})

    def test_manifest_respects_requested_limit(self):
        with TemporaryDirectory() as tmp:
            source = self._locomo_source(Path(tmp))
            manifest = build_native_scale_manifest("locomo", source, limit=3, seed=17)
        self.assertEqual(manifest["task_count"], 3)
        self.assertEqual(len(manifest["tasks"]), 3)

    def test_manifest_rejects_non_positive_limit(self):
        with self.assertRaises(ValueError):
            build_native_scale_manifest("locomo", "missing.json", limit=0)

    def test_appworld_scale_manifest_keeps_exact_public_order_and_split_identity(self):
        with TemporaryDirectory() as tmp:
            source = self._appworld_source(Path(tmp))
            first = build_native_scale_manifest(
                "appworld", source, limit=3, seed=17, split="test_normal"
            )
            other_seed = build_native_scale_manifest(
                "appworld", source, limit=3, seed=99, split="test_normal"
            )

        expected_ids = ["appworld-z-task", "appworld-a-task", "appworld-m-task"]
        self.assertEqual([task["task_id"] for task in first["tasks"]], expected_ids)
        self.assertEqual([task["task_id"] for task in other_seed["tasks"]], expected_ids)
        self.assertEqual(first["benchmark"], "appworld")
        self.assertEqual(first["split"], "test_normal")
        self.assertEqual(first["public_split_identity"]["split"], "test_normal")
        self.assertEqual(first["public_split_identity"]["source_task_count"], 3)
        self.assertEqual(first["public_split_identity"]["selected_task_count"], 3)
        self.assertEqual(
            first["public_split_identity"]["ordered_raw_task_ids_sha256"],
            hashlib.sha256(
                json.dumps(
                    ["z-task", "a-task", "m-task"],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest(),
        )
        positions = {task_id: index for index, task_id in enumerate(expected_ids)}
        task_split = first["task_level_split"]
        for key in ("train_task_ids", "holdout_task_ids"):
            self.assertEqual(
                [positions[task_id] for task_id in task_split[key]],
                sorted(positions[task_id] for task_id in task_split[key]),
            )
        self.assertEqual(
            set(task_split["train_task_ids"]) | set(task_split["holdout_task_ids"]),
            set(expected_ids),
        )
        self.assertEqual(first["source_sha256"], first["source_identity"]["fingerprint"])
        self.assertEqual(len(first["condition_fingerprint"]), 64)

    def test_shard_manifest_is_deterministic_exact_once_and_identity_bound(self):
        shard_manifest = getattr(benchmark_manifests, "shard_manifest", None)
        self.assertTrue(callable(shard_manifest), "shard_manifest must be implemented")
        parent = self._parent_manifest()

        first = shard_manifest(parent, 3)
        second = shard_manifest(parent, 3)

        self.assertEqual(first, second)
        self.assertEqual(len(first), 3)
        flattened = [task for shard in first for task in shard["tasks"]]
        self.assertEqual(
            sorted(task["source_position"] for task in flattened), list(range(7))
        )
        self.assertEqual(len({task["task_id"] for task in flattened}), 7)
        for index, shard in enumerate(first):
            self.assertEqual(shard["shard_index"], index)
            self.assertEqual(shard["shard_count"], 3)
            self.assertEqual(shard["parent_manifest_hash"], parent["manifest_hash"])
            self.assertEqual(shard["benchmark"], "tau-bench")
            self.assertEqual(shard["domain"], "retail")
            self.assertEqual(shard["split"], "test")
            self.assertEqual(
                shard["condition_fingerprint"], parent["condition_fingerprint"]
            )
            self.assertEqual(shard["source_identity"], parent["source_identity"])

    def test_shard_manifest_rejects_invalid_count_and_duplicate_task_ids(self):
        shard_manifest = getattr(benchmark_manifests, "shard_manifest", None)
        self.assertTrue(callable(shard_manifest), "shard_manifest must be implemented")
        parent = self._parent_manifest(task_count=2)
        for count in (0, -1, 3, True):
            with self.subTest(count=count), self.assertRaises(ValueError):
                shard_manifest(parent, count)

        duplicate = self._parent_manifest(task_count=2)
        duplicate["tasks"][1]["task_id"] = duplicate["tasks"][0]["task_id"]
        duplicate_without_hash = dict(duplicate)
        duplicate_without_hash.pop("manifest_hash")
        encoded = json.dumps(
            duplicate_without_hash,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        duplicate["manifest_hash"] = hashlib.sha256(encoded).hexdigest()
        with self.assertRaisesRegex(ValueError, "duplicate"):
            shard_manifest(duplicate, 2)

    def test_shard_manifest_rejects_wrong_parent_hash_and_source_positions(self):
        shard_manifest = getattr(benchmark_manifests, "shard_manifest", None)
        self.assertTrue(callable(shard_manifest), "shard_manifest must be implemented")
        wrong_hash = self._parent_manifest()
        wrong_hash["manifest_hash"] = "f" * 64
        with self.assertRaisesRegex(ValueError, "manifest_hash"):
            shard_manifest(wrong_hash, 2)

        wrong_position = self._parent_manifest()
        wrong_position["tasks"][1]["source_position"] = 4
        without_hash = dict(wrong_position)
        without_hash.pop("manifest_hash")
        wrong_position["manifest_hash"] = hashlib.sha256(
            json.dumps(
                without_hash,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        with self.assertRaisesRegex(ValueError, "source positions"):
            shard_manifest(wrong_position, 2)

    def test_shard_manifest_is_accepted_by_the_native_manifest_loader_with_identity_intact(self):
        shard_manifest = getattr(benchmark_manifests, "shard_manifest", None)
        self.assertTrue(callable(shard_manifest), "shard_manifest must be implemented")
        shard = shard_manifest(self._parent_manifest(), 2)[0]

        loaded, digest = load_task_manifest(shard)

        self.assertEqual(digest, shard["manifest_hash"])
        for field in (
            "benchmark",
            "domain",
            "split",
            "source_identity",
            "condition_fingerprint",
            "parent_manifest_hash",
            "shard_index",
            "shard_count",
        ):
            self.assertEqual(loaded[field], shard[field])

    def test_shard_execution_condition_uses_frozen_parent_manifest_hash(self):
        selector = getattr(experiment_cli, "_benchmark_condition_manifest_hash", None)
        self.assertTrue(
            callable(selector),
            "benchmark shard conditions must select the parent manifest hash",
        )
        shard = benchmark_manifests.shard_manifest(self._parent_manifest(), 2)[0]
        self.assertEqual(
            selector(shard, shard["manifest_hash"]), shard["parent_manifest_hash"]
        )
        parent = self._parent_manifest()
        self.assertEqual(selector(parent, parent["manifest_hash"]), parent["manifest_hash"])

    def test_write_manifest_returns_the_bound_canonical_manifest_hash(self):
        manifest = self._parent_manifest()
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "manifest.json"
            digest = write_manifest(manifest, path)
            written = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(digest, manifest["manifest_hash"])
        self.assertEqual(written, manifest)

    def test_scale_script_merge_only_refuses_existing_merge_without_resume(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "out"
            self._write_merge_fixture(out_dir)
            arguments = [
                "--merge-only",
                "--benchmarks",
                "tau-bench",
                "--out-dir",
                str(out_dir),
                "--shard-count",
                "2",
            ]
            first = self._run_scale_script(arguments)
            self.assertEqual(first.returncode, 0, first.stderr)
            merge_path = out_dir / "merged" / "tau_bench.json"
            existing = merge_path.read_bytes()

            repeated = self._run_scale_script(arguments)

            self.assertNotEqual(repeated.returncode, 0)
            self.assertEqual(merge_path.read_bytes(), existing)

    def test_scale_script_merge_only_resume_accepts_only_canonical_recomputation(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "out"
            self._write_merge_fixture(out_dir)
            arguments = [
                "--merge-only",
                "--benchmarks",
                "tau-bench",
                "--out-dir",
                str(out_dir),
                "--shard-count",
                "2",
            ]
            first = self._run_scale_script(arguments)
            self.assertEqual(first.returncode, 0, first.stderr)
            merge_path = out_dir / "merged" / "tau_bench.json"
            payload = json.loads(merge_path.read_text(encoding="utf-8"))

            canonical_equal = (
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
            ).encode("utf-8")
            with self.subTest(case="canonically equal"):
                merge_path.write_bytes(canonical_equal)
                equal_resume = self._run_scale_script([*arguments, "--resume"])
                self.assertEqual(equal_resume.returncode, 0, equal_resume.stderr)
                self.assertEqual(merge_path.read_bytes(), canonical_equal)

            invalid_existing = {
                "malformed sentinel": b"sentinel: do not overwrite\n",
                "different JSON": b'{"sentinel":true}\n',
            }
            for case, existing in invalid_existing.items():
                with self.subTest(case=case):
                    merge_path.write_bytes(existing)
                    rejected = self._run_scale_script([*arguments, "--resume"])
                    self.assertNotEqual(rejected.returncode, 0)
                    self.assertEqual(merge_path.read_bytes(), existing)

    def test_scale_script_resume_rejects_nested_duplicate_key_merge(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "out"
            self._write_merge_fixture(out_dir)
            arguments = [
                "--merge-only",
                "--benchmarks",
                "tau-bench",
                "--out-dir",
                str(out_dir),
                "--shard-count",
                "2",
            ]
            first = self._run_scale_script(arguments)
            self.assertEqual(first.returncode, 0, first.stderr)
            merge_path = out_dir / "merged" / "tau_bench.json"
            original = merge_path.read_text(encoding="utf-8")
            needle = '  "task_aggregate": {\n'
            self.assertIn(needle, original)
            ambiguous = original.replace(
                needle,
                needle + '    "denominator": 4,\n',
                1,
            )
            self.assertEqual(json.loads(ambiguous), json.loads(original))
            merge_path.write_text(ambiguous, encoding="utf-8")

            rejected = self._run_scale_script([*arguments, "--resume"])

            self.assertNotEqual(rejected.returncode, 0)
            self.assertEqual(merge_path.read_text(encoding="utf-8"), ambiguous)

    def test_scale_script_rejects_duplicate_keys_in_every_formal_input(self):
        artifacts = {
            "parent manifest": (
                Path("manifests/locomo/parent.json"),
                "source_position",
                0,
                99,
            ),
            "shard manifest": (
                Path("manifests/locomo/shard_000.json"),
                "source_position",
                0,
                99,
            ),
            "raw report": (
                Path("runs/locomo/shard_000/results/native_batch_summary.json"),
                "status",
                "available",
                "blocked",
            ),
            "bound report": (
                Path("runs/locomo/shard_000/shard_report.json"),
                "status",
                "available",
                "blocked",
            ),
        }
        for artifact, (relative_path, key, correct, wrong) in artifacts.items():
            for duplicate_case, duplicate_value in (
                ("same value", correct),
                ("wrong then correct", wrong),
            ):
                with self.subTest(artifact=artifact, duplicate_case=duplicate_case):
                    with TemporaryDirectory() as tmp:
                        root = Path(tmp)
                        out_dir, arguments, environment = self._complete_locomo_scale_run(root)
                        formal_path = out_dir / relative_path
                        self._insert_duplicate_scalar(
                            formal_path,
                            key=key,
                            duplicate_value=duplicate_value,
                        )
                        ambiguous = formal_path.read_bytes()
                        model_marker = root / "model-invoked-on-duplicate"
                        python_shim = self._model_side_effect_python_shim(
                            root / "model-side-effect-python"
                        )
                        resume_environment = {
                            **environment,
                            "TXNMEM_PYTHON": str(python_shim),
                            "TXNMEM_TEST_MODEL_MARKER": str(model_marker),
                        }

                        rejected = self._run_scale_script(
                            ["--resume", *arguments],
                            environment=resume_environment,
                        )

                        self.assertFalse(model_marker.exists(), rejected.stderr)
                        self.assertNotEqual(rejected.returncode, 0, rejected.stdout)
                        self.assertEqual(formal_path.read_bytes(), ambiguous)

    def test_scale_script_resume_equality_is_type_strict_for_schema_and_counts(self):
        manifest_cases = (
            ("parent manifest version", Path("parent.json"), "manifest_version"),
            ("shard count", Path("shard_000.json"), "shard_count"),
        )
        for case, relative_path, field in manifest_cases:
            with self.subTest(case=case):
                with TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    source = self._locomo_source(root)
                    out_dir = root / "out"
                    environment = {"TXNMEM_LOCOMO_SOURCE": str(source)}
                    arguments = [
                        "--generate-only",
                        "--benchmarks",
                        "locomo",
                        "--locomo-tasks",
                        "5",
                        "--out-dir",
                        str(out_dir),
                        "--shard-count",
                        "1",
                    ]
                    generated = self._run_scale_script(arguments, environment=environment)
                    self.assertEqual(generated.returncode, 0, generated.stderr)
                    path = out_dir / "manifests" / "locomo" / relative_path
                    original = path.read_text(encoding="utf-8")
                    changed = original.replace(f'"{field}": 1', f'"{field}": true', 1)
                    self.assertNotEqual(changed, original)
                    path.write_text(changed, encoding="utf-8")

                    rejected = self._run_scale_script(
                        [*arguments, "--resume"], environment=environment
                    )

                    self.assertNotEqual(rejected.returncode, 0, rejected.stdout)
                    self.assertEqual(path.read_text(encoding="utf-8"), changed)

        merge_cases = ("schema_version", "shard_count", "repetitions")
        for field in merge_cases:
            with self.subTest(case=f"merged {field}"):
                with TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    out_dir, arguments, environment = self._complete_locomo_scale_run(root)
                    merge_path = out_dir / "merged" / "locomo.json"
                    original = merge_path.read_text(encoding="utf-8")
                    changed = original.replace(f'"{field}": 1', f'"{field}": true', 1)
                    self.assertNotEqual(changed, original)
                    merge_path.write_text(changed, encoding="utf-8")

                    rejected = self._run_scale_script(
                        ["--merge-only", "--resume", *arguments],
                        environment=environment,
                    )

                    self.assertNotEqual(rejected.returncode, 0, rejected.stdout)
                    self.assertEqual(merge_path.read_text(encoding="utf-8"), changed)

    def test_scale_script_rejects_dangling_parent_and_merge_symlink_escapes(self):
        with self.subTest(artifact="parent manifest"):
            with TemporaryDirectory() as tmp:
                root = Path(tmp)
                source = self._locomo_source(root)
                out_dir = root / "out"
                parent_path = out_dir / "manifests" / "locomo" / "parent.json"
                parent_path.parent.mkdir(parents=True)
                escaped = root / "escaped-parent.json"
                parent_path.symlink_to(escaped)

                rejected = self._run_scale_script(
                    [
                        "--generate-only",
                        "--benchmarks",
                        "locomo",
                        "--locomo-tasks",
                        "5",
                        "--out-dir",
                        str(out_dir),
                    ],
                    environment={"TXNMEM_LOCOMO_SOURCE": str(source)},
                )

                self.assertNotEqual(rejected.returncode, 0, rejected.stdout)
                self.assertFalse(escaped.exists())
                self.assertTrue(parent_path.is_symlink())

        with self.subTest(artifact="merged report"):
            with TemporaryDirectory() as tmp:
                root = Path(tmp)
                out_dir = root / "out"
                self._write_merge_fixture(out_dir)
                merge_path = out_dir / "merged" / "tau_bench.json"
                merge_path.parent.mkdir(parents=True)
                escaped = root / "escaped-merge.json"
                merge_path.symlink_to(escaped)

                rejected = self._run_scale_script(
                    [
                        "--merge-only",
                        "--resume",
                        "--benchmarks",
                        "tau-bench",
                        "--out-dir",
                        str(out_dir),
                        "--shard-count",
                        "2",
                    ]
                )

                self.assertNotEqual(rejected.returncode, 0, rejected.stdout)
                self.assertFalse(escaped.exists())
                self.assertTrue(merge_path.is_symlink())

    def test_scale_script_resume_rejects_bad_merge_before_model_execution(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = self._locomo_source(root)
            out_dir = root / "out"
            merge_path = out_dir / "merged" / "locomo.json"
            merge_path.parent.mkdir(parents=True)
            malformed = b"malformed formal merge\n"
            merge_path.write_bytes(malformed)
            model_marker = root / "model-invoked"
            python_shim = self._model_side_effect_python_shim(
                root / "model-side-effect-python"
            )
            environment = {
                "TXNMEM_PYTHON": str(python_shim),
                "TXNMEM_LOCOMO_SOURCE": str(source),
                "TXNMEM_TEST_MODEL_MARKER": str(model_marker),
            }

            rejected = self._run_scale_script(
                [
                    "--resume",
                    "--endpoint",
                    "no-model-call://must-refuse-first",
                    "--model",
                    "must-not-run",
                    "--benchmarks",
                    "locomo",
                    "--locomo-tasks",
                    "5",
                    "--out-dir",
                    str(out_dir),
                    "--shard-count",
                    "1",
                ],
                environment=environment,
            )

            self.assertFalse(model_marker.exists(), rejected.stderr)
            self.assertNotEqual(rejected.returncode, 0)
            self.assertEqual(merge_path.read_bytes(), malformed)

    def test_scale_script_resume_rejects_every_partial_run_before_model_execution(self):
        for partial_case in ("empty", "trace only", "raw only", "bound only"):
            with self.subTest(partial_case=partial_case):
                with TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    source = self._locomo_source(root)
                    out_dir = root / "out"
                    arguments = [
                        "--benchmarks",
                        "locomo",
                        "--locomo-tasks",
                        "5",
                        "--out-dir",
                        str(out_dir),
                        "--shard-count",
                        "1",
                    ]
                    source_environment = {"TXNMEM_LOCOMO_SOURCE": str(source)}
                    generated = self._run_scale_script(
                        ["--generate-only", *arguments],
                        environment=source_environment,
                    )
                    self.assertEqual(generated.returncode, 0, generated.stderr)
                    shard = json.loads(
                        (out_dir / "manifests" / "locomo" / "shard_000.json").read_text(
                            encoding="utf-8"
                        )
                    )
                    run_dir = out_dir / "runs" / "locomo" / "shard_000"
                    run_dir.mkdir(parents=True)
                    raw_rows = [
                        {
                            "task_id": task["task_id"],
                            "status": "completed",
                            "official": {"status": "available", "success": True},
                        }
                        for task in shard["tasks"]
                    ]
                    if partial_case == "trace only":
                        trace_path = run_dir / "data" / "native_model_traces.jsonl"
                        trace_path.parent.mkdir()
                        trace_path.write_text('{"partial":true}\n', encoding="utf-8")
                    elif partial_case == "raw only":
                        raw_path = run_dir / "results" / "native_batch_summary.json"
                        raw_path.parent.mkdir()
                        raw_path.write_text(
                            json.dumps(
                                {
                                    "manifest_sha256": shard["manifest_hash"],
                                    "condition_fingerprint": "e" * 64,
                                    "repetitions": 1,
                                    "task_summaries": raw_rows,
                                }
                            ),
                            encoding="utf-8",
                        )
                    elif partial_case == "bound only":
                        bound_rows = [
                            {
                                **row,
                                "source_position": task["source_position"],
                                "repetition": 1,
                            }
                            for row, task in zip(raw_rows, shard["tasks"])
                        ]
                        (run_dir / "shard_report.json").write_text(
                            json.dumps(
                                {
                                    "parent_manifest_hash": shard["parent_manifest_hash"],
                                    "shard_index": shard["shard_index"],
                                    "shard_count": shard["shard_count"],
                                    "benchmark": shard["benchmark"],
                                    "split": shard["split"],
                                    "source_identity": shard["source_identity"],
                                    "condition_fingerprint": shard["condition_fingerprint"],
                                    "execution_condition_fingerprint": "e" * 64,
                                    "execution_manifest_hash": shard["manifest_hash"],
                                    "repetitions": 1,
                                    "task_summaries": bound_rows,
                                }
                            ),
                            encoding="utf-8",
                        )
                    model_marker = root / "model-invoked"
                    python_shim = self._model_side_effect_python_shim(
                        root / "model-side-effect-python"
                    )
                    environment = {
                        **source_environment,
                        "TXNMEM_PYTHON": str(python_shim),
                        "TXNMEM_TEST_MODEL_MARKER": str(model_marker),
                    }

                    rejected = self._run_scale_script(
                        [
                            "--resume",
                            "--endpoint",
                            "no-model-call://must-refuse-partial",
                            "--model",
                            "must-not-run",
                            *arguments,
                        ],
                        environment=environment,
                    )

                    self.assertFalse(model_marker.exists(), rejected.stderr)
                    self.assertNotEqual(rejected.returncode, 0, rejected.stdout)

    def test_scale_script_preflights_all_shards_before_any_run_or_model_side_effect(self):
        for later_run_state in ("partial", "stale"):
            with self.subTest(later_run_state=later_run_state), TemporaryDirectory() as tmp:
                root = Path(tmp)
                source = self._locomo_source(root)
                out_dir = root / "out"
                base_arguments = [
                    "--benchmarks",
                    "locomo",
                    "--locomo-tasks",
                    "5",
                    "--out-dir",
                    str(out_dir),
                    "--shard-count",
                    "2",
                ]
                source_environment = {"TXNMEM_LOCOMO_SOURCE": str(source)}
                generated = self._run_scale_script(
                    ["--generate-only", *base_arguments],
                    environment=source_environment,
                )
                self.assertEqual(generated.returncode, 0, generated.stderr)

                shard = json.loads(
                    (
                        out_dir
                        / "manifests"
                        / "locomo"
                        / "shard_001.json"
                    ).read_text(encoding="utf-8")
                )
                later_run = out_dir / "runs" / "locomo" / "shard_001"
                later_run.mkdir(parents=True)
                if later_run_state == "stale":
                    raw_rows = [
                        {
                            "task_id": task["task_id"],
                            "status": "completed",
                            "official": {"status": "available", "success": True},
                        }
                        for task in shard["tasks"]
                    ]
                    raw_path = later_run / "results" / "native_batch_summary.json"
                    raw_path.parent.mkdir()
                    raw_path.write_text(
                        json.dumps(
                            {
                                "manifest_sha256": shard["manifest_hash"],
                                "condition_fingerprint": "e" * 64,
                                "repetitions": 1,
                                "task_summaries": raw_rows,
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        encoding="utf-8",
                    )
                    bound_rows = [
                        {
                            **row,
                            "source_position": task["source_position"],
                            "repetition": 1,
                        }
                        for row, task in zip(raw_rows, shard["tasks"])
                    ]
                    bound_rows[0]["status"] = "failed"
                    (later_run / "shard_report.json").write_text(
                        json.dumps(
                            {
                                "parent_manifest_hash": shard["parent_manifest_hash"],
                                "shard_index": shard["shard_index"],
                                "shard_count": shard["shard_count"],
                                "benchmark": shard["benchmark"],
                                "split": shard["split"],
                                "source_identity": shard["source_identity"],
                                "condition_fingerprint": shard["condition_fingerprint"],
                                "execution_condition_fingerprint": "e" * 64,
                                "execution_manifest_hash": shard["manifest_hash"],
                                "repetitions": 1,
                                "task_summaries": bound_rows,
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        encoding="utf-8",
                    )
                later_snapshot = {
                    str(path.relative_to(later_run)): path.read_bytes()
                    for path in later_run.rglob("*")
                    if path.is_file()
                }
                model_marker = root / "model-invoked-before-global-preflight"
                python_shim = self._model_side_effect_python_shim(
                    root / "model-side-effect-python"
                )
                environment = {
                    **source_environment,
                    "TXNMEM_PYTHON": str(python_shim),
                    "TXNMEM_TEST_MODEL_MARKER": str(model_marker),
                }

                rejected = self._run_scale_script(
                    [
                        "--resume",
                        "--endpoint",
                        "no-model-call://global-preflight",
                        "--model",
                        "must-not-run",
                        *base_arguments,
                    ],
                    environment=environment,
                )

                self.assertNotEqual(rejected.returncode, 0, rejected.stdout)
                self.assertFalse(model_marker.exists(), rejected.stderr)
                self.assertFalse(
                    (out_dir / "runs" / "locomo" / "shard_000").exists(),
                    "global preflight must not create an earlier missing run",
                )
                self.assertEqual(
                    {
                        str(path.relative_to(later_run)): path.read_bytes()
                        for path in later_run.rglob("*")
                        if path.is_file()
                    },
                    later_snapshot,
                )

    def test_scale_script_fresh_merge_only_strictly_rebinds_every_raw_report(self):
        for raw_case in ("duplicate", "inconsistent"):
            with self.subTest(raw_case=raw_case), TemporaryDirectory() as tmp:
                out_dir = Path(tmp) / "out"
                self._write_merge_fixture(out_dir)
                raw_path = (
                    out_dir
                    / "runs"
                    / "tau_bench"
                    / "shard_001"
                    / "results"
                    / "native_batch_summary.json"
                )
                if raw_case == "duplicate":
                    self._insert_duplicate_scalar(
                        raw_path,
                        key="status",
                        duplicate_value="completed",
                    )
                else:
                    raw = json.loads(raw_path.read_text(encoding="utf-8"))
                    raw["task_summaries"][0]["status"] = "failed"
                    raw_path.write_text(
                        json.dumps(raw, ensure_ascii=False, indent=2, sort_keys=True)
                        + "\n",
                        encoding="utf-8",
                    )
                original_raw = raw_path.read_bytes()

                rejected = self._run_scale_script(
                    [
                        "--merge-only",
                        "--benchmarks",
                        "tau-bench",
                        "--out-dir",
                        str(out_dir),
                        "--shard-count",
                        "2",
                    ]
                )

                self.assertNotEqual(rejected.returncode, 0, rejected.stdout)
                self.assertEqual(raw_path.read_bytes(), original_raw)
                self.assertFalse((out_dir / "merged" / "tau_bench.json").exists())

    def test_scale_script_rejects_nested_exponent_overflow_in_strict_json(self):
        with TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "out"
            self._write_merge_fixture(out_dir)
            direct_path = out_dir / "nested_overflow.json"
            direct_path.write_text(
                '{"outer":{"inner":[{"value":1e999}]}}\n',
                encoding="utf-8",
            )
            with self.subTest(boundary="FormalStore.load_json"):
                with self.assertRaises(FormalIOError):
                    FormalStore(out_dir).load_json("nested_overflow.json")
            bound_path = (
                out_dir
                / "runs"
                / "tau_bench"
                / "shard_001"
                / "shard_report.json"
            )
            bound = json.loads(bound_path.read_text(encoding="utf-8"))
            bound["task_summaries"][0]["diagnostics"] = {
                "nested": {"overflow": "__EXPONENT_OVERFLOW__"}
            }
            overflow_text = (
                json.dumps(bound, ensure_ascii=False, indent=2, sort_keys=True)
                .replace('"__EXPONENT_OVERFLOW__"', "1e999")
                + "\n"
            )
            self.assertIn("1e999", overflow_text)
            bound_path.write_text(overflow_text, encoding="utf-8")
            original_bound = bound_path.read_bytes()

            rejected = self._run_scale_script(
                [
                    "--merge-only",
                    "--benchmarks",
                    "tau-bench",
                    "--out-dir",
                    str(out_dir),
                    "--shard-count",
                    "2",
                ]
            )

            self.assertNotEqual(rejected.returncode, 0, rejected.stdout)
            self.assertEqual(bound_path.read_bytes(), original_bound)
            self.assertFalse((out_dir / "merged" / "tau_bench.json").exists())

    def test_scale_script_actual_runner_outputs_are_no_follow_exclusive(self):
        protected_outputs = (
            "data/native_model_traces.jsonl",
            "results/native_model_summary.json",
            "results/native_batch_summary.json",
            "data/memory_0001.sqlite",
        )
        for relative_output in protected_outputs:
            with self.subTest(relative_output=relative_output), TemporaryDirectory() as tmp:
                root = Path(tmp)
                source = self._locomo_source(root)
                out_dir = root / "out"
                escaped_target = root / "escaped-output"
                python_shim = self._actual_offline_batch_python_shim(
                    root / "actual-offline-python"
                )
                environment = {
                    "TXNMEM_PYTHON": str(python_shim),
                    "TXNMEM_LOCOMO_SOURCE": str(source),
                    "TXNMEM_TEST_PLANTED_OUTPUT": relative_output,
                    "TXNMEM_TEST_ESCAPED_TARGET": str(escaped_target),
                }

                rejected = self._run_scale_script(
                    [
                        "--endpoint",
                        "offline-fixture://no-network",
                        "--model",
                        "offline-fixture",
                        "--benchmarks",
                        "locomo",
                        "--locomo-tasks",
                        "1",
                        "--out-dir",
                        str(out_dir),
                        "--shard-count",
                        "1",
                    ],
                    environment=environment,
                )

                planted = out_dir / "runs" / "locomo" / "shard_000" / relative_output
                self.assertNotEqual(rejected.returncode, 0, rejected.stdout)
                self.assertTrue(planted.is_symlink())
                self.assertFalse(escaped_target.exists())
                self.assertFalse((out_dir / "merged" / "locomo.json").exists())

    def test_scale_script_normal_resume_verifies_merge_without_model_calls(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            tau_root = self._formal_tau_source(root / "tau-source")
            out_dir = root / "out"
            python_shim = self._no_model_python_shim(root / "no-model-python")
            environment = {
                "TXNMEM_PYTHON": str(python_shim),
                "TXNMEM_TAU_ROOT": str(tau_root),
            }
            base_arguments = [
                "--benchmarks",
                "tau-bench",
                "--out-dir",
                str(out_dir),
                "--shard-count",
                "2",
            ]
            generated = self._run_scale_script(
                ["--generate-only", *base_arguments], environment=environment
            )
            self.assertEqual(generated.returncode, 0, generated.stderr)
            parent = json.loads(
                (out_dir / "manifests" / "tau_bench" / "parent.json").read_text(
                    encoding="utf-8"
                )
            )
            self._write_merge_fixture(out_dir, parent=parent)
            merged = self._run_scale_script(
                ["--merge-only", *base_arguments], environment=environment
            )
            self.assertEqual(merged.returncode, 0, merged.stderr)
            merge_path = out_dir / "merged" / "tau_bench.json"
            payload = json.loads(merge_path.read_text(encoding="utf-8"))
            normal_arguments = [
                "--resume",
                "--endpoint",
                "no-model-call://invalid",
                "--model",
                "must-not-run",
                *base_arguments,
            ]

            canonical_equal = (
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
            ).encode("utf-8")
            with self.subTest(case="canonically equal"):
                merge_path.write_bytes(canonical_equal)
                equal_resume = self._run_scale_script(
                    normal_arguments, environment=environment
                )
                self.assertEqual(equal_resume.returncode, 0, equal_resume.stderr)
                self.assertEqual(merge_path.read_bytes(), canonical_equal)

            with self.subTest(case="different sentinel"):
                sentinel = b'{"sentinel":"normal path must not overwrite"}\n'
                merge_path.write_bytes(sentinel)
                rejected = self._run_scale_script(
                    normal_arguments, environment=environment
                )
                self.assertNotEqual(rejected.returncode, 0)
                self.assertEqual(merge_path.read_bytes(), sentinel)
                self.assertNotIn("model execution forbidden", rejected.stderr)

    def test_scale_script_executes_with_empty_optional_evaluator_args_on_bash_3_2(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            tau_root = self._formal_tau_source(root / "tau-source")
            out_dir = root / "out"
            model_log = root / "model-invocation.json"
            python_shim = self._native_batch_python_shim(root / "native-batch-python")
            environment = {
                "TXNMEM_PYTHON": str(python_shim),
                "TXNMEM_TAU_ROOT": str(tau_root),
                "TXNMEM_LOCOMO_EVALUATOR_COMMAND": "",
                "TXNMEM_TEST_MODEL_LOG": str(model_log),
            }

            completed = self._run_scale_script(
                [
                    "--endpoint",
                    "no-model-call://local-shim",
                    "--model",
                    "local-shim",
                    "--benchmarks",
                    "tau-bench",
                    "--out-dir",
                    str(out_dir),
                    "--shard-count",
                    "1",
                ],
                environment=environment,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            invocation = json.loads(model_log.read_text(encoding="utf-8"))
            self.assertNotIn("--locomo-evaluator-command", invocation)
            self.assertTrue((out_dir / "merged" / "tau_bench.json").is_file())

    def test_scale_script_executes_with_populated_optional_evaluator_args_on_bash_3_2(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            tau_root = self._formal_tau_source(root / "tau-source")
            out_dir = root / "out"
            model_log = root / "model-invocation.json"
            python_shim = self._native_batch_python_shim(root / "native-batch-python")
            evaluator_command = '["python3","evaluate.py"]'
            environment = {
                "TXNMEM_PYTHON": str(python_shim),
                "TXNMEM_TAU_ROOT": str(tau_root),
                "TXNMEM_LOCOMO_EVALUATOR_COMMAND": evaluator_command,
                "TXNMEM_TEST_MODEL_LOG": str(model_log),
            }

            completed = self._run_scale_script(
                [
                    "--endpoint",
                    "no-model-call://local-shim",
                    "--model",
                    "local-shim",
                    "--benchmarks",
                    "tau-bench",
                    "--out-dir",
                    str(out_dir),
                    "--shard-count",
                    "1",
                ],
                environment=environment,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            invocation = json.loads(model_log.read_text(encoding="utf-8"))
            option_index = invocation.index("--locomo-evaluator-command")
            self.assertEqual(invocation[option_index + 1], evaluator_command)
            self.assertTrue((out_dir / "merged" / "tau_bench.json").is_file())

    def test_scale_script_refuses_protected_merge_before_model_execution(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            tau_root = self._formal_tau_source(root / "tau-source")
            out_dir = root / "out"
            merge_path = out_dir / "merged" / "tau_bench.json"
            merge_path.parent.mkdir(parents=True)
            sentinel = b'{"sentinel":"protected before execution"}\n'
            merge_path.write_bytes(sentinel)
            model_marker = root / "model-invoked"
            python_shim = self._model_side_effect_python_shim(
                root / "model-side-effect-python"
            )
            environment = {
                "TXNMEM_PYTHON": str(python_shim),
                "TXNMEM_TAU_ROOT": str(tau_root),
                "TXNMEM_LOCOMO_EVALUATOR_COMMAND": "",
                "TXNMEM_TEST_MODEL_MARKER": str(model_marker),
            }

            rejected = self._run_scale_script(
                [
                    "--endpoint",
                    "no-model-call://must-refuse-first",
                    "--model",
                    "must-not-run",
                    "--benchmarks",
                    "tau-bench",
                    "--out-dir",
                    str(out_dir),
                    "--shard-count",
                    "1",
                ],
                environment=environment,
            )

            self.assertFalse(model_marker.exists(), rejected.stderr)
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("refusing to overwrite existing merge", rejected.stderr)
            self.assertEqual(merge_path.read_bytes(), sentinel)

    def test_scale_script_generate_and_merge_only_are_no_model_exact_count_paths(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            tau_root = root / "tau-source"
            task_source = tau_root / "tau_bench" / "envs" / "retail" / "tasks_test.py"
            task_source.parent.mkdir(parents=True)
            for package in (
                tau_root / "tau_bench",
                tau_root / "tau_bench" / "envs",
                tau_root / "tau_bench" / "envs" / "retail",
            ):
                (package / "__init__.py").write_text("", encoding="utf-8")
            task_source.write_text(
                "TASKS_TEST = ["
                + ",".join(
                    repr({"instruction": f"retail task {index}"})
                    for index in range(115)
                )
                + "]\n",
                encoding="utf-8",
            )
            dist_info = tau_root / "tau_bench-0.1.0.dist-info"
            dist_info.mkdir()
            (dist_info / "METADATA").write_text(
                "Metadata-Version: 2.1\nName: tau-bench\nVersion: 0.1.0\n",
                encoding="utf-8",
            )

            appworld_root = root / "appworld"
            data_root = appworld_root / "data"
            (data_root / "datasets").mkdir(parents=True)
            (data_root / "tasks").mkdir()
            (data_root / "version.txt").write_text("0.2.0\n", encoding="utf-8")
            appworld_ids = [f"official-{index:03d}" for index in range(168)]
            (data_root / "datasets" / "test_normal.txt").write_text(
                "\n".join(appworld_ids) + "\n", encoding="utf-8"
            )
            for task_id in appworld_ids:
                task_dir = data_root / "tasks" / task_id
                task_dir.mkdir()
                (task_dir / "specs.json").write_text(
                    json.dumps({"instruction": task_id}), encoding="utf-8"
                )

            out_dir = root / "out"
            environment = dict(os.environ)
            environment.update(
                {
                    "TXNMEM_PYTHON": sys.executable,
                    "TXNMEM_TAU_ROOT": str(tau_root),
                }
            )
            generated = subprocess.run(
                [
                    str(ROOT / "scripts" / "run_native_scale.sh"),
                    "--generate-only",
                    "--benchmarks",
                    "tau-bench,appworld",
                    "--appworld-root",
                    str(appworld_root),
                    "--out-dir",
                    str(out_dir),
                    "--shard-count",
                    "3",
                ],
                cwd=ROOT,
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(generated.returncode, 0, generated.stderr)

            resumed = subprocess.run(
                [
                    str(ROOT / "scripts" / "run_native_scale.sh"),
                    "--generate-only",
                    "--resume",
                    "--benchmarks",
                    "tau-bench,appworld",
                    "--appworld-root",
                    str(appworld_root),
                    "--out-dir",
                    str(out_dir),
                    "--shard-count",
                    "3",
                ],
                cwd=ROOT,
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(resumed.returncode, 0, resumed.stderr)

            expected_counts = {"tau_bench": 115, "appworld": 168}
            for job, expected_count in expected_counts.items():
                parent_path = out_dir / "manifests" / job / "parent.json"
                parent = json.loads(parent_path.read_text(encoding="utf-8"))
                self.assertEqual(parent["task_count"], expected_count)
                shard_paths = sorted((parent_path.parent).glob("shard_*.json"))
                self.assertEqual(len(shard_paths), 3)
                self.assertEqual(
                    sum(
                        len(json.loads(path.read_text(encoding="utf-8"))["tasks"])
                        for path in shard_paths
                    ),
                    expected_count,
                )
                for shard_path in shard_paths:
                    shard = json.loads(shard_path.read_text(encoding="utf-8"))
                    report_dir = out_dir / "runs" / job / shard_path.stem
                    report_dir.mkdir(parents=True)
                    rows = [
                        {
                            "task_id": task["task_id"],
                            "source_position": task["source_position"],
                            "repetition": 1,
                            "status": "failed",
                            "official": {"status": "blocked"},
                        }
                        for task in shard["tasks"]
                    ]
                    raw_path = report_dir / "results" / "native_batch_summary.json"
                    raw_path.parent.mkdir()
                    raw_path.write_text(
                        json.dumps(
                            {
                                "manifest_sha256": shard["manifest_hash"],
                                "condition_fingerprint": "e" * 64,
                                "repetitions": 1,
                                "task_summaries": [
                                    {
                                        "task_id": row["task_id"],
                                        "status": row["status"],
                                        "official": row["official"],
                                    }
                                    for row in rows
                                ],
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        encoding="utf-8",
                    )
                    bound_report = {
                        "parent_manifest_hash": parent["manifest_hash"],
                        "execution_manifest_hash": shard["manifest_hash"],
                        "shard_index": shard["shard_index"],
                        "shard_count": shard["shard_count"],
                        "benchmark": parent["benchmark"],
                        "split": parent["split"],
                        "source_identity": parent["source_identity"],
                        "condition_fingerprint": parent["condition_fingerprint"],
                        "execution_condition_fingerprint": "e" * 64,
                        "repetitions": 1,
                        "task_summaries": rows,
                    }
                    if "domain" in parent:
                        bound_report["domain"] = parent["domain"]
                    (report_dir / "shard_report.json").write_text(
                        json.dumps(bound_report), encoding="utf-8"
                    )

            merged = subprocess.run(
                [
                    str(ROOT / "scripts" / "run_native_scale.sh"),
                    "--merge-only",
                    "--benchmarks",
                    "tau-bench,appworld",
                    "--out-dir",
                    str(out_dir),
                    "--shard-count",
                    "3",
                ],
                cwd=ROOT,
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(merged.returncode, 0, merged.stderr)
            for job, expected_count in expected_counts.items():
                report = json.loads(
                    (out_dir / "merged" / f"{job}.json").read_text(encoding="utf-8")
                )
                self.assertEqual(report["task_aggregate"]["denominator"], expected_count)
                self.assertEqual(report["task_aggregate"]["failures"], expected_count)

            task_source.write_text(
                "TASKS_TEST = ["
                + ",".join(
                    repr({"instruction": f"retail task {index}"})
                    for index in range(116)
                )
                + "]\n",
                encoding="utf-8",
            )
            wrong_formal_count = subprocess.run(
                [
                    str(ROOT / "scripts" / "run_native_scale.sh"),
                    "--generate-only",
                    "--benchmarks",
                    "tau-bench",
                    "--out-dir",
                    str(root / "wrong-count"),
                ],
                cwd=ROOT,
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(wrong_formal_count.returncode, 0)
            self.assertIn("frozen formal source count", wrong_formal_count.stderr)

    def test_formal_scale_script_advertises_frozen_defaults_and_resume_modes(self):
        completed = subprocess.run(
            [str(ROOT / "scripts" / "run_native_scale.sh"), "--help"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        for expected in (
            "retail/test=115",
            "test_normal=168",
            "--shard-count",
            "--generate-only",
            "--merge-only",
            "--resume",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, completed.stdout)


if __name__ == "__main__":
    unittest.main()
