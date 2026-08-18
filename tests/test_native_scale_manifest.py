"""Reproducible task-level manifests for the public benchmark batch runner."""

from __future__ import annotations

import json
import hashlib
import os
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
                            "status": "failed",
                            "official": {"status": "blocked"},
                        }
                        for task in shard["tasks"]
                    ]
                    (report_dir / "shard_report.json").write_text(
                        json.dumps(
                            {
                                "parent_manifest_hash": parent["manifest_hash"],
                                "execution_manifest_hash": shard["manifest_hash"],
                                "shard_index": shard["shard_index"],
                                "shard_count": shard["shard_count"],
                                "benchmark": parent["benchmark"],
                                "domain": parent.get("domain"),
                                "split": parent["split"],
                                "source_identity": parent["source_identity"],
                                "condition_fingerprint": parent["condition_fingerprint"],
                                "execution_condition_fingerprint": "e" * 64,
                                "repetitions": 1,
                                "task_summaries": rows,
                            }
                        ),
                        encoding="utf-8",
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
