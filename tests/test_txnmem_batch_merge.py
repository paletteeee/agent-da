"""Fail-closed merging for deterministic native benchmark shards."""

from __future__ import annotations

import copy
import hashlib
import importlib
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import txnmem_benchmark_manifests as benchmark_manifests


class NativeShardMergeTests(unittest.TestCase):
    def _merge_function(self):
        try:
            module = importlib.import_module("txnmem_batch_merge")
        except ModuleNotFoundError:
            self.fail("txnmem_batch_merge.merge_native_shards must be implemented")
        merge = getattr(module, "merge_native_shards", None)
        self.assertTrue(callable(merge), "merge_native_shards must be implemented")
        return merge

    def _parent_manifest(self) -> dict:
        manifest = {
            "manifest_version": 1,
            "dataset_name": "appworld-test_normal",
            "benchmark": "appworld",
            "split": "test_normal",
            "task_count": 4,
            "source_identity": {
                "split_file": {"path": "datasets/test_normal.txt", "sha256": "a" * 64},
                "version_file": {"path": "version.txt", "sha256": "b" * 64},
                "fingerprint": "c" * 64,
            },
            "condition_fingerprint": "d" * 64,
            "tasks": [
                {
                    "task_id": f"appworld-task-{index}",
                    "raw_task_id": f"task-{index}",
                    "source_position": index,
                    "prompt": f"task {index}",
                }
                for index in range(4)
            ],
        }
        encoded = json.dumps(
            manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        manifest["manifest_hash"] = hashlib.sha256(encoded).hexdigest()
        return manifest

    def _reports(self, parent: dict, repetitions: int = 1) -> list[dict]:
        shard_manifest = getattr(benchmark_manifests, "shard_manifest", None)
        self.assertTrue(callable(shard_manifest), "shard_manifest must be implemented")
        reports = []
        statuses = ["completed", "failed", "evaluator_error", "blocked"]
        official = [
            {"status": "available", "success": True},
            {"status": "available", "success": False},
            {"status": "error", "error": "fixture"},
            {"status": "blocked", "error": "fixture"},
        ]
        for shard in shard_manifest(parent, 2):
            rows = []
            for task in shard["tasks"]:
                position = task["source_position"]
                for repetition in range(1, repetitions + 1):
                    row = {
                        "task_id": task["task_id"],
                        "source_position": position,
                        "status": statuses[position],
                        "official": official[position],
                    }
                    if repetitions > 1:
                        row["repetition"] = repetition
                    rows.append(row)
            reports.append(
                {
                    "parent_manifest_hash": parent["manifest_hash"],
                    "execution_manifest_hash": shard["manifest_hash"],
                    "shard_index": shard["shard_index"],
                    "shard_count": shard["shard_count"],
                    "benchmark": parent["benchmark"],
                    "split": parent["split"],
                    "source_identity": parent["source_identity"],
                    "condition_fingerprint": parent["condition_fingerprint"],
                    "execution_condition_fingerprint": "e" * 64,
                    "repetitions": repetitions,
                    "task_summaries": rows,
                }
            )
        return reports

    def test_merge_retains_all_statuses_in_source_order_and_denominator(self):
        merge_native_shards = self._merge_function()
        parent = self._parent_manifest()

        merged = merge_native_shards(parent, list(reversed(self._reports(parent))))

        self.assertEqual(
            [row["task_id"] for row in merged["task_summaries"]],
            [task["task_id"] for task in parent["tasks"]],
        )
        self.assertEqual(merged["task_count"], 4)
        self.assertEqual(merged["task_aggregate"]["denominator"], 4)
        self.assertEqual(merged["task_aggregate"]["successes"], 1)
        self.assertEqual(merged["task_aggregate"]["failures"], 3)
        self.assertEqual(merged["official"]["trials"], 4)
        self.assertEqual(merged["official"]["successes"], 1)
        self.assertEqual(merged["official"]["failures"], 3)
        self.assertEqual(
            merged["official"]["evaluator_status_counts"],
            {"available": 2, "blocked": 1, "error": 1},
        )
        self.assertEqual(
            merged["task_aggregate"]["status_counts"],
            {"blocked": 1, "completed": 1, "evaluator_error": 1, "failed": 1},
        )

    def test_merge_preserves_frozen_raw_official_task_ids(self):
        merge_native_shards = self._merge_function()
        parent = self._parent_manifest()

        merged = merge_native_shards(parent, self._reports(parent))

        self.assertEqual(
            [row["raw_task_id"] for row in merged["task_summaries"]],
            [task["raw_task_id"] for task in parent["tasks"]],
        )

    def test_merge_rejects_executed_shard_manifest_hash_mismatch(self):
        merge_native_shards = self._merge_function()
        parent = self._parent_manifest()
        reports = self._reports(parent)
        reports[0]["execution_manifest_hash"] = "0" * 64

        with self.assertRaisesRegex(ValueError, "execution manifest"):
            merge_native_shards(parent, reports)

    def test_merge_does_not_count_evaluator_error_as_success(self):
        merge_native_shards = self._merge_function()
        parent = self._parent_manifest()
        reports = self._reports(parent)
        reports[0]["task_summaries"][0]["official"] = {
            "status": "error",
            "success": True,
            "error": "contradictory evaluator output",
        }

        merged = merge_native_shards(parent, reports)

        self.assertEqual(merged["official"]["successes"], 0)
        self.assertEqual(merged["official"]["failures"], 4)

    def test_merge_requires_every_repetition_execution_to_complete_for_success(self):
        merge_native_shards = self._merge_function()
        parent = self._parent_manifest()

        for execution_status in ("failed", "error", "blocked", "evaluator_error"):
            with self.subTest(execution_status=execution_status):
                reports = self._reports(parent, repetitions=2)
                successful_task_rows = [
                    row
                    for report in reports
                    for row in report["task_summaries"]
                    if row["task_id"] == "appworld-task-0"
                ]
                successful_task_rows[1]["status"] = execution_status

                merged = merge_native_shards(parent, reports)

                self.assertEqual(merged["task_aggregate"]["denominator"], 4)
                self.assertEqual(merged["task_aggregate"]["successes"], 0)
                self.assertEqual(merged["task_aggregate"]["failures"], 4)
                self.assertEqual(merged["official"]["trials"], 4)
                self.assertEqual(merged["official"]["successes"], 0)
                self.assertEqual(merged["official"]["failures"], 4)

    def test_merge_rejects_duplicate_missing_extra_and_condition_mismatch(self):
        merge_native_shards = self._merge_function()
        parent = self._parent_manifest()
        cases = {}

        duplicate = self._reports(parent)
        duplicate[1]["task_summaries"].append(copy.deepcopy(duplicate[0]["task_summaries"][0]))
        cases["duplicate"] = duplicate

        missing = self._reports(parent)
        missing[0]["task_summaries"].pop()
        cases["missing"] = missing

        extra = self._reports(parent)
        extra[0]["task_summaries"].append(
            {"task_id": "appworld-extra", "source_position": 99, "status": "failed"}
        )
        cases["extra"] = extra

        condition = self._reports(parent)
        condition[1]["condition_fingerprint"] = "e" * 64
        cases["condition"] = condition

        execution_condition = self._reports(parent)
        execution_condition[1]["execution_condition_fingerprint"] = "f" * 64
        cases["execution condition"] = execution_condition

        for message, reports in cases.items():
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                merge_native_shards(parent, reports)

    def test_merge_rejects_parent_source_split_assignment_repetition_and_malformed_rows(self):
        merge_native_shards = self._merge_function()
        parent = self._parent_manifest()
        cases = {}

        wrong_parent = self._reports(parent)
        wrong_parent[0]["parent_manifest_hash"] = "0" * 64
        cases["parent"] = wrong_parent

        wrong_source = self._reports(parent)
        wrong_source[0]["source_identity"] = {"fingerprint": "wrong"}
        cases["source"] = wrong_source

        wrong_split = self._reports(parent)
        wrong_split[0]["split"] = "test_other"
        cases["split"] = wrong_split

        wrong_assignment = self._reports(parent)
        wrong_assignment[0]["task_summaries"][0], wrong_assignment[1]["task_summaries"][0] = (
            wrong_assignment[1]["task_summaries"][0],
            wrong_assignment[0]["task_summaries"][0],
        )
        cases["shard assignment"] = wrong_assignment

        conflicting_repetitions = self._reports(parent)
        conflicting_repetitions[1]["repetitions"] = 2
        cases["conflicting repetitions"] = conflicting_repetitions

        malformed = self._reports(parent)
        malformed[0]["task_summaries"][0]["status"] = ["failed"]
        cases["malformed"] = malformed

        for message, reports in cases.items():
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                merge_native_shards(parent, reports)

    def test_merge_accepts_consistent_repetitions_but_keeps_task_denominator(self):
        merge_native_shards = self._merge_function()
        parent = self._parent_manifest()

        merged = merge_native_shards(parent, self._reports(parent, repetitions=2))

        self.assertEqual(merged["repetitions"], 2)
        self.assertEqual(merged["task_count"], 4)
        self.assertEqual(merged["row_count"], 8)
        self.assertEqual(merged["task_aggregate"]["denominator"], 4)
        self.assertEqual(merged["execution_condition_fingerprint"], "e" * 64)
        self.assertEqual(
            [(row["source_position"], row["repetition"]) for row in merged["task_summaries"]],
            [(position, repetition) for position in range(4) for repetition in (1, 2)],
        )

    def test_merge_rejects_parent_content_that_does_not_match_parent_hash(self):
        merge_native_shards = self._merge_function()
        parent = self._parent_manifest()
        reports = self._reports(parent)
        parent["tasks"][0]["prompt"] = "tampered after hashing"

        with self.assertRaisesRegex(ValueError, "parent manifest_hash"):
            merge_native_shards(parent, reports)

    def test_merge_rejects_invalid_shard_count_and_missing_parent_identity(self):
        merge_native_shards = self._merge_function()
        parent = self._parent_manifest()
        rows = [row for report in self._reports(parent) for row in report["task_summaries"]]
        too_many = []
        for shard_index in range(5):
            too_many.append(
                {
                    "parent_manifest_hash": parent["manifest_hash"],
                    "shard_index": shard_index,
                    "shard_count": 5,
                    "benchmark": parent["benchmark"],
                    "split": parent["split"],
                    "source_identity": parent["source_identity"],
                    "condition_fingerprint": parent["condition_fingerprint"],
                    "execution_condition_fingerprint": "e" * 64,
                    "repetitions": 1,
                    "task_summaries": [
                        row for row in rows if row["source_position"] == shard_index
                    ],
                }
            )
        with self.assertRaisesRegex(ValueError, "shard_count"):
            merge_native_shards(parent, too_many)

        missing_identity = copy.deepcopy(parent)
        missing_identity.pop("condition_fingerprint")
        unhashed = dict(missing_identity)
        unhashed.pop("manifest_hash")
        missing_identity["manifest_hash"] = hashlib.sha256(
            json.dumps(
                unhashed,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        reports = self._reports(parent)
        for report in reports:
            report.pop("condition_fingerprint")
            report["parent_manifest_hash"] = missing_identity["manifest_hash"]
        with self.assertRaisesRegex(ValueError, "condition_fingerprint"):
            merge_native_shards(missing_identity, reports)


if __name__ == "__main__":
    unittest.main()
