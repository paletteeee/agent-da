from contextlib import redirect_stdout
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from txnmem_appworld_projection import (
    main as appworld_projection_main,
    regenerate_appworld_projection,
    select_appworld_realism_families,
    select_appworld_realism_families_from_dataset,
)
from txnmem_trace_pipeline import build_trace_instances, load_trace_records


class AppWorldProjectionRegenerationTests(unittest.TestCase):
    def test_regeneration_preserves_method_url_order_and_drops_request_values(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            for task_id, calls in {
                "family_a_1": [
                    {"method": "get", "url": "/spotify/list", "data": {"token": "secret-a"}},
                    {"method": "post", "url": "/spotify/create", "data": {"name": "private"}},
                ],
                "family_a_2": [
                    {"method": "patch", "url": "/todo/update", "data": {"password": "secret-b"}}
                ],
                "family_b_1": [],
            }.items():
                directory = root / "data" / "tasks" / task_id / "ground_truth"
                directory.mkdir(parents=True)
                (directory / "api_calls.json").write_text(json.dumps(calls), encoding="utf-8")
            dataset = root / "data" / "datasets" / "train.txt"
            dataset.parent.mkdir(parents=True)
            dataset.write_text(
                "family_a_1\nfamily_a_2\nfamily_b_1\n", encoding="utf-8"
            )
            output = root / "projection.jsonl"

            inventory = regenerate_appworld_projection(
                root,
                ["family_a_1", "family_a_2", "family_b_1"],
                output,
                official_split="train",
                dataset_path=dataset,
            )
            output_text = output.read_text(encoding="utf-8")
            records = load_trace_records(output)
            instances = build_trace_instances(
                records,
                "appworld",
                source="appworld-official-reference-api-calls-redacted",
                seed=17,
            )

        self.assertEqual(inventory["event_count"], 3)
        self.assertEqual(inventory["task_count"], 3)
        self.assertEqual(inventory["family_count"], 2)
        self.assertEqual(inventory["zero_event_count"], 1)
        self.assertEqual(inventory["zero_event_task_ids"], ["family_b_1"])
        self.assertEqual(inventory["official_split"], "train")
        self.assertTrue(inventory["split_membership_verified"])
        self.assertRegex(inventory["dataset_file_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(inventory["method_counts"], {"get": 1, "patch": 1, "post": 1})
        self.assertFalse(inventory["request_data_values_retained"])
        self.assertNotIn("secret-a", output_text)
        self.assertNotIn("private", output_text)
        self.assertEqual(records[0]["sequence"], 1)
        self.assertEqual(records[0]["url"], "/spotify/list")
        self.assertEqual(records[0]["family_id"], "family_a")
        self.assertEqual(records[0]["official_split"], "train")
        self.assertEqual(
            records[0]["family_derivation_method"],
            "audited_appworld_generator_prefix",
        )
        self.assertEqual(len(instances), 2)

    def test_family_selection_is_seeded_disjoint_and_keeps_whole_families(self):
        task_ids = [
            f"family{family:03d}_{task_number}"
            for family in range(120)
            for task_number in (1, 2)
        ]

        first = select_appworld_realism_families(
            task_ids,
            evaluation_family_count=50,
            calibration_family_count=50,
            seed=17,
            official_split="train",
        )
        second = select_appworld_realism_families(
            task_ids,
            evaluation_family_count=50,
            calibration_family_count=50,
            seed=17,
            official_split="train",
        )

        self.assertEqual(first, second)
        evaluation = set(first["evaluation_family_ids"])
        calibration = set(first["calibration_family_ids"])
        self.assertEqual(len(evaluation), 50)
        self.assertEqual(len(calibration), 50)
        self.assertTrue(evaluation.isdisjoint(calibration))
        self.assertEqual(len(first["evaluation_task_ids"]), 100)
        self.assertEqual(len(first["calibration_task_ids"]), 100)
        self.assertEqual(first["official_split"], "train")
        with self.assertRaisesRegex(ValueError, "calibration"):
            select_appworld_realism_families(
                task_ids,
                evaluation_family_count=120,
                calibration_family_count=None,
                seed=17,
                official_split="train",
            )

    def test_official_family_metadata_takes_precedence(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            directory = root / "data" / "tasks" / "opaque_1" / "ground_truth"
            directory.mkdir(parents=True)
            (directory / "api_calls.json").write_text(
                json.dumps([{"method": "get", "url": "/test"}]),
                encoding="utf-8",
            )
            output = root / "projection.jsonl"

            inventory = regenerate_appworld_projection(
                root,
                ["opaque_1"],
                output,
                official_split="dev",
                task_metadata_by_id={"opaque_1": {"scenario_id": "official-scenario"}},
            )
            record = load_trace_records(output)[0]

        self.assertEqual(record["family_id"], "official-scenario")
        self.assertEqual(record["family_derivation_method"], "official_scenario_id")
        self.assertEqual(inventory["family_derivation_counts"], {"official_scenario_id": 1})

    def test_dataset_selection_binds_split_hash_and_uses_all_remaining_calibration(self):
        task_ids = [
            f"family{family:03d}_{task_number}"
            for family in range(60)
            for task_number in (1, 2, 3)
        ]
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            dataset = root / "train.txt"
            output = root / "selection.json"
            dataset.write_text("\n".join(task_ids) + "\n", encoding="utf-8")

            direct = select_appworld_realism_families_from_dataset(
                dataset,
                evaluation_family_count=50,
                calibration_family_count=None,
                seed=17,
                official_split="train",
            )
            with redirect_stdout(io.StringIO()):
                status = appworld_projection_main(
                    [
                        "--appworld-root",
                        str(root),
                        "--dataset-file",
                        str(dataset),
                        "--selection-output",
                        str(output),
                        "--official-split",
                        "train",
                    ]
                )
            emitted = json.loads(output.read_text())

        self.assertEqual(status, 0)
        self.assertEqual(emitted, direct)
        self.assertEqual(direct["evaluation_family_count"], 50)
        self.assertEqual(direct["calibration_family_count"], 10)
        self.assertEqual(direct["calibration_selection"], "all_remaining_families")
        self.assertEqual(len(direct["evaluation_task_ids"]), 150)
        self.assertEqual(len(direct["calibration_task_ids"]), 30)
        self.assertRegex(direct["dataset_file_sha256"], r"^[0-9a-f]{64}$")

    def test_regeneration_rejects_missing_or_malformed_source(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(FileNotFoundError):
                regenerate_appworld_projection(root, ["missing"], root / "out.jsonl")

    def test_regeneration_rejects_task_outside_declared_official_split(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            directory = root / "data" / "tasks" / "family_a_1" / "ground_truth"
            directory.mkdir(parents=True)
            (directory / "api_calls.json").write_text("[]", encoding="utf-8")
            dataset = root / "data" / "datasets" / "train.txt"
            dataset.parent.mkdir(parents=True)
            dataset.write_text("different_1\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "declared AppWorld split"):
                regenerate_appworld_projection(
                    root,
                    ["family_a_1"],
                    root / "out.jsonl",
                    official_split="train",
                    dataset_path=dataset,
                )


if __name__ == "__main__":
    unittest.main()
