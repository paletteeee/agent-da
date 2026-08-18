import sys
import unittest
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_schema import validate_instance  # noqa: E402
from txnmem_differential import compare_result_to_oracle  # noqa: E402
from txnmem_reference import reference_outcome  # noqa: E402
from txnmem_simulator import run_instance  # noqa: E402
from txnmem_workloads import (  # noqa: E402
    WORKLOADS,
    generate_instance,
    generate_suite,
    sample_semantic_config,
    semantic_fingerprint,
)


PARAMETER_RANGES = {
    "txn_size": [1, 4],
    "provenance_depth": [1, 4],
    "branch_factor": [1, 3],
    "policy_churn": [0, 2],
    "concurrency": [1, 3],
}


class TxnMemWorkloadTests(unittest.TestCase):
    def test_supersession_workload_declares_supersede_policy(self):
        instance = generate_instance("supersession_consistency", 0)
        assert any(p["action"] == "supersede" and p["effect"] == "allow" for p in instance["policies"])

    def test_all_workloads_are_deterministic_and_schema_valid(self):
        self.assertEqual(len(WORKLOADS), 8)
        for workload in WORKLOADS:
            first = generate_instance(workload, seed=13)
            second = generate_instance(workload, seed=13)
            self.assertEqual(first, second)
            validate_instance(first)

    def test_scope_bypass_contains_search_and_direct_id_read(self):
        instance = generate_instance("scope_bypass", seed=10)
        types = [operation["type"] for operation in instance["operations"]]
        self.assertIn("search", types)
        self.assertIn("get_by_id", types)

    def test_branch_repair_has_all_branch_derive_operations(self):
        instance = generate_instance(
            "provenance_branch_repair",
            seed=11,
            config={"branch_factor": 3, "provenance_depth": 2},
        )
        derive_operations = [
            operation for operation in instance["operations"] if operation["type"] == "derive"
        ]
        self.assertEqual(len(derive_operations), 6)
        self.assertEqual(instance["provenance_edges"], [])

    def test_generate_suite_returns_workload_seed_cartesian_product(self):
        suite = generate_suite(["atomic_multi_write", "scope_bypass"], [1, 2])
        self.assertEqual([item["seed"] for item in suite], [1, 2, 1, 2])

    def test_parameter_ranges_are_consumed_deterministically(self):
        """Dropping range sampling would collapse the 8x200 semantic population."""

        first = generate_suite(WORKLOADS, range(200), parameter_ranges=PARAMETER_RANGES)
        second = generate_suite(WORKLOADS, range(200), parameter_ranges=PARAMETER_RANGES)

        self.assertEqual(first, second)
        self.assertEqual(len(first), 8 * 200)
        self.assertEqual(len({row["instance_id"] for row in first}), 8 * 200)
        self.assertGreater(len({row["semantic_fingerprint"] for row in first}), len(WORKLOADS))
        for name, (low, high) in PARAMETER_RANGES.items():
            sampled = {
                row["semantic_parameters"][name]
                for row in first
                if name in row["semantic_parameters"]
            }
            self.assertTrue(sampled <= set(range(low, high + 1)))
            self.assertEqual({low, high}, {low, high} & sampled)
        for workload in WORKLOADS:
            rows = [row for row in first if row["workload"] == workload]
            self.assertEqual({row["seed"] for row in rows}, set(range(200)))
            self.assertTrue(rows[0]["semantic_parameters"])
            self.assertGreater(len({row["semantic_fingerprint"] for row in rows}), 1)

    def test_semantic_sampling_uses_inclusive_ranges(self):
        """Sampling must include both endpoints rather than treating high as exclusive."""

        sampled = sample_semantic_config("atomic_multi_write", 7, {"txn_size": [3, 3]})
        self.assertEqual(sampled, {"txn_size": 3})

    def test_semantic_fingerprint_normalizes_relabelled_target_references(self):
        """Relabeling IDs must include schedule targets without erasing literal actions."""

        instance = generate_instance("atomic_multi_write", seed=7, config={"txn_size": 2})
        instance["failure_schedule"].extend(
            [
                {
                    "trigger": {"before_operation": "op_004"},
                    "type": "invalidate",
                    "target": "m_write_1",
                },
                {
                    "trigger": {"before_operation": "op_004"},
                    "type": "revoke",
                    "target": "write",
                },
            ]
        )
        relabeled = json.loads(json.dumps(instance))
        relabeled["instance_id"] = "different_identifier"
        relabeled["seed"] = 999
        operation_ids = {
            operation["op_id"]: f"relabelled_op_{index}"
            for index, operation in enumerate(relabeled["operations"], start=1)
        }
        for policy in relabeled["policies"]:
            policy["agent_id"] = "another_agent"
        for operation in relabeled["operations"]:
            operation["agent_id"] = "another_agent"
            operation["op_id"] = operation_ids[operation["op_id"]]
            if operation.get("txn_id") == "txn_001":
                operation["txn_id"] = "relabelled_transaction"
            if operation.get("memory_id") == "m_write_1":
                operation["memory_id"] = "relabelled_memory"
        for event in relabeled["failure_schedule"]:
            trigger = event.get("trigger", {})
            for trigger_name, operation_id in trigger.items():
                trigger[trigger_name] = operation_ids[operation_id]
            if event["target"] == "txn_001":
                event["target"] = "relabelled_transaction"
            if event["target"] == "m_write_1":
                event["target"] = "relabelled_memory"

        self.assertEqual(semantic_fingerprint(instance), semantic_fingerprint(relabeled))
        changed_action = json.loads(json.dumps(relabeled))
        changed_action["failure_schedule"][-1]["target"] = "derive"
        self.assertNotEqual(semantic_fingerprint(relabeled), semantic_fingerprint(changed_action))

    def test_semantic_fingerprint_preserves_literal_crash_targets_despite_operation_id_collision(self):
        """The literal crash target ``commit`` is not an operation-id reference."""

        colliding = generate_instance("crash_during_commit", seed=8)
        colliding["operations"][-1]["op_id"] = "commit"
        colliding["failure_schedule"][0]["trigger"] = {"before_operation": "commit"}
        relabeled_operation = json.loads(json.dumps(colliding))
        relabeled_operation["operations"][-1]["op_id"] = "renamed_commit_operation"
        relabeled_operation["failure_schedule"][0]["trigger"] = {
            "before_operation": "renamed_commit_operation"
        }

        self.assertEqual(semantic_fingerprint(colliding), semantic_fingerprint(relabeled_operation))

    def test_semantic_fingerprint_ignores_metadata_and_config_but_tracks_executable_shape(self):
        """Only replay-consumed shape contributes to semantic fingerprinting."""

        metadata_only = generate_suite(
            ["crash_during_commit"], [12], parameter_ranges={"txn_size": [1, 1]}
        )[0]
        changed_metadata = json.loads(json.dumps(metadata_only))
        changed_metadata["config"]["txn_size"] = 99
        changed_metadata["semantic_parameters"] = {"txn_size": 99}
        changed_metadata["seed"] = 999
        changed_metadata["instance_id"] = "metadata_only_change"
        changed_metadata["expected_outcome"] = {"arbitrary": "metadata"}
        changed_metadata["future_metadata"] = {"opaque": ["not", "executable"]}
        low = generate_suite(
            ["crash_during_commit"], [12], parameter_ranges={"txn_size": [1, 1]}
        )[0]
        high = generate_suite(
            ["crash_during_commit"], [12], parameter_ranges={"txn_size": [4, 4]}
        )[0]

        self.assertEqual(semantic_fingerprint(metadata_only), semantic_fingerprint(changed_metadata))
        self.assertNotEqual(semantic_fingerprint(low), semantic_fingerprint(high))
        self.assertNotEqual(len(low["operations"]), len(high["operations"]))

    def test_semantic_fingerprint_changes_for_executable_operation_and_schedule_inputs(self):
        """The executable-input allowlist remains sensitive to replay semantics."""

        instance = generate_instance("atomic_multi_write", seed=20, config={"txn_size": 1})
        changed_operation = json.loads(json.dumps(instance))
        changed_operation["operations"][1]["value"] = "changed_value"
        changed_schedule = json.loads(json.dumps(instance))
        changed_schedule["failure_schedule"][0]["phase"] = "before_validate"

        self.assertNotEqual(semantic_fingerprint(instance), semantic_fingerprint(changed_operation))
        self.assertNotEqual(semantic_fingerprint(instance), semantic_fingerprint(changed_schedule))

    def test_semantic_fingerprint_preserves_reserved_crash_literals_before_transaction_collisions(self):
        """Operation-type selectors remain literal even when a transaction has that identifier."""

        colliding = generate_instance("crash_during_commit", seed=13)
        for operation in colliding["operations"]:
            operation["txn_id"] = "commit"
        relabeled_transaction = json.loads(json.dumps(colliding))
        for operation in relabeled_transaction["operations"]:
            operation["txn_id"] = "txn_relabelled"

        self.assertEqual(semantic_fingerprint(colliding), semantic_fingerprint(relabeled_transaction))

    def test_parameter_ranges_drive_consumed_schedule_and_policy_inputs(self):
        """Replacing consumed policy schedules with ignored annotations must change replay."""

        low = generate_suite(
            ["revoke_before_commit"],
            [7],
            parameter_ranges={"policy_churn": [0, 0], "concurrency": [1, 1]},
        )[0]
        high = generate_suite(
            ["revoke_before_commit"],
            [7],
            parameter_ranges={"policy_churn": [2, 2], "concurrency": [3, 3]},
        )[0]
        low_result = run_instance(low, "TxnMem")
        high_result = run_instance(high, "TxnMem")
        low_oracle = reference_outcome(low)
        high_oracle = reference_outcome(high)

        self.assertNotIn("concurrency_lane", high["operations"][0])
        self.assertNotIn("policy_epoch", high["operations"][0])
        self.assertEqual(
            sum(event["type"] == "revoke" for event in high["failure_schedule"]),
            sum(event["type"] == "revoke" for event in low["failure_schedule"]) + 2,
        )
        self.assertEqual(low_result["metrics"]["policy_version_at_end"], 2)
        self.assertEqual(high_result["metrics"]["policy_version_at_end"], 4)
        self.assertEqual(low_oracle["allowed_outcomes"][0]["policy_version"], 2)
        self.assertEqual(high_oracle["allowed_outcomes"][0]["policy_version"], 4)

    def test_concurrency_generates_interleaved_transaction_sequences(self):
        """Concurrency must create real transaction lifecycles, not ignored lane labels."""

        concurrent = generate_suite(
            ["revoke_before_commit"], [7], parameter_ranges={"concurrency": [3, 3]}
        )[0]
        transaction_ids = {
            operation["txn_id"] for operation in concurrent["operations"] if operation.get("txn_id")
        }
        result = run_instance(concurrent, "TxnMem")
        begin_positions = [
            index for index, operation in enumerate(concurrent["operations"])
            if operation["type"] == "begin_txn"
        ]
        concurrent_work_positions = [
            index for index, operation in enumerate(concurrent["operations"])
            if operation.get("txn_id", "").startswith("txn_001_concurrent_")
            and operation["type"] not in {"begin_txn", "commit"}
        ]
        concurrent_commit_positions = [
            index for index, operation in enumerate(concurrent["operations"])
            if operation.get("txn_id", "").startswith("txn_001_concurrent_")
            and operation["type"] == "commit"
        ]

        self.assertEqual(len(transaction_ids), 3)
        self.assertLess(max(begin_positions), min(concurrent_work_positions))
        self.assertLess(max(concurrent_work_positions), min(concurrent_commit_positions))
        for transaction_id in transaction_ids:
            sequence = [
                operation["type"]
                for operation in concurrent["operations"]
                if operation.get("txn_id") == transaction_id
            ]
            self.assertEqual(sequence[0], "begin_txn")
            self.assertEqual(sequence[-1], "commit")
            self.assertGreaterEqual(len(sequence), 3)
        self.assertTrue(compare_result_to_oracle(concurrent, result)["matches"])

    def test_concurrency_interleaves_real_provenance_writes(self):
        """Concurrent lanes overlap begin/derive/commit work that both executors consume."""

        concurrent = generate_suite(
            ["provenance_chain_repair"],
            [9],
            parameter_ranges={"concurrency": [3, 3], "provenance_depth": [1, 1]},
        )[0]
        concurrent_derives = [
            operation
            for operation in concurrent["operations"]
            if operation["type"] == "derive" and "concurrent" in operation.get("txn_id", "")
        ]
        transaction_ids = [operation["txn_id"] for operation in concurrent_derives]

        self.assertEqual(len(concurrent_derives), 2)
        self.assertEqual({operation["source_ids"][0] for operation in concurrent_derives}, {"m_root"})
        self.assertEqual(len(set(transaction_ids)), 2)
        primary_read_position = next(
            index
            for index, operation in enumerate(concurrent["operations"])
            if operation.get("txn_id") == "txn_derive" and operation["type"] == "read"
        )
        first_concurrent_commit = next(
            index
            for index, operation in enumerate(concurrent["operations"])
            if operation.get("txn_id", "").startswith("txn_derive_concurrent_")
            and operation["type"] == "commit"
        )
        self.assertLess(primary_read_position, first_concurrent_commit)
        self.assertTrue(compare_result_to_oracle(concurrent, run_instance(concurrent, "TxnMem"))["matches"])

    def test_revoke_and_supersession_concurrency_lanes_commit_real_memory_effects(self):
        """These lanes carry observable writes rather than miss-only probes and empty commits."""

        for workload in ("revoke_before_commit", "supersession_consistency"):
            instance = generate_suite(
                [workload], [14], parameter_ranges={"concurrency": [3, 3]}
            )[0]
            result = run_instance(instance, "TxnMem")
            lane_ids = {
                operation["txn_id"]
                for operation in instance["operations"]
                if operation.get("txn_id", "").endswith(("_concurrent_2", "_concurrent_3"))
            }
            lane_writes = [
                operation["memory_id"]
                for operation in instance["operations"]
                if operation.get("txn_id") in lane_ids and operation["type"] == "write"
            ]

            self.assertEqual(len(lane_ids), 2)
            self.assertEqual(len(lane_writes), 2)
            self.assertTrue(set(lane_writes) <= set(result["committed_memory_ids"]))
            self.assertTrue(all(result["transaction_states"][lane] == "committed" for lane in lane_ids))
            self.assertTrue(compare_result_to_oracle(instance, result)["matches"])

    def test_provenance_chain_records_real_derive_operations(self):
        instance = generate_instance("provenance_chain_repair", seed=14, config={"provenance_depth": 2})
        types = [operation["type"] for operation in instance["operations"]]

        self.assertIn("read", types)
        self.assertIn("derive", types)
        self.assertIn("commit", types)
        self.assertEqual(instance["provenance_edges"], [])

    def test_generator_does_not_emit_expected_outcome_ground_truth(self):
        instance = generate_instance("atomic_multi_write", seed=15)

        self.assertNotIn("expected_outcome", instance)


if __name__ == "__main__":
    unittest.main()
