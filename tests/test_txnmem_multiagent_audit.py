import csv
import hashlib
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from txnmem_multiagent_audit import (
    audit_controlled_artifacts,
    audit_storage_report,
    evaluate_claim_status,
)
from txnmem_multiagent_workloads import MULTI_AGENT_WORKLOADS
from txnmem_mutation import MULTI_AGENT_MUTANTS
from txnmem_simulator import VARIANTS


def _write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path, values):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(
            json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n"
            for value in values
        ),
        encoding="utf-8",
    )


def _write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


class MultiAgentAuditTests(unittest.TestCase):
    def _fixture(self, root, *, instance_count=300, seeds=50):
        instances = []
        for index in range(instance_count):
            workload = MULTI_AGENT_WORKLOADS[index % len(MULTI_AGENT_WORKLOADS)]
            seed = index // len(MULTI_AGENT_WORKLOADS)
            instance_id = f"{workload}_seed_{seed}"
            instances.append(
                {
                    "instance_id": instance_id,
                    "workload": workload,
                    "seed": seed,
                    "evidence_tier": "seeded",
                    "structure_signature": hashlib.sha256(
                        instance_id.encode("utf-8")
                    ).hexdigest(),
                    "config": {
                        "agent_count": 2,
                        "txn_size": 1,
                        "provenance_depth": 1,
                        "branch_factor": 1,
                        "concurrency": 2,
                        "policy_churn": 0,
                        "sampled_parameters": {"fixture": seed},
                    },
                    "initial_memories": [],
                    "operations": [
                        {
                            "op_id": f"{instance_id}_a",
                            "step": 1,
                            "agent_id": "agent_1",
                            "type": "begin_txn",
                            "txn_id": "txn_agent_1",
                        },
                        {
                            "op_id": f"{instance_id}_b",
                            "step": 2,
                            "agent_id": "agent_2",
                            "type": "begin_txn",
                            "txn_id": "txn_agent_2",
                        },
                    ],
                    "policies": [],
                    "failure_schedule": [],
                    "provenance_edges": [],
                }
            )
        rows = [
            {
                "instance_id": instance["instance_id"],
                "workload": instance["workload"],
                "seed": instance["seed"],
                "variant": variant,
                "agent_count": 2,
                "transaction_count": 2,
                "structure_signature": instance["structure_signature"],
                "any_violation": 0,
                "violations": "",
                "oracle_match": 1,
            }
            for instance in instances
            for variant in VARIANTS
        ]
        oracles = [
            {"instance_id": instance["instance_id"], "allowed_outcomes": [{}]}
            for instance in instances
        ]
        exhaustive_instance = {
            **instances[0],
            "instance_id": "multi_agent_visibility_exhaustive_0000",
            "evidence_tier": "exhaustive",
        }
        exhaustive_rows = [
            {
                **rows[index],
                "instance_id": exhaustive_instance["instance_id"],
                "variant": variant,
            }
            for index, variant in enumerate(VARIANTS)
        ]
        _write_jsonl(root / "data/generated_instances.jsonl", instances)
        _write_jsonl(root / "data/reference_oracles.jsonl", oracles)
        _write_jsonl(
            root / "data/exhaustive_interleavings.jsonl", [exhaustive_instance]
        )
        _write_jsonl(
            root / "data/exhaustive_reference_oracles.jsonl",
            [{"instance_id": exhaustive_instance["instance_id"]}],
        )
        _write_csv(root / "results/experiment_results.csv", rows)
        _write_csv(root / "results/exhaustive_results.csv", exhaustive_rows)
        _write_json(
            root / "results/controlled_report.json",
            {
                "seeded_instance_count": instance_count,
                "seeded_observation_count": len(rows),
                "exhaustive_instance_count": 1,
                "exhaustive_observation_count": len(exhaustive_rows),
                "replication_count": 3,
                "full_txnmem_seeded_violations": 0,
                "full_txnmem_seeded_oracle_mismatches": 0,
                "full_txnmem_exhaustive_violations": 0,
                "full_txnmem_exhaustive_oracle_mismatches": 0,
                "controlled_gates_passed": True,
            },
        )
        _write_json(root / "results/summary.json", {"schema_version": 1})
        _write_json(root / "results/coverage.json", {"schema_version": 1})
        _write_json(root / "results/mutation_report.json", {"schema_version": 1})
        _write_json(
            root / "results/minimal_witnesses.json",
            {
                "witness_count": 5,
                "all_prefix_minimal": True,
                "witnesses": {
                    name: {
                        "target_violation": violation,
                        "observed": {"reproduces": True},
                        "minimality": {
                            "predecessor_reproduces_target_violation": False
                        },
                    }
                    for name, violation in MULTI_AGENT_MUTANTS.items()
                },
            },
        )
        digest_record = {
            "seeded_input_sha256": "a" * 64,
            "seeded_oracle_sha256": "b" * 64,
            "seeded_results_sha256": "c" * 64,
            "exhaustive_input_sha256": "d" * 64,
            "exhaustive_results_sha256": "e" * 64,
        }
        _write_json(
            root / "results/reproducibility.json",
            {
                "replication_count": 3,
                "all_hashes_identical": True,
                "replications": [
                    {"replication": index, **digest_record} for index in range(1, 4)
                ],
            },
        )
        return instances, rows, oracles

    def _formal_storage(self):
        return {
            "schema_version": 1,
            "status": "passed",
            "real_service_evidence": True,
            "observation_count": 240,
            "unknown_count": 0,
            "partial_visible_count": 0,
            "policy_violation_count": 0,
            "provenance_violation_count": 0,
        }

    def test_rejects_299_or_301_seeded_instances(self):
        for count in (299, 301):
            with self.subTest(count=count), TemporaryDirectory() as tmp:
                root = Path(tmp)
                self._fixture(root, instance_count=count)
                with self.assertRaisesRegex(ValueError, "seeded instance count"):
                    audit_controlled_artifacts(root, expected_seeds=50)

    def test_rejects_family_with_too_few_unique_signatures(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            instances, _, _ = self._fixture(root)
            instances[6]["structure_signature"] = instances[0][
                "structure_signature"
            ]
            _write_jsonl(root / "data/generated_instances.jsonl", instances)
            with self.assertRaisesRegex(ValueError, "unique structure signatures"):
                audit_controlled_artifacts(root, expected_seeds=50)

    def test_rejects_missing_variant(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, rows, _ = self._fixture(root)
            _write_csv(root / "results/experiment_results.csv", rows[:-1])
            with self.assertRaisesRegex(ValueError, "variant rows"):
                audit_controlled_artifacts(root, expected_seeds=50)

    def test_rejects_misaligned_oracle_ids(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, _, oracles = self._fixture(root)
            oracles[0]["instance_id"] = "wrong_instance"
            _write_jsonl(root / "data/reference_oracles.jsonl", oracles)
            with self.assertRaisesRegex(ValueError, "oracle instance IDs"):
                audit_controlled_artifacts(root, expected_seeds=50)

    def test_rejects_inactive_or_missing_mutation_witness(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._fixture(root)
            path = root / "results/minimal_witnesses.json"
            report = json.loads(path.read_text(encoding="utf-8"))
            report["witnesses"].pop("expose_other_txn_pending")
            _write_json(path, report)
            with self.assertRaisesRegex(ValueError, "mutation witnesses"):
                audit_controlled_artifacts(root, expected_seeds=50)

    def test_rejects_inconsistent_replication_hashes(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._fixture(root)
            path = root / "results/reproducibility.json"
            report = json.loads(path.read_text(encoding="utf-8"))
            report["replications"][1]["seeded_results_sha256"] = "f" * 64
            _write_json(path, report)
            with self.assertRaisesRegex(ValueError, "replication hashes"):
                audit_controlled_artifacts(root, expected_seeds=50)

    def test_rejects_unknown_real_storage_state(self):
        report = self._formal_storage()
        report["unknown_count"] = 1
        with self.assertRaisesRegex(ValueError, "unknown"):
            audit_storage_report(report)

    def test_rejects_secrets_home_paths_and_routable_addresses(self):
        slash = "/"
        unsafe_values = (
            "password=hunter2",
            "api_key=secret-value",
            f"{slash}Users/alice/private/result.json",
            "service=8.8.8.8",
        )
        for unsafe in unsafe_values:
            with self.subTest(unsafe=unsafe), TemporaryDirectory() as tmp:
                root = Path(tmp)
                self._fixture(root)
                _write_json(root / "results/coverage.json", {"note": unsafe})
                with self.assertRaisesRegex(ValueError, "sensitive or host-specific"):
                    audit_controlled_artifacts(root, expected_seeds=50)

    def test_exact_fixture_passes_and_activates_with_storage(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            self._fixture(root)
            controlled = audit_controlled_artifacts(root, expected_seeds=50)
            storage = audit_storage_report(self._formal_storage())
            claim = evaluate_claim_status(controlled, storage)

            self.assertEqual(controlled["status"], "passed")
            self.assertEqual(claim["status"], "active")
            self.assertFalse(claim["failed_gates"])


if __name__ == "__main__":
    unittest.main()
