from __future__ import annotations

import csv
import hashlib
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from txnmem_claim_audit import (  # noqa: E402
    audit_claim_ledger,
    build_controlled_suite_evidence,
)


class ControlledSuiteEvidenceTests(unittest.TestCase):
    def _fixture(self, root: Path) -> tuple[Path, Path]:
        instances_path = root / "generated_instances.jsonl"
        result_path = root / "experiment_results.csv"
        instances = [
            {"instance_id": "w1-s0", "workload": "w1", "seed": 0},
            {"instance_id": "w2-s0", "workload": "w2", "seed": 0},
        ]
        instances_path.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in instances),
            encoding="utf-8",
        )
        rows = [
            {"instance_id": "w1-s0", "workload": "w1", "variant": "TxnMem", "any_violation": 0, "oracle_match": 1},
            {"instance_id": "w2-s0", "workload": "w2", "variant": "TxnMem", "any_violation": 0, "oracle_match": 1},
            {"instance_id": "w1-s0", "workload": "w1", "variant": "Naive", "any_violation": 1, "oracle_match": 0},
            {"instance_id": "w2-s0", "workload": "w2", "variant": "Naive", "any_violation": 0, "oracle_match": 1},
        ]
        with result_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        return instances_path, result_path

    def test_builds_counts_from_rows_instead_of_accepting_handwritten_totals(self):
        with TemporaryDirectory() as tmp:
            instances_path, result_path = self._fixture(Path(tmp))
            evidence = build_controlled_suite_evidence(instances_path, result_path)

        self.assertEqual(evidence["instance_count"], 2)
        self.assertEqual(evidence["workload_family_count"], 2)
        self.assertEqual(evidence["seed_count"], 1)
        self.assertEqual(evidence["variant_count"], 2)
        self.assertEqual(evidence["variant_row_count"], 4)
        self.assertEqual(
            evidence["variants"]["TxnMem"],
            {"row_count": 2, "violation_count": 0, "oracle_match_count": 2},
        )
        self.assertEqual(
            evidence["variants"]["Naive"],
            {"row_count": 2, "violation_count": 1, "oracle_match_count": 1},
        )
        self.assertEqual(len(evidence["sources"]["instances"]["sha256"]), 64)
        self.assertEqual(len(evidence["sources"]["results"]["sha256"]), 64)
        self.assertFalse(evidence["production_latency_claim"])

    def test_rejects_missing_instance_variant_row_instead_of_reporting_old_count(self):
        with TemporaryDirectory() as tmp:
            instances_path, result_path = self._fixture(Path(tmp))
            with result_path.open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))[:-1]
            with result_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)

            with self.assertRaisesRegex(ValueError, "complete instance-by-variant Cartesian product"):
                build_controlled_suite_evidence(instances_path, result_path)

    def test_rejects_duplicate_instance_variant_row(self):
        with TemporaryDirectory() as tmp:
            instances_path, result_path = self._fixture(Path(tmp))
            with result_path.open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            rows.append(dict(rows[0]))
            with result_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)

            with self.assertRaisesRegex(ValueError, "duplicate instance/variant row"):
                build_controlled_suite_evidence(instances_path, result_path)


class PaperClaimLedgerTests(unittest.TestCase):
    @staticmethod
    def _sha256(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def _fixture(self, root: Path) -> tuple[Path, Path]:
        artifact = root / "results" / "evidence.json"
        artifact.parent.mkdir(parents=True)
        artifact.write_text(
            json.dumps({"status": "complete", "task_count": 5}),
            encoding="utf-8",
        )
        manifest = root / "configs" / "manifest.json"
        manifest.parent.mkdir(parents=True)
        manifest.write_text(json.dumps({"tasks": [1, 2, 3, 4, 5]}), encoding="utf-8")
        supersession = root / "results" / "supersession.json"
        supersession.write_text(
            json.dumps({"schema_version": 1, "superseded_artifacts": []}),
            encoding="utf-8",
        )
        ledger = root / "configs" / "paper_claims.json"
        ledger.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "supersession_index": "results/supersession.json",
                    "claims": [
                        {
                            "claim_id": "e2e_task_count",
                            "status": "active",
                            "paper_location": ["Experiments/E2E"],
                            "artifact_path": "results/evidence.json",
                            "artifact_format": "json",
                            "artifact_sha256": self._sha256(artifact),
                            "assertions": [
                                {
                                    "pointer": "/task_count",
                                    "operator": "equals",
                                    "expected": 5,
                                }
                            ],
                            "run_command": "python scripts/run_e2e.py --limit 5",
                            "manifest": {
                                "path": "configs/manifest.json",
                                "sha256": self._sha256(manifest),
                            },
                            "source_commit": "a" * 40,
                            "claim_boundary": "single-host smoke; not production latency",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        return ledger, supersession

    def test_audit_accepts_complete_traceable_claim(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger, _ = self._fixture(root)
            report = audit_claim_ledger(root, ledger)

        self.assertEqual(report["status"], "passed")
        self.assertEqual(report["finding_count"], 0)
        self.assertEqual(report["claim_count"], 1)
        self.assertEqual(report["checked_assertion_count"], 1)

    def test_audit_reports_missing_artifact_and_json_pointer(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger, _ = self._fixture(root)
            payload = json.loads(ledger.read_text())
            payload["claims"][0]["assertions"][0]["pointer"] = "/missing"
            ledger.write_text(json.dumps(payload), encoding="utf-8")
            pointer_report = audit_claim_ledger(root, ledger)
            (root / "results/evidence.json").unlink()
            missing_report = audit_claim_ledger(root, ledger)

        self.assertIn("json_pointer_missing", {item["code"] for item in pointer_report["findings"]})
        self.assertIn("artifact_missing", {item["code"] for item in missing_report["findings"]})

    def test_audit_reports_value_artifact_hash_and_manifest_hash_mismatches(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger, _ = self._fixture(root)
            payload = json.loads(ledger.read_text())
            payload["claims"][0]["assertions"][0]["expected"] = 50
            payload["claims"][0]["artifact_sha256"] = "0" * 64
            payload["claims"][0]["manifest"]["sha256"] = "1" * 64
            ledger.write_text(json.dumps(payload), encoding="utf-8")
            report = audit_claim_ledger(root, ledger)

        codes = {item["code"] for item in report["findings"]}
        self.assertIn("assertion_mismatch", codes)
        self.assertIn("artifact_hash_mismatch", codes)
        self.assertIn("manifest_hash_mismatch", codes)

    def test_audit_rejects_active_claim_on_superseded_artifact(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger, supersession = self._fixture(root)
            supersession.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "superseded_artifacts": [
                            {
                                "artifact_path": "results/evidence.json",
                                "replacement_path": "results/current.json",
                                "reason": "historical pilot",
                                "superseded_on": "2026-08-11",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            report = audit_claim_ledger(root, ledger)

        self.assertIn(
            "active_claim_uses_superseded_artifact",
            {item["code"] for item in report["findings"]},
        )

    def test_audit_rejects_missing_traceability_metadata(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger, _ = self._fixture(root)
            payload = json.loads(ledger.read_text())
            for field in ("run_command", "manifest", "source_commit", "claim_boundary"):
                payload["claims"][0].pop(field)
            ledger.write_text(json.dumps(payload), encoding="utf-8")
            report = audit_claim_ledger(root, ledger)

        missing_fields = {
            item.get("field")
            for item in report["findings"]
            if item["code"] == "claim_metadata_missing"
        }
        self.assertEqual(
            missing_fields,
            {"run_command", "manifest", "source_commit", "claim_boundary"},
        )

    def test_audit_rejects_incomplete_tau_task_set(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger, _ = self._fixture(root)
            artifact = root / "results/evidence.json"
            artifact.write_text(
                json.dumps(
                    {
                        "task_count": 49,
                        "unique_task_count": 49,
                        "evaluator_available_task_count": 49,
                        "task_ids": [f"task-{index}" for index in range(49)],
                    }
                ),
                encoding="utf-8",
            )
            payload = json.loads(ledger.read_text())
            claim = payload["claims"][0]
            claim["artifact_sha256"] = self._sha256(artifact)
            claim["assertions"] = [
                {"pointer": "/task_count", "operator": "equals", "expected": 49}
            ]
            claim["validation_profile"] = "tau_bench_50"
            ledger.write_text(json.dumps(payload), encoding="utf-8")
            report = audit_claim_ledger(root, ledger)

        self.assertIn(
            "tau_task_set_incomplete", {item["code"] for item in report["findings"]}
        )

    def test_audit_rejects_untriggered_toxiproxy_scenario(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger, _ = self._fixture(root)
            artifact = root / "results/evidence.json"
            artifact.write_text(
                json.dumps(
                    {
                        "repetitions_per_scenario": 30,
                        "total_partial_commit_count": 0,
                        "scenarios": {
                            "normal": {},
                            "delay": {
                                "trigger_fired_count": 0,
                                "toxic_installed_count": 0,
                                "proxy_path_verified_count": 30,
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            payload = json.loads(ledger.read_text())
            claim = payload["claims"][0]
            claim["artifact_sha256"] = self._sha256(artifact)
            claim["assertions"] = [
                {
                    "pointer": "/total_partial_commit_count",
                    "operator": "equals",
                    "expected": 0,
                }
            ]
            claim["validation_profile"] = "toxiproxy_fault_matrix"
            ledger.write_text(json.dumps(payload), encoding="utf-8")
            report = audit_claim_ledger(root, ledger)

        self.assertIn(
            "toxiproxy_trigger_evidence_incomplete",
            {item["code"] for item in report["findings"]},
        )


if __name__ == "__main__":
    unittest.main()
