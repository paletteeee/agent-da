from __future__ import annotations

import csv
import hashlib
import json
import subprocess
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
from txnmem_conditions import canonical_fingerprint  # noqa: E402
from txnmem_simulator import VARIANTS  # noqa: E402
from txnmem_statistics import binomial_interval, controlled_diversity  # noqa: E402
from txnmem_workloads import WORKLOADS, generate_suite  # noqa: E402


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
                    "expected_active_claim_count": 1,
                    "expected_assertion_count": 1,
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

    def test_audit_rejects_truthy_wrong_active_claim_containers(self):
        replacements = {
            "assertions": "truthy-but-not-a-list",
            "manifest": "truthy-but-not-an-object",
            "paper_location": "truthy-but-not-a-list",
        }
        for field, replacement in replacements.items():
            with self.subTest(field=field), TemporaryDirectory() as tmp:
                root = Path(tmp)
                ledger, _ = self._fixture(root)
                payload = json.loads(ledger.read_text())
                payload["claims"][0][field] = replacement
                ledger.write_text(json.dumps(payload), encoding="utf-8")

                report = audit_claim_ledger(root, ledger)

            self.assertEqual(report["status"], "failed")
            self.assertIn(
                field,
                {
                    finding.get("field")
                    for finding in report["findings"]
                    if finding["code"] == "claim_schema_invalid"
                },
            )

    def test_audit_rejects_malformed_assertion_objects(self):
        malformed = (
            "not-an-object",
            {"operator": "equals", "expected": 5},
            {"pointer": "/task_count", "operator": "unknown", "expected": 5},
            {"pointer": "/task_count", "operator": "equals"},
        )
        for assertion in malformed:
            with self.subTest(assertion=assertion), TemporaryDirectory() as tmp:
                root = Path(tmp)
                ledger, _ = self._fixture(root)
                payload = json.loads(ledger.read_text())
                payload["claims"][0]["assertions"] = [assertion]
                ledger.write_text(json.dumps(payload), encoding="utf-8")

                report = audit_claim_ledger(root, ledger)

            self.assertEqual(report["status"], "failed")
            self.assertIn(
                "assertion_invalid", {item["code"] for item in report["findings"]}
            )

    def test_audit_rejects_unknown_claim_status_and_validation_profile(self):
        for field, value in (
            ("status", "archived"),
            ("validation_profile", "unregistered-profile"),
        ):
            with self.subTest(field=field), TemporaryDirectory() as tmp:
                root = Path(tmp)
                ledger, _ = self._fixture(root)
                payload = json.loads(ledger.read_text())
                payload["claims"][0][field] = value
                ledger.write_text(json.dumps(payload), encoding="utf-8")

                report = audit_claim_ledger(root, ledger)

            self.assertEqual(report["status"], "failed")
            self.assertIn(
                "claim_schema_invalid", {item["code"] for item in report["findings"]}
            )

    def test_audit_rejects_assertion_count_drop(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger, _ = self._fixture(root)
            payload = json.loads(ledger.read_text())
            payload["claims"][0]["assertions"] = [
                {"pointer": "/status", "operator": "equals", "expected": "complete"},
                {"pointer": "/task_count", "operator": "equals", "expected": 5},
            ]
            payload["expected_assertion_count"] = 2
            ledger.write_text(json.dumps(payload), encoding="utf-8")
            self.assertEqual(audit_claim_ledger(root, ledger)["status"], "passed")

            payload["claims"][0]["assertions"].pop()
            ledger.write_text(json.dumps(payload), encoding="utf-8")
            report = audit_claim_ledger(root, ledger)

        self.assertIn(
            "assertion_count_mismatch", {item["code"] for item in report["findings"]}
        )

    def test_audit_rejects_wrong_scalar_field_types(self):
        replacements = {
            "artifact_path": ["results/evidence.json"],
            "artifact_format": ["json"],
            "artifact_sha256": {"digest": "0" * 64},
            "run_command": ["python", "run.py"],
            "source_commit": 1,
            "claim_boundary": ["not production latency"],
        }
        for field, replacement in replacements.items():
            with self.subTest(field=field), TemporaryDirectory() as tmp:
                root = Path(tmp)
                ledger, _ = self._fixture(root)
                payload = json.loads(ledger.read_text())
                payload["claims"][0][field] = replacement
                ledger.write_text(json.dumps(payload), encoding="utf-8")

                report = audit_claim_ledger(root, ledger)

            self.assertEqual(report["status"], "failed")
            self.assertIn(
                field,
                {
                    finding.get("field")
                    for finding in report["findings"]
                    if finding["code"] == "claim_schema_invalid"
                },
            )

    SCALED_SOURCE_PATHS = (
        "configs/controlled_scale_200.json",
        "src/txnmem_conditions.py",
        "src/txnmem_differential.py",
        "src/txnmem_experiment.py",
        "src/txnmem_invariants.py",
        "src/txnmem_metrics.py",
        "src/txnmem_reference.py",
        "src/txnmem_schedules.py",
        "src/txnmem_schema.py",
        "src/txnmem_simulator.py",
        "src/txnmem_statistics.py",
        "src/txnmem_workloads.py",
    )
    PARAMETER_INTERVALS = {
        "txn_size": [1, 4],
        "provenance_depth": [1, 4],
        "branch_factor": [1, 3],
        "policy_churn": [0, 2],
        "concurrency": [1, 3],
    }

    def _make_scaled_controlled_claim(self, root: Path, ledger: Path) -> dict:
        for relative in self.SCALED_SOURCE_PATHS:
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            if relative == "configs/controlled_scale_200.json":
                path.write_bytes((ROOT / relative).read_bytes())
            else:
                path.write_text(f"# fixture for {relative}\n", encoding="utf-8")
        subprocess.run(["git", "init", "-q"], cwd=root, check=True)
        subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=root, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=root, check=True)
        subprocess.run(["git", "add", *self.SCALED_SOURCE_PATHS], cwd=root, check=True)
        subprocess.run(["git", "commit", "-q", "-m", "scaled source"], cwd=root, check=True)
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, check=True,
            capture_output=True, text=True,
        ).stdout.strip()
        components = {
            relative: self._sha256(root / relative)
            for relative in self.SCALED_SOURCE_PATHS
        }

        controlled = root / "results/final_controlled_200"
        data = controlled / "data"
        results = controlled / "results"
        figures = results / "figures"
        data.mkdir(parents=True)
        figures.mkdir(parents=True)
        generated = data / "generated_instances.jsonl"
        oracles = data / "reference_oracles.jsonl"
        csv_path = results / "experiment_results.csv"
        saturation = results / "saturation.json"
        diversity = results / "diversity.json"
        svg = figures / "saturation.svg"

        generated_rows = generate_suite(
            WORKLOADS,
            range(200),
            parameter_ranges=self.PARAMETER_INTERVALS,
        )
        oracle_rows = [
            {
                "instance_id": row["instance_id"],
                "oracle_version": "0.4",
                "allowed_outcomes": [],
                "event_trace": [],
                "minimal_counterexample": None,
                "safety_invariants": {
                    "atomicity": True,
                    "commit_authorization": True,
                    "no_invalid_visibility": True,
                    "supersession_consistency": True,
                    "provenance_closure": True,
                    "graph_validity": True,
                },
            }
            for row in generated_rows
        ]
        generated.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in generated_rows),
            encoding="utf-8",
        )
        oracles.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in oracle_rows),
            encoding="utf-8",
        )
        csv_fields = [
            "instance_id", "workload", "seed", "variant", "any_violation",
            "oracle_version", "oracle_match",
        ]
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=csv_fields)
            writer.writeheader()
            for row in generated_rows:
                for variant in VARIANTS:
                    writer.writerow(
                        {
                            "instance_id": row["instance_id"],
                            "workload": row["workload"],
                            "seed": row["seed"],
                            "variant": variant,
                            "any_violation": 0,
                            "oracle_version": "0.4",
                            "oracle_match": 1,
                        }
                    )

        checkpoints = [10, 25, 50, 100, 150, 200]
        saturation_document = {
            "schema_version": 1,
            "evidence_id": "controlled_violation_saturation",
            "confidence": 0.95,
            "interval_method": "wilson_score",
            "interpretation_boundary": "intervals describe only the tested controlled parameter space",
            "families": sorted(WORKLOADS),
            "seed_domain": list(range(200)),
            "variant_domain": sorted(VARIANTS),
            "checkpoint_seed_counts": checkpoints,
            "checkpoints": [],
        }
        for checkpoint in checkpoints:
            trials = len(WORKLOADS) * checkpoint
            saturation_document["checkpoints"].append(
                {
                    "checkpoint_seed_count": checkpoint,
                    "checkpoint_seed_unit": "seeds_per_family",
                    "family_count": 8,
                    "instance_count": trials,
                    "instance_unit": "family_seed",
                    "variants": [
                        {
                            "variant": variant,
                            "variant_result_count": trials,
                            "variant_result_unit": "family_seed_variant",
                            "violations": 0,
                            "violation_rate": 0.0,
                            "violation_interval": binomial_interval(0, trials),
                            "oracle_matches": trials,
                            "oracle_match_rate": 1.0,
                            "oracle_match_interval": binomial_interval(trials, trials),
                        }
                        for variant in sorted(VARIANTS)
                    ],
                }
            )
        saturation.write_text(json.dumps(saturation_document, sort_keys=True), encoding="utf-8")

        diversity.write_text(
            json.dumps(controlled_diversity(generated_rows), sort_keys=True),
            encoding="utf-8",
        )
        svg.write_text('<svg xmlns="http://www.w3.org/2000/svg"></svg>\n', encoding="utf-8")

        artifact_specs = {
            "generated_instances.jsonl": (generated, "data/generated_instances.jsonl", {"line_count": 1600}),
            "reference_oracles.jsonl": (oracles, "data/reference_oracles.jsonl", {"line_count": 1600}),
            "experiment_results.csv": (csv_path, "results/experiment_results.csv", {"row_count": 8000}),
            "saturation.json": (saturation, "results/saturation.json", {}),
            "diversity.json": (diversity, "results/diversity.json", {}),
            "saturation.svg": (svg, "results/figures/saturation.svg", {}),
        }
        artifacts = {
            name: {
                "relative_path": relative,
                "sha256": self._sha256(path),
                **counts,
            }
            for name, (path, relative, counts) in artifact_specs.items()
        }
        config_document = json.loads((root / "configs/controlled_scale_200.json").read_text())
        manifest = controlled / "run_manifest.json"
        manifest.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "runner_version": "controlled-experiment/1",
                    "source": {
                        "commit": commit,
                        "components": components,
                        "fingerprint": canonical_fingerprint(components),
                        "contained_in_commit": True,
                    },
                    "oracle_version": "0.4",
                    "config": {
                        "relative_path": "configs/controlled_scale_200.json",
                        "sha256": self._sha256(root / "configs/controlled_scale_200.json"),
                        "canonical_fingerprint": canonical_fingerprint(config_document),
                    },
                    "domains": {
                        "families": sorted(WORKLOADS),
                        "seeds": list(range(200)),
                        "variants": list(VARIANTS),
                    },
                    "counts": {
                        "families": 8,
                        "seeds_per_family": 200,
                        "instances": 1600,
                        "variant_results": 8000,
                    },
                    "artifacts": artifacts,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        payload = json.loads(ledger.read_text(encoding="utf-8"))
        claim = payload["claims"][0]
        claim.update(
            {
                "claim_id": "controlled_scale_200",
                "artifact_path": "results/final_controlled_200/results/saturation.json",
                "artifact_sha256": self._sha256(saturation),
                "assertions": [
                    {
                        "pointer": "/checkpoint_seed_counts",
                        "operator": "equals",
                        "expected": checkpoints,
                    }
                ],
                "validation_profile": "controlled_scale_200",
                "manifest": {
                    "path": "results/final_controlled_200/run_manifest.json",
                    "sha256": self._sha256(manifest),
                },
                "source_commit": commit,
                "controlled_evidence": {
                    name: {
                        "path": f"results/final_controlled_200/{relative}",
                        "sha256": self._sha256(path),
                    }
                    for name, (path, relative, _) in artifact_specs.items()
                },
            }
        )
        ledger.write_text(json.dumps(payload), encoding="utf-8")
        return payload

    def _rehash_scaled_bundle(self, root: Path, ledger: Path) -> dict:
        payload = json.loads(ledger.read_text(encoding="utf-8"))
        claim = payload["claims"][0]
        manifest_path = root / claim["manifest"]["path"]
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for name, reference in claim["controlled_evidence"].items():
            path = root / reference["path"]
            digest = self._sha256(path)
            reference["sha256"] = digest
            manifest["artifacts"][name]["sha256"] = digest
        manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
        claim["manifest"]["sha256"] = self._sha256(manifest_path)
        claim["artifact_sha256"] = self._sha256(root / claim["artifact_path"])
        ledger.write_text(json.dumps(payload), encoding="utf-8")
        return payload

    def test_scaled_controlled_claim_requires_exact_profile_and_complete_bundle(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger, _ = self._fixture(root)
            self._make_scaled_controlled_claim(root, ledger)
            passed = audit_claim_ledger(root, ledger)

            payload = json.loads(ledger.read_text(encoding="utf-8"))
            payload["claims"][0].pop("validation_profile")
            ledger.write_text(json.dumps(payload), encoding="utf-8")
            missing_profile = audit_claim_ledger(root, ledger)

        self.assertEqual(passed["status"], "passed")
        self.assertIn(
            "controlled_profile_required",
            {item["code"] for item in missing_profile["findings"]},
        )

    def test_scaled_controlled_claim_signal_comes_from_renamed_artifact_declarations(self):
        documents = (
            {
                "task_count": 5,
                "domains": {
                    "families": sorted(WORKLOADS),
                    "seeds": list(range(200)),
                    "variants": list(VARIANTS),
                },
                "counts": {
                    "families": 8,
                    "seeds_per_family": 200,
                    "instances": 1600,
                    "variant_results": 8000,
                },
            },
            {
                "task_count": 5,
                "evidence_id": "controlled_violation_saturation",
                "families": sorted(WORKLOADS),
                "seed_domain": list(range(200)),
                "variant_domain": sorted(VARIANTS),
                "checkpoint_seed_counts": [10, 25, 50, 100, 150, 200],
            },
            {
                "task_count": 5,
                "evidence_id": "controlled_suite",
                "instance_count": 1600,
                "workload_family_count": 8,
                "seed_count": 200,
                "variant_count": 5,
                "variant_row_count": 8000,
            },
        )
        for document in documents:
            with self.subTest(identity=document.get("evidence_id", "domains")), TemporaryDirectory() as tmp:
                root = Path(tmp)
                ledger, _ = self._fixture(root)
                artifact = root / "results/evidence.json"
                artifact.write_text(json.dumps(document), encoding="utf-8")
                payload = json.loads(ledger.read_text())
                payload["claims"][0]["artifact_sha256"] = self._sha256(artifact)
                ledger.write_text(json.dumps(payload), encoding="utf-8")

                report = audit_claim_ledger(root, ledger)

            codes = {item["code"] for item in report["findings"]}
            self.assertIn("controlled_profile_required", codes)
            self.assertIn("controlled_evidence_missing", codes)

    def test_scaled_controlled_claim_cross_checks_bundle_hashes_against_manifest(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger, _ = self._fixture(root)
            payload = self._make_scaled_controlled_claim(root, ledger)
            payload["claims"][0]["controlled_evidence"]["diversity.json"]["sha256"] = "0" * 64
            ledger.write_text(json.dumps(payload), encoding="utf-8")

            report = audit_claim_ledger(root, ledger)

        codes = {item["code"] for item in report["findings"]}
        self.assertIn("controlled_evidence_hash_mismatch", codes)
        self.assertIn("controlled_manifest_hash_mismatch", codes)

    def test_scaled_controlled_claim_rejects_manifest_domain_count_config_and_source_tampering(self):
        mutations = {
            "domain": lambda document: document["domains"].__setitem__("variants", list(VARIANTS[:-1])),
            "count": lambda document: document["counts"].__setitem__("instances", 1599),
            "config": lambda document: document["config"].__setitem__("sha256", "0" * 64),
            "source": lambda document: document["source"].__setitem__("contained_in_commit", False),
            "component_hash": lambda document: document["source"]["components"].__setitem__(
                "src/txnmem_workloads.py", "0" * 64
            ),
            "closure": lambda document: document["artifacts"].pop("saturation.svg"),
            "oracle": lambda document: document.__setitem__("oracle_version", "0.3"),
        }
        for label, mutate in mutations.items():
            with self.subTest(label=label), TemporaryDirectory() as tmp:
                root = Path(tmp)
                ledger, _ = self._fixture(root)
                self._make_scaled_controlled_claim(root, ledger)
                payload = json.loads(ledger.read_text())
                manifest_path = root / payload["claims"][0]["manifest"]["path"]
                manifest = json.loads(manifest_path.read_text())
                mutate(manifest)
                manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
                payload["claims"][0]["manifest"]["sha256"] = self._sha256(manifest_path)
                ledger.write_text(json.dumps(payload), encoding="utf-8")

                report = audit_claim_ledger(root, ledger)

            self.assertEqual(report["status"], "failed")
            self.assertIn("controlled_manifest_invalid", {item["code"] for item in report["findings"]})

    def test_scaled_controlled_claim_rejects_saturation_arithmetic_and_diversity_contract_tampering(self):
        for name in ("saturation.json", "diversity.json"):
            with self.subTest(name=name), TemporaryDirectory() as tmp:
                root = Path(tmp)
                ledger, _ = self._fixture(root)
                self._make_scaled_controlled_claim(root, ledger)
                payload = json.loads(ledger.read_text())
                reference = payload["claims"][0]["controlled_evidence"][name]
                path = root / reference["path"]
                document = json.loads(path.read_text())
                if name == "saturation.json":
                    document["checkpoints"][0]["variants"][0]["variant_result_count"] -= 1
                else:
                    document["families"]["provenance_branch_repair"]["parameter_combination_coverage"]["approved_combination_count"] = 1
                path.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")
                self._rehash_scaled_bundle(root, ledger)

                report = audit_claim_ledger(root, ledger)

            expected = "controlled_saturation_invalid" if name == "saturation.json" else "controlled_diversity_invalid"
            self.assertIn(expected, {item["code"] for item in report["findings"]})

    def test_scaled_controlled_claim_cross_checks_saturation_against_result_rows(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger, _ = self._fixture(root)
            payload = self._make_scaled_controlled_claim(root, ledger)
            reference = payload["claims"][0]["controlled_evidence"]["saturation.json"]
            path = root / reference["path"]
            document = json.loads(path.read_text())
            variant = document["checkpoints"][0]["variants"][0]
            variant["violations"] = 1
            variant["violation_rate"] = 1 / 80
            variant["violation_interval"] = binomial_interval(1, 80)
            path.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")
            self._rehash_scaled_bundle(root, ledger)

            report = audit_claim_ledger(root, ledger)

        self.assertIn(
            "controlled_manifest_invalid",
            {item["code"] for item in report["findings"]},
        )

    def test_scaled_controlled_claim_cross_checks_diversity_against_generated_instances(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger, _ = self._fixture(root)
            payload = self._make_scaled_controlled_claim(root, ledger)
            reference = payload["claims"][0]["controlled_evidence"]["diversity.json"]
            path = root / reference["path"]
            document = json.loads(path.read_text())
            counts = document["families"]["atomic_multi_write"]["parameter_value_counts"]["txn_size"]
            counts["1"] -= 1
            counts["2"] += 1
            path.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")
            self._rehash_scaled_bundle(root, ledger)

            report = audit_claim_ledger(root, ledger)

        self.assertIn(
            "controlled_diversity_invalid",
            {item["code"] for item in report["findings"]},
        )

    def test_scaled_controlled_claim_requires_a_real_svg_root(self):
        replacements = (
            "not svg at all",
            '<html xmlns="http://www.w3.org/1999/xhtml"></html>',
        )
        for replacement in replacements:
            with self.subTest(replacement=replacement[:8]), TemporaryDirectory() as tmp:
                root = Path(tmp)
                ledger, _ = self._fixture(root)
                payload = self._make_scaled_controlled_claim(root, ledger)
                reference = payload["claims"][0]["controlled_evidence"]["saturation.svg"]
                (root / reference["path"]).write_text(replacement, encoding="utf-8")
                self._rehash_scaled_bundle(root, ledger)

                report = audit_claim_ledger(root, ledger)

            self.assertIn(
                "controlled_svg_invalid",
                {item["code"] for item in report["findings"]},
            )

    def test_scaled_controlled_claim_rejects_malformed_paths_and_symlink_escapes(self):
        for replacement in (
            "/absolute/saturation.json",
            "results/final_controlled_200/results/../saturation.json",
        ):
            with self.subTest(replacement=replacement), TemporaryDirectory() as tmp:
                root = Path(tmp)
                ledger, _ = self._fixture(root)
                payload = self._make_scaled_controlled_claim(root, ledger)
                payload["claims"][0]["controlled_evidence"]["saturation.json"]["path"] = replacement
                ledger.write_text(json.dumps(payload), encoding="utf-8")
                report = audit_claim_ledger(root, ledger)
            self.assertIn("invalid_path", {item["code"] for item in report["findings"]})

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger, _ = self._fixture(root)
            payload = self._make_scaled_controlled_claim(root, ledger)
            saturation_ref = payload["claims"][0]["controlled_evidence"]["saturation.json"]
            saturation = root / saturation_ref["path"]
            outside = root / "outside.json"
            outside.write_bytes(saturation.read_bytes())
            saturation.unlink()
            saturation.symlink_to(outside)
            ledger.write_text(json.dumps(payload), encoding="utf-8")
            report = audit_claim_ledger(root, ledger)
        self.assertIn("invalid_path", {item["code"] for item in report["findings"]})

    def test_scaled_controlled_claim_rejects_truthy_malformed_or_extra_bundle_entries(self):
        replacements = (
            "truthy-not-an-object",
            {"saturation.json": {"path": "x", "sha256": "0" * 64}},
        )
        for replacement in replacements:
            with self.subTest(replacement=type(replacement).__name__), TemporaryDirectory() as tmp:
                root = Path(tmp)
                ledger, _ = self._fixture(root)
                payload = self._make_scaled_controlled_claim(root, ledger)
                payload["claims"][0]["controlled_evidence"] = replacement
                ledger.write_text(json.dumps(payload), encoding="utf-8")
                report = audit_claim_ledger(root, ledger)
            self.assertIn(
                "controlled_evidence_missing",
                {item["code"] for item in report["findings"]},
            )

    def test_existing_ledger_assertions_cover_reviewed_artifact_values(self):
        ledger = json.loads((ROOT / "configs" / "paper_claims.json").read_text())
        claims = {claim["claim_id"]: claim for claim in ledger["claims"]}
        expected = {
            "controlled_correctness_400x5": {
                "/variants/Naive/oracle_match_count": 50,
                "/variants/TxnMem-NoTxn/oracle_match_count": 200,
                "/variants/TxnMem-NoPolicyCommit/oracle_match_count": 350,
                "/variants/TxnMem-NoRepair/oracle_match_count": 300,
            },
            "controlled_mutation_matrix_350": {
                "/mutation_cases": 350,
                "/mutation_killed": 300,
                "/mutation_kill_rate": 0.8571428571428571,
            },
            "minimal_mutant_witnesses_4": {
                "/witnesses/partial_commit/minimal_operation_count": 2,
                "/witnesses/remove_commit_revalidation/minimal_operation_count": 1,
                "/witnesses/disable_provenance_traversal/minimal_operation_count": 6,
                "/witnesses/bypass_scope_check/minimal_operation_count": 1,
            },
            "appworld_prompt_profile_pair": {"/tuned_status_counts/failed": 6},
        }

        for claim_id, expected_assertions in expected.items():
            assertions = {
                row["pointer"]: row["expected"]
                for row in claims[claim_id]["assertions"]
            }
            self.assertEqual(
                {pointer: assertions.get(pointer) for pointer in expected_assertions},
                expected_assertions,
            )

    def test_state_verified_ledger_covers_the_reviewed_artifact_values(self):
        ledger = json.loads((ROOT / "configs" / "paper_claims.json").read_text())
        claims = {claim["claim_id"]: claim for claim in ledger["claims"]}
        toxiproxy = claims["toxiproxy_fault_matrix_5x30"]
        self.assertEqual(toxiproxy["validation_profile"], "toxiproxy_state_verified")
        self.assertEqual(
            toxiproxy["artifact_path"],
            "results/submission_evidence/toxiproxy_state_verified_30/aggregate.json",
        )
        self.assertEqual(
            toxiproxy["artifact_sha256"],
            "04de2a3c7da3b8c2dcda06d88afdb18e6f224d8f0e1fcaae4847f1277b3bbcad",
        )
        self.assertEqual(
            toxiproxy["manifest"],
            {
                "path": "configs/submission_evidence/toxiproxy_state_verified_30.json",
                "sha256": "2301e650fc8ae02c25ca608bf161045db170f5b90ec08e9b74d8cda8d6d5dc11",
            },
        )
        self.assertEqual(
            toxiproxy["source_commit"],
            "33a334dc7c4e6d2e0250bb54cd25f0e2f080ed5d",
        )
        self.assertEqual(
            toxiproxy["run_command"],
            "COMPOSE_PROGRESS=plain TXNMEM_PYTHON=.venv/bin/python "
            "TXNMEM_REPETITIONS=30 TXNMEM_EVENTS=2 "
            "TXNMEM_OUT_DIR=results/real_backend_faults_state_verified_30_v2 "
            "bash scripts/run_real_backend_smoke.sh",
        )
        self.assertEqual(
            toxiproxy["claim_boundary"],
            "single-host real Qdrant/Neo4j with deterministic Toxiproxy fault injection "
            "and post-operation readback for the tested workload and five scenarios; "
            "not general distributed transactions, cross-host fault tolerance, "
            "availability, linearizability, or production latency",
        )
        assertions = {
            row["pointer"]: row["expected"] for row in toxiproxy["assertions"]
        }
        expected = {
            "/schema_version": 2,
            "/evidence_id": "toxiproxy_state_verified_30",
            "/status": "complete_state_verified_fault_observations",
            "/scenario_count": 5,
            "/repetitions_per_scenario": 30,
            "/total_repetitions": 150,
            "/all_scenarios_evidence_valid": True,
            "/all_scenarios_state_verified": True,
            "/all_observed_states_consistent": True,
            "/state_totals/complete": 90,
            "/state_totals/absent": 60,
            "/state_totals/partial": 0,
            "/state_totals/unknown": 0,
            "/production_latency_claim": False,
            "/runtime_attestation/image_digests/neo4j": "9317a2941a9641169aa2ea8470cdda184ff7a9ee1914b5429126d0db4828edd2",
            "/runtime_attestation/image_digests/qdrant": "7a4788934788a7ed9cbf6b8cc3ca1ee880dcd969cf8c6639dc7d0e446cbd4b47",
            "/runtime_attestation/image_digests/toxiproxy": "927c797a2115a193ae3a527e5a36782b938419904ac6706ca0efa029ebea58cb",
        }
        expected.update(
            {
                f"/scenarios/{scenario}/state_counts/{state}": count
                for scenario, complete, absent in (
                    ("normal", 30, 0),
                    ("delay", 30, 0),
                    ("retry_success", 30, 0),
                    ("timeout", 0, 30),
                    ("connection_drop", 0, 30),
                )
                for state, count in (
                    ("complete", complete),
                    ("absent", absent),
                    ("partial", 0),
                    ("unknown", 0),
                )
            }
        )
        expected.update(
            {
                f"/scenarios/{scenario}/{field}": 30
                for scenario in ("delay", "retry_success", "timeout", "connection_drop")
                for field in (
                    "trigger_fired_count",
                    "toxic_installed_count",
                    "proxy_path_verified_count",
                )
            }
        )
        expected.update(
            {
                "/scenarios/retry_success/retry_count": 30,
                "/scenarios/retry_success/retry_success_count": 30,
                "/scenarios/timeout/abort_count": 30,
                "/scenarios/connection_drop/abort_count": 30,
            }
        )
        self.assertEqual(assertions, expected)

    def _state_verified_profile_fixture(self, root: Path) -> Path:
        ledger, _ = self._fixture(root)
        artifact = root / "results" / "evidence.json"
        source = ROOT / "results/submission_evidence/toxiproxy_state_verified_30/aggregate.json"
        artifact.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
        payload = json.loads(ledger.read_text())
        claim = payload["claims"][0]
        claim["artifact_sha256"] = self._sha256(artifact)
        claim["assertions"] = [
            {
                "pointer": "/status",
                "operator": "equals",
                "expected": "complete_state_verified_fault_observations",
            }
        ]
        claim["validation_profile"] = "toxiproxy_state_verified"
        ledger.write_text(json.dumps(payload), encoding="utf-8")
        return ledger

    def test_state_verified_profile_rejects_superficially_asserted_incomplete_evidence(self):
        cases = (
            ("partial", lambda document: document["state_totals"].__setitem__("partial", 1)),
            ("unknown", lambda document: document["state_totals"].__setitem__("unknown", 1)),
            (
                "missing_state_flag",
                lambda document: document.pop("all_scenarios_state_verified"),
            ),
            (
                "missing_consistency_flag",
                lambda document: document.pop("all_observed_states_consistent"),
            ),
            ("wrong_scenario_set", lambda document: document["scenarios"].pop("delay")),
            (
                "wrong_complete_absent_distribution",
                lambda document: document["scenarios"]["timeout"]["state_counts"].__setitem__("complete", 30),
            ),
            (
                "incomplete_proxy_evidence",
                lambda document: document["scenarios"]["delay"].__setitem__(
                    "proxy_path_verified_count", 29
                ),
            ),
            (
                "wrong_retry_abort_semantics",
                lambda document: document["scenarios"]["retry_success"].__setitem__(
                    "retry_success_count", 29
                ),
            ),
        )
        for name, mutate in cases:
            with self.subTest(name=name), TemporaryDirectory() as tmp:
                root = Path(tmp)
                ledger = self._state_verified_profile_fixture(root)
                artifact = root / "results" / "evidence.json"
                document = json.loads(artifact.read_text())
                mutate(document)
                artifact.write_text(json.dumps(document), encoding="utf-8")
                payload = json.loads(ledger.read_text())
                payload["claims"][0]["artifact_sha256"] = self._sha256(artifact)
                ledger.write_text(json.dumps(payload), encoding="utf-8")

                report = audit_claim_ledger(root, ledger)

            self.assertIn(
                "toxiproxy_state_evidence_incomplete",
                {item["code"] for item in report["findings"]},
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

    def test_audit_rejects_legacy_toxiproxy_atomicity_profile(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger, _ = self._fixture(root)
            payload = json.loads(ledger.read_text())
            payload["claims"][0]["validation_profile"] = "toxiproxy_fault_matrix"
            ledger.write_text(json.dumps(payload), encoding="utf-8")

            report = audit_claim_ledger(root, ledger)

        self.assertIn(
            "claim_schema_invalid", {item["code"] for item in report["findings"]}
        )

    def test_audit_rejects_untriggered_toxiproxy_fault_path(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger, _ = self._fixture(root)
            artifact = root / "results/evidence.json"
            artifact.write_text(
                json.dumps(
                    {
                        "repetitions_per_scenario": 30,
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
                    "pointer": "/repetitions_per_scenario",
                    "operator": "equals",
                    "expected": 30,
                }
            ]
            claim["validation_profile"] = "toxiproxy_fault_path"
            ledger.write_text(json.dumps(payload), encoding="utf-8")
            report = audit_claim_ledger(root, ledger)

        self.assertIn(
            "toxiproxy_trigger_evidence_incomplete",
            {item["code"] for item in report["findings"]},
        )


if __name__ == "__main__":
    unittest.main()
