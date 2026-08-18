from pathlib import Path
from tempfile import TemporaryDirectory
import json
import unittest

from txnmem_artifact_audit import audit_result_paths
from txnmem_workloads import semantic_fingerprint


class TxnMemArtifactAuditTests(unittest.TestCase):
    WORKLOAD_PARAMETERS = {
        "atomic_multi_write": ("txn_size",),
        "crash_during_commit": ("txn_size",),
        "revoke_before_commit": ("policy_churn", "concurrency"),
        "scope_bypass": ("concurrency",),
        "supersession_consistency": ("concurrency",),
        "provenance_chain_repair": ("provenance_depth", "concurrency"),
        "provenance_branch_repair": ("provenance_depth", "branch_factor", "concurrency"),
        "mixed_stress": ("txn_size",),
    }

    @classmethod
    def _write_controlled_pair(cls, root: Path, tree: str, seed_count: int) -> list[Path]:
        data = root / "results" / tree / "data"
        data.mkdir(parents=True, exist_ok=True)
        generated = data / "generated_instances.jsonl"
        oracles = data / "reference_oracles.jsonl"
        scaled = tree == "final_controlled_200"
        generated_rows = []
        oracle_rows = []
        for family, parameters in cls.WORKLOAD_PARAMETERS.items():
            for seed in range(seed_count):
                instance_id = f"{family}_seed_{seed}"
                config = {
                    "agent_count": 2,
                    "txn_size": 1,
                    "provenance_depth": 1,
                    "branch_factor": 1,
                    "policy_churn": 0,
                    "concurrency": 1,
                }
                row = {
                    "instance_id": instance_id,
                    "workload": family,
                    "seed": seed,
                    "config": config,
                    "initial_memories": [],
                    "operations": [],
                    "policies": [],
                    "failure_schedule": [],
                    "provenance_edges": [],
                }
                if scaled:
                    row["semantic_parameters"] = {name: config[name] for name in parameters}
                    row["semantic_fingerprint"] = semantic_fingerprint(row)
                generated_rows.append(row)
                oracle_rows.append(
                    {
                        "instance_id": instance_id,
                        "oracle_version": "0.4" if scaled else "0.1",
                        "allowed_outcomes": [
                            {
                                "txn_states": {},
                                "committed_memory_ids": [],
                                "visible_memory_ids": [],
                                "invalid_memory_ids": [],
                                "superseded_memory_ids": [],
                                "provenance_edges": [],
                                "policy_version": 1,
                                "invariants": {
                                    "atomicity": True,
                                    "commit_authorization": True,
                                    "no_invalid_visibility": True,
                                    "supersession_consistency": True,
                                    "provenance_closure": True,
                                    "graph_validity": True,
                                },
                            }
                        ],
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
                )
        generated.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in generated_rows),
            encoding="utf-8",
        )
        oracles.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in oracle_rows),
            encoding="utf-8",
        )
        return [generated, oracles]

    def test_safe_sanitized_summary_passes(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "results" / "run" / "summary.json"
            path.parent.mkdir(parents=True)
            path.write_text(
                '{"argument_keys":["access_token"],"raw_reports_committed":false}',
                encoding="utf-8",
            )
            self.assertEqual(audit_result_paths([path]), [])

    def test_raw_path_and_secret_value_are_rejected(self):
        with TemporaryDirectory() as tmp:
            raw = Path(tmp) / "results" / "run" / "data" / "trace.jsonl"
            raw.parent.mkdir(parents=True)
            raw.write_text('{"password":"private"}\n', encoding="utf-8")
            findings = audit_result_paths([raw])

        codes = {finding["code"] for finding in findings}
        self.assertIn("raw_result_path", codes)
        self.assertIn("sensitive_result_key", codes)

    def test_exact_controlled_synthetic_allowlist_passes_but_lookalikes_fail(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            approved = [
                *self._write_controlled_pair(root, "final_controlled", 50),
                *self._write_controlled_pair(root, "final_controlled_200", 200),
            ]
            self.assertEqual(audit_result_paths(approved), [])

            lookalike = root / "results/final_controlled_200-copy/data/generated_instances.jsonl"
            nested = root / "results/final_controlled_200/data/raw/generated_instances.jsonl"
            public_trace = root / "results/public_scale_20260818/data/generated_instances.jsonl"
            for path in (lookalike, nested, public_trace):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('{"synthetic":true}\n', encoding="utf-8")
            findings = audit_result_paths([lookalike, nested, public_trace])

        self.assertEqual(
            {finding["path"] for finding in findings if finding["code"] == "raw_result_path"},
            {str(lookalike), str(nested), str(public_trace)},
        )

    def test_sensitive_values_and_symlink_escapes_are_rejected(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            sensitive = root / "results/run/summary.json"
            sensitive.parent.mkdir(parents=True)
            sensitive.write_text('{"note":"sk-secret-token-value"}', encoding="utf-8")
            outside = root / "outside.jsonl"
            outside.write_text('{"synthetic":true}\n', encoding="utf-8")
            escaped = root / "results/final_controlled_200/data/generated_instances.jsonl"
            escaped.parent.mkdir(parents=True)
            escaped.symlink_to(outside)
            findings = audit_result_paths([sensitive, escaped])

        codes = {finding["code"] for finding in findings}
        self.assertIn("sensitive_result_value", codes)
        self.assertIn("result_path_escape", codes)

    def test_raw_capable_public_paths_are_rejected_outside_data_directories(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = [
                root / "results/public_scale/tau_bench/traces/summary.json",
                root / "results/public_scale/appworld/events.csv",
                root / "results/public_scale/locomo/prompts/report.json",
                root / "results/public_scale/tau_bench/native/task.json",
                root / "results/public_scale/appworld/payloads/summary.json",
                root / "results/public_scale/locomo/conversations/summary.json",
                root / "results/public_scale/tau_bench/transcripts/summary.json",
                root / "results/public_scale/appworld/messages/summary.json",
                root / "results/public_scale/locomo/prompt_messages/summary.json",
            ]
            for path in paths:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('{"status":"complete"}', encoding="utf-8")

            findings = audit_result_paths(paths)

        self.assertEqual(
            {finding["path"] for finding in findings if finding["code"] == "raw_result_path"},
            {str(path) for path in paths},
        )

    def test_allowlisted_controlled_filename_rejects_public_payload_and_wrong_count(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            generated, _ = self._write_controlled_pair(root, "final_controlled_200", 200)
            generated.write_text(
                '{"benchmark":"tau-bench","prompt":"raw task"}\n',
                encoding="utf-8",
            )

            findings = audit_result_paths([generated])

        codes = {finding["code"] for finding in findings}
        self.assertIn("controlled_artifact_schema", codes)
        self.assertIn("sensitive_result_key", codes)

    def test_controlled_exceptions_reject_nested_unknown_and_raw_payload_content(self):
        mutations = (
            lambda row: row["config"].__setitem__(
                "customer", {"conversation": "private benchmark dialogue"}
            ),
            lambda row: row.__setitem__(
                "operations",
                [
                    {
                        "op_id": "op_hidden",
                        "step": 0,
                        "type": "write",
                        "value": {"customer": {"transcript": "hidden"}},
                    }
                ],
            ),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate.__code__.co_firstlineno), TemporaryDirectory() as tmp:
                root = Path(tmp)
                generated, _ = self._write_controlled_pair(
                    root, "final_controlled_200", 200
                )
                rows = [json.loads(line) for line in generated.read_text().splitlines()]
                mutate(rows[0])
                rows[0]["semantic_fingerprint"] = semantic_fingerprint(rows[0])
                generated.write_text(
                    "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
                    encoding="utf-8",
                )

                findings = audit_result_paths([generated])

            self.assertIn(
                "controlled_artifact_schema",
                {item["code"] for item in findings},
            )

    def test_controlled_oracle_exception_rejects_nested_conversation_payload(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            _, oracles = self._write_controlled_pair(
                root, "final_controlled_200", 200
            )
            rows = [json.loads(line) for line in oracles.read_text().splitlines()]
            rows[0]["event_trace"] = [
                {"conversation": {"messages": ["private benchmark dialogue"]}}
            ]
            oracles.write_text(
                "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
                encoding="utf-8",
            )

            findings = audit_result_paths([oracles])

        self.assertIn(
            "controlled_artifact_schema",
            {item["code"] for item in findings},
        )

    def test_schema_safe_trace_aggregate_is_explicitly_allowed_but_payload_is_not(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            safe = root / "results/official_trace_runs/appworld/trace_realism.json"
            safe.parent.mkdir(parents=True)
            safe.write_text(
                '{"trace_grounded_status":"trace_supplied","instance_count":168}',
                encoding="utf-8",
            )
            unsafe = root / "results/official_trace_runs/locomo/trace_realism.json"
            unsafe.parent.mkdir(parents=True)
            unsafe.write_text(
                '{"trace_grounded_status":"trace_supplied","events":[{"content":"raw"}]}',
                encoding="utf-8",
            )

            safe_findings = audit_result_paths([safe])
            unsafe_findings = audit_result_paths([unsafe])

        self.assertEqual(safe_findings, [])
        self.assertIn("raw_result_path", {item["code"] for item in unsafe_findings})

    def test_safe_aggregate_never_overrides_a_raw_capable_ancestor(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = (
                root / "results/public/payloads/trace_realism.json",
                root / "results/public/conversations/native_model_summary.json",
                root / "results/public/transcripts/blocked_report.json",
            )
            for path in paths:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('{"status":"sanitized"}', encoding="utf-8")

            findings = audit_result_paths(paths)

        self.assertEqual(
            {item["path"] for item in findings if item["code"] == "raw_result_path"},
            {str(path) for path in paths},
        )

    def test_exact_schema_safe_aggregate_roots_remain_compatible(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = (
                root / "results/prompt_profile_formal_v4/appworld_baseline/native_batch_summary.json",
                root / "results/remaining_tasks/native_memory_replay/appworld/results/native_model_summary.json",
                root / "results/remaining_tasks/native_repetitions5/repetition_report.json",
                root / "results/remaining_tasks/public_native/appworld/results/blocked_report.json",
            )
            for path in paths:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text('{"status":"sanitized"}', encoding="utf-8")

            findings = audit_result_paths(paths)

        self.assertEqual(findings, [])

    def test_controlled_instances_reject_list_payloads_in_typed_fields(self):
        mutations = (
            lambda row: row.__setitem__(
                "operations",
                [{"op_id": "op_hidden", "step": 0, "type": "write", "value": ["customer", "dialogue"]}],
            ),
            lambda row: row.__setitem__(
                "policies",
                [{
                    "policy_id": "p_hidden", "version": 1, "agent_id": "agent_1",
                    "action": "read", "scope": ["customer", "dialogue"],
                    "effect": "allow", "effective_step": 0,
                }],
            ),
            lambda row: row.__setitem__(
                "failure_schedule",
                [{"type": "crash", "target": ["customer", "dialogue"], "step": 1}],
            ),
            lambda row: row.__setitem__(
                "provenance_edges",
                [{"source_id": "m_1", "derived_id": "m_2", "relation": ["customer", "dialogue"]}],
            ),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate.__code__.co_firstlineno), TemporaryDirectory() as tmp:
                root = Path(tmp)
                generated, _ = self._write_controlled_pair(root, "final_controlled_200", 200)
                rows = [json.loads(line) for line in generated.read_text().splitlines()]
                mutate(rows[0])
                rows[0]["semantic_fingerprint"] = semantic_fingerprint(rows[0])
                generated.write_text(
                    "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
                    encoding="utf-8",
                )

                findings = audit_result_paths([generated])

            self.assertIn(
                "controlled_artifact_schema",
                {item["code"] for item in findings},
            )

    def test_controlled_oracles_reject_unapproved_nested_trace_and_outcome_shapes(self):
        mutations = (
            lambda row: row.__setitem__(
                "allowed_outcomes", [{"turns": ["customer", "dialogue"]}]
            ),
            lambda row: row.__setitem__(
                "event_trace", [{"turns": ["customer", "dialogue"]}]
            ),
            lambda row: row.__setitem__(
                "minimal_counterexample", {"turns": ["customer", "dialogue"]}
            ),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate.__code__.co_firstlineno), TemporaryDirectory() as tmp:
                root = Path(tmp)
                _, oracles = self._write_controlled_pair(root, "final_controlled_200", 200)
                rows = [json.loads(line) for line in oracles.read_text().splitlines()]
                mutate(rows[0])
                oracles.write_text(
                    "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
                    encoding="utf-8",
                )

                findings = audit_result_paths([oracles])

            self.assertIn(
                "controlled_artifact_schema",
                {item["code"] for item in findings},
            )

    def test_controlled_records_reject_raw_dialogue_in_approved_scalar_fields(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            generated, oracles = self._write_controlled_pair(
                root, "final_controlled_200", 200
            )
            generated_rows = [json.loads(line) for line in generated.read_text().splitlines()]
            generated_rows[0]["policies"] = [
                {
                    "policy_id": "p_hidden",
                    "version": 1,
                    "agent_id": "agent_1",
                    "action": "read",
                    "scope": "customer dialogue",
                    "effect": "allow",
                    "effective_step": 0,
                }
            ]
            generated_rows[0]["semantic_fingerprint"] = semantic_fingerprint(
                generated_rows[0]
            )
            generated.write_text(
                "".join(json.dumps(row, sort_keys=True) + "\n" for row in generated_rows),
                encoding="utf-8",
            )
            oracle_rows = [json.loads(line) for line in oracles.read_text().splitlines()]
            oracle_rows[0]["allowed_outcomes"][0]["txn_states"] = {
                "txn_001": "customer dialogue"
            }
            oracles.write_text(
                "".join(json.dumps(row, sort_keys=True) + "\n" for row in oracle_rows),
                encoding="utf-8",
            )

            findings = audit_result_paths([generated, oracles])

        self.assertEqual(
            {
                item["path"]
                for item in findings
                if item["code"] == "controlled_artifact_schema"
            },
            {str(generated), str(oracles)},
        )


if __name__ == "__main__":
    unittest.main()
