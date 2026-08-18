# Task 3 implementation report

Status: DONE

Base HEAD: `5dd171fd8e3b9aff0da37d311c187c21ceff2bd6`

Commit: `315b42a`

## Scope implemented

- Added `controlled_violation_saturation(rows, checkpoints, confidence=0.95)` with a complete family × contiguous seed-prefix × variant cube check. It rejects duplicate cells, missing cells, extra partial variants, imbalanced family seed domains, inconsistent instance coordinates, malformed binary metrics, and invalid/non-nested checkpoints.
- Added deterministic Wilson 95% intervals with explicit units for checkpoint seeds, instances, variant result rows, violations, and oracle matches.
- Added `controlled_diversity(instances)` that recomputes Task 2's executable `semantic_fingerprint`, rejects recorded/computed mismatches, and reports per-family fingerprint counts, parameter value counts, inclusive approved-interval coverage, and combination coverage.
- Extended the controlled experiment runner to write `saturation.json`, `diversity.json`, deterministic `saturation.svg`, and canonical `run_manifest.json` binding source commit and component identity, oracle 0.4, config byte hash and canonical fingerprint, family/seed/variant domains, counts, and primary artifact hashes.
- Added the `controlled_scale_200` claim-validation profile. Active scaled controlled claims now require manifest, saturation, and diversity paths/hashes; the audit verifies actual bytes, JSON shape, manifest-relative paths, manifest artifact hashes, and source-commit continuity.
- Added an exact controlled synthetic/oracle raw-artifact allowlist for the historical and new approved controlled trees. Lookalike prefixes, nested raw payloads, public benchmark data, sensitive keys/values, traversal, and symlink escapes fail closed.
- No formal result artifacts were added to Git. Deterministic verification outputs remain under `/tmp`.

## TDD evidence

Initial RED command:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -v tests.test_txnmem_statistics tests.test_txnmem_claim_audit tests.test_txnmem_artifact_audit tests.test_cli_outputs`

Result: exit 1; 43 tests ran; expected Task 3 gaps produced one statistics import error plus five behavior failures (scaled claim bundle checks, exact controlled allowlist, sensitive-value/symlink rejection, and missing CLI artifacts).

Compatibility regression RED:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -v tests.test_txnmem_artifact_audit.TxnMemArtifactAuditTests.test_exact_controlled_synthetic_allowlist_passes_but_lookalikes_fail`

Result: exit 1; the newly asserted historical exact controlled paths were rejected before their explicit entries were restored.

Focused GREEN command:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -v tests.test_txnmem_statistics tests.test_txnmem_claim_audit tests.test_txnmem_artifact_audit tests.test_cli_outputs`

Result: exit 0; 49 tests passed.

Compatibility GREEN command:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -v tests.test_txnmem_artifact_audit`

Result: exit 0; 4 tests passed.

## Deterministic 200-seed evidence

Run A: `/tmp/txnmem-task3-run-a.xYSVaI`

Run B: `/tmp/txnmem-task3-run-b.NCyBx2`

Both commands used:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B src/txnmem_experiment.py experiment --config configs/controlled_scale_200.json --out-dir <run> --seeds 200`

Both exited 0 and wrote 1,600 instances, 1,600 reference oracles, and 8,000 variant rows. `diff -rq` across the complete two output trees exited 0 with no output, so every generated JSONL, CSV, JSON, and SVG byte matched. The canonical privacy scan for timestamps, absolute paths, host/user fields, endpoints, URLs, and secret-key/value markers returned no matches.

Manifest-bound hashes, independently checked with `shasum -a 256 -c`:

- `generated_instances.jsonl`: `46bfb35ad694e551ab2191b2b0a2e9747ff0fb074c5fdbd0fcff9b17889d939d`
- `reference_oracles.jsonl`: `0d4df56fa5afbe0820b0bf1bf5838703972dfa0210c28ca35a9ce95ded134409`
- `experiment_results.csv`: `6541b46e61531e7adb368bd4eec806010f4d6403902cb40ce32f080b6ffa8c7a`
- `saturation.json`: `8f3475f219e33a63fb0a7628b1791c3c01bb8db1077cec2738044f621758a3be`
- `diversity.json`: `0d232a35fe18e7eff3c0deb499fe49c6361594aad52db1b610e97c52e5ca68e9`
- `saturation.svg`: `db213c021926e1a9909057acb13db12f12536968c74fa29d86fb35cf223d5c98`

The manifest reports oracle `0.4`, config SHA-256 `76cd8b4231f57d1fa28a24594635104bbcd69cdc52638f9459db730ed58edc9e`, 8 families, seeds 0–199, all 5 variants, 1,600 instances, and 8,000 rows. Saturation checkpoints are exactly `10/25/50/100/150/200`. At 200 seeds, TxnMem has 0/1,600 violations (rate 0.0; Wilson 95% CI `[0.0, 0.0023951611922532253]`) and 1,600/1,600 oracle matches (rate 1.0; CI `[0.9976048388077468, 1.0]`).

Per-family executable fingerprint counts are: atomic multi-write 4, crash during commit 4, mixed stress 4, provenance branch repair 36, provenance chain repair 12, revoke before commit 9, scope bypass 3, and supersession consistency 3. Every reported parameter and parameter-combination coverage ratio is 1.0 against the approved inclusive intervals.

## Final verification

- Full suite: `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest discover -s tests -v` → exit 0; 578 tests passed, 4 optional dependency/data tests skipped.
- Claim audit: `... src/txnmem_claim_audit.py audit --root . --ledger configs/paper_claims.json --out /tmp/txnmem-task3-claim-audit-final.json` → exit 0; 15 claims, 0 findings.
- Artifact audit: `... src/txnmem_artifact_audit.py --root .` → exit 0; 0 findings.
- `git diff --check` → exit 0 with no output.
- Canonical privacy `rg` scan → exit 1 with no matches (expected no-match status).

## Files changed

- `src/txnmem_statistics.py`
- `src/txnmem_metrics.py`
- `src/txnmem_experiment.py`
- `src/txnmem_claim_audit.py`
- `src/txnmem_artifact_audit.py`
- `tests/test_txnmem_statistics.py`
- `tests/test_txnmem_claim_audit.py`
- `tests/test_txnmem_artifact_audit.py`
- `tests/test_cli_outputs.py`

## Self-review

- Rechecked every Task 3 brief item and the binding design cautions against the final diff.
- Confirmed no Task 1/2 semantics or oracle 0.4 behavior changed.
- Confirmed only the nine Task 3 source/test files are tracked as modified and no result artifacts are staged.
- Confirmed the raw-data exception is exact-path only and sensitive scanning still applies to allowlisted controlled files.
- Confirmed manifest artifact paths are relative and no varying diagnostic field was needed; the entire output tree is byte-identical across runs.

Concerns: none.

## Review fix round 3

Status: DONE

Implementation commit: `00b4c71bc2ccf2789084df171b3e607bc6cb9543`

### Findings closed

- Scaled-controlled signatures are discovered recursively across claim metadata, active artifact objects and nested arrays/objects, independent of wrapper/field names. Exact registered domains, the 1,600/8,000 formal count pair and controlled-scale identities all force the strict profile and bundle gate.
- A scaled claim's primary `artifact_path`, hash and assertions must now be bound to one of the exact six controlled artifacts. A renamed seventh artifact cannot borrow a valid bundle.
- Every stored oracle is schema-validated, required to contain an outcome, regenerated with `reference_outcome` from its generated instance and compared structurally with the regenerated record. Every CSV oracle field (`oracle_version`, `oracle_match`, `allowed_outcome_count`, `oracle_mismatches`) is recomputed from a fresh variant execution against that regenerated oracle.
- Raw-capable ancestors such as `payloads`, `conversations` and `transcripts` dominate safe aggregate basenames. Exact historical aggregate roots remain compatible only with an explicitly schema-safe aggregate filename/document.
- Controlled generated instances and reference-oracle records use positive recursive schemas for config, memories, operations/new-memory, policies, schedules/triggers, provenance, allowed outcomes, invariants and event traces. Unknown nested shapes, type-confused lists and raw customer/dialogue marker values fail closed.

### TDD evidence

Primary adversarial RED command:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -v tests.test_txnmem_claim_audit.PaperClaimLedgerTests.test_scaled_controlled_claim_signal_recurses_through_renamed_nested_fields tests.test_txnmem_claim_audit.PaperClaimLedgerTests.test_scaled_controlled_claim_rejects_primary_seventh_artifact tests.test_txnmem_claim_audit.PaperClaimLedgerTests.test_scaled_controlled_claim_regenerates_oracles_and_csv_oracle_fields tests.test_txnmem_artifact_audit.TxnMemArtifactAuditTests.test_safe_aggregate_never_overrides_a_raw_capable_ancestor tests.test_txnmem_artifact_audit.TxnMemArtifactAuditTests.test_controlled_instances_reject_list_payloads_in_typed_fields tests.test_txnmem_artifact_audit.TxnMemArtifactAuditTests.test_controlled_oracles_reject_unapproved_nested_trace_and_outcome_shapes`

Primary RED result: exit 1; 6 tests ran and exactly 6 failed. Each failure observed an empty finding set for its intended bypass.

Scalar-content RED command:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -v tests.test_txnmem_artifact_audit.TxnMemArtifactAuditTests.test_controlled_records_reject_raw_dialogue_in_approved_scalar_fields`

Scalar-content RED result: exit 1; 1 test ran and failed because both mutated controlled records were accepted.

Aggregate-compatibility RED command:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -v tests.test_txnmem_artifact_audit.TxnMemArtifactAuditTests.test_exact_schema_safe_aggregate_roots_remain_compatible`

Compatibility RED result: exit 1; 1 test ran and failed because all four established schema-safe aggregate paths were over-classified as raw.

Adversarial GREEN result: exit 0; all 7 bypass tests passed together. Aggregate attack/compatibility GREEN result: exit 0; 2 tests passed.

### Final verification

Focused command:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -q tests.test_txnmem_statistics tests.test_txnmem_conditions tests.test_txnmem_claim_audit tests.test_txnmem_artifact_audit tests.test_cli_outputs`

Focused result: exit 0; 80 tests passed in 36.285 seconds.

Full command:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest discover -s tests -q`

Full result: exit 0; 606 tests passed, 4 optional dependency/data tests skipped, in 42.028 seconds.

- Claim audit: exit 0; 15 active claims, 0 findings; output only at `/tmp/txnmem-scale-claim-audit-fix3-final-precommit.json`.
- Artifact audit: exit 0; 0 findings.
- `git diff --check`: exit 0 with no output.
- No formal result artifact was added to Git.

### Deterministic committed-HEAD verification

- Run A: `/tmp/txnmem-fix3-formal-a.3AtLEO/results/final_controlled_200`
- Run B: `/tmp/txnmem-fix3-formal-b.UYlSZZ/results/final_controlled_200`
- Both used `experiment --config configs/controlled_scale_200.json --seeds 200 --require-clean-source` and exited 0 with 1,600 instances and 8,000 result rows.
- `diff -rq` over the complete 14-file trees exited 0 with no output.
- Both manifests declare commit `00b4c71bc2ccf2789084df171b3e607bc6cb9543`, `contained_in_commit=true`, oracle `0.4`, 1,600 instances and 8,000 variant rows.
- The generated-instance and reference-oracle JSONL files from both trees passed the controlled artifact audit with 0 findings.

### Files changed

- `src/txnmem_claim_audit.py`
- `src/txnmem_artifact_audit.py`
- `tests/test_txnmem_claim_audit.py`
- `tests/test_txnmem_artifact_audit.py`
- `.superpowers/sdd/2026-08-18-evidence-scale-up/task-3-brief.md`
- `.superpowers/sdd/2026-08-18-evidence-scale-up/progress.md`
- `.superpowers/sdd/2026-08-18-evidence-scale-up/task-3-report.md`

### Self-review

- Confirmed each realistic mutation—non-recursive signal, unbound primary artifact, trusted empty oracle, trusted CSV flag, safe-basename ancestor override, open list shape and hidden raw scalar—causes a targeted regression failure.
- Confirmed actual 200-seed generated/oracle shapes and the historical 50/200 controlled fixtures satisfy the positive schemas.
- Confirmed historical non-scaled claims, Task 1/2 behavior and oracle version 0.4 remain unchanged.

Concerns: none.

## Review fix round 2

Status: DONE

Implementation commit: `42c984aede04e8471ba02959ceac3cb11ecf550c`

### Findings closed

- Scaled-claim detection now consumes the parsed active claim artifact as well as the manifest and claim metadata. Exact registered manifest-style domains/counts, saturation/diversity identities at 200 seeds, and top-level `controlled_suite` 8×200×5/1,600/8,000 declarations all force the exact profile and six-artifact bundle even after path/claim renaming.
- The bundle now recomputes `controlled_diversity` from `generated_instances.jsonl` and requires exact equality with `diversity.json`, retaining the prior oracle/result/saturation reconciliation. `saturation.svg` must parse as XML with an SVG namespace root.
- Public raw-path vocabulary now includes payload(s), conversation(s), transcript(s), message(s), chat/dialogue forms, and tool-argument forms. Controlled generated instances enforce closed config and executable-container key schemas recursively; nested mappings are permitted only for the approved `new_memory` and schedule `trigger` shapes, while hidden customer/conversation payloads fail closed. Controlled oracle exceptions retain their historical exact safety schema and recursively reject raw payload keys.

### TDD evidence

Primary RED command:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -v tests.test_txnmem_claim_audit tests.test_txnmem_artifact_audit`

Primary RED result: exit 1; 38 tests ran; exactly 6 failures. The failures were the renamed scale declaration bypass, raw/diversity mismatch, malformed SVG acceptance, expanded raw-path vocabulary, nested controlled instance payload acceptance, and nested controlled oracle payload acceptance.

Additional evidence-declaration RED command:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -v tests.test_txnmem_claim_audit.PaperClaimLedgerTests.test_scaled_controlled_claim_signal_comes_from_renamed_artifact_declarations`

Additional RED result: exit 1; 1 test ran; 1 failure reproduced a renamed top-level `controlled_suite` declaration with exact 1,600-instance/8,000-row scale.

Immediate GREEN results:

- Primary claim/artifact modules: exit 0; 38 tests passed.
- Top-level controlled-suite signal regression: exit 0; 1 test passed.

### Final verification

Focused command:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -q tests.test_txnmem_statistics tests.test_txnmem_conditions tests.test_txnmem_claim_audit tests.test_txnmem_artifact_audit tests.test_cli_outputs`

Focused result: exit 0; 72 tests passed.

Full command:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest discover -s tests -q`

Full result: exit 0; 598 tests passed, 4 optional dependency/data tests skipped.

- Claim audit: exit 0; 15 active claims, 0 findings; output only at `/tmp/txnmem-scale-claim-audit-fix2-final.json`.
- Artifact audit: exit 0; 0 findings.
- `git diff --check`: exit 0 with no output.
- No formal evidence artifact was added to Git.

### Files changed

- `src/txnmem_claim_audit.py`
- `src/txnmem_artifact_audit.py`
- `tests/test_txnmem_claim_audit.py`
- `tests/test_txnmem_artifact_audit.py`
- `.superpowers/sdd/2026-08-18-evidence-scale-up/task-3-brief.md`
- `.superpowers/sdd/2026-08-18-evidence-scale-up/progress.md`
- `.superpowers/sdd/2026-08-18-evidence-scale-up/task-3-report.md`

### Self-review

- Verified scale identity comes from evidence declarations rather than filenames or optional profile fields.
- Verified diversity equality is derived from raw executable fingerprints and family-specific parameter counts/coverage, not an internally plausible summary.
- Verified malformed text and non-SVG XML roots are rejected after all hashes are consistently rebound.
- Verified the closed controlled schema still accepts the tracked historical 400-instance/oracle pair and the current 200-seed generated shape.
- Verified Task 1/2 behavior and oracle 0.4 were not modified.

Concerns: none.

## Review fix round 1

Status: DONE

### Findings closed

- Saturation now defaults to the exact registered 8-family × 5-variant domain and accepts alternate domains only when the caller supplies them explicitly. It rejects whole missing/extra domains, partial cubes, duplicates, non-contiguous or imbalanced seeds, noncanonical instance coordinates, invalid checkpoints, and non-binary metrics.
- Diversity now binds every family to exactly `WORKLOAD_SEMANTIC_PARAMETERS`, checks every semantic value against its approved inclusive interval and executable config value, recomputes Task 2's executable fingerprint, and derives every denominator from the approved family-specific Cartesian product.
- The controlled manifest now carries canonical relative source/config component hashes and a source fingerprint. Git blob containment verifies that the commit exists, every component is a tracked blob, and declared/current/blob SHA-256 values agree. `--require-clean-source` runs this preflight before output creation; external, untracked, dirty, or missing source/config fails closed.
- Scaled active claims are recognized by profile, evidence, identity/path, command, or 1,600/8,000 counts. They require the exact `controlled_scale_200` profile and six-artifact bundle, canonical non-symlink paths, exact domains/counts/checkpoints, strict manifest/saturation/diversity/raw schemas, Wilson arithmetic, CSV-to-saturation reconciliation, oracle 0.4, approved config identity, complete hash closure, and independent Git containment verification.
- Artifact audit now rejects raw-capable path components globally, permits only explicit schema-safe aggregate filenames, and validates both historical and 200-seed controlled JSONL exceptions line by line, including exact coordinates/counts, semantic contracts/fingerprints, oracle versions, and continued sensitive-key/value scanning.

### Fix-round TDD evidence

RED command:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -v tests.test_txnmem_statistics tests.test_txnmem_conditions tests.test_txnmem_claim_audit tests.test_txnmem_artifact_audit tests.test_cli_outputs`

RED result: exit 1; 62 tests ran; 11 failures and 17 errors. Failures were the intended missing exact-domain, diversity/config, Git-containment/formal-gate, strict six-artifact claim, and artifact-schema behaviors. No unrelated fixture failure was observed.

Final focused GREEN command: same five modules with `-q`.

Final focused result: exit 0; 67 tests passed.

### Fix-round deterministic and final verification

- Diagnostic run A: `/tmp/txnmem-fix1-diagnostic-a.jOYnoV`
- Diagnostic run B: `/tmp/txnmem-fix1-diagnostic-b.TC0KB7`
- Both 200-seed runs exited 0 with 1,600 instances and 8,000 variant rows. `diff -rq` over both complete trees exited 0 with no output. Because this run intentionally preceded the fix commit, both manifests recorded commit `315b42a8946de38eb6bf7cd3146963eec467918e` and `contained_in_commit=false`.
- Full suite: exit 0; 593 tests passed, 4 optional dependency/data tests skipped.
- Claim audit: exit 0; 15 active claims, 0 findings; diagnostic report written only to `/tmp/txnmem-scale-claim-audit-fix1.json`.
- Artifact audit: exit 0; 0 findings.
- `git diff --check`: exit 0 with no output.
- No formal result artifact was added to Git.

After the fix commit, the required formal-source verification is run twice from committed HEAD into `/tmp/txnmem-fix1-formal-a` and `/tmp/txnmem-fix1-formal-b` with `--require-clean-source`; the handoff records the final commit identity, containment result, and complete-tree byte comparison.

### Fix-round files changed

- `src/txnmem_artifact_audit.py`
- `src/txnmem_claim_audit.py`
- `src/txnmem_conditions.py`
- `src/txnmem_experiment.py`
- `src/txnmem_statistics.py`
- `tests/test_cli_outputs.py`
- `tests/test_txnmem_artifact_audit.py`
- `tests/test_txnmem_claim_audit.py`
- `tests/test_txnmem_conditions.py`
- `tests/test_txnmem_statistics.py`
- `.superpowers/sdd/2026-08-18-evidence-scale-up/task-3-report.md`

### Fix-round self-review

- Re-read the updated brief and ledger rulings and mapped every review item to a targeted regression.
- Verified historical non-scaled claim validation remains green and oracle version 0.4 is unchanged.
- Verified the source gate executes before any artifact writer and that diagnostic runs remain possible with explicit `contained_in_commit=false`.
- Verified canonical outputs contain no timestamps, host/user identity, endpoints, secrets, or absolute paths.
- Verified only Task 3 implementation, tests, and report files changed; no formal evidence tree is tracked.

Concerns: none.
