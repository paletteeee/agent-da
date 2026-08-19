# Task 4 implementation report

Status: IMPLEMENTATION AND REVIEW FIX ROUND 3 GREEN; AUTHORIZED-SERVER PREFLIGHT COMPLETE

Base HEAD: `94b42759ff9434e506f6ade992a4206140fdaa72`

Primary implementation commit: `06de7289ad99eb5362f879ed3352b1ce22ba4b2c`

Blocker-fix commit: `274f509773a6a01cb4e69bc3cbf6547f4676283c`

Review-fix round 1 implementation commit: `504abff49ad8d9340d3d0e5d589bd5a4130fc491`

Review-fix round 2 implementation commit: `ddf371dfa6d50c7305153f75b70653872f3c4a57`

Review-fix round 3 implementation commit: this report's commit

## Scope implemented

- Froze legacy τ-bench `retail/test` in official task-source order and AppWorld `test_normal` in official split-file order. Parent manifests retain raw official IDs, source positions, benchmark/domain/split, package or version-file identity, source hashes, ordered-ID hashes, condition fingerprints, and canonical manifest hashes.
- Added deterministic modulo-by-source-position shards. Every shard carries the parent manifest hash, source identity, shared condition fingerprint, benchmark/domain/split, source positions, raw IDs, shard coordinates, and its own canonical hash.
- Added fail-closed shard merging for missing/extra/duplicate tasks, missing/duplicate shards, source-position or shard-assignment changes, parent/source/split/condition mismatches, executed-shard manifest mismatches, conflicting repetitions, and malformed rows. Failed, evaluator-error, and blocked tasks remain in the task denominator.
- Added formal script defaults (`retail/test=115`, `test_normal=168`), generate-only, merge-only, deterministic shard execution, and resume verification. Existing manifests and runs are never overwritten without `--resume`.
- Preserved new benchmark metadata through `load_task_manifest`, bound shard execution conditions to the frozen parent hash, and recorded formal domain/split in the runtime condition. New explicitly scoped τ manifests are rejected before execution if CLI domain/split disagree; legacy manifests without the new benchmark marker retain their prior argument-driven behavior.
- Closed the controller blockers by restoring `raw_task_id` from the frozen parent, requiring each report's executed shard hash, and requiring evaluator status `available` before an official boolean success can count.
- Added one launcher-scoped strict formal store for recursive duplicate-key rejection, type-strict canonical JSON equality, exact schema/count typing, descriptor-relative no-follow path traversal, exclusive formal-file creation, complete pre-model merge replay, strict raw-to-bound resume verification, and exclusive new run-directory creation.

## TDD RED evidence

The inherited focused command initially had no failure:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -v tests.test_benchmark_bridge tests.test_native_scale_manifest tests.test_txnmem_batch_merge`

Result: exit 0; 52 tests ran in 0.331 seconds; 4 optional dependency/data tests skipped.

The inherited directly relevant CLI/loader/public-batch command also exited 0: 85 tests ran in 3.263 seconds.

The controller then reported the complete six-blocker RED: `Ran 643`, `FAILED (failures=4, errors=2, skipped=4)`. The failures were missing condition domain/split, missing merged raw IDs, two τ runtime-scope mismatches returning 0 instead of 2, evaluator-error success inflation, and acceptance of an executed-shard hash mismatch.

Local adversarial RED command:

`PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -v tests.test_txnmem_batch_merge tests.test_cli_outputs`

Result: exit 1; 29 tests ran in 2.943 seconds; 2 failures and 4 errors reproduced the six boundaries. Exact observations were `KeyError: 'raw_task_id'`, `KeyError: 'domain'`, official successes `1 != 0`, no `ValueError` for the executed-shard mismatch, and both τ mismatch paths proceeding into the runner. After correcting the mock so the latter test failed on behavior rather than serialization, the isolated test produced two intended subtest failures, each `0 != 2`.

Review fix round 1 used two separate RED cycles:

- Execution-gated success: the isolated adversarial test ran once in 0.002 seconds and failed four subtests, each with `AssertionError: 1 != 0`, for execution statuses `failed`, `error`, `blocked`, and `evaluator_error`. Every fixture kept `official.status=available` and `official.success=true`, and only one of two repetitions was changed from `completed`.
- Protected merge output: after correcting a fixture-only `FileExistsError`, three real shell integration tests ran in 1.977 seconds and failed six behavioral assertions. A repeated merge-only run returned 0 without `--resume`; canonical-equal files were rewritten in merge-only and normal modes; malformed and valid-different sentinels returned 0 and were overwritten. The normal-mode fixture used pre-bound reports and a Python shim that aborts if `txnmem_experiment.py` is invoked, so no model call was made.

Review fix round 2 used four separate RED cycles:

- Explicit evaluator availability: 1 test ran in 0.002 seconds and failed four subtests, each `AssertionError: 1 != 0`, for missing, `unavailable`, `evaluator_error`, and unknown evaluator statuses paired with `success=true`.
- Bash 3.2 empty optional arguments: 1 real launcher test ran in 1.330 seconds and failed with return code 1 at `TXNMEM_LOCOMO_EVALUATOR_ARGS[@]: unbound variable` before the local batch shim could run.
- Early protected-output refusal: after correcting a fixture-only generated-shim escape, 1 test ran in 0.618 seconds and failed because the model-runner invocation marker existed before the launcher discovered the protected merge.
- Recursive duplicate keys: 1 merge-only resume test ran in 0.085 seconds and failed because a nested duplicate `denominator` key parsed equal to the recomputation and returned 0.

Review fix round 3 used five end-to-end launcher regressions before any production change. The combined RED command exited 1 after 16.092 seconds: 5 test methods produced 20 intended subtest failures.

- Recursive formal JSON: 8 failures showed same-value and wrong-then-correct nested duplicate keys being accepted in parent manifests, shard manifests, raw summaries, and bound reports; every launcher returned 0.
- Type-strict resume equality: 5 failures showed `true` accepted as integer `1` for parent `manifest_version`, shard `shard_count`, and merged `schema_version`, `shard_count`, and `repetitions`.
- Path safety: 2 failures showed dangling parent/merge symlinks being followed to targets outside the output directory and the launcher returning 0.
- Existing-merge preflight: 1 failure showed the model-invocation marker created before a malformed existing merge with no shard reports was rejected.
- Completed-only run reuse: 4 failures covered empty, trace-only, raw-only, and bound-only run directories. Empty/trace-only invoked the model; raw-only was silently rebound; bound-only was silently reused.

## GREEN evidence

- Immediate regression GREEN: 11 tests passed in 0.086 seconds for all merge blocker tests and both CLI scope/condition tests.
- Final affected command (`tests.test_cli_outputs` plus the three Task 4 modules): exit 0; 75 tests ran in 3.566 seconds; 4 optional dependency/data tests skipped; 0 failures/errors.
- Full suite: `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest discover -s tests` → exit 0; 643 tests ran in 113.631 seconds; 4 optional dependency/data tests skipped; 0 failures/errors.
- Claim audit: exit 0; 15 active claims, 0 findings; diagnostic output only at `/tmp/txnmem-task4-claim-audit.json`.
- Artifact audit: exit 0; 0 findings.
- `bash -n scripts/run_native_scale.sh` and `git diff --check`: exit 0 with no output.

Review fix round 1 fresh GREEN evidence:

- Execution-status regression: 1 test passed in 0.001 seconds; the complete merge module then passed 10 tests in 0.003 seconds.
- Merge-output regressions: 3 shell integration tests passed in 1.226 seconds with no model call.
- Complete affected set (`tests.test_cli_outputs`, `tests.test_benchmark_bridge`, `tests.test_native_scale_manifest`, `tests.test_txnmem_batch_merge`): exit 0; 79 tests ran in 4.433 seconds; 4 optional dependency/data tests skipped; 0 failures/errors.
- Full suite: exit 0; 647 tests ran in 101.506 seconds; 4 optional dependency/data tests skipped; 0 failures/errors.
- Claim audit: exit 0; 15 active claims, 0 findings; diagnostic output only at `/tmp/txnmem-task4-fix1-claim-audit.json`.
- Artifact audit: exit 0; 0 findings.
- Fresh `bash -n scripts/run_native_scale.sh`, `git diff --check`, and staged diff check: exit 0 with no output.

Review fix round 2 fresh GREEN evidence:

- Isolated regressions: evaluator-status 1 test in 0.001 seconds; Bash 3.2 empty-array 1 test in 0.929 seconds; early protected-output refusal 1 test in 0.834 seconds; recursive duplicate-key rejection 1 test in 1.137 seconds.
- Complete merge module: 11 tests in 0.004 seconds. Existing canonical-resume regression remained green: 1 test in 0.150 seconds.
- Four new regressions together: 4 tests in 1.881 seconds; 0 failures/errors.
- Complete Task 4 set (`tests.test_cli_outputs`, `tests.test_benchmark_bridge`, `tests.test_native_scale_manifest`, `tests.test_txnmem_batch_merge`): exit 0; 83 tests ran in 5.409 seconds; 4 optional dependency/data tests skipped; 0 failures/errors.
- Full suite: exit 0; 651 tests ran in 101.898 seconds; 4 optional dependency/data tests skipped; 0 failures/errors.
- Claim audit: exit 0; 15 active claims, 0 findings; diagnostic output only at `/tmp/txnmem-task4-fix2-claim-audit.json`.
- Artifact audit: exit 0; 0 findings.
- Host Bash: GNU Bash `3.2.57(1)-release`. Fresh `bash -n scripts/run_native_scale.sh` and `git diff --check`: exit 0 with no output.

Review fix round 3 fresh GREEN evidence:

- The five new reviewer regressions passed in 20.645 seconds, including all 20 adversarial subcases and explicit no-model markers.
- The complete native-scale manifest module passed 23 tests in 20.441 seconds before the populated-array preservation case was added. Empty and populated Bash 3.2 evaluator-array paths then passed 2 tests in 2.188 seconds.
- Complete Task 4 set (`tests.test_cli_outputs`, `tests.test_benchmark_bridge`, `tests.test_native_scale_manifest`, `tests.test_txnmem_batch_merge`): exit 0; 89 tests ran in 24.634 seconds; 4 optional dependency/data tests skipped; 0 failures/errors.
- Full suite: exit 0; 657 tests ran in 121.210 seconds; 4 optional dependency/data tests skipped; 0 failures/errors.
- Claim audit: exit 0; 15 active claims, 0 findings; diagnostic output only at `/tmp/txnmem-task4-fix3-claim-audit.json`.
- Artifact audit: exit 0; 0 findings.
- Host Bash remains GNU Bash `3.2.57(1)-release`. Fresh `bash -n scripts/run_native_scale.sh` and `git diff --check`: exit 0 with no output.
- Fresh local read-only source verification again found τ-bench `retail/test` count 115 and source SHA-256 `6f09468923c6cfb6162e94fe659264d6aee70816c18d51278fb4a581db7765a4`; AppWorld `test_normal` count/unique count 168/168, zero missing task directories, split SHA-256 `c3af41497b6f2f0860a2ff8c09b335dca527e2cf48e59b4aabdb301b6b68db8f`, version `0.2.0`, and version-file SHA-256 `911fc0c48cb0c70601db5775a9bef1b740dc4cc9f9b46389b9f0563fe7eb94d7`.

Review fix round 4 used four end-to-end RED regressions before production changes. The combined command ran 4 test methods in 5.7 seconds and produced 9 intended failures:

- Complete-launch preflight: 2 failures showed a valid first shard invoking the model before a later partial or stale shard was rejected.
- Fresh merge-only rebinding: 2 failures showed duplicate or inconsistent raw summaries being ignored when a pre-bound report was merged without `--resume`.
- Finite-number parsing: 1 failure showed exponent overflow (`1e999`) being parsed as positive infinity despite named NaN/Infinity rejection.
- Runner output confinement: 4 failures showed the actual benchmark runner following planted symlinks for the raw trace, repetition summary, final batch summary, and SQLite backend reservation.

Review fix round 4 fresh GREEN evidence:

- The four new adversarial regressions passed in 5.380 seconds with all 9 subcases protected.
- The complete affected set (`tests.test_native_scale_manifest`, `tests.test_txnmem_batch_merge`, `tests.test_benchmark_bridge`, `tests.test_cli_outputs`, `tests.test_txnmem_real_model`, `tests.test_public_batch_reporting`) passed 155 tests in 29.043 seconds; 4 optional dependency/data tests skipped; 0 failures/errors.
- Full suite: exit 0; 661 tests ran in 100.649 seconds; 4 optional dependency/data tests skipped; 0 failures/errors.
- Claim audit: exit 0; 15 active claims, 0 findings; diagnostic output only at `/tmp/txnmem-task4-round4-controller-claim-audit.json`.
- Artifact audit: exit 0; 0 findings.
- Fresh `bash -n scripts/run_native_scale.sh` and `git diff --check`: exit 0 with no output.

## Local no-model formal preflight

Source checks against the local authorized dataset copies:

- τ-bench package version: `0.1.0`.
- τ-bench retail/test source count: 115; unique source positions: 115.
- τ-bench retail/test source SHA-256: `6f09468923c6cfb6162e94fe659264d6aee70816c18d51278fb4a581db7765a4`.
- AppWorld data version: `0.2.0`; version-file SHA-256: `911fc0c48cb0c70601db5775a9bef1b740dc4cc9f9b46389b9f0563fe7eb94d7`.
- AppWorld `test_normal` count: 168; unique IDs: 168; missing task directories: 0.
- AppWorld `test_normal` split SHA-256: `c3af41497b6f2f0860a2ff8c09b335dca527e2cf48e59b4aabdb301b6b68db8f`.

`scripts/run_native_scale.sh --generate-only --benchmarks tau-bench,appworld --shard-count 8` ran with no endpoint/model arguments and exited 0 under `/tmp/txnmem-task4-local-preflight.xRYQdc`:

- τ parent manifest: `d14cb66efa0eed828db236a016f139a563c085005485c1428e4d637e7b193eeb`; source-identity fingerprint `c15a8f2d36c5dd97fd6c92af779af4c6ae848207440604510da2a4018e19f9f5`; ordered raw-ID hash `a350b909b54f6799bf681af77ba87358d90d9f6a335d7ef8234a005afbb24949`.
- AppWorld parent manifest: `f1b5946abf818fab5cfba9319b50f006878ed1cef1c4a48989a0d8d704803c61`; source-identity fingerprint `1586d08451f1ad1f1b6d1fc8ce2c62f14ad1539ed5b0893724976de8575c3627`; ordered raw-ID hash `990c25609f0777893feec8a72385c0457e5e19f0c17c575159ff263dbe809e83`.
- Both eight-shard sets had exact parent cardinality, unique task IDs, and source positions equal to the complete contiguous parent domain.

No model endpoint was configured or called. Importing the legacy τ package emitted a failed optional LiteLLM model-price metadata fetch and used its local fallback; this did not invoke a model or alter the source evidence.

## Authorized-server preflight

The controller re-established the authorized SSH session and ran a fresh
read-only source check on the configured server. No model or benchmark process
was started. The server copy reported:

- AppWorld `test_normal`: 168 IDs, 168 unique; split SHA-256 `c3af41497b6f2f0860a2ff8c09b335dca527e2cf48e59b4aabdb301b6b68db8f`.
- AppWorld version: `0.2.0`; version-file SHA-256 `911fc0c48cb0c70601db5775a9bef1b740dc4cc9f9b46389b9f0563fe7eb94d7`.
- τ-bench retail/test: 115 tasks; source SHA-256 `6f09468923c6cfb6162e94fe659264d6aee70816c18d51278fb4a581db7765a4`.

All counts and source hashes match the local formal preflight exactly. The
authorized-server source-identity gate is therefore complete.

## Files changed from the Task 3 base

- `.superpowers/sdd/2026-08-18-evidence-scale-up/progress.md`
- `.superpowers/sdd/2026-08-18-evidence-scale-up/task-4-report.md`
- `scripts/run_native_scale.sh`
- `src/txnmem_batch_merge.py`
- `src/txnmem_benchmark_manifests.py`
- `src/txnmem_experiment.py`
- `src/txnmem_formal_io.py`
- `src/txnmem_real_experiment.py`
- `tests/test_benchmark_bridge.py`
- `tests/test_cli_outputs.py`
- `tests/test_native_scale_manifest.py`
- `tests/test_txnmem_batch_merge.py`

## Security/correctness review

- AppWorld split IDs are canonical single path components; traversal, whitespace variants, missing/duplicate IDs, missing versions, and malformed task specs fail closed.
- Parent and shard hashes are canonical content hashes. Merge now independently regenerates the expected shard hash from the frozen parent and rejects report rebinding.
- Raw official IDs come only from the hashed parent manifest; a conflicting report-supplied raw ID is rejected.
- Runtime benchmark/domain/split scope is checked before adapter/model execution for new formal manifests, while the compatibility marker keeps legacy single-batch semantics unchanged.
- A task contributes one official success only when every repetition has execution `status=completed`, an explicit official evaluator field `status=available`, and boolean `success=true`; missing, unavailable, evaluator-error, or unknown statuses remain denominator failures.
- Merged artifacts use parsed canonical equality under `--resume` with recursive duplicate-key rejection. Equal content is accepted without a write; malformed, ambiguous, or different content is rejected unchanged.
- Any existing merged destination is rejected before shard/model execution without resume, and the final write-time guard remains in place for overwrite safety. Dangling symlink destinations also fail the early check.
- Optional evaluator arguments use Bash-3.2 nounset-safe array expansion while preserving populated multi-word arguments.
- Normal-path regressions use deterministic local shims and explicitly detect or forbid model-runner invocation. No endpoint or model was called during review-fix verification.
- Every formal launcher read now uses one recursive duplicate-key-rejecting parser. Resume comparisons use canonical JSON bytes, so booleans, integers, and floating-point values cannot compare through Python coercion.
- Formal paths are traversed from an opened output-root directory descriptor; symlink components and final symlinks fail closed. Parent, shard, summary, bound, and merged files use no-follow exclusive creation. Each new model run starts only after exclusive creation of its shard run directory.
- Existing merges under `--resume` are strictly loaded and recomputed from complete frozen shard manifests plus raw and rebound reports before any model/shard process. Existing runs require both strict raw and bound summaries and exact fresh rebinding; all partial shapes abort unchanged.
- No raw public prompts, tool arguments, benchmark payloads, endpoint, credential, or formal result artifact was added to Git.

Implementation concern: none. External completion gate: none for Task 4. Environment note: the current local checkout has the authorized source copies but lacks τ-bench's optional `litellm` import dependency, so the fresh round-3 local check used read-only AST/count/hash inspection rather than rerunning the real-package generate-only command. The earlier no-model local generator preflight and authorized-server read-only preflight remain recorded above, and no source-selection code changed in round 3.

Final fresh independent review: PASS on `4395858..2c6866e`. The reviewer inspected all 12 Task 4 files and the brief/report/ledger, passed 155 focused tests with 4 expected optional skips, passed 8 final boundary regressions plus independent fixtures for cross-benchmark preflight, non-finite JSON, two repetitions, and no-model resume, verified host Bash 3.2.57 syntax and `git diff --check`, and found no credentials, routable endpoints, or raw public benchmark prompts in tracked changes.
