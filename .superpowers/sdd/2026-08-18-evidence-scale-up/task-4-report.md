# Task 4 implementation report

Status: IMPLEMENTATION AND REVIEW FIX ROUND 1 GREEN; AUTHORIZED-SERVER PREFLIGHT COMPLETE

Base HEAD: `94b42759ff9434e506f6ade992a4206140fdaa72`

Primary implementation commit: `06de7289ad99eb5362f879ed3352b1ce22ba4b2c`

Blocker-fix commit: `274f509773a6a01cb4e69bc3cbf6547f4676283c`

Review-fix round 1 implementation commit: `504abff49ad8d9340d3d0e5d589bd5a4130fc491`

## Scope implemented

- Froze legacy τ-bench `retail/test` in official task-source order and AppWorld `test_normal` in official split-file order. Parent manifests retain raw official IDs, source positions, benchmark/domain/split, package or version-file identity, source hashes, ordered-ID hashes, condition fingerprints, and canonical manifest hashes.
- Added deterministic modulo-by-source-position shards. Every shard carries the parent manifest hash, source identity, shared condition fingerprint, benchmark/domain/split, source positions, raw IDs, shard coordinates, and its own canonical hash.
- Added fail-closed shard merging for missing/extra/duplicate tasks, missing/duplicate shards, source-position or shard-assignment changes, parent/source/split/condition mismatches, executed-shard manifest mismatches, conflicting repetitions, and malformed rows. Failed, evaluator-error, and blocked tasks remain in the task denominator.
- Added formal script defaults (`retail/test=115`, `test_normal=168`), generate-only, merge-only, deterministic shard execution, and resume verification. Existing manifests and runs are never overwritten without `--resume`.
- Preserved new benchmark metadata through `load_task_manifest`, bound shard execution conditions to the frozen parent hash, and recorded formal domain/split in the runtime condition. New explicitly scoped τ manifests are rejected before execution if CLI domain/split disagree; legacy manifests without the new benchmark marker retain their prior argument-driven behavior.
- Closed the controller blockers by restoring `raw_task_id` from the frozen parent, requiring each report's executed shard hash, and requiring evaluator status `available` before an official boolean success can count.

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
- A task contributes one official success only when every repetition has execution `status=completed`, normalized official evaluator status `available`, and boolean `success=true`; all other executions remain denominator failures.
- Merged artifacts now use parsed canonical equality under `--resume`. Equal content is accepted without a write; malformed or different content is rejected unchanged; any existing destination is rejected unchanged without resume in both merge-only and normal paths.
- Normal-path output-contract tests use pre-bound reports and explicitly abort if a model runner is invoked. No endpoint or model was called during review-fix verification.
- No raw public prompts, tool arguments, benchmark payloads, endpoint, credential, or formal result artifact was added to Git.

Implementation concern: none. External completion gate: none for Task 4. Process note: the code-review skill's independent reviewer subagent was unavailable in this session, so the final requirement/mutation/security review was performed directly and is not represented as an independent review.
