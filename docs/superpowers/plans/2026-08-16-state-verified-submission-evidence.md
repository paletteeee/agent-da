# State-Verified Submission Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to execute this plan task-by-task.

**Goal:** Promote the completed 5-scenario × 30-repetition Qdrant/Neo4j/Toxiproxy run into the paper's sole active real-backend fault artifact, with fail-closed persistent-state validation, reproducible environment attestation, supersession metadata, audited claims, and visually verified Chinese DOCX output.

**Architecture:** Keep the raw runner output immutable and add a strict projection layer in `txnmem_evidence_aggregates.py`. The projection independently recomputes proxy-path and post-operation state invariants from every repetition, binds the result to a sanitized environment attestation and source hashes, and emits one compact schema-v2 artifact. The claim ledger and manuscript consume only this compact artifact; the previous proxy-only aggregate remains archived and is made unusable as active evidence through the supersession index.

**Tech Stack:** Python 3 standard library, `unittest`, JSON/JSON Pointer claim audit, Docker Compose service metadata, Markdown, `python-docx`, LibreOffice/Poppler-based DOCX rendering, Git.

## Global Constraints

- Work only in the current isolated worktree on branch `codex/pre-submission-evidence`.
- Preserve the raw source bytes at `results/real_backend_faults_state_verified_30_v2/results/backend_performance.json`; do not normalize or rewrite them.
- Track the raw JSON result but exclude transient `pid` and `run.log` files from formal evidence and commits.
- Treat `33a334dc7c4e6d2e0250bb54cd25f0e2f080ed5d` as the experiment source commit and verify it is present in the local repository.
- Never store a password, SSH invocation containing a password, full hostname, routable IP address, user name, or secret-bearing environment variable in an artifact. The immutable raw result may retain `127.0.0.1` loopback endpoints because they describe only the tested local proxy path and cannot identify the remote host; run commands and attestations receive no such exception.
- Keep the claim boundary narrow: single-host real Qdrant/Neo4j, deterministic Toxiproxy injection, and post-operation readback for the tested workload/scenarios only. Do not claim general distributed transactions, cross-host fault tolerance, availability, linearizability, or production latency.
- Follow red-green-refactor for all Python behavior changes and run focused tests before each commit.
- Before any completion claim, run the full test suite, clean archive verification, claim/manuscript audits, DOCX structure/privacy/accessibility checks, and page-by-page PNG visual inspection.

---

## Task 1: Add a fail-closed state-verified aggregate

**Files:**

- Modify: `tests/test_txnmem_evidence_aggregates.py`
- Modify: `src/txnmem_evidence_aggregates.py`
- Modify: `scripts/aggregate_submission_evidence.py`

### Step 1: Add a realistic positive fixture

- [ ] Add `_state_verified_toxiproxy_source(repetitions: int = 2)` that models all five fixed scenarios.
- [ ] Include one performance row with `event_count == 2`; this is the sole source of `workload_events`.
- [ ] Give every repetition exactly two distinct expected memory IDs and exactly two `state_verification` entries.
- [ ] For `normal`, `delay`, and `retry_success`, mark both Qdrant and Neo4j reads successful, present, and matching.
- [ ] For `timeout` and `connection_drop`, mark both Qdrant and Neo4j reads successful and absent.
- [ ] Preserve the existing trigger, toxic, proxy, response, retry, and abort counters so the new aggregate is a strict superset of the old fault-path check.

### Step 2: Write the positive contract test and observe RED

- [ ] Call the aggregate with a mapping-valued `runtime_attestation` and assert the exact schema-v2 contract:

```python
self.assertEqual(result["schema_version"], 2)
self.assertEqual(result["evidence_id"], "toxiproxy_state_verified_30")
self.assertEqual(result["status"], "complete_state_verified_fault_observations")
self.assertEqual(result["workload_events"], 2)
self.assertEqual(result["total_repetitions"], 10)
self.assertEqual(result["state_totals"], {
    "complete": 6,
    "absent": 4,
    "partial": 0,
    "unknown": 0,
})
self.assertTrue(result["all_scenarios_state_verified"])
self.assertTrue(result["all_observed_states_consistent"])
```

- [ ] Assert the output includes the attestation SHA-256 and image digests but contains no host identity material beyond a one-way host hash.
- [ ] Run:

```bash
python3 -m unittest tests.test_txnmem_evidence_aggregates.SubmissionEvidenceAggregateTests.test_toxiproxy_aggregate_requires_consistent_post_operation_state
```

- [ ] Confirm failure is caused by the missing `runtime_attestation` argument or old schema contract.

### Step 3: Add negative state-oracle tests and observe RED

- [ ] Add table-driven tests that independently mutate the fixture and require `ValueError` for:

```text
partial state
unknown state
Qdrant read_ok=false
Neo4j read_ok=false
present/matches disagreement
duplicate expected memory IDs
state-verification row count mismatch
memory-event count mismatch
scenario repetition mismatch
aggregate all_scenarios_state_verified=false
aggregate all_observed_states_consistent=false
```

- [ ] Add an attestation test that rejects missing image digest, mismatched source SHA-256, non-zero exit code, missing runtime version, or secret-bearing command text.
- [ ] Run all aggregate tests and confirm only the newly introduced contracts fail:

```bash
python3 -m unittest tests.test_txnmem_evidence_aggregates
```

### Step 4: Implement strict state recomputation

- [ ] Extend the function signature:

```python
def aggregate_toxiproxy_submission_evidence(
    source_path: str | Path,
    *,
    expected_repetitions: int = 30,
    toxiproxy_version: str,
    source_commit: str,
    run_command: str,
    runtime_attestation: Mapping[str, Any],
) -> dict[str, Any]:
```

- [ ] Validate the attestation as data, not prose: schema, captured time, source commit, source artifact hash, exit code, Python/Docker/Compose/kernel versions, three service tags and digests, sanitized command, network boundary, and hashed host identity.
- [ ] Derive `workload_events` from the sole performance row and require it to equal two.
- [ ] For every repetition, require exactly `workload_events` distinct expected IDs and exactly that many state-verification rows.
- [ ] Recompute each repetition's state from backend readback fields. Never trust top-level `state`, `state_verified`, `all_scenarios_state_verified`, or count fields without recomputing and comparing them.
- [ ] Require `complete` for `normal`, `delay`, and `retry_success`; require `absent` for `timeout` and `connection_drop`.
- [ ] Reject any `partial`, `unknown`, read failure, Qdrant/Neo4j disagreement, duplicate ID, missing ID, count mismatch, or aggregate-flag mismatch.
- [ ] Emit compact per-scenario counts plus `state_totals`, not raw per-event payloads.
- [ ] Set:

```python
"schema_version": 2,
"evidence_id": "toxiproxy_state_verified_30",
"status": "complete_state_verified_fault_observations",
"all_scenarios_state_verified": True,
"all_observed_states_consistent": True,
"production_latency_claim": False,
```

### Step 5: Require runtime attestation in the CLI

- [ ] Add `--runtime-attestation` as a required `Path` argument to the `toxiproxy` subcommand.
- [ ] Load it with UTF-8 JSON and pass it to the aggregate.
- [ ] Keep the command name `toxiproxy` so existing automation changes only by one required argument.

### Step 6: Verify GREEN and commit

- [ ] Run:

```bash
python3 -m unittest tests.test_txnmem_evidence_aggregates
python3 -m unittest tests.test_real_backend_script
```

- [ ] Confirm every aggregate and backend-bootstrap test passes.
- [ ] Commit:

```bash
git add src/txnmem_evidence_aggregates.py scripts/aggregate_submission_evidence.py tests/test_txnmem_evidence_aggregates.py
git commit -m "feat: validate persistent state in backend evidence"
```

---

## Task 2: Bind the formal run to an environment attestation and generate the aggregate

**Files:**

- Add: `configs/submission_evidence/toxiproxy_state_verified_30.json`
- Add: `results/real_backend_faults_state_verified_30_v2/results/backend_performance.json`
- Add: `results/submission_evidence/toxiproxy_state_verified_30/environment_attestation.json`
- Add: `results/submission_evidence/toxiproxy_state_verified_30/aggregate.json`
- Modify: `.gitignore`

### Step 1: Verify immutable source inputs

- [ ] Verify the experiment source commit exists:

```bash
git cat-file -e 33a334dc7c4e6d2e0250bb54cd25f0e2f080ed5d^{commit}
```

- [ ] Verify the raw result SHA-256 equals:

```text
2ec4db6df575a61b8c203777e0c552bc061e9af44f6f1c8cfb3ec719a3800220
```

- [ ] Verify `exit_code` contains `0` and that no credential string occurs under the synchronized result directory.

### Step 2: Add the state-verified scenario manifest

- [ ] Copy the five deterministic trigger schedules from the old manifest.
- [ ] Set schema version 2 and add `workload_events: 2`, `state_oracle: post_operation_dual_backend_readback`, and the expected state for every scenario.
- [ ] Use this exact claim boundary:

```text
single-host real Qdrant/Neo4j with deterministic Toxiproxy fault injection and post-operation readback for the tested workload and five scenarios; not general distributed transactions, cross-host fault tolerance, availability, linearizability, or production latency
```

### Step 3: Add a sanitized environment attestation

- [ ] Record the source result hash, source commit, exit code 0, and sanitized command.
- [ ] Record Python, Docker 29.1.3, Compose 2.40.3, kernel, service versions, canonical tags, actual pull sources, and image digests:

```text
Qdrant sha256:7a4788934788a7ed9cbf6b8cc3ca1ee880dcd969cf8c6639dc7d0e446cbd4b47
Neo4j  sha256:9317a2941a9641169aa2ea8470cdda184ff7a9ee1914b5429126d0db4828edd2
Toxiproxy sha256:927c797a2115a193ae3a527e5a36782b938419904ac6706ca0efa029ebea58cb
```

- [ ] State that execution was single-host, clients reached both data services only through Toxiproxy, and data ports were not directly published.
- [ ] Store only a SHA-256 hash of the host identity.

### Step 4: Exclude transient files

- [ ] Add exact ignore entries for:

```text
results/real_backend_faults_state_verified_30_v2/pid
results/real_backend_faults_state_verified_30_v2/run.log
```

- [ ] Confirm the raw JSON remains visible to `git status` and the transient files do not.

### Step 5: Generate the compact aggregate

- [ ] Run:

```bash
PYTHONPATH=src python3 scripts/aggregate_submission_evidence.py toxiproxy \
  --source results/real_backend_faults_state_verified_30_v2/results/backend_performance.json \
  --out results/submission_evidence/toxiproxy_state_verified_30/aggregate.json \
  --expected-repetitions 30 \
  --toxiproxy-version 2.5.0 \
  --source-commit 33a334dc7c4e6d2e0250bb54cd25f0e2f080ed5d \
  --run-command "COMPOSE_PROGRESS=plain TXNMEM_PYTHON=.venv/bin/python TXNMEM_REPETITIONS=30 TXNMEM_EVENTS=2 TXNMEM_OUT_DIR=results/real_backend_faults_state_verified_30_v2 bash scripts/run_real_backend_smoke.sh" \
  --runtime-attestation results/submission_evidence/toxiproxy_state_verified_30/environment_attestation.json
```

- [ ] Run the same command twice and confirm the aggregate SHA-256 is stable.
- [ ] Independently inspect the aggregate with a short read-only Python assertion and confirm 150 total repetitions, 90 complete, 60 absent, 0 partial, and 0 unknown.

### Step 6: Commit evidence inputs and projection

- [ ] Run focused tests again.
- [ ] Commit only the raw JSON, manifest, attestation, aggregate, and ignore rules:

```bash
git add .gitignore configs/submission_evidence/toxiproxy_state_verified_30.json results/real_backend_faults_state_verified_30_v2/results/backend_performance.json results/submission_evidence/toxiproxy_state_verified_30
git commit -m "data: add state-verified backend evidence"
```

---

## Task 3: Supersede the proxy-only claim and strengthen semantic auditing

**Files:**

- Add: `results/submission_evidence/toxiproxy_faults_30/SUPERSEDED.md`
- Modify: `results/paper_evidence/supersession_index.json`
- Modify: `tests/test_txnmem_claim_audit.py`
- Modify: `src/txnmem_claim_audit.py`
- Modify: `configs/paper_claims.json`
- Modify: `configs/txnmem_ccfa_paper.json`
- Regenerate: `results/paper_evidence/claim_audit.json`

### Step 1: Write claim-audit regression tests and observe RED

- [ ] Update the current-ledger test to require `validation_profile == "toxiproxy_state_verified"`.
- [ ] Require assertions for schema 2, evidence ID, status, all evidence/state flags, total complete/absent/partial/unknown counts, expected per-scenario states, and three image digests.
- [ ] Add a semantic-profile fixture in which an artifact passes superficial JSON assertions but has one non-zero partial or unknown count; require a `toxiproxy_state_evidence_incomplete` finding.
- [ ] Add a fixture with missing `all_scenarios_state_verified` or `all_observed_states_consistent`; require the same finding.
- [ ] Keep the legacy `toxiproxy_fault_path` negative test so archived ledgers can still be audited.
- [ ] Run:

```bash
python3 -m unittest tests.test_txnmem_claim_audit
```

- [ ] Confirm the new profile is initially rejected or unenforced.

### Step 2: Implement the semantic profile

- [ ] Add `toxiproxy_state_verified` to `_KNOWN_VALIDATION_PROFILES`.
- [ ] Require exactly five scenario keys and 30 repetitions each.
- [ ] Require global evidence/state flags true and `production_latency_claim` false.
- [ ] Require total counts complete=90, absent=60, partial=0, unknown=0.
- [ ] Require complete=30 for normal/delay/retry_success and absent=30 for timeout/connection_drop, with partial=unknown=0 for every scenario.
- [ ] Require per-repetition proxy evidence counts for each non-normal scenario and retry/abort response semantics.

### Step 3: Mark old evidence as superseded

- [ ] Add a human-readable marker to the old evidence directory stating that it is retained for history but must not support active paper claims.
- [ ] Add this supersession entry dated `2026-08-16`:

```json
{
  "artifact_path": "results/submission_evidence/toxiproxy_faults_30/aggregate.json",
  "replacement_path": "results/submission_evidence/toxiproxy_state_verified_30/aggregate.json",
  "reason": "the replacement rerun verifies Qdrant and Neo4j persistent state after every repetition",
  "superseded_on": "2026-08-16"
}
```

### Step 4: Rebind the active claim

- [ ] Keep claim ID `toxiproxy_fault_matrix_5x30` so manuscript evidence markers remain stable.
- [ ] Point its artifact and manifest to the new files and update both SHA-256 values.
- [ ] Set `validation_profile` to `toxiproxy_state_verified`, `source_commit` to `33a334dc7c4e6d2e0250bb54cd25f0e2f080ed5d`, and use the strict claim boundary from Task 2.
- [ ] Replace the old 21 assertions with assertions that cover identity/status, cardinality, state flags and totals, scenario outcomes, proxy path counts, retry/abort semantics, production-latency denial, and image digests.
- [ ] Recompute `expected_assertion_count` from the declared active assertions and store the exact integer in the ledger.
- [ ] Replace the old required boundary in `configs/txnmem_ccfa_paper.json` with the strict state-verified boundary.

### Step 5: Regenerate and verify the audit

- [ ] Run the repository's claim-audit command discovered from its CLI help or build script.
- [ ] Confirm:

```text
status=passed
finding_count=0
active_claim_count=15
checked_assertion_count=expected_assertion_count
```

- [ ] Run:

```bash
python3 -m unittest tests.test_txnmem_claim_audit tests.test_txnmem_ccfa_docx
```

- [ ] Commit:

```bash
git add src/txnmem_claim_audit.py tests/test_txnmem_claim_audit.py configs/paper_claims.json configs/txnmem_ccfa_paper.json results/paper_evidence/claim_audit.json results/paper_evidence/supersession_index.json results/submission_evidence/toxiproxy_faults_30/SUPERSEDED.md
git commit -m "feat: promote state-verified backend claim"
```

---

## Task 4: Update the paper and experiment-status documents

**Files:**

- Modify: `docs/paper/txnmem_ccfa_draft_zh.md`
- Modify: `docs/current_experiment_report_zh.md`
- Modify: `docs/formal_paper_task_status_zh.md`
- Modify: `docs/paper/evidence_map_zh.md`
- Modify: manuscript-audit tests discovered under `tests/`
- Regenerate: manuscript/evidence audit JSON files used by the DOCX build

### Step 1: Add manuscript regression expectations and observe RED

- [ ] Find manuscript tests and add expectations for the new claim boundary and the 150/150 state-verified result.
- [ ] Add an expectation that the old sentence “post-fault Qdrant/Neo4j persistent state was not independently verified” no longer appears in active manuscript/report text.
- [ ] Keep checks that forbid general atomicity, cross-host fault-tolerance, availability, linearizability, or production-latency claims.
- [ ] Run the focused manuscript tests and confirm they fail on the old prose.

### Step 2: Revise the Chinese paper conservatively

- [ ] In the system/limitation discussion, keep the distinction between TxnMem's tested compensation behavior and a general cross-service transaction protocol.
- [ ] In the experiment table and RQ4, report:

```text
5 scenarios × 30 repetitions = 150 observations
complete readbacks: 90/90
absent readbacks: 60/60
partial: 0/150
unknown: 0/150
retry_success: 30/30
```

- [ ] Explain that each repetition read back two memory IDs from both Qdrant and Neo4j after the operation, and that the aggregate recomputed rather than trusted runner summary flags.
- [ ] Label latency numbers as backend-only diagnostic measurements for two events: p50 25.748 ms, p95 32.029 ms, p99 42.234 ms, throughput 76.256 operations/s; explicitly state `production_latency_claim=false`.
- [ ] Replace “state-verified rerun blocked” language in all status/report files with the completed result and its narrow boundary.
- [ ] Point every evidence reference to `results/submission_evidence/toxiproxy_state_verified_30/aggregate.json`.

### Step 3: Regenerate audits and verify no stale evidence remains

- [ ] Run the manuscript and evidence-map generators/audits.
- [ ] Use `rg` to ensure active paper/config/report files contain neither the superseded artifact path nor the old no-readback boundary.
- [ ] Allow those strings only in `SUPERSEDED.md`, the supersession index, historical artifacts, tests specifically covering history, and design/implementation records.
- [ ] Run focused manuscript, claim-audit, and DOCX tests.
- [ ] Commit:

```bash
git add docs/paper/txnmem_ccfa_draft_zh.md docs/current_experiment_report_zh.md docs/formal_paper_task_status_zh.md docs/paper/evidence_map_zh.md tests results/paper_evidence
git commit -m "docs: report state-verified backend results"
```

---

## Task 5: Rebuild and visually verify the Chinese DOCX

**Files:**

- Modify if required: `scripts/build_txnmem_ccfa_docx.py`
- Modify if required: `tests/test_txnmem_ccfa_docx.py`
- Modify: `docs/paper/txnmem_ccfa_docx_qa_zh.md`
- Generate outside the Git worktree: `../../../../TxnMem_CCF-A中文论文初稿_state_verified.docx`

### Step 1: Run deterministic structure tests

- [ ] Load the bundled document runtime paths.
- [ ] Run:

```bash
python3 -m unittest tests.test_txnmem_ccfa_docx
```

- [ ] Fix only deterministic layout or content issues exposed by the updated text; do not relax privacy, structure, figure, table, or accessibility assertions.

### Step 2: Build a release candidate outside the worktree

- [ ] Build twice into separate temporary directories and compare SHA-256 values.
- [ ] Run the existing DOCX privacy, accessibility, relationship, and structure audits.
- [ ] Confirm the expected anonymous metadata, 6 figures, 8 tables, complete reference list, and no tracked-artifact mutation.

### Step 3: Render and inspect every page

- [ ] Render the release candidate with `scripts/render_docx_with_bundled_libs.sh` into `/private/tmp/txnmem-ccfa-state-verified-render`.
- [ ] Open every page PNG with the image inspection tool.
- [ ] Check for clipped or overlapping text, orphan headings, split tables, unreadable figures, broken Chinese fonts, blank pages, stale old-boundary text, malformed references, and incorrect page numbering.
- [ ] If any defect appears, change the source/build script, rebuild, rerender, and inspect every affected page plus adjacent pages.

### Step 4: Record QA and publish the local deliverable

- [ ] Update `docs/paper/txnmem_ccfa_docx_qa_zh.md` with the new page count, SHA-256, figure/table/reference counts, audit commands, and page-by-page inspection result.
- [ ] Copy the visually approved file to:

```text
../../../../TxnMem_CCF-A中文论文初稿_state_verified.docx
```

- [ ] Commit source and QA documentation only; keep the release binary outside the isolated worktree unless repository policy explicitly tracks it.

---

## Task 6: Final archive-grade verification and handoff

**Files:**

- Verify all changed files
- Add if generated by existing tooling: final audit summaries under `results/paper_evidence/`

### Step 1: Run full verification from a clean process

- [ ] Run:

```bash
python3 -m unittest discover -s tests
```

- [ ] Require all tests to pass; compare the final count with the pre-change baseline of 348 tests and explain any delta as newly added regression coverage.
- [ ] Rerun claim audit, manuscript audit, DOCX structure/privacy/accessibility audit, and evidence-map validation.

### Step 2: Verify hashes and claim reachability

- [ ] Recompute hashes for raw result, environment attestation, manifest, aggregate, claim ledger, claim audit, manuscript source, and final DOCX.
- [ ] Verify every active claim artifact exists and matches its ledger hash.
- [ ] Verify no active claim or paper config points to any artifact listed in the supersession index.
- [ ] Verify no password, token, routable IP address, SSH command, user name, or unhashed host identity occurs in newly tracked artifacts; permit `127.0.0.1` only inside the immutable raw result's local proxy-path fields.

### Step 3: Verify repository state

- [ ] Inspect `git diff --check`, `git status --short`, and the commits added by this plan.
- [ ] Ensure only intentional source/evidence/document changes are tracked and transient remote logs remain untracked/ignored.
- [ ] Make one final commit only if verification regenerated tracked audit metadata:

```bash
git add results/paper_evidence docs/paper/txnmem_ccfa_docx_qa_zh.md
git commit -m "chore: finalize submission evidence audit"
```

### Step 4: Deliver an evidence-backed completion report

- [ ] Report the new aggregate path and hash, source raw hash, source commit, final test count, audit statuses, DOCX path/hash/page count, and exact claim boundary.
- [ ] Separate completed local Git commits from remote push status. Do not claim a push unless a configured remote is present and the push command succeeds.
- [ ] State any remaining non-experimental submission work separately from experimental evidence closure.
