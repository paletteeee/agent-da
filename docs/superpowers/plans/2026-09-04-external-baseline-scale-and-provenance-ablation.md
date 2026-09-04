# External Baseline Scale and Provenance Ablation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce auditable 400-instance external-baseline results and a three-cell real-backend provenance ablation with valid uncertainty estimates and paper-ready evidence.

**Architecture:** Extend the existing external-baseline runner in the dedicated `agent-da-external-baselines` worktree, then import only validated aggregate artifacts into the TxnMem evidence branch. Extend the existing fail-closed provenance performance stack with a separately registered `MemoryOnly-NoProvenance` variant and a new three-cell candidate/validate/promote pipeline; preserve the completed v10 bundle unchanged.

**Tech Stack:** Python 3.11/3.12, `unittest`, CSV/JSONL, Mem0 2.0.18, LangGraph Store, Qdrant, Neo4j, whole-repetition bootstrap, Wilson score intervals, existing formal controller and topology attestation.

**Spec:** `docs/superpowers/specs/2026-09-04-external-baseline-scale-and-provenance-ablation-design.md`

## Global Constraints

- Preserve all pre-existing dirty files in both worktrees; commit only files owned by the current task.
- Use the 400-instance `results/final_controlled/data/generated_instances.jsonl` and bind its SHA-256.
- Never add TxnMem transaction, policy-revalidation, or repair semantics to external adapters.
- `capability_absent` remains orthogonal to the mutually exclusive `run_status` field.
- Never overwrite the completed `results/provenance_performance_v10_measurements` bundle.
- Formal real-service results use candidate → validate → promote and fail closed on any eligibility mismatch.
- No production-latency, linear-scalability, cross-host-storage-fault-tolerance, or general distributed-transaction claim.

---

### Task 1: Harden the 400-instance external manifest contract

**Files:**
- Modify: `/Users/xiaoyan_zhu/Desktop/agent-db/.worktrees/agent-da-external-baselines/src/txnmem_external_experiment.py`
- Modify: `/Users/xiaoyan_zhu/Desktop/agent-db/.worktrees/agent-da-external-baselines/tests/test_txnmem_external_experiment.py`
- Create: `/Users/xiaoyan_zhu/Desktop/agent-db/.worktrees/agent-da-external-baselines/configs/external_baselines_scale_400.json`

**Interfaces:**
- Consumes: validated TxnMemBench JSONL instances.
- Produces: `validate_formal_instance_domain(instances) -> dict[str, Any]` with exact family/seed/count facts.

- [ ] **Step 1: Write failing tests for exact 8×50 coverage**

Add tests that accept exactly 400 unique `(workload, seed)` coordinates and reject a missing seed, duplicate ID, extra family, or 51st seed before output creation.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `python3 -m unittest tests.test_txnmem_external_experiment -v`

Expected: new formal-domain tests fail because the validator/config do not exist.

- [ ] **Step 3: Implement the minimal validator and config**

The config must declare `workload_count=8`, `seeds=0..49`, `instance_count=400`, registry order, and the canonical input path. Return sorted families, seeds, coordinate count, instance count, and SHA-256 material; reject booleans as seeds.

- [ ] **Step 4: Run focused and baseline contract tests**

Run: `python3 -m unittest tests.test_txnmem_external_experiment tests.test_baseline_contract -v`

Expected: PASS.

- [ ] **Step 5: Commit Task 1**

```bash
git add src/txnmem_external_experiment.py tests/test_txnmem_external_experiment.py configs/external_baselines_scale_400.json
git commit -m "feat: bind external baselines to formal 400 instances"
```

### Task 2: Add status accounting and Wilson intervals

**Files:**
- Modify: `/Users/xiaoyan_zhu/Desktop/agent-db/.worktrees/agent-da-external-baselines/src/txnmem_external_experiment.py`
- Modify: `/Users/xiaoyan_zhu/Desktop/agent-db/.worktrees/agent-da-external-baselines/tests/test_txnmem_external_experiment.py`
- Modify: `/Users/xiaoyan_zhu/Desktop/agent-db/.worktrees/agent-da-external-baselines/tests/test_external_baseline_protocol.py`

**Interfaces:**
- Produces: `wilson_interval(successes: int, total: int, confidence: float = 0.95) -> dict[str, Any]`.
- Produces: summary counts with `unsupported_mapping`, `runtime_error`, and per-variant/per-workload Wilson intervals.

- [ ] **Step 1: Write failing tests for interval values and count identities**

Cover 0/0 unavailable, 0/n, n/n, a known 60/80 interval, non-integer rejection, boolean rejection, and exact identities from the approved design.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_txnmem_external_experiment tests.test_external_baseline_protocol -v`

Expected: failures for missing interval and expanded counters.

- [ ] **Step 3: Implement status and interval aggregation**

Use the closed `run_status` values `success`, `unsupported_mapping`, and `runtime_error`. Derive `excluded`, preserve `capability_absent_observed`, and compute violation/oracle intervals only over successful rows.

- [ ] **Step 4: Verify GREEN and artifact round-trip**

Run: `python3 -m unittest tests.test_txnmem_external_experiment tests.test_external_baseline_protocol tests.test_cli_outputs -v`

Expected: PASS with no output-schema regression.

- [ ] **Step 5: Commit Task 2**

```bash
git add src/txnmem_external_experiment.py tests/test_txnmem_external_experiment.py tests/test_external_baseline_protocol.py
git commit -m "feat: report external baseline uncertainty and exclusions"
```

### Task 3: Run external adapter smoke and formal 400-instance batch

**Files:**
- Create: `/Users/xiaoyan_zhu/Desktop/agent-db/.worktrees/agent-da-external-baselines/results/external_baselines_scale_400/*`
- Modify only if required by a test-first defect fix: adapter source/tests in the external-baselines worktree.

**Interfaces:**
- Produces: manifest, environment, capabilities JSON/CSV, results CSV, errors JSONL, and summary JSON.

- [ ] **Step 1: Verify optional dependency versions and clean persistent namespaces**

Run the existing dependency tests and record Mem0/LangGraph versions. Use a fresh run ID and a non-existing result directory.

- [ ] **Step 2: Run a five-adapter one-seed smoke**

Run the external runner against one instance per workload. Require correct status classification, no secret/DSN leakage, and deterministic artifact hashes for identical controlled inputs.

- [ ] **Step 3: Audit smoke before formal execution**

Check count identities, capability matrix, Mem0 isolation, LangGraph backend mode, and oracle fields. If a defect appears, write a failing regression test before changing code.

- [ ] **Step 4: Run the formal 400-instance batch**

Execute all five adapters against the canonical 400-instance JSONL using a fresh output root and durable log. Do not substitute an unavailable external dependency with a fake adapter.

- [ ] **Step 5: Recompute and validate the formal bundle**

Require 2,000 attempted rows, five adapters × 400, unique `(adapter, instance_id)`, valid identities, valid Wilson intervals, hashes for every artifact, and zero unclassified status.

- [ ] **Step 6: Commit only validated aggregate artifacts**

Do not commit secrets, DSNs, backend databases, caches, or raw private payloads.

### Task 4: Import external-scale evidence into the TxnMem evidence branch

**Files:**
- Create: `results/paper_evidence/external_baselines_scale_400.json`
- Modify: `src/txnmem_paper_projection.py`
- Modify: `tests/test_txnmem_paper_projection.py`
- Modify: `configs/paper_claims.json`
- Modify: `tests/test_txnmem_claim_audit.py`

**Interfaces:**
- Consumes: validated external summary/manifest hashes.
- Produces: paper-safe projection containing counts, rates, intervals, exclusions, versions, and claim boundary.

- [ ] **Step 1: Write failing projection and claim-audit tests**

Require source hashes, exact 2,000 attempts, per-adapter denominators, Wilson metadata, and four separated reporting concepts.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_txnmem_paper_projection tests.test_txnmem_claim_audit -v`

- [ ] **Step 3: Implement the projection and ledger entry**

Reject stale 80-instance artifacts and any aggregate inconsistent with the raw result hash.

- [ ] **Step 4: Regenerate and verify evidence**

Run the paper projection CLI, then the focused audit tests. Expected: PASS.

- [ ] **Step 5: Commit Task 4 without absorbing existing unrelated paper edits**

Stage exact files or isolated hunks only.

### Task 5: Introduce the MemoryOnly-NoProvenance backend mode

**Files:**
- Modify: `src/txnmem_provenance_performance.py`
- Modify: `src/txnmem_vector_graph_backend.py` only if the mode cannot be expressed by the performance-layer factory.
- Modify: `tests/test_txnmem_provenance_performance.py`
- Modify: `tests/test_txnmem_vector_graph_backend.py` only when production backend behavior changes.
- Create: `configs/provenance_ablation_v10.json`

**Interfaces:**
- Produces: closed variant domain `TxnMem` and `MemoryOnly-NoProvenance`.
- Produces: factory behavior that persists memory objects but emits no provenance edge/index/repair side effects in the control variant.

- [ ] **Step 1: Write failing behavior tests**

Prove both variants perform memory CRUD; prove the control persists zero provenance edges, performs no traversal/repair, and cannot silently instantiate the full backend path.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_txnmem_provenance_performance tests.test_txnmem_vector_graph_backend -v`

- [ ] **Step 3: Implement the minimal closed mode**

Prefer a performance-layer strategy/factory wrapper. Do not add a global feature flag or weaken v10 formal semantics.

- [ ] **Step 4: Add and validate the three-cell config**

Declare cells `(100,1)`, `(1000,4)`, `(10000,16)`, two variants, 30 repetitions, fixed seeds, bootstrap 10,000, timeouts, and a distinct schema/run identity.

- [ ] **Step 5: Verify GREEN and v10 non-regression**

Run provenance performance, vector-graph backend, CLI, and formal-smoke focused suites. Expected: PASS and unchanged v10 projection/hash tests.

- [ ] **Step 6: Commit Task 5**

Stage only ablation implementation, tests, and config.

### Task 6: Add paired repetition aggregation

**Files:**
- Modify: `src/txnmem_provenance_performance.py`
- Modify: `tests/test_txnmem_provenance_performance.py`
- Modify: `tests/test_cli_outputs.py`

**Interfaces:**
- Produces: `aggregate_ablation(...)` with per-variant/cell distributions, common-operation comparison, traversal absolute metrics, pair eligibility, and bootstrap intervals.

- [ ] **Step 1: Write failing paired-statistics tests**

Cover matched seeds, missing variant pairs, ineligible repetitions, zero denominator, no fake control traversal samples, deterministic whole-repetition bootstrap, and percent-change signs.

- [ ] **Step 2: Verify RED**

Run the exact new test selectors and confirm missing aggregation behavior.

- [ ] **Step 3: Implement minimal aggregation**

Pair only equal cell/repetition coordinates. Bootstrap repetition-level statistics, not operation rows. Separate common operations from full-only traversal and total mechanism-package wall clock.

- [ ] **Step 4: Verify GREEN**

Run: `python3 -m unittest tests.test_txnmem_provenance_performance tests.test_cli_outputs -v`

- [ ] **Step 5: Commit Task 6**

Stage only aggregation and tests.

### Task 7: Add candidate validation, promotion, and formal smoke

**Files:**
- Modify: `src/txnmem_formal_smoke.py`
- Modify: `src/txnmem_formal_controller.py`
- Modify: `src/txnmem_provenance_execution_collector.py`
- Modify: `tests/test_txnmem_formal_smoke.py`
- Modify: `tests/test_txnmem_formal_controller.py`
- Modify: `tests/test_txnmem_provenance_execution_collector.py`
- Create: `scripts/run_formal_provenance_ablation.sh`

**Interfaces:**
- Produces: ablation-specific smoke, candidate receipt, environment/topology attestation, validator, and one-time promotion.

- [ ] **Step 1: Write failing fail-closed tests**

Reject wrong source commit/config hash, existing output, missing variant, wrong cell count, namespace residue, timeout cleanup failure, sample mismatch, and any attempt to reuse the v10 run identity.

- [ ] **Step 2: Verify RED**

Run focused formal smoke/controller/collector selectors.

- [ ] **Step 3: Implement the ablation lifecycle**

Reuse existing protected lifecycle primitives; add only a distinct schema/config/identity path. Never relax v10 checks.

- [ ] **Step 4: Run local diagnostic smoke**

Use a tiny graph and one repetition per variant. Local protected-host skips remain skips and are not formal evidence.

- [ ] **Step 5: Run the full local suite**

Run: `python3 -m unittest discover -s tests -v`

Expected: zero failures/errors; platform/environment skips explicitly counted.

- [ ] **Step 6: Commit Task 7**

Stage only lifecycle implementation, tests, script, and config-related changes.

### Task 8: Execute the real-backend three-cell ablation remotely

**Files:**
- Create remotely under persistent `/data`: formal candidate, logs, receipts, and service evidence.
- Promote locally/remote only to: `results/provenance_ablation_v10/`.

**Interfaces:**
- Produces: 180 attempted repetitions plus samples, errors, environment, manifest, and aggregate.

- [ ] **Step 1: Establish/reuse SSH and verify remote identity**

Run `hostname`, `pwd`, `whoami`, inspect remote repository status, and locate Qdrant/Neo4j services. Preserve remote changes.

- [ ] **Step 2: Sync or fetch the exact reviewed commit**

Install the formal runtime from an immutable source commit and record executable/config hashes.

- [ ] **Step 3: Run protected real-service smoke**

Require zero-skip host gates, service health, empty namespace, exact control behavior, timeout isolation, and zero residue.

- [ ] **Step 4: Create a fresh formal run identity**

Bind source commit, config hash, environment, topology, service versions, run nonce, and output target.

- [ ] **Step 5: Launch the single formal matrix with durable logging**

Run under the existing durable remote mechanism. Record command, working directory, PID/session, progress path, and log path without exposing credentials.

- [ ] **Step 6: Monitor to terminal completion**

Use progress snapshots; do not restart or merge partial candidates silently.

- [ ] **Step 7: Validate and promote**

Require 3 cells × 2 variants × 30 = 180 attempted repetitions, valid pair accounting, no cross-cell namespace visibility, exact source/environment bindings, and reproducible aggregate hashes.

- [ ] **Step 8: Transfer only approved artifacts back to the workspace**

Keep private raw data and credentials remote; transfer paper-safe artifacts and their source hashes.

### Task 9: Connect provenance ablation evidence to the paper

**Files:**
- Create: `results/paper_evidence/provenance_ablation_v10.json`
- Modify: `src/txnmem_paper_projection.py`
- Modify: `tests/test_txnmem_paper_projection.py`
- Modify: `configs/paper_claims.json`
- Modify: `tests/test_txnmem_claim_audit.py`
- Modify: `scripts/build_txnmem_paper_figures.py`
- Modify: `tests/test_txnmem_paper_figures.py`
- Modify existing manuscript/docx files only after preserving and integrating the user's current dirty edits.

**Interfaces:**
- Produces: paper-safe table/figure data and narrow claim boundary.

- [ ] **Step 1: Write failing projection, claim, and figure tests**

Require three cells, both variants, 30 repetitions per side, bootstrap metadata, common-operation comparison, absolute traversal metrics, and no production claim.

- [ ] **Step 2: Verify RED**

Run focused paper projection/claim/figure tests.

- [ ] **Step 3: Implement projection and figure/table generation**

Keep completed v10 scaling evidence separate from the new ablation evidence.

- [ ] **Step 4: Integrate manuscript wording**

Report the comparison as mechanism-package cost on three representative conditions. Do not treat absent control traversal as zero latency.

- [ ] **Step 5: Run all evidence audits and document tests**

Run claim, artifact, manuscript, projection, figure, and DOCX tests; regenerate final evidence and render the DOCX for visual inspection.

- [ ] **Step 6: Commit Task 9 in reviewed, exact-scope commits**

Do not absorb unrelated existing modifications.

### Task 10: Final verification and handoff

**Files:**
- Verify all changed source, tests, configs, results, claims, figures, and manuscript artifacts.

**Interfaces:**
- Produces: final evidence inventory and exact claim boundaries.

- [ ] **Step 1: Run both repositories' complete test suites**

External worktree: `python3 -m unittest discover -s tests -v`.

TxnMem evidence worktree: `python3 -m unittest discover -s tests -v`.

- [ ] **Step 2: Run artifact/claim/manuscript audits**

Require zero findings and current-byte hashes.

- [ ] **Step 3: Verify repository scopes**

Run `git diff --check`, inspect status, ensure no caches/secrets/raw private material are staged, and confirm user-owned dirty files were preserved or deliberately integrated.

- [ ] **Step 4: Verify final counts**

External: five adapters × 400 = 2,000 attempts.

Ablation: three cells × two variants × 30 = 180 attempted repetitions, with eligible/pair counts reported rather than assumed.

- [ ] **Step 5: Deliver the result inventory**

Report local and remote paths, commits, commands/tests, formal run status, exclusions/errors, and precise manuscript-safe conclusions.
