# TxnMem Evidence Scale-Up Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace mechanically repeated evidence with a reproducible 1,600-instance controlled suite, correctly split public benchmark batches, group-aware realism statistics, and a real provenance-backend performance matrix.

**Architecture:** Four independently testable tracks share one fail-closed evidence layer. Controlled correctness is the release gate; public benchmark runners use frozen hashed manifests and resumable shards; realism uses group-aware resampling; backend performance uses deterministic DAGs, unique namespaces, per-operation samples, and environment attestations. Raw public traces remain remote-only while sanitized aggregates and configs are versioned.

**Tech Stack:** Python 3.12 standard library, `unittest`, existing TxnMem simulator/reference semantics, official τ-bench 0.1.0, AppWorld 0.2.0, LoCoMo and LongMemEval datasets, Qwen2.5-7B OpenAI-compatible endpoint, SQLite, Qdrant 1.11.5, Neo4j 5.22, Toxiproxy 2.5, Docker Compose, Git.

**Spec:** `docs/superpowers/specs/2026-08-18-evidence-scale-up-design.md`

## Global Constraints

- Preserve every historical artifact; write all new outputs to versioned directories.
- Never save passwords, routable endpoints, raw prompts, tool arguments, benchmark payloads, or user values in Git.
- Treat benchmark task/conversation IDs, not questions/events/repetitions, as independent sampling units.
- Do not update an active paper claim until the current runner reproduces it and claim/artifact audits pass.
- Use TDD for every code change and run the exact failing test before production code.
- AppWorld means official `test_normal` 168; τ-bench means frozen legacy retail/test 115; LoCoMo means 10 full session streams; LongMemEval means cleaned S 500 only.
- Backend matrix is 3 graph sizes × 5 concurrency levels × 30 repetitions; model, backend-only, and cross-host results remain separate claims.

---

### Task 1: Restore the supersession reference/workload contract

**Files:**
- Modify: `tests/test_txnmem_differential.py`
- Modify: `tests/test_txnmem_workloads.py`
- Modify: `src/txnmem_workloads.py`

**Interfaces:**
- Consumes: `generate_instance(workload, seed, config)` and `reference_outcome(instance)`.
- Produces: a `supersession_consistency` instance with an explicit allow policy for action `supersede` and a whole-suite differential regression.

- [ ] **Step 1: Write the failing regression tests**

```python
def test_every_generated_workload_is_accepted_by_full_txnmem_and_reference():
    for instance in generate_suite(WORKLOADS, range(3)):
        result = run_instance(instance, "TxnMem")
        row = result_row(instance, result)
        assert row["any_violation"] == 0
        assert row["oracle_match"] == 1

def test_supersession_workload_declares_supersede_policy():
    instance = generate_instance("supersession_consistency", 0)
    assert any(p["action"] == "supersede" and p["effect"] == "allow" for p in instance["policies"])
```

- [ ] **Step 2: Verify RED**

Run: `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -v tests.test_txnmem_differential tests.test_txnmem_workloads`

Expected: the whole-suite test fails for `supersession_consistency` because the oracle denies the undeclared action.

- [ ] **Step 3: Add the minimal explicit policy in the workload generator**

Append one policy with stable ID `p_supersede`, version 1, the selected agent, action `supersede`, tenant scope, allow effect, and effective step 0 only for workloads that use supersession.

- [ ] **Step 4: Verify GREEN and the historical 50-seed boundary**

Run the two test modules, then run `experiment --seeds 50` into `/tmp/txnmem-controlled-regression`; assert TxnMem has 0 violations and 400 oracle matches.

- [ ] **Step 5: Commit**

Commit message: `fix: restore supersession workload authorization`

### Task 2: Make workload parameter ranges drive non-trivial deterministic instances

**Files:**
- Modify: `src/txnmem_schema.py`
- Modify: `src/txnmem_workloads.py`
- Modify: `src/txnmem_experiment.py`
- Modify: `tests/test_txnmem_schema.py`
- Modify: `tests/test_txnmem_workloads.py`
- Modify: `tests/test_cli_outputs.py`
- Create: `configs/controlled_scale_200.json`

**Interfaces:**
- Produces: `sample_semantic_config(workload: str, seed: int, ranges: Mapping[str, Sequence[int]]) -> dict[str, int]` and stable `semantic_fingerprint(instance) -> str`.
- `generate_suite(..., parameter_ranges=...)` emits `semantic_parameters` and `semantic_fingerprint` in every instance.

- [ ] **Step 1: Write failing tests for deterministic range sampling and diversity**

```python
def test_parameter_ranges_are_consumed_deterministically():
    ranges = {"txn_size": [1, 4], "provenance_depth": [1, 4], "branch_factor": [1, 3], "policy_churn": [0, 2], "concurrency": [1, 3]}
    first = generate_suite(WORKLOADS, range(200), parameter_ranges=ranges)
    second = generate_suite(WORKLOADS, range(200), parameter_ranges=ranges)
    assert first == second
    assert len({row["semantic_fingerprint"] for row in first}) > len(WORKLOADS)
```

Also assert each sampled value is inside its inclusive range and changing `--config` changes the generated operation/config distribution.

- [ ] **Step 2: Verify RED**

Run the three target test modules; expect missing keyword/interface failures.

- [ ] **Step 3: Implement stable SHA-256-derived sampling**

Use `sha256(f"{workload}\0{seed}\0{name}")` as the random source; avoid Python's process-randomized `hash()`. Normalize IDs, seed and agent labels before hashing the semantic shape.

- [ ] **Step 4: Connect `load_workload_config(args.config)["parameter_ranges"]` to generation**

Reject non-positive seed counts and malformed `[low, high]` ranges before writing artifacts.

- [ ] **Step 5: Verify GREEN and exact cardinality**

Assert 8×200 unique instance IDs, 200 seeds per family, all parameter endpoints represented, and non-trivial fingerprints per parameterized family.

- [ ] **Step 6: Commit**

Commit message: `feat: parameterize controlled workloads deterministically`

### Task 3: Add controlled saturation, diversity, manifest and strict evidence audit

**Files:**
- Modify: `src/txnmem_statistics.py`
- Modify: `src/txnmem_metrics.py`
- Modify: `src/txnmem_experiment.py`
- Modify: `src/txnmem_claim_audit.py`
- Modify: `src/txnmem_artifact_audit.py`
- Modify: `tests/test_txnmem_statistics.py`
- Modify: `tests/test_txnmem_claim_audit.py`
- Modify: `tests/test_txnmem_artifact_audit.py`
- Modify: `tests/test_cli_outputs.py`

**Interfaces:**
- Produces: `controlled_violation_saturation(rows, checkpoints, confidence=0.95)` and `controlled_diversity(instances)`.
- Experiment writes `run_manifest.json`, `saturation.json`, `diversity.json`, and `saturation.svg`.

- [ ] **Step 1: Write failing tests for balanced nested prefixes**

Create rows for two families, two variants and seeds 0–3. Assert checkpoints select equal seed prefixes per family, Wilson intervals are deterministic, and duplicate/missing family×seed×variant rows raise `ValueError`.

- [ ] **Step 2: Verify RED**

Run statistics/claim/CLI tests; expect missing interfaces/artifacts.

- [ ] **Step 3: Implement the fail-closed aggregators and SVG writer**

The saturation report includes checkpoint seed count, instance count, variant, violations, rate, interval, oracle matches and interval. Diversity includes family, instance count, unique fingerprints, parameter value counts and coverage ratios.

- [ ] **Step 4: Add precise public allowlist for only controlled synthetic/oracle files**

Keep sensitive-key scanning active and reject lookalike paths, nested raw files and any public benchmark raw trace.

- [ ] **Step 5: Verify GREEN and deterministic two-run hashes**

Run 200 seeds twice into two `/tmp` paths; compare all canonical JSONL/CSV/JSON bytes except a manifest field explicitly designed to vary. No wall-clock time or absolute path may enter canonical files.

- [ ] **Step 6: Commit**

Commit message: `feat: add controlled evidence saturation and diversity`

### Task 4: Select official public benchmark splits and add resumable shard merging

**Files:**
- Modify: `src/txnmem_benchmark_manifests.py`
- Create: `src/txnmem_batch_merge.py`
- Modify: `scripts/run_native_scale.sh`
- Modify: `tests/test_benchmark_bridge.py`
- Create: `tests/test_txnmem_batch_merge.py`
- Modify: `tests/test_native_scale_manifest.py`

**Interfaces:**
- `generate_appworld_manifest(..., task_split="test_normal")` reads the official split file in source order.
- `shard_manifest(manifest, shard_count)` assigns each task exactly once.
- `merge_native_shards(manifest, shard_reports)` rejects duplicate, missing, extra or condition-mismatched tasks.

- [ ] **Step 1: Write failing fixture tests**

Build a temporary AppWorld data tree with `datasets/test_normal.txt` and out-of-split task directories. Assert only listed IDs are selected and split/hash metadata are preserved.

- [ ] **Step 2: Verify RED**

Run manifest and merge tests; expect the current directory-sorting behavior to include wrong IDs.

- [ ] **Step 3: Implement official split selection, code/data identity and sharding**

For τ-bench pass domain/split end-to-end. For AppWorld hash the split file and version file. Merge uses the frozen parent manifest hash and includes failed evaluator rows in the denominator.

- [ ] **Step 4: Verify GREEN with formal source preflight**

On the authorized server assert τ retail/test=115 and AppWorld test_normal=168 before any model call.

- [ ] **Step 5: Commit**

Commit message: `feat: freeze public benchmark splits and shards`

### Task 5: Stream full LoCoMo sessions and compute conversation-cluster statistics

**Files:**
- Modify: `src/locomo_official_eval.py`
- Modify: `src/locomo_paired_eval.py`
- Create: `src/txnmem_resampling.py`
- Modify: `tests/test_locomo_official_eval.py`
- Create: `tests/test_txnmem_resampling.py`

**Interfaces:**
- `iter_conversation_sessions(sample)` returns all sessions in chronological order.
- `ingest_session_stream(..., max_session_chars)` records exact ingestion coverage.
- `cluster_bootstrap_interval(rows, group_key, value_key, repetitions=10000, seed=17)` resamples whole conversations.

- [ ] **Step 1: Write failing tests for no truncation and cluster preservation**

Use three synthetic sessions whose total exceeds the old character limit; assert all three are passed through ingestion calls and coverage is 1.0. Assert every bootstrap draw contains whole groups, not individual questions.

- [ ] **Step 2: Verify RED**

Run LoCoMo and resampling tests; expect old head/tail truncation or missing APIs.

- [ ] **Step 3: Implement session-stream ingestion and per-conversation summaries**

Bound each model request, not the entire history. Preserve timestamp order, use one namespace per conversation/profile/repetition, and write only counts/hashes to aggregate artifacts.

- [ ] **Step 4: Add exactly five paired seeds and cluster intervals**

Reject baseline/tuned reports unless task IDs, seeds, model identity and condition fingerprints match.

- [ ] **Step 5: Verify GREEN**

Fixture run must report 100% session/character ingestion coverage and deterministic intervals.

- [ ] **Step 6: Commit**

Commit message: `feat: stream full locomo histories`

### Task 6: Add LongMemEval-S cleaned 500 ingestion and deterministic retrieval evaluation

**Files:**
- Create: `src/longmemeval_eval.py`
- Create: `tests/test_longmemeval_eval.py`
- Create: `scripts/setup_longmemeval.sh`
- Modify: `.gitignore`

**Interfaces:**
- `load_longmemeval_s(path) -> list[LongMemEvalItem]` validates exactly 500 unique question IDs for formal mode.
- `run_longmemeval_item(item, backend, model) -> dict` uses an isolated namespace and returns hypothesis plus evidence-session retrieval facts.
- `write_official_hypotheses(rows, path)` writes only `question_id` and `hypothesis` JSONL remotely.

- [ ] **Step 1: Write failing schema, isolation and retrieval tests**

Assert malformed roles/dates fail, two questions cannot see each other's memories, evidence-session recall denominator excludes the 30 abstention questions, and official output has only the two required keys.

- [ ] **Step 2: Verify RED**

Run the new test module; expect import failure.

- [ ] **Step 3: Implement the smallest session-stream runner and sanitized aggregate**

Record `official_qa_status=blocked` unless the official evaluator command succeeds. Never substitute a local judge into the official field.

- [ ] **Step 4: Verify GREEN and 500-item source preflight**

Download the cleaned official S file on the authorized server, verify source hash/size and exact 500 IDs, then run a two-item offline fixture.

- [ ] **Step 5: Commit**

Commit message: `feat: add longmemeval session-stream evaluation`

### Task 7: Add group-aware AppWorld and LoCoMo realism

**Files:**
- Modify: `src/txnmem_trace_pipeline.py`
- Modify: `src/txnmem_realism.py`
- Modify: `src/txnmem_appworld_projection.py`
- Modify: `src/txnmem_experiment.py`
- Create: `configs/realism_scale.json`
- Modify: `tests/test_txnmem_realism.py`
- Modify: `tests/test_txnmem_appworld_projection.py`
- Modify: `tests/test_txnmem_remaining_tasks.py`

**Interfaces:**
- `leave_one_group_out(records, group_key)` from Task 5.
- `cross_fitted_realism(records, group_key, parameter_ranges, seeds, ...)` returns one fold per group and a cluster aggregate.
- AppWorld projection inventory records official split, family ID, task count, event count and zero-event count.

- [ ] **Step 1: Write failing leakage tests**

Assert 10 groups produce 10 folds, each group appears once in holdout, train/holdout groups never overlap, and calibration is invoked independently per fold.

- [ ] **Step 2: Verify RED**

Run the three test modules; expect missing group-aware interface.

- [ ] **Step 3: Implement LOO/cross-fitted generation and family-safe AppWorld selection**

Use the official AppWorld scenario/family ID when available; otherwise use the audited task base ID and record that derivation method. Select 50 evaluation families and disjoint calibration families with seed 17.

- [ ] **Step 4: Verify GREEN**

Fixture reports must carry low-sample warnings without interpreting non-significance as equivalence.

- [ ] **Step 5: Commit**

Commit message: `feat: add group-aware realism evaluation`

### Task 8: Implement provenance graph performance matrix

**Files:**
- Create: `src/txnmem_provenance_performance.py`
- Create: `configs/provenance_performance_matrix.json`
- Create: `tests/test_txnmem_provenance_performance.py`
- Modify: `src/txnmem_experiment.py`
- Create: `scripts/run_provenance_performance.sh`

**Interfaces:**
- `build_layered_dag(node_count, seed) -> GraphSpec` returns exact nodes/edges/hash.
- `expand_matrix(config) -> 15 cells`.
- `run_matrix_cell(backend_factory, graph, concurrency, repetitions, ...)` returns sanitized per-operation samples and repetition summaries.
- `aggregate_matrix(samples, bootstrap_repetitions=10000)` reports p50/p95/p99, successful throughput and CIs.

- [ ] **Step 1: Write failing DAG, matrix and accounting tests**

Assert exact 15 cells, 30 repetitions per cell, unique namespaces, deterministic graph hashes, success-only throughput, ordered percentiles and deterministic bootstrap intervals.

- [ ] **Step 2: Verify RED**

Run the new module test; expect import failure.

- [ ] **Step 3: Implement the deterministic DAG and in-memory runner**

Collect operation latency in nanoseconds; record failures/retries separately; do not include payload values in samples.

- [ ] **Step 4: Add real vector/graph backend factory, preload/readback and environment attestation**

Fail closed on graph count/hash mismatch, partial/unknown state, unavailable health checks, or unexpected co-tenant load for a formal run.

- [ ] **Step 5: Verify GREEN with a small 2×2 fixture matrix**

Use graph sizes 10/20, concurrency 1/2 and two repetitions in tests; assert exact expansion and state closure.

- [ ] **Step 6: Commit**

Commit message: `feat: add provenance backend performance matrix`

### Task 9: Extend topology attestation without widening claims

**Files:**
- Create: `src/txnmem_topology_attestation.py`
- Create: `tests/test_txnmem_topology_attestation.py`
- Create: `scripts/run_cross_host_provenance_performance.sh`

**Interfaces:**
- `sanitize_topology_attestation(raw) -> dict` keeps role, hashed host identity, listener ownership, transport, service version, RTT and workload hash.
- It rejects routable addresses, usernames, passwords and hostnames in committed output.

- [ ] **Step 1: Write failing privacy and continuity tests**

Assert secret-bearing/routable fields are rejected, host hashes differ across roles, and before/after listener ownership plus workload hashes are identical.

- [ ] **Step 2: Verify RED**

Run the new test; expect import failure.

- [ ] **Step 3: Implement sanitized topology attestation and wrapper**

Keep model, Qdrant and Neo4j endpoints as separate roles; do not infer a third host when two services share one host.

- [ ] **Step 4: Verify GREEN**

Run fixture attestations and artifact audit.

- [ ] **Step 5: Commit**

Commit message: `feat: attest benchmark service topology`

### Task 10: Run CPU and real-backend formal batches

**Files:**
- Create: `results/final_controlled_200/**`
- Create: `results/realism_scale/appworld/**`
- Create: `results/realism_scale/locomo/**`
- Create: `results/provenance_performance/**`

**Interfaces:** Uses Tasks 1–9 only after the full local suite passes.

- [ ] **Step 1: Run controlled 200 twice and compare canonical hashes**
- [ ] **Step 2: Regenerate at least 50 AppWorld family projections and run realism**
- [ ] **Step 3: Run LoCoMo 10-fold realism and cluster bootstrap**
- [ ] **Step 4: Run 15×30 real Qdrant/Neo4j performance cells under an attested idle environment**
- [ ] **Step 5: Validate every aggregate from raw sanitized rows and commit**

Commit message: `data: add scaled controlled realism and performance evidence`

### Task 11: Run GPU public benchmark formal batches

**Files:**
- Remote-only raw directories under a new versioned run root.
- Local sanitized aggregates under `results/public_scale_20260818/`.

- [ ] **Step 1: Sync the clean source commit to a new remote directory**
- [ ] **Step 2: Preflight GPU/model identity, runtime commits, split hashes and task counts**
- [ ] **Step 3: Run one-task smoke for τ retail, AppWorld Test-N, LoCoMo full stream and LongMemEval**
- [ ] **Step 4: Launch τ retail 115 shards and merge**
- [ ] **Step 5: Launch AppWorld baseline/tuned Test-N 168 shards and paired merge**
- [ ] **Step 6: Launch LoCoMo baseline/tuned 5 repetitions and cluster aggregate**
- [ ] **Step 7: Launch LongMemEval-S 500; keep official QA blocked if no official judge credential**
- [ ] **Step 8: Copy only sanitized aggregates locally, audit and commit**

Commit message: `data: add scaled public benchmark evidence`

### Task 12: Update paper claims and perform release verification

**Files:**
- Modify: `configs/paper_claims.json`
- Modify: `docs/paper/evidence_map_zh.md`
- Modify: `docs/formal_paper_task_status_zh.md`
- Modify: `docs/paper/txnmem_ccfa_draft_zh.md`
- Modify: paper build inputs only after claims are active.

- [ ] **Step 1: Derive every manuscript number from validated aggregates**
- [ ] **Step 2: Keep blocked/diagnostic evidence out of active claims**
- [ ] **Step 3: Run the full test suite**

Run: `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest discover -s tests -v`

- [ ] **Step 4: Run claim and artifact audits**

Run: `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B src/txnmem_claim_audit.py audit --root . --ledger configs/paper_claims.json --out /tmp/txnmem-scale-claim-audit.json`

Run: `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B src/txnmem_artifact_audit.py --root .`

- [ ] **Step 5: Verify Git diff and sensitive-data scan**

Run `git diff --check`, inspect `git status --short`, and scan tracked additions for passwords, API keys, routable IP addresses, usernames, raw prompts and tool payloads.

- [ ] **Step 6: Commit, push `codex/evidence-scale-up`, and report exact completed/blocked boundaries**

Commit message: `docs: publish scaled TxnMem evidence`

