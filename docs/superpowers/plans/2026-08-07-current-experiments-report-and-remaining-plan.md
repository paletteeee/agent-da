# Current Experiments Report and Remaining Work Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce an auditable report of all completed TxnMem experiments and continue the highest-value remaining experiments without overstating single-host or trace-grounded evidence.

**Architecture:** The report is a repository-visible Markdown artifact backed only by sanitized aggregate JSON already committed in `results/`. Remaining experiments reuse the existing Qwen2.5-7B remote runners: LoCoMo paired QA repetition and AppWorld official baseline/tuned comparison. Cross-host concurrency, production cost, and Git push remain explicit blocked or pending items when their required external conditions are absent.

**Tech Stack:** Python 3.12, existing TxnMem runners, official τ-bench/AppWorld/LoCoMo boundaries, Qwen2.5-7B OpenAI-compatible endpoint, SQLite/Qdrant/Neo4j/Toxiproxy, Markdown, `unittest`, Git.

## Global Constraints

- The independent reference executor remains separate from TxnMem and model output.
- Official evaluator scores, native event-contract checks, TxnMem oracle matches, and backend consistency metrics remain separate fields.
- Native workflow evidence is not renamed as public benchmark memory ground truth.
- Raw prompts, tool arguments, memory payloads, database files, and raw model traces remain remote-only.
- Every performance report keeps `production_latency_claim: false`.
- No cross-host or production-cost conclusion is made from single-host measurements.
- Git push occurs only after the user supplies a remote URL and push authorization.

---

### Task 1: Write the completed-experiment report

**Files:**
- Create: `docs/current_experiment_report_zh.md`
- Read: `docs/formal_paper_task_status_zh.md`
- Read: `results/final_controlled/results/summary.json`
- Read: `results/remaining_tasks/native_repetitions5/repetition_report.json`
- Read: `results/locomo_paired_full_retrieval/locomo_paired_summary.json`
- Read: `results/real_backend_performance_reps30_v2/results/backend_performance.json`

**Interfaces:**
- The report must cite repository-relative evidence paths and list sample unit, denominator, model, service versions, and claim boundary for every result family.
- The report must distinguish completed, partially completed, and blocked work.

- [x] **Step 1: Extract sanitized aggregate values**

Run:

```bash
python3 -m json.tool results/real_backend_performance_reps30_v2/results/backend_performance.json >/dev/null
python3 -m json.tool results/locomo_paired_full_retrieval/locomo_paired_summary.json >/dev/null
```

- [x] **Step 2: Write the report sections**

Include controlled correctness, independent oracle, failure schedules, mutation/differential evaluation, Qwen native repetitions, τ-bench/AppWorld/LoCoMo runtime evidence, LoCoMo QA, real backend/fault/performance, realism bootstrap, document QA, limitations, and remaining experiments.

- [x] **Step 3: Validate report paths and claims**

Run:

```bash
rg -n "0\.3222|0\.1642|30|production_latency_claim|blocked|remote" docs/current_experiment_report_zh.md
git diff --check
```

- [x] **Step 4: Commit the report**

```bash
git add docs/current_experiment_report_zh.md
git commit -m "docs: add completed experiment report"
```

### Task 2: Expand LoCoMo paired memory QA repetitions

**Files:**
- Use: `src/locomo_paired_eval.py`
- Use: remote `/data/txnmem_run_20260806`
- Create locally: `results/locomo_paired_repetitions/`

**Interfaces:**
- Each repetition uses the same ten conversations and independent per-run SQLite state.
- The aggregate reports repetition count, conversation count, question count, native events, mean F1, category F1, evaluator status, model ID, and raw-report location without committing raw traces.

- [ ] **Step 1: Verify remote runner help and service health**

Only the short remote identity command succeeded; the composite service/evaluator check was closed by the server after authentication.

Check the remote vLLM endpoint, LoCoMo evaluator import, and runner arguments before launching.

- [ ] **Step 2: Launch three repetitions in a durable remote session**

Blocked on 2026-08-07: the remote SSH connection closed after authentication before a verifiable repetition output was created.

Use a distinct tmux session and output directory under `/data/txnmem_run_20260806/results/locomo_paired_repetitions`; do not overwrite the existing full paired result.

- [ ] **Step 3: Pull only sanitized aggregate JSON**

Use an rsync dry-run first, then transfer only the aggregate summary to `results/locomo_paired_repetitions/`.

- [ ] **Step 4: Validate repetition denominators**

Require 3 repetitions × 10 conversations × 1,986 questions, evaluator status `available`, and no raw-content keys in the committed aggregate.

### Task 3: Run AppWorld official baseline/tuned comparison

**Files:**
- Use: `src/txnmem_experiment.py`
- Use: `src/txnmem_benchmark_bridge.py`
- Use: fixed AppWorld 20-task manifest and task-specific app/API allowlists on the remote server.
- Create locally: `results/appworld_baseline_tuned/`

**Interfaces:**
- Baseline and tuned runs use identical task IDs and seeds.
- Tuning may change only task reset order, app/API schema selection, tool argument adaptation, termination, and retry limits; it may not change official state or evaluator.
- Report official `task_completed`, assertion pass counts, evaluator availability, native events, and TxnMem oracle separately.

- [ ] **Step 1: Verify manifest and command-line boundary**

Blocked before execution because the remote SSH session was unstable after authentication.

Run the remote batch help and compare baseline/tuned manifest hashes before launching.

- [ ] **Step 2: Launch baseline and tuned runs**

Use separate durable remote output directories and retain raw traces remotely.

- [ ] **Step 3: Pull sanitized aggregates and compute paired deltas**

Compare official task success and assertion pass rates using task-level denominators; do not convert oracle matches into task success.

### Task 4: Audit remaining production-grade blockers

**Files:**
- Modify only if evidence changes: `docs/formal_paper_task_status_zh.md`, `results/remaining_tasks/final_status.json`
- Read: `git remote -v`

- [ ] **Step 1: Check remote host and running sessions**

The host identity was reachable, but service/session status could not be verified reliably in the same remote turn.

Record whether the remote connection, Qdrant, Neo4j, Toxiproxy, and vLLM remain available.

- [x] **Step 2: Check cost instrumentation**

If model usage/token accounting is absent, write a blocked report stating the missing measurement rather than estimating cost from wall-clock time.

- [x] **Step 3: Check Git remote configuration**

If `git remote -v` is empty, leave push blocked and preserve local commits.

### Task 5: Final verification and handoff

- [ ] **Step 1: Run the full local test suite**

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p 'test*.py'
```

- [ ] **Step 2: Validate every committed JSON aggregate**

Run `python3 -m json.tool` on each new JSON report and reject raw-content keys.

- [ ] **Step 3: Commit code/results/status updates**

Use focused commits and verify `git status --short` before reporting.
