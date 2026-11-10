# TxnMem Remaining Tasks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the remaining reproducible evidence layers for public-workflow native traces, deterministic distributed-fault correctness, larger native repetition statistics, document visual QA, and Git delivery checks.

**Architecture:** Add a benchmark-neutral public-workflow runner that never silently falls back from native execution to projection replay; add a dependency-free coordinator/participant state machine with an independent invariant checker; extend the existing Qwen manifest runner with repetition aggregation and confidence intervals; render the paper through the bundled document toolchain; and report Git remote availability without inventing a push target.

**Tech Stack:** Python 3 standard library, existing TxnMem reference executor/backend, `unittest`, JSON/JSONL, bundled LibreOffice/render_docx.py, local Git.

## Global Constraints

- Ground truth remains independent of TxnMem and never uses model final text.
- Public projection replay remains labeled projection; only actual backend/tool events are labeled native.
- Raw prompts, tool arguments, memory values, API bodies, credentials, and source-content instances remain outside Git.
- Missing benchmark environments or credentials produce `status=blocked` evidence rather than synthetic success.
- Distributed results are protocol-smoke evidence, not production throughput or database compatibility claims.
- Every implementation step ends with a focused test before moving to the next step.

---

### Task 1: Public workflow native runner and blocked-dependency reports

**Files:**
- Create: `src/txnmem_public_native.py`
- Create: `configs/public_native_tasks.json`
- Modify: `src/txnmem_real_experiment.py`
- Modify: `examples/real_model_smoke.py`
- Test: `tests/test_txnmem_public_native.py`

**Interfaces:**
- `PublicWorkflowTask(dataset: str, episode_id: str, context: str, prompt: str, metadata: dict[str, Any])`
- `PublicWorkflowAdapter.load_tasks(source: Path, limit: int | None = None) -> list[PublicWorkflowTask]`
- `PublicWorkflowAdapter.check_environment() -> dict[str, Any]`
- `run_public_native_manifest(manifest, model, out_dir) -> dict[str, Any]`
- `write_blocked_report(out_dir, dataset, reason, checks) -> Path`

- [ ] **Step 1: Write failing tests** for deterministic episode IDs, context-to-prompt conversion, environment-unavailable blocked reports, and raw-content removal from aggregate output.
- [ ] **Step 2: Run the focused tests** with `PYTHONPATH=src python3 -m unittest tests.test_txnmem_public_native`; confirm the new module/API is missing.
- [ ] **Step 3: Implement adapters** for the local τ-bench and LoCoMo files and an AppWorld environment checker. The adapter must return `blocked_external_dependency` when the executable AppWorld package or required credentials are absent.
- [ ] **Step 4: Connect runnable tasks** to `run_real_agent`, `evaluate_native_trace`, and `sanitize_run_report`; write only aggregate reports under `results/remaining_tasks/public_native/`.
- [ ] **Step 5: Run focused tests and local blocked-report smoke**; verify projection replay outputs are not relabeled native.
- [ ] **Step 6: Commit** with `feat: add public workflow native runner boundary`.

### Task 2: Deterministic distributed protocol smoke

**Files:**
- Create: `src/txnmem_distributed_protocol.py`
- Modify: `src/txnmem_experiment.py`
- Test: `tests/test_txnmem_distributed_protocol.py`

**Interfaces:**
- `ProtocolCoordinator(participant_ids: list[str])`
- `ProtocolCoordinator.execute(schedule: list[dict[str, Any]]) -> dict[str, Any]`
- `check_protocol_invariants(report: Mapping[str, Any]) -> dict[str, Any]`
- `run_protocol_matrix(schedules: Iterable[Mapping[str, Any]]) -> dict[str, Any]`

- [ ] **Step 1: Write failing tests** for prepare/commit, abort, crash-after-prepare, network-drop retry, idempotent commit retry, and no-half-commit invariants.
- [ ] **Step 2: Run the focused tests** and confirm the protocol module is absent.
- [ ] **Step 3: Implement explicit participant states** `INIT`, `PREPARED`, `COMMITTED`, `ABORTED`, `CRASHED`; make commit idempotent and make dropped messages produce retry/abort without visible partial commit.
- [ ] **Step 4: Implement the independent checker** that compares only protocol states and committed IDs, not TxnMem output.
- [ ] **Step 5: Add `process-protocol-smoke` CLI** and write `schedule_coverage`, `invariant_coverage`, `minimal_counterexamples`, and per-schedule final states.
- [ ] **Step 6: Run focused tests and matrix smoke**; verify all required invariants and commit the change with `feat: add distributed protocol fault smoke`.

### Task 3: Native repetition and confidence intervals

**Files:**
- Create: `src/txnmem_statistics.py`
- Modify: `src/txnmem_real_experiment.py`
- Modify: `configs/real_model_tasks.json`
- Test: `tests/test_txnmem_statistics.py`
- Create: `examples/run_native_repetitions.py`

**Interfaces:**
- `binomial_interval(successes: int, trials: int, confidence: float = 0.95) -> dict[str, float]`
- `aggregate_native_repetitions(reports: Iterable[Mapping[str, Any]]) -> dict[str, Any]`
- `run_repetitions(manifest, model, out_dir, repetitions: int = 5) -> dict[str, Any]`

- [ ] **Step 1: Write failing tests** for zero trials, perfect success, expected-failure separation, and deterministic aggregation.
- [ ] **Step 2: Run focused tests** and confirm the statistics module is missing.
- [ ] **Step 3: Implement Wilson binomial intervals** using only the standard library and aggregate contract success, evaluation errors, TxnMem oracle matches, and expected failure counts.
- [ ] **Step 4: Implement the five-repetition CLI** with seed offsets `0, 100, 200, 300, 400`; write sanitized aggregate only.
- [ ] **Step 5: Run local fixture repetition tests** and, when the remote endpoint is reachable, run the Qwen manifest remotely with five repetitions.
- [ ] **Step 6: Commit** the statistics code, tests, and sanitized aggregate with `feat: add native repetition confidence intervals`.

### Task 4: DOCX visual QA

**Files:**
- Modify: `/Users/xiaoyan_zhu/Desktop/agent-db/build_txnmem_paper_draft.py`
- Create: `outputs/txnmem_docx_qa/report.json`
- Test: document render/a11y/OOXML commands

- [ ] **Step 1: Regenerate** `outputs/TxnMem_论文初稿.docx` with the bundled Python runtime.
- [ ] **Step 2: Run** `unzip -t`, heading/section checks, and `a11y_audit.py`.
- [ ] **Step 3: Run** `render_docx.py --emit_pdf`; if the bundled LibreOffice fails on `liblcms2`, test an available alternate LibreOffice binary or a temporary library path without modifying system directories.
- [ ] **Step 4: Inspect every generated `page-*.png`** with `view_image`; record page count and any clipping/overflow result in `outputs/txnmem_docx_qa/report.json`.
- [ ] **Step 5: If rendering remains unavailable**, record the exact loader error and mark visual QA blocked; do not claim it passed.

### Task 5: Git remote and final handoff

**Files:**
- Modify: `docs/remaining_tasks_implementation_zh.md`
- Modify: `docs/superpowers/plans/2026-08-04-real-model-experiment.md`
- Create: `results/remaining_tasks/final_status.json`

- [ ] **Step 1: Inspect** `git remote -v`, branch, status, and the latest commit without changing configuration.
- [ ] **Step 2: If a remote exists**, run a read-only connectivity check before any push; push only the current branch if the configured remote is clearly the project target.
- [ ] **Step 3: If no remote exists**, write `status=blocked`, `reason=missing_git_remote`, and the exact command the user must provide (`git remote add origin <URL>`); do not fabricate a URL.
- [ ] **Step 4: Update** the status documents with completed evidence, blocked external dependencies, and remaining claims.
- [ ] **Step 5: Run** the full test suite, aggregate assertions, `git diff --check`, and final status inspection.
- [ ] **Step 6: Commit** the final evidence and documentation with `chore: close remaining task audit`.
