# Remaining Public Benchmark and Artifact Tasks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the executable public-benchmark evidence, preserve the native/projection boundary, and close artifact verification gaps without overstating benchmark accuracy.

**Architecture:** Reuse the existing official-runtime adapters and Qwen OpenAI-compatible endpoint. Run larger, sanitized smoke manifests remotely, keep raw traces remote-only, and commit only aggregate counts and status. Treat DOCX rendering as a separate local artifact check; do not modify the system runtime or fabricate a visual pass.

**Tech Stack:** Python 3.12, official τ-bench/AppWorld runtimes, LoCoMo contextual runtime, Qwen2.5-7B-Instruct, unittest, python-docx, LibreOffice headless renderer.

## Global Constraints

- Ground truth remains independent of TxnMem and the model.
- Public benchmark results are named native workflow/runtime smoke unless they include a benchmark-native evaluator score.
- Raw prompts, tool arguments, conversations, credentials, and native traces remain on the remote server.
- No production latency, throughput, or public benchmark accuracy claim is made from smoke runs.
- Preserve existing user changes in `/data/txnmem`; use `/data/txnmem/_txnmem_sync` only.

### Task 1: Expand official-runtime smoke manifests

**Files:**
- Modify: remote `configs/native_tau_airline_smoke*.json` only if a manifest needs a larger task count.
- Modify: remote `configs/native_appworld_smoke*.json` only if a manifest needs a larger task count.
- Create: local `results/remaining_tasks/public_native/native_smoke_summary.json` aggregate update.

- [ ] Run τ-bench and AppWorld with bounded multi-task manifests and task-specific schemas.
- [ ] Run LoCoMo with a bounded multi-conversation manifest and fixed context limit.
- [ ] Read only sanitized summaries and record task counts, events, evaluator errors, and endpoint failures.

### Task 2: Verify native-memory event boundary

**Files:**
- Inspect: `src/txnmem_benchmark_bridge.py` and `tests/test_benchmark_bridge.py`.
- Modify only if a failing regression test identifies missing provenance or native-event labeling.

- [ ] Add a failing test only for a concrete missing event-contract behavior.
- [ ] Implement the minimal behavior and rerun the focused test.
- [ ] Confirm benchmark-tool projection events and model-issued memory events remain separately labeled.

### Task 3: Reconcile formal artifacts

**Files:**
- Modify: `docs/formal_paper_task_status_zh.md`.
- Modify: `docs/official_trace_replay_zh.md`.
- Modify: `outputs/TxnMem_论文初稿.docx` through `build_txnmem_paper_draft.py`.

- [ ] Update only sanitized aggregate counts and caveats.
- [ ] Run structural and accessibility checks.
- [ ] Attempt visual render and record the exact environment blocker if the renderer cannot start.

### Task 4: Final verification and local Git backup

**Files:**
- Modify: `results/remaining_tasks/final_status.json` if evidence changes.

- [ ] Run all unit tests, compileall, shell syntax checks, JSON validation, and `git diff --check`.
- [ ] Commit repository changes with a focused message.
- [ ] Inspect `git remote -v`; do not push without an explicit configured remote URL.
