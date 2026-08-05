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

- [x] Run τ-bench and AppWorld with bounded multi-task manifests and task-specific schemas.
- [x] Run LoCoMo with a bounded multi-conversation manifest and fixed context limit.
- [x] Read only sanitized summaries and record task counts, events, evaluator errors, and endpoint failures.

### Task 2: Verify native-memory event boundary

**Files:**
- Inspect: `src/txnmem_benchmark_bridge.py` and `tests/test_benchmark_bridge.py`.
- Modify only if a failing regression test identifies missing provenance or native-event labeling.

- [x] Add a failing test only for a concrete missing event-contract behavior.
- [x] Implement the minimal behavior and rerun the focused test.
- [x] Confirm benchmark-tool projection events and model-issued memory events remain separately labeled.
- [x] Add and test `SQLiteInstrumentedMemoryBackend` plus a backend factory path in the benchmark manifest runner.
- [x] Run Qwen2.5-7B against τ-bench, AppWorld and LoCoMo with per-task SQLite state; keep raw traces remote-only.

### Task 3: Reconcile formal artifacts

**Files:**
- Modify: `docs/formal_paper_task_status_zh.md`.
- Modify: `docs/official_trace_replay_zh.md`.
- Modify: `outputs/TxnMem_论文初稿.docx` through `build_txnmem_paper_draft.py`.

- [x] Update only sanitized aggregate counts and caveats.
- [x] Run structural and accessibility checks.
- [x] Attempt visual render and record the exact environment blocker if the renderer cannot start.

### Task 4: Final verification and local Git backup

**Files:**
- Modify: `results/remaining_tasks/final_status.json` if evidence changes.

- [x] Run all unit tests, compileall, shell syntax checks, JSON validation, and `git diff --check`.
- [x] Commit repository changes with focused commits `0ff4fe4` and `e6f689d`.
- [x] Inspect `git remote -v`; no remote is configured, so no push was attempted.

## Residual blockers after execution

- DOCX visual PNG QA remains blocked by the bundled LibreOffice dependency on `/opt/homebrew/opt/little-cms2/lib/liblcms2.2.dylib`; structural and accessibility checks pass.
- Public benchmark native memory backend instrumentation is now complete at small smoke scale; large-scale native memory sampling, production vector/graph storage, and official accuracy remain future work. The committed results are native workflow/runtime smoke with explicit benchmark-tool projection boundaries, not public benchmark memory ground truth.
- AppWorld's SQLite smoke uses a Venmo-only schema and produced official 0/7; LoCoMo's current boundary has no official QA evaluator. These are recorded limitations, not hidden failures.
