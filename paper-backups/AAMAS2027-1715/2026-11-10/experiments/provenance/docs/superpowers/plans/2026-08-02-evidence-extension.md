# TxnMem Evidence Extension Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add dependency-free process-concurrency evidence, canonical native memory-event validation, and auditable trace-shape reports while preserving the distinction between replay evidence and production validation.

**Architecture:** Add focused `txnmem_event_contract.py` and `txnmem_distributed.py` modules. The event contract validates and normalizes connector output before it enters the existing trace pipeline; the process harness runs worker sequences against one serialized backend owner and records observed linearization. Extend the CLI and reports only with aggregate, non-sensitive evidence.

**Tech Stack:** Python 3.12 standard library (`multiprocessing`, `queue`, `json`, `dataclasses` only where useful), existing `unittest` suite, existing JSON/CSV reports.

## Global Constraints

- Do not add required third-party dependencies.
- Do not commit raw τ-bench, AppWorld, or LoCoMo inputs, transformed instances containing source content, credentials, or backend response bodies.
- Do not claim production 2PC, production latency, or independent LLM-agent results from the new harness.
- Every behavior change follows red-green-refactor: write a failing test, run it, implement minimally, rerun the focused test, then run the full suite.
- Preserve the existing CLI commands, JSONL schema, oracle implementation, and variant names.

---

### Task 1: Canonical native memory-event contract

**Files:**
- Create: `src/txnmem_event_contract.py`
- Modify: `src/txnmem_backend.py`
- Test: `tests/test_txnmem_remaining_tasks.py`

**Interfaces:**
- Produce `validate_event(event: Mapping[str, Any], seen_ids: set[str] | None = None) -> dict[str, Any]` that returns a normalized JSON-safe event or raises `EventContractError` with a stable `code` attribute.
- Produce `validate_events(events: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]` that checks unique event IDs and increasing positive `step` values.
- Add `InstrumentedMemoryBackend.validated_events() -> list[dict[str, Any]]` that validates the backend event log before calling `trace_to_instance`.

- [ ] **Step 1: Write failing contract tests**

Add tests for: valid write, valid derive preserving `source_ids`, missing `event_id`, unsupported `kind`, duplicate IDs, non-positive steps, and malformed source IDs.

- [ ] **Step 2: Run focused tests to verify failure**

Run:

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_remaining_tasks -v
```

Expected: the new contract tests fail because the module and validation boundary do not exist.

- [ ] **Step 3: Implement the minimal validator**

Use a fixed `SUPPORTED_KINDS` set, copy only JSON-safe values, normalize `event_id`, `agent_id`, `kind`, and `step`, and raise `EventContractError(code=...)` for each invalid case. Do not create `source_ids` or provenance edges when absent.

- [ ] **Step 4: Add the backend validation boundary**

Implement `validated_events()` as a read-only validation of the existing event list. Update `AgentReplayRunner.to_instance()` to pass validated events to `trace_to_instance`.

- [ ] **Step 5: Run focused and regression tests**

Run the new tests and then:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

- [ ] **Step 6: Commit**

```bash
git add src/txnmem_event_contract.py src/txnmem_backend.py tests/test_txnmem_remaining_tasks.py
git commit -m "feat: validate native memory event contract"
```

### Task 2: Cross-process linearization smoke harness

**Files:**
- Create: `src/txnmem_distributed.py`
- Modify: `src/txnmem_concurrency.py`
- Test: `tests/test_txnmem_remaining_tasks.py`

**Interfaces:**
- Produce `run_process_action_sequences(sequences: Iterable[Iterable[dict[str, Any]]], timeout_s: float = 5.0) -> dict[str, Any]`.
- Return `concurrency_model`, `worker_count`, `submitted_operation_count`, `event_count`, `unique_event_ids`, `completed`, `failed_worker_ids`, `unacknowledged_operation_ids`, `events`, and `final_memories`.
- Preserve each worker’s local sequence order and assign `linearization_index` only in the single backend-owner process.

- [ ] **Step 1: Write failing process-harness tests**

Test two workers writing independent memories, assert completion, two unique backend events, valid linearization indexes, and per-worker local order. Add a forced invalid action test that asserts `completed=False` and a non-empty failed-worker report.

- [ ] **Step 2: Run focused tests to verify failure**

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_remaining_tasks -v
```

Expected: import or attribute failures for the new process harness.

- [ ] **Step 3: Implement the owner/worker protocol**

Use `multiprocessing.get_context("spawn")`, one command queue, one result queue, and an owner process holding an `InstrumentedMemoryBackend`. Worker messages contain worker ID, local sequence index, operation ID, and action payload. The owner acknowledges each action after applying it and appends a linearization index. Send a sentinel after all workers finish and join with `timeout_s`.

- [ ] **Step 4: Implement failure reporting and cleanup**

Convert worker exceptions and timeouts into JSON-serializable report fields. Terminate only the newly created child processes after timeout; do not affect unrelated processes. Keep raw exception text short and exclude action payload values from reports.

- [ ] **Step 5: Run focused and full tests**

Run the new process tests and the full unittest suite. Repeat the process test several times to check it does not rely on one scheduler order.

- [ ] **Step 6: Commit**

```bash
git add src/txnmem_distributed.py src/txnmem_concurrency.py tests/test_txnmem_remaining_tasks.py
git commit -m "feat: add process linearization smoke harness"
```

### Task 3: Trace evidence and CLI report integration

**Files:**
- Modify: `src/txnmem_realism.py`
- Modify: `src/txnmem_experiment.py`
- Modify: `src/txnmem_trace_pipeline.py`
- Test: `tests/test_cli_outputs.py`
- Test: `tests/test_txnmem_remaining_tasks.py`

**Interfaces:**
- Produce `trace_evidence_summary(instances, rows) -> dict[str, Any]` with source-operation count, replay-operation count, envelope count, holdout grouping label, oracle-match counts by variant, and explicit `trace_ground_truth_native=False` / `production_latency_claim=False` flags.
- Add CLI command `process-smoke --out-dir PATH` that writes `results/process_concurrency.json` without raw event values.
- Add CLI output fields to `trace_realism.json` without changing existing fields.

- [ ] **Step 1: Write failing report and CLI tests**

Test that source operation count excludes synthetic `begin_txn`/`commit`, replay operation count includes them, full TxnMem oracle matches are counted, and `process-smoke` writes a JSON report with the required flags.

- [ ] **Step 2: Run focused tests to verify failure**

```bash
PYTHONPATH=src python3 -m unittest tests.test_cli_outputs tests.test_txnmem_remaining_tasks -v
```

- [ ] **Step 3: Implement the evidence summary**

Compute only aggregate values from existing instances and result rows. Keep source metadata and holdout grouping explicit. Do not change oracle generation or synthetic calibration behavior.

- [ ] **Step 4: Add the CLI command**

Call `run_process_action_sequences` on a fixed two-worker smoke fixture, write sorted JSON through the existing summary helper, and print the output path.

- [ ] **Step 5: Run CLI tests and inspect generated JSON**

Assert the JSON parses, contains no `value`, `arguments`, `data`, `password`, or `token` fields, and has stable top-level keys.

- [ ] **Step 6: Commit**

```bash
git add src/txnmem_realism.py src/txnmem_experiment.py src/txnmem_trace_pipeline.py tests/test_cli_outputs.py tests/test_txnmem_remaining_tasks.py
git commit -m "feat: report trace and process evidence"
```

### Task 4: Native connector example and documentation

**Files:**
- Create: `examples/native_memory_agent.py`
- Modify: `README.md`
- Modify: `docs/remaining_tasks_implementation_zh.md`
- Modify: `docs/official_trace_replay_zh.md`
- Test: `tests/test_txnmem_remaining_tasks.py`

**Interfaces:**
- Example function `run_native_example() -> list[dict[str, Any]]` emits a read, derive, and write sequence through `InstrumentedMemoryBackend`, validates the resulting events, and returns only canonical metadata.

- [ ] **Step 1: Write failing example test**

Assert the example returns valid events, preserves `source_ids`, and produces a trace instance with provenance generated from the actual derive event rather than a manually appended edge.

- [ ] **Step 2: Run the focused test to verify failure**

```bash
PYTHONPATH=src python3 -m unittest tests.test_txnmem_remaining_tasks -v
```

- [ ] **Step 3: Implement the example**

Use only the existing backend and contract validator. Keep values generic and non-sensitive. Document that this is an instrumentation example, not an LLM run.

- [ ] **Step 4: Update documentation**

Document the command, the canonical event boundary, process-smoke interpretation, and the remaining production gaps. State that public benchmark projections are not native memory ground truth.

- [ ] **Step 5: Run docs/example tests**

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 examples/native_memory_agent.py
```

- [ ] **Step 6: Commit**

```bash
git add examples/native_memory_agent.py README.md docs/remaining_tasks_implementation_zh.md docs/official_trace_replay_zh.md tests/test_txnmem_remaining_tasks.py
git commit -m "docs: add native memory instrumentation example"
```

### Task 5: Evidence artifact, paper update, and final verification

**Files:**
- Modify: `build_txnmem_paper_draft.py`
- Modify: `docs/superpowers/specs/2026-08-02-evidence-extension-design.md` only if implementation constraints require a correction
- Generate: `results/process_concurrency.json`
- Generate: `outputs/TxnMem_论文初稿.docx`

- [ ] **Step 1: Run the process smoke command and verify aggregate-only output**

Check that the output contains no raw event values or external trace content.

- [ ] **Step 2: Update the paper draft**

Add the process-smoke results, native event contract, and exact caveat that the harness is a cross-process linearization smoke test rather than production distributed correctness.

- [ ] **Step 3: Rebuild the DOCX and run structural QA**

Use the bundled Python runtime to rebuild the document, run `unzip -t`, and run the accessibility audit. Attempt `render_docx.py`; if the known LibreOffice dependency failure persists, record it without claiming visual QA passed.

- [ ] **Step 4: Run final verification**

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
git diff --check
git status --short
```

Expected: all tests pass, no whitespace errors, and only intended generated evidence is tracked.

- [ ] **Step 5: Commit the final evidence and paper synchronization**

```bash
git add README.md docs results src tests examples
git commit -m "feat: complete local evidence extension"
```

## Review checklist

- [ ] No raw external dataset or credential-bearing log is tracked.
- [ ] Process harness reports incomplete runs instead of silently dropping failures.
- [ ] Event validation never invents provenance.
- [ ] Source and replay-envelope operation counts are separate.
- [ ] Production and native-ground-truth caveats appear in README, Chinese task status, and paper draft.
- [ ] Full test suite and structural document QA pass; visual QA is reported accurately if the renderer remains unavailable.
