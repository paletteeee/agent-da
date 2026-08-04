# TxnMem Real Model Experiment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a dependency-light real-model Agent experiment harness that emits canonical native memory traces, evaluates them with the independent oracle, and produces sanitized evidence reports.

**Architecture:** An OpenAI-compatible HTTP client talks to any remote GPU inference server. A model-agnostic Agent runner executes structured memory tools against the existing instrumented backend, while a trace/evaluation layer converts the recorded events into TxnMem instances and compares all variants against the independent reference executor. Raw traces remain outside Git; only aggregate reports are committed.

**Tech Stack:** Python 3 standard library, existing TxnMemBench modules, JSON/JSONL, `unittest`, OpenAI-compatible HTTP JSON protocol.

## Global Constraints

- No model output is an expected outcome or ground truth.
- Native derive/propagate provenance comes from actual tool arguments/backend calls.
- No third-party dependency is required for local tests.
- Real model results require an explicitly configured endpoint and model id.
- Raw prompt/content logs are not committed; aggregate reports must be sanitized.

---

### Task 1: Model protocol client

**Files:**
- Create: `src/txnmem_model_protocol.py`
- Test: `tests/test_txnmem_real_model.py`

**Interfaces:**
- `OpenAICompatibleClient(endpoint: str, model: str, api_key: str | None = None, timeout_s: float = 60.0)`
- `OpenAICompatibleClient.complete(messages, tools, seed=None, temperature=0.0) -> ModelResponse`
- `ModelResponse.text: str`, `ModelResponse.tool_calls: list[ToolCall]`, `ToolCall.name: str`, `ToolCall.arguments: dict[str, Any]`, `ToolCall.call_id: str`

- [x] **Step 1: Write failing tests** for parsing a tool-call response, malformed JSON, HTTP error, and request metadata redaction.
- [x] **Step 2: Run the focused tests and verify the missing module/API failure.**
- [x] **Step 3: Implement the standard-library JSON/HTTP client and strict response parser.**
- [x] **Step 4: Run focused tests and verify all protocol cases pass.**
- [x] **Step 5: Commit** with `feat: add compatible model protocol client` (`819465c`).

### Task 2: Real Agent tool loop

**Files:**
- Create: `src/txnmem_real_agent.py`
- Modify: `src/txnmem_backend.py`
- Test: `tests/test_txnmem_real_model.py`

**Interfaces:**
- `run_real_agent(task: dict[str, Any], model, backend: InstrumentedMemoryBackend, max_steps: int = 12, seed: int = 0) -> dict[str, Any]`
- `NativeMemoryToolGateway(backend).call(name: str, arguments: dict[str, Any]) -> dict[str, Any]`

- [x] **Step 1: Write failing tests** for read/write/derive tool dispatch, unknown tool failure, max-step failure, and validated native event output.
- [x] **Step 2: Run focused tests and verify the missing runner/gateway failure.**
- [x] **Step 3: Implement the minimal tool schema, gateway dispatch, conversation loop, and structured run report.**
- [x] **Step 4: Run focused tests and verify derive preserves `source_ids` from the actual call.**
- [x] **Step 5: Commit** with `feat: add real agent memory tool loop` (`1156dd2`).

### Task 3: Real-model trace evaluator

**Files:**
- Create: `src/txnmem_real_experiment.py`
- Test: `tests/test_txnmem_real_model.py`

**Interfaces:**
- `evaluate_native_trace(events, instance_id, seed=0) -> dict[str, Any]`
- `run_experiment_manifest(manifest, model, out_dir) -> dict[str, Any]`
- `sanitize_run_report(report) -> dict[str, Any]`

- [x] **Step 1: Write failing tests** for trace-to-instance conversion, variant oracle-match aggregation, raw payload removal, and missing model configuration.
- [x] **Step 2: Run focused tests and verify the missing evaluator failure.**
- [x] **Step 3: Implement independent differential evaluation and aggregate-only report generation.**
- [x] **Step 4: Run focused tests and verify reports contain no raw values/content/arguments.**
- [x] **Step 5: Commit** with `feat: evaluate native model traces independently` (`2162f41`).

### Task 4: CLI, manifest, and fake-server smoke test

**Files:**
- Create: `configs/real_model_smoke.json`
- Create: `examples/real_model_smoke.py`
- Modify: `src/txnmem_experiment.py`
- Modify: `README.md`
- Test: `tests/test_cli_outputs.py`

- [x] **Step 1: Write failing CLI tests** for manifest validation and aggregate-only output.
- [x] **Step 2: Run focused CLI tests and verify the missing command failure.**
- [x] **Step 3: Add `real-model-smoke` CLI and a deterministic fake-compatible model path for local tests.**
- [x] **Step 4: Run the CLI tests and example; verify no model result is claimed without endpoint configuration.**
- [x] **Step 5: Commit** with `feat: add real model experiment CLI` (`4d5d484`).

### Task 5: Realistic task manifest, splits, and failure schedules

**Files:**
- Create: `configs/real_model_tasks.json`
- Create: `src/txnmem_failure_controller.py`
- Modify: `src/txnmem_real_agent.py`, `src/txnmem_real_experiment.py`
- Modify: `docs/remaining_tasks_implementation_zh.md`
- Test: `tests/test_txnmem_real_model.py`

- [x] **Step 1: Write failing tests** for episode-level split, trigger-based policy/crash schedules, and stable manifest hashes.
- [x] **Step 2: Run focused tests and verify the missing manifest/schedule behavior.**
- [x] **Step 3: Add a small public-task manifest schema and schedule predicates without embedding private raw data.**
- [x] **Step 4: Run focused tests and verify deterministic split and schedule coverage.**
- [x] **Step 5: Commit** with `feat: add trigger-based native experiment schedules` (`a824a34`).

### Task 6: Full local verification and paper synchronization

**Files:**
- Modify: `docs/official_trace_replay_zh.md`
- Modify: `docs/remaining_tasks_implementation_zh.md`
- Modify: `/Users/xiaoyan_zhu/Desktop/agent-db/build_txnmem_paper_draft.py`
- Test: all existing tests and document audits

- [x] **Step 1: Run the full test suite and all aggregate-output assertions.**
- [x] **Step 2: Update paper/documentation to distinguish harness-ready from real-endpoint evidence.**
- [x] **Step 3: Regenerate DOCX, run unzip/a11y/heading/section audits, and record renderer limitations.**
- [x] **Step 4: Commit local code and docs.** (`d5c943b`)

### Task 7: Remote GPU execution handoff

**External prerequisite:** remote SSH password/approved credential, accessible GPU server, model endpoint or model weights, and task/data licenses.

- [x] Verify remote `hostname`, `pwd`, `whoami`, `nvidia-smi`, and project path. Remote project: `/data/txnmem`; GPU: RTX 4090; model server: vLLM on `127.0.0.1:8000`.
- [x] Sync the committed harness to `/data/txnmem` or another persistent remote path.
- [x] Start the selected OpenAI-compatible model server with a durable `tmux` command. Model id: `qwen2.5-7b-instruct`; weights remain under `/data/models/Qwen/Qwen2___5-7B-Instruct`.
- [x] Run the one-task endpoint smoke matrix and inspect native event contract plus sanitized report. Final smoke output: `/data/txnmem/results/real_model_qwen2.5_7b_smoke_final/`.
- [x] Run the recommended train matrix and holdout evaluation. Final train output: `/data/txnmem/results/real_model_qwen2.5_7b_splits_rerun/train/`; final holdout output: `/data/txnmem/results/real_model_qwen2.5_7b_splits_rerun/holdout/`.
- [x] Copy only aggregate evidence back to the local repository and commit it. Local sanitized aggregate path: `results/real_model_qwen2.5_7b_aggregate_final/`; raw prompts, arguments, and event traces remain outside Git.

Remote native-model evidence: train has 8/8 task contracts, 17 native events, 0 replay errors, and 8/8 TxnMem oracle matches; holdout has 2/2 task contracts, 5 native events, 0 replay errors, and 2/2 TxnMem oracle matches. The two train failures are expected schedule outcomes (`injected_crash` and `policy_denied`).
