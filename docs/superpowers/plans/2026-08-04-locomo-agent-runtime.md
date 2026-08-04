# LoCoMo Executable Agent Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make LoCoMo available as a native contextual-agent run using the existing TxnMem memory-tool loop and Qwen2.5-7B endpoint.

**Architecture:** The official LoCoMo checkout and data remain external benchmark inputs. A small importable runtime marker gates the native boundary, while `LoCoMoPublicAdapter` creates chronologically ordered conversation prompts and delegates execution to `run_real_agent`, `InstrumentedMemoryBackend`, and the independent TxnMem replay oracle.

**Tech Stack:** Python 3.12 virtual environment, standard-library OpenAI-compatible client already present in TxnMem, LoCoMo official Git repository, Qwen2.5-7B vLLM endpoint, `unittest`.

## Global Constraints

- Keep LoCoMo QA/event annotations separate from TxnMem ground truth.
- Do not fall back from native execution to projection replay.
- Keep raw prompts, model responses, and raw event traces on the remote server only.
- Preserve unrelated local and remote uncommitted changes.
- Store persistent runtime files under `/data`.

### Task 1: Define the native LoCoMo runtime boundary

**Files:**
- Create: `src/locomo_agent_runtime.py`
- Modify: `src/txnmem_public_native.py`
- Test: `tests/test_txnmem_public_native.py`

**Interfaces:**
- `LoCoMoPublicAdapter.required_module == "locomo_agent_runtime"` gates availability.
- `LoCoMoPublicAdapter._conversation_context(conversation)` returns sessions in numeric order.

- [x] Write failing tests for the required runtime module and numeric session ordering.
- [x] Run `PYTHONPATH=src python3 -m unittest tests.test_txnmem_public_native` and observe the expected failures.
- [x] Implement the marker module, remove the unconditional LoCoMo block, and sort `session_2` before `session_10`.
- [x] Update the existing missing-model assertion to reflect that the runtime is now available.
- [x] Re-run the focused test suite and confirm all tests pass.

### Task 2: Add reproducible remote bootstrap and smoke entry points

**Files:**
- Create: `scripts/bootstrap_locomo_agent.sh`
- Create: `scripts/run_locomo_native_smoke.sh`

**Interfaces:**
- Bootstrap creates `/data/locomo` and `/data/venvs/locomo-agent` without installing the obsolete full Conda lockfile.
- Smoke entry point accepts `LOCOMO_ENDPOINT`, `LOCOMO_MODEL`, `LOCOMO_LIMIT`, and `LOCOMO_OUT_DIR` environment overrides.

- [x] Add bootstrap script with explicit `/data` paths and no credentials.
- [x] Add smoke script that exports `PYTHONPATH` and calls `public-native-smoke` with the LoCoMo source.
- [x] Run shell syntax checks and mark both scripts executable.

### Task 3: Synchronize only the selected changes to the remote server

**Files:**
- Remote: `/data/txnmem/src/locomo_agent_runtime.py`
- Remote: `/data/txnmem/src/txnmem_public_native.py`
- Remote: `/data/txnmem/tests/test_txnmem_public_native.py`
- Remote: `/data/txnmem/scripts/bootstrap_locomo_agent.sh`
- Remote: `/data/txnmem/scripts/run_locomo_native_smoke.sh`

- [ ] Inspect local/remote status and perform a dry-run transfer of only the five selected files.
- [ ] Transfer without `--delete`, preserving unrelated remote changes.
- [ ] Run the focused remote unit test suite.

### Task 4: Install and validate the LoCoMo environment

**Files:**
- Remote: `/data/locomo`
- Remote: `/data/venvs/locomo-agent`

- [ ] Run `scripts/bootstrap_locomo_agent.sh` remotely.
- [ ] Verify the official checkout, `locomo10.json` sample count, and `locomo_agent_runtime` import.
- [ ] Check the Qwen endpoint `/v1/models` without recording credentials.

### Task 5: Execute and verify the LoCoMo smoke run

**Files:**
- Remote output: `/data/txnmem/results/locomo_native_smoke`

- [ ] Run one LoCoMo conversation through Qwen2.5-7B and the native memory gateway.
- [ ] Confirm the run is not blocked, the event contract validates, and independent oracle replay completes.
- [ ] Inspect only sanitized aggregate metrics and report the raw trace location privately as remote-only.

### Task 6: Final regression and handoff

- [ ] Run the full relevant public-native and benchmark unit tests remotely.
- [ ] Run `git diff --check` on synchronized files.
- [ ] Report runtime paths, smoke metrics, warnings, and SSH session state.
