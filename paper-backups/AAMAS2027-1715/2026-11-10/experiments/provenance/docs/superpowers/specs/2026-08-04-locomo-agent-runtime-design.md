# LoCoMo Executable Agent Runtime Design

## Goal

Provide an executable LoCoMo contextual-agent environment that runs the existing TxnMem memory-tool loop against the Qwen2.5-7B OpenAI-compatible endpoint, records real memory events, and keeps LoCoMo's QA/event annotations as an external evaluation layer rather than TxnMem ground truth.

## Scope and non-goals

The runtime covers one LoCoMo conversation episode at a time, sequential session context, TxnMem memory-tool calls, native event validation, and a one-episode smoke run. It does not claim that the official LoCoMo repository supplies a transaction runtime or provenance ground truth, and it does not rewrite the official QA answers.

## Architecture

1. The official LoCoMo repository is cloned under `/data/locomo` for source provenance and evaluator scripts. The existing `external_data/raw/locomo10.json` remains the input snapshot used by TxnMem.
2. A dedicated `/data/venvs/locomo-agent` environment contains only the lightweight runtime dependencies. Model inference is delegated to the existing Qwen2.5-7B vLLM OpenAI-compatible endpoint; no second model copy is downloaded.
3. `LoCoMoPublicAdapter` becomes available only when the dedicated `locomo_agent_runtime` module is importable. It converts each conversation into a contextual task, while the existing `run_real_agent` and `InstrumentedMemoryBackend` perform the actual tool loop and event recording.
4. The runtime writes raw local traces only under the remote results directory and writes sanitized aggregate reports for repository-visible evidence. LoCoMo QA/event annotations are reported separately and never used to manufacture expected TxnMem events.

## Data flow

`locomo10.json` → conversation/session prompt → Qwen2.5-7B tool loop → `InstrumentedMemoryBackend` events → event-contract validation → independent TxnMem reference replay → sanitized smoke report.

## Failure and availability behavior

- Missing LoCoMo source, runtime module, or model endpoint produces an explicit blocked report.
- Model/tool protocol errors remain structured failures and do not fall back to projection replay.
- A smoke run succeeds only if the model completes, emits valid native events, and the independent replay accepts the trace.

## Verification

- Unit tests cover runtime availability, task conversion, session ordering, and the non-projection execution label.
- Environment checks verify official source checkout, Python imports, LoCoMo sample count, and model endpoint reachability.
- The remote smoke run uses one LoCoMo conversation and Qwen2.5-7B with a bounded step count; its report records task status, event count, validation/oracle outcomes, and model metadata without raw prompt or response payloads.
