# Task 5 report: complete LoCoMo session streams and conversation-cluster statistics

## Outcome

The LoCoMo paired evaluator now ingests every chronological session without a whole-conversation head/tail cap, bounds each individual model request, isolates memory by conversation/profile/repetition seed, enforces the formal five-seed schedule, and reports deterministic conversation-cluster bootstrap intervals. The baseline/tuned comparison rejects mismatched task manifests, model identities, condition fingerprints, seeds, per-repetition denominators, conversation IDs, or conversation denominators.

Task 5 implementation and evaluator-environment preparation are complete. The formal five-repetition GPU batch belongs to Task 11 and has not been started while an unrelated user-owned GPU process occupies the server.

## Interfaces implemented

- `iter_conversation_sessions(sample)` yields every `session_<n>` in numeric chronological order with exact rendered character count and SHA-256.
- `ingest_session_stream(sample, ingest_session, max_session_chars=...)` splits only oversized individual sessions, passes every chunk exactly once in order, and records attempted/completed session, chunk, character, and stream-hash coverage.
- `conversation_namespace(sample_id, prompt_profile, seed)` isolates state across conversation, baseline/tuned treatment, and repetition.
- `cluster_bootstrap_interval(rows, group_key, value_key, repetitions=10000, seed=17)` resamples whole conversation groups and computes a row-weighted mean interval without treating QA rows as independent.
- `run_paired_repetitions(...)` accepts only the formal seeds `17, 1017, 2017, 3017, 4017`.
- Repetition summaries include sanitized per-conversation score sums/counts and a profile-level cluster interval; the paired comparison adds a paired conversation-cluster interval.
- `scripts/bootstrap_locomo_agent.sh` downloads the fixed official source archive, verifies hashes, installs only pinned lightweight evaluator dependencies into an isolated target directory, runs an official F1 smoke, and writes an environment lock.

## Formal source preflight

Fresh read-only inspection of `/home/suma/txnmem/external_data/raw/locomo10.json` produced:

- Source SHA-256: `79fa87e90f04081343b8c8debecb80a9a6842b76a7aa537dc9fdf651ea698ff4`.
- File size: 2,805,274 bytes.
- Conversations: 10.
- Chronological sessions: 272.
- Official QA questions: 1,986.
- Per-session rendered content: 827,164 characters. The legacy joined context has 262 additional separator newlines, for 827,426 characters; no dialogue content is represented by those separators.
- Maximum individual session: 6,227 characters, below the formal 12,000-character per-request limit. Therefore the real source requires 272 ordered requests per repetition and no session chunking or truncation.

## Official evaluator environment

- Repository: `https://github.com/snap-research/locomo`.
- Fixed commit: `3eb6f2c585f5e1699204e3c3bdf7adc5c28cb376`.
- Downloaded archive SHA-256: `6b79b8bc2637397c7297ada08a30c6f57aa77cddbe28106ee91db33680a2f6d3`.
- `task_eval/evaluation.py` SHA-256: `8e3be5d57ff2ff9ec5cd05939592f468c5f3f1fd95d13e431932bdf6bf0fd6fd`.
- Runtime: Python 3.14.6, `bert-score` 0.3.13, `nltk` 3.9.2, `pandas` 3.0.5, `matplotlib` 3.11.1, Torch 2.13.0, Transformers 5.15.0.
- Official `eval_question_answering` smoke: one exact-answer category-1 item returned F1 `1.0`.
- The isolated lightweight target directory is about 165 MB and reuses the existing Torch/Transformers installation. A mistaken partial virtual environment was stopped and removed before use; it contained no user data and did not modify the existing vLLM environment.

## RED evidence

- Initial Task 5 RED: two modules failed to import because `iter_conversation_sessions` and `txnmem_resampling` did not exist.
- Pairing identity RED: two tests failed because mismatched task manifests/model identities and a non-formal seed schedule were accepted.
- Namespace RED: the namespace-isolation test failed to import `conversation_namespace`.
- Paired cluster RED: the profile comparison lacked `paired_conversation_count` and a conversation-cluster interval.
- Reproducible evaluator setup RED: three tests failed because the legacy script used an unpinned Git clone, had no isolated pinned dependencies, and did not run the official F1 smoke.
- First full-suite run exposed one path-privacy failure for an added environment-specific absolute runtime default; the default was replaced by a caller-provided/TxnMem-relative runtime root.

## GREEN evidence

- LoCoMo/resampling/prompt-comparison core: 29 tests passed in 0.045 seconds.
- Combined LoCoMo, resampling, prompt comparison, public reporting, setup, and path-privacy set: 49 tests passed in 0.238 seconds.
- Python compile verification passed after redirecting the host bytecode cache to `/tmp`; the earlier failure was only a macOS cache-directory permission error.
- `bash -n scripts/bootstrap_locomo_agent.sh` and `git diff --check` passed.
- Full suite: 674 tests passed in 107.043 seconds; 4 optional dependency/data tests skipped; 0 failures/errors.
- Claim audit: 15 active claims, 0 findings; diagnostic output only at `/tmp/txnmem-task5-controller-claim-audit.json`.
- Artifact audit: 0 findings.

## Security and evidence boundaries

- Aggregate files contain counts, hashes, scores, coverage ratios, condition/model identities, and token counters only.
- Raw LoCoMo conversations, prompts, predictions, memory payloads, SQLite files, credentials, endpoints, and user-specific server paths remain remote-only and are not added to Git.
- The official evaluator and dataset identities are independently hashed; no local judge is substituted into the official field.
- GPU status at environment completion: a user-owned `unimatch.py` process occupied approximately 16.6 GiB on the RTX 5090. It was inspected read-only and not interrupted. No Task 5 formal model call was made.

## Files changed

- `scripts/bootstrap_locomo_agent.sh`
- `src/locomo_official_eval.py`
- `src/locomo_paired_eval.py`
- `src/txnmem_prompt_comparison.py`
- `src/txnmem_resampling.py`
- `tests/test_locomo_official_eval.py`
- `tests/test_remote_setup_script.py`
- `tests/test_txnmem_prompt_comparison.py`
- `tests/test_txnmem_resampling.py`
