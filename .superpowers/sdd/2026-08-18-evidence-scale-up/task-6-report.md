# Task 6 report: LongMemEval-S cleaned ingestion and deterministic retrieval

## Outcome

The official cleaned LongMemEval-S source, its separate official-QA oracle reference, and the official evaluator source are now immutable and preflighted. The runner stores every released session under one question-specific opaque namespace, reads model context back through the memory backend, applies deterministic session-level BM25-style ranking, excludes the 30 abstention questions from retrieval denominators, and writes only the two fields accepted by the official evaluator. Official QA remains fail-closed until the pinned supported judge actually succeeds.

Task 6 implementation, source preflight, and independent review are complete. The first review found two fail-closed gaps in the official-QA handoff; commit `a9ad933` closes both and the same reviewer returned PASS after running focused and adversarial checks. The full 500-question Qwen generation batch belongs to the subsequent formal-run task; no result in this task is presented as a full-model benchmark score.

## Immutable source identities

- Cleaned S split revision: `98d7416c24c778c2fee6e6f3006e7a073259d48f`.
- Cleaned S split SHA-256: `d6f21ea9d60a0d56f34a05b609c79c88a451d2ae03597821ea3d5a9678c3a442`; size: 277,383,467 bytes.
- Official-QA oracle SHA-256: `821a2034d219ab45846873dd14c14f12cfe7776e73527a483f9dac095d38620c`; size: 15,388,478 bytes.
- S/oracle question-ID set SHA-256: `cb86ceffd101201672cb678968d0742d970e08472acc01690018b999d2856ecb`; all 500 IDs match.
- Official repository commit: `9e0b455f4ef0e2ab8f2e582289761153549043fc`.
- Official `evaluate_qa.py` SHA-256: `ecce9c4c79dc89d99534ac17b383a5cbb5b9f0c69ee98adaf0684742e3d95251`.
- Official `print_qa_metrics.py` SHA-256: `e9283933a0cefb7a0ded7365e436ae3d1be5aac41853325e6155d83bf07607f0`.

The execution server could not reach the dataset host directly. The same immutable URLs were downloaded on the authorized client, verified against the pinned byte sizes and SHA-256 values, transferred over the authorized internal SSH channel, and reverified by the server setup script. This transport fallback does not change source identity.

## Formal source inventory

- Questions: 500, including 30 abstention questions and 470 retrieval-scored questions.
- Sessions: 23,867.
- Turns: 246,750.
- Evidence sessions across the 470 retrieval-scored questions: 890.
- Question types: 78 knowledge-update, 133 multi-session, 56 single-session-assistant, 30 single-session-preference, 70 single-session-user, and 133 temporal-reasoning.

The strict preflight also records three anomalies in the released cleaned file rather than silently rewriting it: 13 questions contain one repeated session ID each, 12 turns have empty string content, and 211 questions contain within-day timestamp inversions. There are no calendar-day inversions. Duplicate IDs remain distinct positional events, empty turns remain present, and released source order is preserved within each calendar day.

## Evaluation boundary

- `load_longmemeval_s(...)` validates duplicate JSON keys, exact formal cardinality, roles, timestamps and weekday labels, parallel arrays, evidence references, unique question IDs, and the exact 30-question abstention count.
- `preflight_longmemeval_oracle(...)` independently verifies the official-QA reference hash, size, schema, 500 unique IDs, and exact S/oracle ID-set equality.
- `run_longmemeval_item(...)` writes all sessions, searches the backend, explicitly filters returned records by the question namespace because backend filters are not trusted, ranks only the backend-returned values, and calls the model once.
- The public answer and `answer_session_ids` are used only after retrieval to compute evidence recall; neither enters ranking or the model prompt.
- `write_official_hypotheses(...)` creates an immutable JSONL file containing exactly `question_id` and `hypothesis`, but only after its IDs exactly match all 500 formal questions, including the 30 abstentions. A partial or substituted set is rejected before file creation.
- `aggregate_longmemeval(...)` reports retrieval micro/macro recall over non-abstentions and endpoint-returned token counters. It does not trust caller-supplied hashes or metrics: it opens and hashes the hypotheses, evaluator log, oracle, `evaluate_qa.py`, and `print_qa_metrics.py`; validates 500 ordered unique results, the fixed GPT-4o judge label, exact ID/hypothesis equality, and the 30 abstentions; then recomputes overall, task-averaged, and abstention accuracy from the per-question Boolean labels.
- Official QA becomes available only after that artifact-backed validation succeeds under the pinned commit and hashes. A partial log, wrong judge, altered script, mismatched hypothesis, fabricated summary, or unavailable artifact leaves it `blocked`.

The released official script supports GPT-4o and a local Llama-3.1-70B judge; it does not support Qwen2.5-7B as an official judge. Qwen2.5-7B remains the evaluated Agent/reader model, while the official QA field will not be populated without a successful supported judge.

## RED/GREEN evidence

- Initial RED: the new module did not exist.
- Source-conformance REDs exposed official duplicate session IDs, 32 numeric answers, 12 empty turns, and day-stable but minute-unstable ordering; each case received a regression test before the minimal semantics-preserving adjustment.
- Backend-boundary RED: a deliberately changed backend round-trip value showed that source-side context could bypass returned memory; the runner now ranks and prompts only backend-returned values.
- Oracle/evaluator RED: no pinned oracle/evaluator identities or strict official-score activation contract existed.
- Initial local Task-6 suite: 18 tests passed; post-review suite: 20/20 passed.
- The post-review combined LongMemEval/LoCoMo/resampling suite passed 42/42 locally and 42/42 under the server model Python. The earlier broader Task-5/6 server suite passed 54/54 before the two additional regressions were added.
- Local and server S/oracle preflights both passed with byte-identical counts and hashes.
- Bash syntax, Python compilation with an isolated bytecode cache, and `git diff --check` passed.
- Independent review initially rejected caller-trusted summaries and partial official inputs. After `a9ad933`, the reviewer returned PASS: 20 focused tests plus 10 extra adversarial assertions rejected forged summaries, 499-row logs, wrong judges, altered evaluator/metrics scripts, and partial results.

## Real-source offline smoke

The first two real released questions were run through a shared filter-blind in-memory backend and deterministic offline model to verify wiring without claiming model quality:

- Source sessions ingested: 98.
- Isolated namespaces: 2.
- Retrieved sessions: 10 (`top_k=5` per question).
- Evidence sessions retrieved: 2/2; micro and macro recall: 1.0 on this two-question smoke only.
- Model requests: 2; endpoint-usage records complete in the fixture.
- Official QA status: `blocked` with reason `pinned_official_evaluator_has_not_succeeded`.

## Data and privacy boundary

The 277 MB S split, 15 MB oracle, raw session payloads, backend state, prompts, hypotheses, evaluator logs, endpoints, credentials, and host-specific paths remain execution-host-only. Git contains code, tests, immutable public hashes, sanitized counts, and this report. Generated LongMemEval hypotheses and item-level rows are explicitly ignored.

## Files changed

- `.gitignore`
- `scripts/setup_longmemeval.sh`
- `src/longmemeval_eval.py`
- `tests/test_longmemeval_eval.py`
