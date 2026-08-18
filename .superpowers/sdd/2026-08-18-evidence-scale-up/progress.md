# SDD ledger — plan: docs/superpowers/plans/2026-08-18-evidence-scale-up.md

## Pre-flight dependency and conflict scan

| Tasks | Producer → consumer / shared surface | Finding |
| --- | --- | --- |
| 1 → 2 | Task 1 restores the supersede policy contract in `txnmem_workloads.py`; Task 2 parameterizes the same generator. | Compatible. Task 2 must preserve the explicit `p_supersede` policy and whole-suite differential test. |
| 2 → 3 | Task 2 emits `semantic_parameters` and `semantic_fingerprint`; Task 3 aggregates and audits them. | Compatible. Task 3 must reject legacy rows missing the new fields only under the new validation profile, not globally. |
| 2 → 7 | Task 2 exposes parameter ranges and deterministic generation; Task 7 uses them per realism fold. | Compatible. Cross-fitted calibration must call the new range-aware generator, not the legacy defaults. |
| 3 → 10 | Task 3 defines controlled result paths and allowlist; Task 10 writes the formal artifacts. | Compatible. Exact versioned paths must be allowlisted without allowing public raw traces. |
| 4 → 7 | Task 4 freezes AppWorld Test-N IDs/split metadata; Task 7 chooses disjoint realism families. | Compatible. Family selection is derived from the frozen Test-N manifest and cannot cross split. |
| 4 → 11 | Task 4 produces shard and merge contracts; Task 11 runs formal GPU shards. | Compatible. Merge keeps failed tasks in the denominator and checks parent manifest hash. |
| 5 → 7 | Task 5 produces group-aware resampling and full-session LoCoMo summaries; Task 7 consumes them. | Compatible. A session longer than a model request budget is chunked, never dropped, so character coverage can remain 1.0. |
| 5 → 11 | Task 5 defines five seeds and condition identity; Task 11 performs the formal runs. | Compatible. Existing three-repetition artifacts cannot be appended unless source identity matches; formal run defaults to a clean five-repetition rerun. |
| 6 → 11 | Task 6 provides LongMemEval-S runner; Task 11 runs 500 items. | Compatible. Official QA may remain explicitly blocked without judge credentials; deterministic retrieval and hypotheses remain valid separate evidence. |
| 8 → 9 | Task 8 creates workload/hash/performance rows; Task 9 adds sanitized topology attestation. | Compatible. Topology never changes backend metric definitions or merges backend/model latency. |
| 8 → 10 | Task 8 defines 15×30 matrix; Task 10 executes it. | Compatible but resource-sensitive. Formal cells require an attested idle backend host; otherwise write diagnostic status and rerun later. |
| 9 → 11 | Task 9 supplies endpoint continuity/privacy checks; Task 11 uses them for remote runs. | Compatible. Remote raw host data stays outside Git; committed topology uses only hashes/roles. |
| 10 → 12 | CPU/backend aggregates feed active paper claims. | Compatible. Only validated complete cells become active; partial/unknown/diagnostic results stay inactive. |
| 11 → 12 | Public benchmark aggregates feed active paper claims. | Compatible. Statistical units remain tasks/conversations, not calls/questions/repetitions. |
| 1 | Test text and minimal fix agree. | Self-consistent. |
| 2 | Range sampling, generator metadata and CLI wiring agree. | Self-consistent after ruling below on inclusive integer ranges. |
| 3 | Saturation tests and output artifacts agree. | Self-consistent. |
| 4 | Official split selection and merge tests agree. | Self-consistent. |
| 5 | Full-history requirement versus request budgets. | Ruling required: split oversized sessions into ordered chunks; never head/tail truncate. |
| 6 | Official judge availability versus completion. | Self-consistent because official status may be blocked while deterministic evidence completes. |
| 7 | AppWorld 50 independent families plus disjoint calibration. | Feasible on Test-N's scenario families; select 50 evaluation families and use remaining families for calibration. |
| 8 | 15×30 matrix and formal idle environment. | Self-consistent; runtime may be long but is not a design conflict. |
| 9 | Two-host evidence versus multi-host language. | Self-consistent; claim boundary stays two-host when only two distinct hashes exist. |
| 10 | Formal artifacts versus raw sample privacy. | Self-consistent; per-operation samples contain IDs/hashes/metrics only. |
| 11 | GPU availability versus formal batch scope. | External resource risk, not a plan contradiction; provision a new endpoint without stopping unrelated processes. |
| 12 | Push side effect. | Stop only if repository authorization is no longer valid; otherwise push the isolated branch, never force-update main. |

Ruling: Treat every two-element parameter range as an inclusive integer interval — the config and spec call these ranges — cost if wrong: historical users who intended two discrete endpoints would see additional intermediate values.

Ruling: Split oversized LoCoMo/LongMemEval sessions into ordered request-sized chunks and ingest every chunk — this preserves full-history coverage under the model context bound — cost if wrong: more model calls and tokens than a truncating implementation.

Ruling: Existing 3-repetition LoCoMo results are not appended; formal evidence is a clean 5-repetition rerun from one source identity — cost if wrong: additional GPU time but no mixed-version statistics.

Ruling: LongMemEval official QA may remain `blocked` when no official judge credential is available; deterministic retrieval/hypothesis evidence is reported separately — cost if wrong: no official LongMemEval answer-accuracy claim in this submission.

Ruling: Push only `codex/evidence-scale-up` and do not update remote `main` — preserves reviewability and avoids an unrequested shared-branch merge — cost if wrong: user must merge the branch separately.

Ruling: Expand Task 2 minimally to transaction-scope the simulator's pending writes, provenance edges, and policy snapshots — true interleaving exposed that the simulator incorrectly lets one transaction's crash clear another transaction's state, while the independent reference executor already isolates transactions — cost if wrong: a wider simulator change increases regression risk, so it requires focused differential tests, full-suite verification, and fresh independent re-review.

Ruling: Treat process crash as aborting every active simulator transaction, explicit abort as terminal, unfinished transactions with pending state as aborted, and policy revalidation as action-specific to match the unchanged reference semantics — cost if wrong: the simulator refactor touches legacy result behavior, so differential regressions and complete per-transaction evidence export are mandatory.

Ruling: Require every one of the eight controlled families to consume at least one sampled parameter and produce more than one normalized semantic fingerprint over 200 seeds — row count alone is not evidence scale — cost if wrong: some family-specific parameterization must be added instead of keeping mechanically duplicated seeds.

Ruling: Correct the reference executor's post-commit process-crash behavior under an explicit micro-witness: the just-committed transaction remains committed and all sibling active transactions abort — this is a normative process-failure rule, not a change made to force agreement with TxnMem — cost if wrong: historical oracle hashes change for this previously uncovered boundary and must be versioned.

Ruling: Exclude `config`, `semantic_parameters`, seed, and identifier metadata from normalized semantic fingerprints; diversity must be recoverable from executable state/operations/policies/schedules/provenance alone — cost if wrong: reported fingerprint counts may decrease, requiring additional real workload variation.

Ruling: A concurrency lane must perform a real memory or provenance operation consumed by both executors; a guaranteed-miss read plus empty commit is not counted as meaningful concurrent work — cost if wrong: parameterized generators become more complex and require stronger oracle regressions.

Ruling: An unfinished transaction with any staged write, provenance edge, supersession, or invalidation is normatively aborted; correct the shared simulator/reference omission under a direct micro-witness and advance the oracle version — cost if wrong: oracle hashes change again, but the versioned change prevents a shared bug from masquerading as differential agreement.

Ruling: Construct semantic fingerprints from an executable top-level allowlist (`initial_memories`, `operations`, `policies`, `failure_schedule`, `provenance_edges`) rather than a metadata denylist — future non-executable fields cannot manufacture diversity — cost if wrong: cross-family labels no longer contribute, so all reported diversity must remain visible in executable shapes.

Ruling: Apply after-operation revoke, invalidate, delay, and crash events in the same event order as the independent semantics; post events are not crash-only — cost if wrong: previously untested after-operation schedules may change simulator outcomes and require new differential witnesses.

Ruling: Treat invalidate as transactional only when its `txn_id` names an already-begun active transaction; otherwise it is an autocommit repair operation even if a `txn_id` label is present — this preserves the existing provenance-repair workload contract — cost if wrong: callers expecting an implicit transaction must issue `begin_txn` explicitly.

Ruling: Differential oracle matching requires exact equality of transaction-ID domains in addition to matching states; extra candidate transactions are evidence of divergence — cost if wrong: previously tolerated spurious simulator state now fails closed.

Ruling: A pre-boundary process crash aborts only currently active transactions; committed, aborted, and completed transactions remain terminal — correct the reference under a direct expected-state witness and advance the oracle version — cost if wrong: historical oracle hashes change for this uncovered boundary.

Ruling: Encode a crash target's semantic selector relations in the fingerprint (literal operation selector and normalized transaction reference can both be present) rather than choosing one by precedence — ID/type collisions are behaviorally meaningful under the current executor contract — cost if wrong: some previously merged shapes split into separate fingerprints.

Ruling: Project nested `new_memory` with its own executor-derived key allowlist, not the initial-memory schema; include consumed `output_id` and exclude fields overwritten or ignored by staging — cost if wrong: nested supersession fingerprints either collapse real effects or count inert metadata.

Task 1: complete (commits d90e161..98d4ac0, review clean; controller verification: 11/11 focused tests, 400 TxnMem rows, 0 violations, 400 oracle matches)

Task 2: complete (commits 98d4ac0..5dd171f, seven fix rounds, final independent review PASS; controller verification: 66/66 focused tests, 571 full tests with 4 optional skips, deterministic 1,600 unique instances, 1,600/1,600 oracle-0.4 matches, 0 violations, per-family executable fingerprints 4/4/9/3/3/12/36/4, clean diff check)

Ruling: Saturation validation uses the exact registered eight-family and five-variant domains, not domains inferred from observed rows — deleting a whole variant/family or adding an unknown complete domain must fail — cost if wrong: generic callers must pass an explicit approved domain rather than relying on inference.

Ruling: Diversity validation binds each family to the exact `WORKLOAD_SEMANTIC_PARAMETERS` set and requires every semantic value to equal the generated executable config — parameter subsets cannot redefine their own denominator — cost if wrong: legacy parameterized instances missing metadata fail the scaled profile and must be regenerated.

Ruling: Active scaled-controlled claims require `validation_profile=controlled_scale_200` plus a complete, strictly typed six-artifact bundle; the profile cannot be omitted or replaced when a claim uses scaled-controlled identity/evidence — cost if wrong: older claims must remain non-scaled or be upgraded with the full bundle.

Ruling: Commit provenance is valid only when the declared commit exists and every source/config component hash is equal to the corresponding blob contained in that commit; formal runs expose a fail-closed source-containment mode — cost if wrong: development runs from dirty trees remain diagnostic and cannot activate claims until rerun from committed source.

Ruling: Controlled raw-file exceptions are exact-path and schema-validated, while public benchmark raw-capable directories/files are rejected regardless of whether their path includes a `data` component — cost if wrong: new sanitized public aggregates require an explicit schema-safe allowlist entry.

Ruling: Detect a scaled controlled claim from its declared evidence identity, registered domains, or 1,600-instance/8,000-row scale—not from its filename or an optional profile field—so renaming or omitting metadata cannot bypass the strict bundle gate — cost if wrong: any legacy artifact that happens to assert the formal scale must either supply the complete formal bundle or lower the claim to diagnostic status.

Ruling: Validate the six controlled artifacts as one closed evidence object: recompute diversity and saturation facts from the raw generated instances/results/oracles, require cross-document equality, and parse `saturation.svg` as a real SVG rooted document — cost if wrong: malformed or internally plausible summaries that are not supported by raw controlled rows become hard failures.

Ruling: Treat raw-capable public path vocabulary as semantic path components (including payloads, conversations, transcripts, prompts, messages, and equivalents), and recursively reject unapproved keys/content inside controlled synthetic exceptions — cost if wrong: future aggregate schemas need explicit narrow additions rather than inheriting a permissive nested object.

Ruling: Recursively detect formal controlled-scale markers in every active claim artifact regardless of key name or nesting, including exact registered domains and 1,600/8,000 counts; an attached six-artifact bundle does not authorize assertions from an unrelated seventh artifact — cost if wrong: generic diagnostic JSON that coincidentally contains the full formal signature must be explicitly classified or kept inactive.

Ruling: Treat stored reference oracles and CSV `oracle_match` flags as untrusted evidence: regenerate the oracle record from each generated instance with the independent reference semantics, require exact canonical equality and at least one allowed outcome, then reconcile every result row's oracle fields — cost if wrong: claim audit becomes more computationally expensive but cannot certify a mutually tampered instance/oracle/result bundle.

Ruling: A raw-capable ancestor path always dominates a schema-safe aggregate filename; safe basenames never sanitize `payloads`, `conversations`, `transcripts`, prompts/messages, or equivalent parent directories — cost if wrong: sanitized aggregates must live only in approved aggregate directories.

Ruling: Controlled JSONL exceptions use record-type-specific closed recursive schemas derived from executable workload and reference-oracle exports; generic scalar/list acceptance and blacklist-only nested checks are insufficient — cost if wrong: adding a new legitimate executable field requires an intentional schema and regression update.

Ruling: Normalize and recursively inspect both mapping keys and values when detecting formal scale identity; integral floats and numeric strings equal to 1,600/8,000, plus punctuation/case variants of `controlled_scale_200`, trigger the exact scaled bundle gate — cost if wrong: loosely typed legacy JSON cannot evade validation and may need canonicalization.

Ruling: Normalize every path component both as tokens and as a punctuation-free compound before raw-capability classification, so `Tool Args`, `tool_args`, `tool-arg`, and `tool.args` are equivalent — cost if wrong: unusual but safe directory names that normalize to a raw channel must be renamed.

Ruling: Historical public-result exceptions are exact file-and-schema entries, never directory-wide exemptions; a safe aggregate basename or historical parent cannot authorize arbitrary sibling files — cost if wrong: every new sanitized aggregate requires an explicit validator entry.

Ruling: For the current formal 1,600-instance controlled corpus, regenerate each instance from the registered family, seed, and approved config and require canonical equality; dynamic IDs/keys in stored oracles must be referentially closed to that regenerated instance and the regenerated reference result — cost if wrong: edited or hand-authored formal instances become diagnostic only, while legacy 400-instance evidence needs a separately versioned exact compatibility contract.

Ruling: Re-execute all five registered variants for every regenerated formal instance and require every exported CSV result field—including `any_violation`, metrics, counts, transaction state, violations, latency model, and oracle fields—to equal the canonical recomputation; saturation then derives only from those verified rows — cost if wrong: scaled claim audit performs 8,000 deterministic simulations but cannot certify self-consistent forged summaries.

Ruling: Parse every JSON/JSONL member of the controlled six-artifact closure with a recursive duplicate-key-rejecting loader before schema or hash validation — cost if wrong: noncanonical producers that emit duplicate keys must be fixed rather than relying on parser last-key wins.

Ruling: Formal count signatures trigger only under normalized approved semantic count keys (for example instance and variant-row counts), while controlled-scale identity is recognized as a normalized token inside paths, commands, keys, or values regardless of case/punctuation; unrelated account/discount numbers do not trigger — cost if wrong: scale detection is stricter but avoids both false negatives and false positives.

Ruling: Raw-capable path detection matches normalized denied stems inside camelCase or concatenated components as well as separated tokens, so `promptMessages`, `payloadStore`, and analogous archive/bundle/history/export names fail — cost if wrong: benign names containing a denied semantic stem require renaming or an exact schema-safe file registration.

Ruling: Numeric formal-scale detection uses only an explicit normalized key-role registry derived from the controlled evidence schemas (`instance_count`/`instances` and `variant_row_count`/`variant_results` equivalents); generic request, token, account, discount, or arbitrary `*_count` pairs never trigger — cost if wrong: new formal count field names require an intentional registry/test update.

Task 3: complete (commits 5dd171f..94b4275, six adversarial fix rounds, final fresh independent review PASS; controller verification: 91/91 focused tests, 617 full tests with 613 passed and 4 optional skips, 15 active claims/163 assertions with 0 findings, artifact audit 0 findings, two byte-identical formal trees with 1,600 instances/8,000 rows, oracle 0.4, 6-artifact closure, 12 contained source components, clean diff/worktree)

Ruling: Freeze the public formal domains as τ-bench `retail/test` with exactly 115 source-ordered tasks and AppWorld `test_normal` with exactly 168 source-ordered unique IDs; no random subsampling or cross-split task discovery is allowed — cost if wrong: prior 50/20 mixed or airline runs remain diagnostic and cannot satisfy the scaled public claim.

Ruling: Deterministic sharding partitions the frozen ordered manifest by stable task position while preserving raw official task ID, parent manifest hash, benchmark/domain/split, code/data identity and shared condition fingerprint; merging rejects empty coverage, duplicates, omissions, extras, mismatches or conflicting repetitions — cost if wrong: partial reruns must be repaired rather than silently changing the denominator.

Ruling: Failed, evaluator-error and blocked task rows remain one task each in the merged official denominator; shard success is an execution status and never filters statistical units — cost if wrong: formal aggregate rates may be lower than success-only diagnostics but are unbiased by execution failures.

Ruling: A merged task counts as official success only when every repetition has execution `status=completed`, official evaluator `status=available`, and `success=true`; evaluator output can never override a failed/error/blocked execution — cost if wrong: inconsistent task/evaluator records lower success rather than being optimistically resolved.

Ruling: The merged aggregate is a protected formal artifact: without `--resume` an existing merge path is never overwritten; with `--resume` an existing byte-equivalent/canonically equal merge is accepted, while any mismatch fails closed — cost if wrong: intentional replacement requires a fresh output directory or explicit cleanup outside the formal workflow.

Ruling: Official evaluator evidence is fail-closed: only the explicit status `available` can contribute success; missing, unavailable, evaluator-error, or unknown statuses remain denominator failures even if `success=true` is present — cost if wrong: legacy evaluator rows without an explicit availability status must be regenerated or remain unsuccessful.

Ruling: Protect an existing merged formal artifact before launching any shard or model process in both merge-only and normal execution; `--resume` is the only permitted reuse path — cost if wrong: a refused rerun could otherwise spend GPU time or create shard side effects before discovering that its final destination is immutable.

Ruling: Existing merge artifacts must be parsed with recursive duplicate-key rejection before resume equality is evaluated, and the launcher must remain executable under the host Bash 3.2 with empty optional argument lists — cost if wrong: ambiguous JSON or platform-specific array behavior could bypass or abort the formal replay contract.
