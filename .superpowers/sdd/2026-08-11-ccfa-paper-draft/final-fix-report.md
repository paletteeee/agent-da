# Final Review Fix Report

Date: 2026-08-14

Branch: `codex/pre-submission-evidence`

Baseline: `d91c2bbe51766e125d8f59667e8394f6dce9ea70`

Review range audited: `321e7a7d122584a96f69e1a5dd421eadc52dc4fc..d91c2bbe51766e125d8f59667e8394f6dce9ea70`

## Outcome

The coherent final-review wave closes the implementation, audit, portability, projection, privacy, and deterministic-release defects identified by the senior review. The historical Toxiproxy dataset is deliberately retained only as fault/response-path evidence. No state-verified 5×30 rerun was executed or fabricated. Therefore, the new state-verified 5×30 Qdrant/Neo4j/Toxiproxy rerun remains a submission-blocking experiment and no “150 runs / 0 partial commit” or historical atomicity claim is active.

New persisted-state behavior fails closed: a result is accepted only after both stores can be read and classified. An ambiguous Neo4j commit now triggers independent compensation of both stores followed by persisted-state verification; unreadable or non-absent state raises `VectorGraphBackendError`.

## Finding-to-fix mapping

### Critical 1 — unsupported Toxiproxy atomicity conclusion

- Downgraded `toxiproxy_fault_matrix_5x30` to `toxiproxy_fault_path_30`, status `complete_fault_path_observations`, with the explicit boundary: “single-host proxy/fault-response observations; post-fault Qdrant/Neo4j persistent state was not independently verified; not atomicity/availability/latency evidence.”
- Removed/downgraded historical zero-partial-commit and atomicity language in the ledger, manuscript, figures, evidence map, status documents, plan/spec, and generated DOCX.
- The historical denominator remains 150 observations (five scenarios × 30) only as a fault/response-path fact.
- Added state verification to the real-backend fault runner. Missing/unreadable Qdrant or Neo4j state is an error, not a zero count.
- Added ambiguous-commit dual-store compensation and post-compensation absence verification.
- Added regressions for commit-success-then-client-exception and unreadable state after compensation.
- Remaining blocker: execute and evidence a real, complete, state-verified 5×30 rerun before making atomicity claims.

### Important 1 — fail-open claim schema audit

- Added complete active-claim schema/type validation before evidence evaluation.
- Enforced non-empty object-list assertions, object manifests with path/hash, non-empty string-list paper locations, string command/boundary/identity fields, accepted status values, exact assertion counts, and rejected unknown fields/status combinations.
- Added malformed-container, malformed-manifest, malformed-location, unknown-status/field, and assertion-count regressions.
- Fresh result: 15/15 active claims; declared/checked/expected assertions 131/131/131; 15 checked artifacts; 3 superseded artifacts; 0 findings.

### Important 2 — manuscript audit not bound to current bytes

- Manuscript audit now performs a fresh claim audit in-process and checks the current figure manifest and figure-source hashes.
- Stale saved reports, artifact drift, manifest drift, and figure-source drift fail closed.
- Fresh result: 15 allowed claims, 61 allowed numeric values, 15 required boundaries, 0 findings.

### Important 3 — NoRepair reader projection drift

- Added a shared paper projection used by manuscript, figure, and DOCX generation.
- Removed the NoRepair special-case omission; Table 5 now reports the artifact-backed `300/400` oracle result alongside `100/400` violations.
- Added exact five-row projection tests against `controlled_suite.json`.

### Important 4 — clean-tree failure and workstation dependencies

- Added the tracked redacted LoCoMo fixture and removed the test dependency on ignored external data.
- Made paper output repo-relative by default and forced tests to use temporary outputs/cache.
- Injected raster cache, browser executable, bundled runtime, and documents renderer paths.
- Removed repo-depth and author-layout assumptions.
- The index-derived clean archive passed all 346 tests with four skips: the same three optional-runtime skips as the worktree plus one explicit no-`.git` skip for the Git-range integration test.
- The clean archive contained 273 source files; tests left zero output files and a fresh-extraction comparison found no source mutation.

### Important 5 — diff/package path privacy

- Replaced personal, version-pinned cache, and remote run paths with repo-relative paths or stable placeholders.
- Expanded `scan_git_range` from publication-only pathspecs to the complete added Git diff.
- Added macOS user-home, Linux user-home, Windows drive, file-URI, and project run-root regressions.
- Final full-added-diff path scan: 0 findings.
- Final high-confidence credential scan over all added lines: 0 findings (AWS access key, GitHub token, OpenAI key, PEM private key, and JWT patterns all zero).

### Important 6 — final DOCX not reproducible byte-for-byte

- Removed the time-varying post-build rewrite from the release chain; metadata/privacy normalization is deterministic and idempotent in the builder.
- Added a release-level deterministic/idempotence regression.
- Ran the documented direct release command twice with bundled Python and injected browser/cache; both outputs had SHA-256 `6673155ad304ab39f59d6455a1c6ff546f6459735f00dacdc31b617819227714` and compared byte-for-byte equal.
- The external deliverable at `<external-output-dir>/TxnMem_CCF-A中文论文初稿.docx` compares byte-for-byte equal to the second build. The repo-relative `outputs/...` file was only a reproducible QA copy and was removed before commit.

### Minor 1 — drafting-era text

- Updated the reference audit to describe the completed R01–R32 manuscript/reader handoff.
- Replaced the manuscript’s future-tense promise with a current chapter guide.

### Minor 2 — push incorrectly treated as manuscript blocker

- Moved remote/push status into a repository-operations note.
- Manuscript readiness now lists only scientific/template/author work. No push or merge was performed.

## TDD evidence

The predecessor’s large staged change and existing regression tests were preserved. For the two missing behaviors discovered during independent audit, tests were changed first and a fresh RED was recorded.

RED command:

```text
PYTHONPYCACHEPREFIX=<temp-dir>/pycache PYTHONPATH=src:scripts <bundled-python> -m unittest \
  tests.test_txnmem_vector_graph_backend.VectorGraphMemoryBackendTests.test_commit_then_client_exception_compensates_and_verifies_both_stores_absent \
  tests.test_txnmem_vector_graph_backend.VectorGraphMemoryBackendTests.test_ambiguous_commit_recovery_fails_closed_when_absence_cannot_be_read \
  tests.test_txnmem_path_privacy.AddedPathPrivacyTests.test_scan_git_range_requests_the_full_added_diff -v
```

RED result: `Ran 3`; failures=2, errors=1.

- Ambiguous commit remained classified as `partial` instead of verified `absent`.
- Unreadable post-compensation state leaked the raw `ConnectionResetError` instead of failing closed with `VectorGraphBackendError`.
- Git range scanning still supplied publication-only pathspecs.

GREEN: the identical command returned `Ran 3 ... OK`. The related path-privacy integration group returned `Ran 4 ... OK`, and the complete added-diff CLI scan returned `finding_count: 0`.

## Test and archive verification

Focused final-review regression suite before the two additional tests: `Ran 117 ... OK (skipped=3)`.

Final worktree command:

```text
PYTHONPYCACHEPREFIX=<temp-dir>/pycache PYTHONPATH=src:scripts TMPDIR=<temp-dir>/outputs \
  <bundled-python> -m unittest discover -s tests -p 'test*.py'
```

Result: `Ran 346 ... OK (skipped=3)`.

Index-derived clean-archive gate used `git write-tree` plus `git archive`, extracted outside the repository, and the same bundled-Python command with archive-local `PYTHONPATH`, pycache, TMPDIR, and outputs.

Result: `Ran 346 ... OK (skipped=4)`. The additional skip is the explicit Git-range integration skip because a source archive has no `.git` metadata. The scanner remains mandatory and passed in the real worktree. The archive contained 274 source files; fresh-extraction comparison found no differences. Archive test outputs: 0 files.

No full-suite, archive, audit, build, or render process remained active at handoff. No test/release artifacts remain in the committed worktree.

## Evidence, audit, and figure regeneration

All generation used bundled workspace Python and temporary output directories. Fresh outputs compared byte-for-byte equal to tracked outputs.

- Claim ledger SHA-256: `596ef06eaf6a107e27c82fda1a9c520a9c064c5049e5e8be3af114ad126d664f`
- Claim audit SHA-256: `aec2181bdea52f9c2829d717e90fd8d80c03c521a35f8ac6eb199bd390671caa`
- Manuscript audit SHA-256: `e4a2a5588f1c017a65d29af05fdab1bef202d30c11f2a472bc8cb64eded89414`
- Figure manifest SHA-256: `f52d48007e31d14dda08905612ed0ec27da880615db8f92c39a2dafabd60149d`
- Claim audit: passed; 15 active claims; 131 declared/checked/expected assertions; 15 artifacts; 3 superseded; 0 findings.
- Manuscript audit: passed; 15 claims; 61 numeric values; 15 boundaries; 0 findings.
- Figures generated: 6; fresh directory exactly matched tracked assets.

Figure hashes:

| Figure | SHA-256 |
| --- | --- |
| `architecture.svg` | `9a132f6bc2240bdde399af8ab2735f805f33544697346ea1af1cfcb28518bb92` |
| `commit_protocol.svg` | `dc66463a6dfb8654395ddd21e8a4e10b6ff748f320297354b8bf0460a6d77d1d` |
| `controlled_results.svg` | `3b716af178b3247a9ea9f0949465a65baeeb5666779056243007df6eae3065bf` |
| `evidence_layers.svg` | `52130544b2d01f7f2a8d00ceea3be4e6ac7082c21983bdee456507f26d06050a` |
| `motivation_timeline.svg` | `50504849cad425ee2a5e1ca17e8217088462eed88e23cbc007de6de1face86db` |
| `provenance_repair.svg` | `40efbf5782697e6ab2c9138f8d2512558ff336a1ae2c0adb7b6965f45ff07970` |

## Deterministic DOCX release and render

Documented release command:

```text
<bundled-python> scripts/build_txnmem_ccfa_docx.py --root . \
  --output <external-output-dir>/TxnMem_CCF-A中文论文初稿.docx
```

- Build 1 SHA-256: `6673155ad304ab39f59d6455a1c6ff546f6459735f00dacdc31b617819227714`
- Build 2 SHA-256: `6673155ad304ab39f59d6455a1c6ff546f6459735f00dacdc31b617819227714`
- Equality: build 1 = build 2 = final external deliverable.
- Size: 1,163,774 bytes.
- Render: 27 consecutive page PNGs plus a non-empty 27-page PDF.
- Render PDF SHA-256: `455abe3bcf3b22b2caabbfc955c8ee060bdc6a51456e362f72f6055c58ccd8c3`.

All-page original-detail inspection record:

- Pages 1–6: clean; titles, abstract, Figure 1, continued Table 1, and model text are readable; repeated table header present.
- Pages 7–12: clean; invariants, Figures 2–4, captions, and pseudocode have no clipping, overlap, or orphaning.
- Pages 13–18: clean; implementation/evaluation flow, Tables 2–5, and Figure 5 are readable. NoRepair correctly shows 100/400 violations and 300/400 oracle matches.
- Pages 19–24: clean; downgraded fault evidence is explicit, Figure 6 and continued Table 6 are intact with repeated header, and references begin without truncation.
- Pages 25–27: clean; references R16–R32 and Appendix Tables 7–8 continue naturally; no empty or isolated-content page.

Overall visual result: no clipping, overlap, missing CJK glyph, malformed table, orphan figure/caption, abnormal blank page, or hidden content.

## Accessibility, OOXML, and privacy

- Accessibility: high=0, medium=0, low=0.
- Heading styles: Heading 1/2/3 = 12/18/4.
- Sections: one portrait Letter section, 1-inch margins.
- Images: 6 inline images, all 5.50 inches wide, with meaningful alt text.
- Tables: 8/8 have matching `tblW`, `tblGrid`, and `tcW`; repeated headers/non-splitting rows are regression-tested.
- References: 32 consecutive verified entries.
- ZIP integrity: passed; 25 members, 18 XML/relationship parts, 6 media parts.
- Privacy/OOXML counts: rsid=0; comments/people/custom-property parts=0; tracked-change tags=0; creator=0; lastModifiedBy=0; company=0; absolute workstation/run path tokens=0; high-confidence credential tokens=0.
- Style lint reported 208 direct run-formatting runs and 190 direct paragraph-formatting paragraphs. These are intentional controlled CJK/title/caption/table formatting; the apparent non-heading examples are the document title and table cells, while the heading hierarchy audit passed.

## Changed files

Review/evidence reports:

- `.superpowers/sdd/2026-08-11-ccfa-paper-draft/final-fix-report.md`
- `.superpowers/sdd/2026-08-11-ccfa-paper-draft/task-7-report.md`
- `results/paper_evidence/claim_audit.json`
- `results/paper_evidence/manuscript_audit.json`
- `results/submission_evidence/qwen_vector_graph_e2e_5/aggregate.json`
- `results/submission_evidence/tau_bench_50/aggregate.json`
- `results/submission_evidence/toxiproxy_faults_30/aggregate.json`

Configuration and source:

- `configs/paper_claims.json`
- `configs/submission_evidence/remote_runs.json`
- `configs/submission_evidence/toxiproxy_faults_30.json`
- `configs/txnmem_ccfa_paper.json`
- `src/txnmem_backend_performance.py`
- `src/txnmem_claim_audit.py`
- `src/txnmem_evidence_aggregates.py`
- `src/txnmem_manuscript_audit.py`
- `src/txnmem_paper_projection.py`
- `src/txnmem_path_privacy.py`
- `src/txnmem_vector_graph_backend.py`

Generation/release scripts and assets:

- `scripts/build_txnmem_ccfa_docx.py`
- `scripts/build_txnmem_paper_figures.py`
- `scripts/render_docx_with_bundled_libs.sh`
- `scripts/run_real_backend_smoke.sh`
- `paper_assets/figures/controlled_results.svg`
- `paper_assets/figures/evidence_layers.svg`
- `paper_assets/figures/manifest.json`

Manuscript/status/planning documents:

- `docs/current_experiment_report_zh.md`
- `docs/formal_paper_task_status_zh.md`
- `docs/paper/evidence_map_zh.md`
- `docs/paper/reference_audit_zh.md`
- `docs/paper/txnmem_ccfa_docx_qa_zh.md`
- `docs/paper/txnmem_ccfa_draft_zh.md`
- `docs/superpowers/plans/2026-08-11-ccfa-paper-draft.md`
- `docs/superpowers/specs/2026-08-11-ccfa-paper-draft-design.md`

Tests and fixtures:

- `tests/fixtures/locomo_redacted_minimal.json`
- `tests/test_benchmark_bridge.py`
- `tests/test_cli_outputs.py`
- `tests/test_real_backend_script.py`
- `tests/test_txnmem_backend_performance.py`
- `tests/test_txnmem_ccfa_docx.py`
- `tests/test_txnmem_claim_audit.py`
- `tests/test_txnmem_evidence_aggregates.py`
- `tests/test_txnmem_manuscript_audit.py`
- `tests/test_txnmem_paper_figures.py`
- `tests/test_txnmem_paper_projection.py`
- `tests/test_txnmem_path_privacy.py`
- `tests/test_txnmem_vector_graph_backend.py`

## Remaining blockers and concerns

1. Submission-blocking: execute a real state-verified Qdrant/Neo4j/Toxiproxy 5×30 rerun and regenerate the evidence chain before restoring any partial-commit/atomicity conclusion.
2. Venue-dependent: adapt the document to the selected venue’s official template and perform normal author revision when that venue/template is known.
3. Repository operation: no remote push or merge was performed, as required; this is not a manuscript-readiness blocker.
