# TxnMem Paper Work Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a versioned Chinese technical report and visually verified DOCX that explains the complete TxnMem paper, innovations, experiments, data provenance, sample sizes, results, and claim boundaries.

**Architecture:** A Markdown source contains the human-readable report and stable table markers. A deterministic Python builder reads the source plus active evidence JSON, validates required values, inserts existing paper figures, and emits a styled DOCX. Focused tests prevent stale counts and overclaiming; bundled document tools provide render, privacy, geometry, and accessibility QA.

**Tech Stack:** Python 3, `python-docx`, JSON, Markdown, existing TxnMem evidence artifacts, LibreOffice/Poppler renderer, `unittest`, Git.

## Global Constraints

- Use only active claims from `configs/paper_claims.json` and current artifacts.
- Preserve exact statistical units and claim boundaries.
- Build with bundled workspace Python and render every final page.
- Store no credential, routable host identity, private runtime path, or raw benchmark payload in the report.
- Final DOCX path is `/Users/xiaoyan_zhu/Desktop/agent-db/TxnMem_论文工作与实验总报告.docx`.

---

### Task 1: Author the evidence-grounded report source

**Files:**
- Create: `docs/txnmem_paper_work_and_experiment_report_zh.md`
- Test: `tests/test_txnmem_paper_work_report.py`

**Interfaces:**
- Consumes: active claim ledger and experiment artifacts.
- Produces: stable headings, figure markers, tables, exact sample sizes and boundary language for the DOCX builder.

- [ ] **Step 1: Write report-content tests**

Assert that the source contains all required sections, the controlled 400/2,000 counts, Qwen/public-runtime/service/cross-host denominators, and required negative claim boundaries.

- [ ] **Step 2: Run the focused test and confirm RED**

Run: `PYTHONPATH=src:scripts python3 -m unittest tests.test_txnmem_paper_work_report -v`

Expected: FAIL because the source and builder do not exist.

- [ ] **Step 3: Write the Markdown source**

Use the structure in the approved design. Explain each experiment with purpose, data origin, statistical unit, scale, result, and limitation.

- [ ] **Step 4: Run source tests**

Run: `PYTHONPATH=src:scripts python3 -m unittest tests.test_txnmem_paper_work_report -v`

Expected: source-level assertions pass while builder assertions remain pending or fail for the missing output.

### Task 2: Build a deterministic DOCX

**Files:**
- Create: `scripts/build_txnmem_paper_work_report_docx.py`
- Modify: `tests/test_txnmem_paper_work_report.py`

**Interfaces:**
- Consumes: `docs/txnmem_paper_work_and_experiment_report_zh.md`, active JSON artifacts, and existing SVG figures.
- Produces: one DOCX with fixed styles, explicit table geometry, page header/footer, figures, captions, and privacy-safe metadata.

- [ ] **Step 1: Add builder tests**

Check deterministic bytes, heading hierarchy, figure/table counts, reference to active artifacts, no superseded claim use, no unresolved markers, and no private metadata.

- [ ] **Step 2: Run builder tests and confirm RED**

Run the focused test module; expect failure because the builder is absent.

- [ ] **Step 3: Implement the minimal builder**

Implement the approved `standard_business_brief` token map, `editorial_cover` opening, Markdown parser, fixed-width tables, SVG rasterization, alt text, and deterministic package normalization.

- [ ] **Step 4: Build twice and verify deterministic output**

Use two fresh temporary directories, compare SHA-256 and bytes, then copy only the second audited build to the external output path.

- [ ] **Step 5: Run focused and full tests**

Run the report test module, then `PYTHONPATH=src:scripts python3 -m unittest discover -s tests`.

### Task 3: Render and archive QA

**Files:**
- Create: `docs/paper/txnmem_paper_work_report_qa_zh.md`

**Interfaces:**
- Consumes: final DOCX.
- Produces: recorded hash, page count, structure/privacy/a11y audit results and page-by-page visual QA summary.

- [ ] **Step 1: Run structural audits**

Run ZIP integrity, heading, section, image, table geometry, style, privacy, and accessibility audits.

- [ ] **Step 2: Render the final DOCX**

Use bundled `render_docx.py --emit_pdf` with a fresh output directory.

- [ ] **Step 3: Inspect every PNG page**

Verify typography, tables, figures, captions, headers/footers, page breaks, and final page at 100% zoom. Fix and repeat if any defect appears.

- [ ] **Step 4: Record QA and commit intended files**

Commit the source, builder, tests, QA report, design and plan. Do not add unrelated pre-existing result directories or the local `exit_code` provenance file.

