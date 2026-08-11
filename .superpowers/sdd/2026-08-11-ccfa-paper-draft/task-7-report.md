# Task 7 Report — 文档渲染、可访问性与隐私 QA

## Status

Complete. The delivered external DOCX is the scrubbed file with SHA-256 `870feaf210bf3a7b9507795988aaf242ae6d759f69a65b2c0fef54b40fe04e6b`, and `render_final_v4` was generated and inspected from those exact bytes.

## Builder/test changes

- Fixed SVG screenshot scaling so the six figures occupy their intended canvas and remain readable.
- Kept figure paragraphs with captions while breaking the Caption style's incorrect chain to the following heading; Figure 2 and its caption now share page 8.
- Prevented all table-row splits and kept first-row table headers repeatable.
- Compactly but readably formatted only Appendix Tables 7–8 so the final explanation does not spill into an isolated page 28.
- Added regression tests for figure raster occupancy, figure/caption grouping, non-splittable table rows, and readable compact appendix typography.
- Removed every OOXML `rsid` element/container and `rsid*` attribute from all XML parts while preserving unchanged XML bytes for LibreOffice compatibility.
- Replaced reader-facing repository artifact paths in Table 7 with payload evidence IDs or stable `E-<claim_id>` labels.

## Final artifact

- DOCX: `/Users/xiaoyan_zhu/Desktop/agent-db/outputs/TxnMem_CCF-A中文论文初稿.docx`
- SHA-256: `870feaf210bf3a7b9507795988aaf242ae6d759f69a65b2c0fef54b40fe04e6b`
- Size/pages: 1,160,752 bytes / 27 pages
- Final render/PDF: `/Users/xiaoyan_zhu/Desktop/agent-db/outputs/TxnMem_CCF-A中文论文初稿_render_final_v4`
- A11y JSON: `/Users/xiaoyan_zhu/Desktop/agent-db/outputs/TxnMem_CCF-A中文论文初稿_a11y.json` (`high=0`, `medium=0`, `low=0`)

## QA evidence

- Every final page (1–27) was inspected at original detail; no clipping, overlap, missing CJK glyphs, broken tables, isolated captions, or unexpected blank page remains.
- Structure: Heading 1/2/3 = 12/18/4; 8 tables; 6 inline figures with meaningful alt text; 32 ordered references.
- OOXML: all 8 table header rows repeat; all table rows are non-splittable; no comments, tracked changes, people data, or custom properties.
- Strict privacy: 0 `rsid` strings in every XML part and no reader-facing `results/`, absolute-path, or `file:` artifact text; six meaningful alt texts remain.
- The full suite rebuilds the default external DOCX, so privacy scrub was run again through a temporary DOCX and atomic replacement after tests; `render_final_v4`, a11y, privacy, and hash checks then ran read-only against that final file.
- The final page has meaningful appendix continuation plus natural trailing whitespace; no empty or isolated-content page remains.
- The complete command log, page-by-page first/final checklists, audit outputs, and renderer boundary are in `docs/paper/txnmem_ccfa_docx_qa_zh.md`.

## Verification

```text
PYTHONPATH=src:scripts <bundled-python> -m unittest tests.test_txnmem_ccfa_docx tests.test_document_render_config -v
# 13 tests, OK

PYTHONPATH=src:scripts <bundled-python> -m unittest discover -s tests -v
# 319 tests, OK (skipped=3 optional dependencies)
```

## Independent-review remediation

- RED: the added package/path/readability regressions failed as expected: residual `rsid` XML, `results/` text in Table 7, and effective 8.5 pt appendix body text.
- GREEN: 13 focused tests passed after source changes; a first post-test `render_final_v3` caught a LibreOffice failure from reserializing unchanged XML. The serializer was narrowed to rewrite only XML parts actually changed by rsid removal; a fresh 27-page probe rendered, then the 319-test full suite passed.
- Final chain: after that full suite, atomic scrub → `render_final_v4` (27 PNGs + non-empty PDF) → all-page original-detail inspection → a11y 0/0/0 → read-only hash/OOXML checks. No DOCX write occurred after this chain.
