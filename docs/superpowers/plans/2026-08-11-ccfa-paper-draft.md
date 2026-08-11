# TxnMem CCF-A 中文论文初稿 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 基于已审计实验结果，生成一篇按 OSDI/SOSP 系统论文标准组织、可编辑且通过逐页视觉验收的 TxnMem 中文论文初稿 DOCX。

**Architecture:** 以版本化 Markdown 保存论文正文，以机器可读 evidence map 和 verified reference catalog 约束数字与引用；Python 构建器将正文、结果表和矢量图统一生成 DOCX。独立 manuscript audit 在构建前检查章节、正式数字、被作废结果和 claim boundary，文档管线在构建后执行结构、视觉、可访问性和隐私审计。

**Tech Stack:** Python 3 标准库、python-docx、matplotlib、现有 TxnMem claim/artifact audit、LibreOffice DOCX renderer、Word OOXML。

## Global Constraints

- 论文主线固定为 OSDI/SOSP 风格的系统正确性论文，中文单栏工作稿，不预先套具体会议模板。
- 中心命题、四项贡献和章节范围以 `docs/superpowers/specs/2026-08-11-ccfa-paper-draft-design.md` 为准。
- 只引用 active claim artifact；旧 backend timing、v6/v7 cross-host 和历史 status 文件不得承担正文结论。
- AppWorld、LoCoMo、realism 和 cross-host 结果按设计规格中的 claim boundary 描述，不写成总体显著提升或生产性能。
- 所有外部技术引用只使用原论文、正式 proceedings、作者/项目官方页面等 primary sources；题名、作者、年份和 venue 必须核验。
- 新 DOCX 采用 `narrative_proposal` 预设的克制学术变体，使用真实 Heading/Caption/List 样式、显式表格几何和连续页码。
- 最终 DOCX 固定输出到 `/Users/xiaoyan_zhu/Desktop/agent-db/outputs/TxnMem_CCF-A中文论文初稿.docx`。
- 每个有意义的 DOCX 修改批次都必须重新 render 并检查全部页面；最终 a11y high/medium/low findings 均为 0。

---

### Task 1: 冻结论文证据接口与验收测试

**Files:**
- Create: `configs/txnmem_ccfa_paper.json`
- Create: `src/txnmem_manuscript_audit.py`
- Create: `tests/test_txnmem_manuscript_audit.py`
- Create: `docs/paper/evidence_map_zh.md`

**Interfaces:**
- Consumes: `configs/paper_claims.json`, `results/paper_evidence/claim_audit.json`, `results/paper_evidence/supersession_index.json`。
- Produces: `load_paper_config(path: Path) -> dict`, `audit_text(text: str, root: Path, config: dict) -> dict`, `audit_manuscript(source: Path, root: Path, config: dict) -> dict`，以及正文允许引用的 claim ID/数字/边界清单。

- [ ] **Step 1: 写 manuscript audit 的失败测试**

```python
class ManuscriptAuditTests(unittest.TestCase):
    def test_rejects_superseded_artifact(self):
        report = audit_text(
            "结果来自 results/real_backend_performance_reps30_v2/results/backend_performance.json",
            root=ROOT,
            config=CONFIG,
        )
        self.assertIn("superseded_artifact", {item["code"] for item in report["findings"]})

    def test_accepts_required_sections_and_active_claims(self):
        report = audit_manuscript(FIXTURE, ROOT, CONFIG)
        self.assertEqual(report["finding_count"], 0)
```

- [ ] **Step 2: 运行测试并确认缺少模块**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_manuscript_audit -v`

Expected: FAIL，提示 `txnmem_manuscript_audit` 不存在。

- [ ] **Step 3: 创建论文配置**

`configs/txnmem_ccfa_paper.json` 明确记录：标题、匿名状态、`drafting_mode=true`、必需章节、active claim IDs、禁止 artifact、必须出现的 claim boundary、正文和附录图表 ID、目标输出路径。active claim IDs 直接取自当前 `configs/paper_claims.json`，不得复制未经 ledger 覆盖的新数字。`drafting_mode` 只允许缺少尚未撰写的后半章节，不放宽 superseded artifact、正式数字或 claim-boundary 检查。

- [ ] **Step 4: 实现 fail-closed manuscript audit**

```python
def audit_text(text: str, root: Path, config: dict) -> dict:
    findings = []
    findings.extend(_check_required_sections(text, config["required_sections"]))
    findings.extend(_check_forbidden_artifacts(text, config["forbidden_artifacts"]))
    findings.extend(_check_required_boundaries(text, config["required_claim_boundaries"]))
    findings.extend(_check_claim_values(text, root / "configs/paper_claims.json", config))
    return {"finding_count": len(findings), "findings": findings}

def audit_manuscript(source: Path, root: Path, config: dict) -> dict:
    return audit_text(source.read_text(encoding="utf-8"), root, config)
```

CLI 固定为：

`PYTHONPATH=src python3 src/txnmem_manuscript_audit.py --root . --config configs/txnmem_ccfa_paper.json --source docs/paper/txnmem_ccfa_draft_zh.md --out results/paper_evidence/manuscript_audit.json`

- [ ] **Step 5: 写 evidence map**

`docs/paper/evidence_map_zh.md` 按 RQ1--RQ5 列出正文可使用数字、artifact、统计单位、claim boundary 和预定章节。该文件只做作者侧索引，不直接粘贴进论文。

- [ ] **Step 6: 运行测试与现有 claim audit**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_manuscript_audit tests.test_txnmem_claim_audit -v`

Expected: PASS。

- [ ] **Step 7: 提交证据接口**

```bash
git add configs/txnmem_ccfa_paper.json src/txnmem_manuscript_audit.py tests/test_txnmem_manuscript_audit.py docs/paper/evidence_map_zh.md
git commit -m "paper: freeze manuscript evidence contract"
```

### Task 2: 核验并固化参考文献

**Files:**
- Create: `configs/txnmem_paper_references.json`
- Create: `docs/paper/reference_audit_zh.md`
- Create: `tests/test_txnmem_paper_references.py`

**Interfaces:**
- Consumes: primary-source metadata collected from official proceedings/arXiv/project pages。
- Produces: 30--45 条稳定编号文献记录，每条包含 `id`, `authors`, `title`, `venue`, `year`, `url`, `topics`, `verified_source`。

- [ ] **Step 1: 写 reference catalog 的失败测试**

```python
def test_reference_catalog_is_complete_and_unique(self):
    rows = json.loads(CATALOG.read_text(encoding="utf-8"))["references"]
    self.assertGreaterEqual(len(rows), 30)
    self.assertEqual(len({row["id"] for row in rows}), len(rows))
    for row in rows:
        self.assertTrue(all(row.get(k) for k in ("authors", "title", "venue", "year", "url")))
```

- [ ] **Step 2: 收集 primary sources**

分组核验 Agent memory、RAG/long-term memory、τ-bench/AppWorld/LoCoMo、transaction/serializability、provenance/lineage、access control、failure injection/model checking。技术检索只采用原论文或正式 proceedings；每条记录在 `verified_source` 中保存核验 URL。

- [ ] **Step 3: 生成 reference catalog 与审计说明**

按正文首次出现顺序预分配稳定 ID；`docs/paper/reference_audit_zh.md` 记录每组文献为何相关、与 TxnMem 的差距，以及被排除的无法确认引用。删除或修正当前工作稿中无法由 primary source 确认的 venue/年份。

- [ ] **Step 4: 运行 reference tests**

Run: `python3 -m unittest tests.test_txnmem_paper_references -v`

Expected: PASS，文献数在 30--45，ID 唯一，字段完整。

- [ ] **Step 5: 提交参考文献**

```bash
git add configs/txnmem_paper_references.json docs/paper/reference_audit_zh.md tests/test_txnmem_paper_references.py
git commit -m "paper: verify related-work references"
```

### Task 3: 重写论文前半部分

**Files:**
- Create: `docs/paper/txnmem_ccfa_draft_zh.md`
- Test: `tests/test_txnmem_manuscript_audit.py`

**Interfaces:**
- Consumes: design spec、evidence map、reference catalog、原始 idea DOCX/PDF。
- Produces: 摘要、引言、背景、问题模型、TxnMem 设计和实现章节，以及 `[[FIG:...]]`/`[[TABLE:...]]` 插入标记。

- [ ] **Step 1: 建立完整章节骨架**

按以下一级结构创建正文：摘要、1 引言、2 背景与动机、3 系统模型与正确性、4 TxnMem 设计、5 实现、6 评估、7 讨论与局限性、8 相关工作、9 结论、参考文献、附录。所有章节立即填入内容，不保留空标题或占位段落。

- [ ] **Step 2: 写摘要与引言**

摘要采用“问题--洞见--设计--方法--关键结果--边界”六句群结构；引言以地址/订单协作案例串联 crash、revoke 和 source invalidation。引言贡献固定为设计规格中的四项，不列工程清单。

- [ ] **Step 3: 写背景与差距**

使用 `[[TABLE:requirements_gap]]` 对比 semantic retrieval、shared writes、commit-time policy、derived-state repair 和 fault-aware validation。每个比较结论都关联 reference catalog 中的已核验文献 ID。

- [ ] **Step 4: 写系统模型与不变量**

定义 `F=(A,M,P,T,G)`、memory object、操作、policy version、合法线性化结果和六类不变量。把 reference semantics 的独立性与 commit-boundary crash 的多合法结果写入正式定义。

- [ ] **Step 5: 写三个设计机制**

每个机制按威胁、目标、算法、失败处理和边界组织。加入 `[[FIG:motivation_timeline]]`、`[[FIG:architecture]]`、`[[FIG:commit_protocol]]`、`[[FIG:provenance_repair]]`，并给出 commit/repair 伪代码块。

- [ ] **Step 6: 写实现章节**

说明模块边界、event contract、failure controller、SQLite/VectorGraph backend 和 Qwen tool loop。仅描述已实现的 abort、invalid/stale/repair 路径；redact 和 scope downgrade 明确为扩展。

- [ ] **Step 7: 运行 manuscript audit 的章节检查**

Run: `PYTHONPATH=src python3 -m unittest tests.test_txnmem_manuscript_audit -v`

Expected: 章节、禁用路径和 claim-boundary 检查通过；评估章节尚未完成的 fixture 检查通过配置中的 drafting mode。

- [ ] **Step 8: 提交论文前半部分**

```bash
git add docs/paper/txnmem_ccfa_draft_zh.md tests/test_txnmem_manuscript_audit.py
git commit -m "paper: draft system model and TxnMem design"
```

### Task 4: 写完整评估、讨论与相关工作

**Files:**
- Modify: `docs/paper/txnmem_ccfa_draft_zh.md`
- Modify: `configs/txnmem_ccfa_paper.json`
- Modify: `tests/test_txnmem_manuscript_audit.py`

**Interfaces:**
- Consumes: RQ1--RQ5 evidence map、active claim artifacts、reference catalog。
- Produces: 完整可审计正文，所有正式数字均由 manuscript audit 覆盖。

- [ ] **Step 1: 写评估方法与统计单位**

明确 workload、seed、variant、task、conversation、native event 和 repetition 的不同分母；将 official evaluator、TxnMem oracle、event contract 和 backend consistency 分为四层。

- [ ] **Step 2: 写 RQ1 正确性结果**

报告 400/2,000 口径、TxnMem 0/400 与 400/400 oracle match、四个对照的 350/200/50/100 违规实例。使用 `[[FIG:controlled_results]]` 和 `[[TABLE:controlled_results]]`，回答机制与对应违规之间的因果关系。

- [ ] **Step 3: 写 RQ2 schedule 与 mutation**

报告 causal 400、detection 0.875，random 4,000、detection 0.750，mutation kill rate 和四个 2/1/6/1 prefix-minimal witness。说明该比较不是生产故障概率估计。

- [ ] **Step 4: 写 RQ3 真实模型和公开 runtime**

主结果报告 Qwen 5×10 的 50/50 contract、50/50 oracle；τ-bench、AppWorld、LoCoMo 放入外部有效性小节。AppWorld 使用全 20 task 分母并保留 6 execution failures；LoCoMo +0.00162 只作描述性结果。

- [ ] **Step 5: 写 RQ4 真实服务与跨主机边界**

报告 Toxiproxy 5×30、四个 non-normal 路径证据 30/30、150 次 0 partial commit，以及 5-task E2E 5/5、30 events。cross-host v8 只用于证明 client-to-model-server 拓扑与 token accounting，不写成生产性能。

- [ ] **Step 6: 写 RQ5 realism 负结果**

报告 τ-bench/LoCoMo/AppWorld 的 MMD²、p 值和 holdout 样本边界，结论固定为 generator 仍需校准。不得使用“接近真实分布”或“证明等价”的表述。

- [ ] **Step 7: 写讨论、相关工作和结论**

讨论 synthetic benchmark 的说服力来源、确定性 policy 范围和内容正确性边界。相关工作按四组对比并使用 verified reference IDs。结论只回扣中心命题、三个机制和最强证据。

- [ ] **Step 8: 将 `drafting_mode` 设为 `false` 并运行严格 audit**

Run: `PYTHONPATH=src python3 src/txnmem_manuscript_audit.py --root . --config configs/txnmem_ccfa_paper.json --source docs/paper/txnmem_ccfa_draft_zh.md --out results/paper_evidence/manuscript_audit.json`

Expected: `finding_count=0`。

- [ ] **Step 9: 提交完整正文**

```bash
git add docs/paper/txnmem_ccfa_draft_zh.md configs/txnmem_ccfa_paper.json tests/test_txnmem_manuscript_audit.py results/paper_evidence/manuscript_audit.json
git commit -m "paper: complete CCF-A Chinese manuscript"
```

### Task 5: 生成论文图表

**Files:**
- Create: `scripts/build_txnmem_paper_figures.py`
- Create: `tests/test_txnmem_paper_figures.py`
- Create: `paper_assets/figures/*.svg`
- Create: `paper_assets/figures/manifest.json`

**Interfaces:**
- Consumes: controlled summary、schedule baseline、minimal witnesses、submission aggregates。
- Produces: `build_all(root: Path, out_dir: Path) -> dict`，生成设计规格中的六幅图和带 source hash 的 manifest。

- [ ] **Step 1: 写 figure manifest 的失败测试**

```python
def test_all_required_figures_are_generated(self):
    manifest = build_all(ROOT, OUT)
    self.assertEqual(
        set(manifest["figures"]),
        {"motivation_timeline", "architecture", "commit_protocol",
         "provenance_repair", "controlled_results", "evidence_layers"},
    )
    self.assertTrue(all((OUT / item["file"]).stat().st_size > 0 for item in manifest["figures"].values()))
```

- [ ] **Step 2: 运行测试并确认缺少构建器**

Run: `PYTHONPATH=scripts python3 -m unittest tests.test_txnmem_paper_figures -v`

Expected: FAIL，提示 figure builder 不存在。

- [ ] **Step 3: 实现四幅系统示意图**

使用 matplotlib patches/SVG 生成 motivation timeline、architecture、commit protocol 和 provenance repair。所有标签为中文或必要英文术语，字体使用 Arial Unicode MS/Hiragino Sans GB，颜色保持深蓝、灰、红三色以内。

- [ ] **Step 4: 实现两幅结果图**

controlled results 直接读取 `results/paper_evidence/controlled_suite.json`；evidence layers 从 paper config 生成层级图。图中不编码不存在的显著性标记。

- [ ] **Step 5: 运行测试并视觉检查 SVG/PNG**

Run: `PYTHONPATH=scripts python3 scripts/build_txnmem_paper_figures.py --root . --out-dir paper_assets/figures`

Expected: 6 幅 SVG 和 manifest；逐图渲染检查无裁切、乱码和标签重叠。

- [ ] **Step 6: 提交图表**

```bash
git add scripts/build_txnmem_paper_figures.py tests/test_txnmem_paper_figures.py paper_assets/figures
git commit -m "paper: add evidence-backed manuscript figures"
```

### Task 6: 构建可重复生成的 DOCX

**Files:**
- Create: `scripts/build_txnmem_ccfa_docx.py`
- Create: `tests/test_txnmem_ccfa_docx.py`
- Modify: `scripts/fontconfig-macos.conf`
- Create: `/Users/xiaoyan_zhu/Desktop/agent-db/outputs/TxnMem_CCF-A中文论文初稿.docx`

**Interfaces:**
- Consumes: Markdown 正文、paper config、reference catalog、figure manifest、active result JSON。
- Produces: `build_document(root: Path, output: Path) -> Path` 和最终 DOCX。

- [ ] **Step 1: 阅读并固化文档 token**

完整读取 documents skill 的 `references/design_presets.md`、`references/header_templates.md`、`tasks/create_edit.md`、`tasks/verify_render.md`、`tasks/accessibility_a11y.md` 和 `tasks/privacy_scrub_metadata.md`。把 `narrative_proposal` 学术变体的页面、字体、段落、标题、表格、图注和页码 token 写入构建器常量。

- [ ] **Step 2: 写 DOCX 结构失败测试**

```python
def test_generated_docx_has_paper_structure(self):
    path = build_document(ROOT, OUT)
    doc = Document(path)
    headings = [p.text for p in doc.paragraphs if p.style.name.startswith("Heading")]
    self.assertIn("1 引言", headings)
    self.assertIn("6 评估", headings)
    self.assertGreaterEqual(len(doc.inline_shapes), 6)
    self.assertGreaterEqual(len(doc.tables), 6)
```

- [ ] **Step 3: 运行测试并确认构建器缺失**

Run: `/Users/xiaoyan_zhu/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m unittest tests.test_txnmem_ccfa_docx -v`

Expected: FAIL，提示 builder 不存在。

- [ ] **Step 4: 实现 Markdown/figure/table 渲染**

构建器解析一级至三级标题、段落、真实列表、伪代码和 `[[FIG:id]]`/`[[TABLE:id]]` 标记。表格使用显式 DXA geometry、重复表头和单元格 padding；图片插入后紧跟 Caption style。中英文标题、匿名标记、摘要和关键词直接放在第一页，不生成报告式封面。

- [ ] **Step 5: 实现参考文献与页眉页脚**

参考文献按 catalog 稳定 ID 生成 hanging indent；页眉只显示短标题，页脚使用连续页码。清除默认 Word 作者、公司和 lastModifiedBy 元数据。

- [ ] **Step 6: 生成 DOCX 并运行结构测试**

Run: `/Users/xiaoyan_zhu/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 scripts/build_txnmem_ccfa_docx.py --root . --output /Users/xiaoyan_zhu/Desktop/agent-db/outputs/TxnMem_CCF-A中文论文初稿.docx`

Run: `/Users/xiaoyan_zhu/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m unittest tests.test_txnmem_ccfa_docx -v`

Expected: PASS，DOCX 非空且结构满足配置。

- [ ] **Step 7: 提交构建管线**

```bash
git add scripts/build_txnmem_ccfa_docx.py tests/test_txnmem_ccfa_docx.py scripts/fontconfig-macos.conf
git commit -m "paper: build reproducible CCF-A manuscript DOCX"
```

### Task 7: 文档渲染、可访问性与隐私 QA

**Files:**
- Create: `/Users/xiaoyan_zhu/Desktop/agent-db/outputs/TxnMem_CCF-A中文论文初稿_render_<version>/page-*.png`
- Create: `/Users/xiaoyan_zhu/Desktop/agent-db/outputs/TxnMem_CCF-A中文论文初稿_a11y.json`
- Create: `docs/paper/txnmem_ccfa_docx_qa_zh.md`
- Modify: `scripts/build_txnmem_ccfa_docx.py`（仅在视觉缺陷修正时）

**Interfaces:**
- Consumes: Task 6 DOCX。
- Produces: 逐页 render、a11y report、结构/样式/隐私审计结果和最终修正版 DOCX。

- [ ] **Step 1: 渲染 DOCX 为 PNG/PDF**

Run: `scripts/render_docx_with_bundled_libs.sh /Users/xiaoyan_zhu/Desktop/agent-db/outputs/TxnMem_CCF-A中文论文初稿.docx --output_dir /Users/xiaoyan_zhu/Desktop/agent-db/outputs/TxnMem_CCF-A中文论文初稿_render_v1 --emit_pdf`

Expected: 每页一个 PNG，PDF 非空。

- [ ] **Step 2: 逐页视觉检查**

以 100% 查看每一页，检查 CJK glyph、标题孤行、表格跨页、图注、图片分辨率、引用悬挂缩进、页眉页脚、异常空白、裁切和重叠。发现任何问题即修改 builder、重新生成并使用新的 render 目录；最终版本必须再次检查全部页面。

- [ ] **Step 3: 执行结构与样式审计**

运行 heading audit、section audit、images audit、style lint 和 table geometry audit。将页数、heading/table/figure 数、发现与修复记录写入 `docs/paper/txnmem_ccfa_docx_qa_zh.md`。

- [ ] **Step 4: 执行隐私清理、最终 render 与 a11y**

先用 `privacy_scrub.py` 生成脱敏临时文件并原子替换最终输出；然后重新 render 脱敏后的最终 DOCX，逐页复查，再运行：

Run: `/Users/xiaoyan_zhu/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 /Users/xiaoyan_zhu/.codex/plugins/cache/openai-primary-runtime/documents/26.805.11740/skills/documents/scripts/a11y_audit.py /Users/xiaoyan_zhu/Desktop/agent-db/outputs/TxnMem_CCF-A中文论文初稿.docx --out_json /Users/xiaoyan_zhu/Desktop/agent-db/outputs/TxnMem_CCF-A中文论文初稿_a11y.json`

Expected: high=0、medium=0、low=0，最终 render 对应的正是隐私清理后的交付文件。

- [ ] **Step 5: 提交 QA 记录与必要修复**

```bash
git add scripts/build_txnmem_ccfa_docx.py docs/paper/txnmem_ccfa_docx_qa_zh.md
git commit -m "docs: verify CCF-A manuscript rendering"
```

### Task 8: 最终复验与交付

**Files:**
- Modify: `docs/current_experiment_report_zh.md`
- Modify: `docs/formal_paper_task_status_zh.md`
- Final output: `/Users/xiaoyan_zhu/Desktop/agent-db/outputs/TxnMem_CCF-A中文论文初稿.docx`

**Interfaces:**
- Consumes: 全部论文源、构建器、图表、audit 和最终 DOCX。
- Produces: 可交付初稿、更新后的任务状态和干净 Git 提交。

- [ ] **Step 1: 运行全部代码与论文测试**

Run: `PYTHONPYCACHEPREFIX=/private/tmp/txnmem_pycache PYTHONPATH=src python3 -m unittest discover -s tests -p 'test*.py'`

Expected: 0 failures；跳过项只允许为已有环境条件跳过。

- [ ] **Step 2: 重跑正式审计**

Run: `PYTHONPATH=src python3 src/txnmem_claim_audit.py audit --root . --ledger configs/paper_claims.json --out results/paper_evidence/claim_audit.json`

Run: `PYTHONPATH=src python3 src/txnmem_artifact_audit.py --root .`

Run: `PYTHONPATH=src python3 src/txnmem_manuscript_audit.py --root . --config configs/txnmem_ccfa_paper.json --source docs/paper/txnmem_ccfa_draft_zh.md --out results/paper_evidence/manuscript_audit.json`

Expected: 三项均为 0 findings。

- [ ] **Step 3: 检查最终文档与凭据**

确认最终 DOCX 的 SHA-256、页数、a11y counts 和最新 render 目录；扫描新增 Git diff，确保没有 SSH 密码、私有 token、未脱敏 endpoint 凭据或本机个人元数据。

- [ ] **Step 4: 更新正式状态文档**

在 `docs/current_experiment_report_zh.md` 和 `docs/formal_paper_task_status_zh.md` 中记录中文 CCF-A 初稿路径、页数、图表数、引用数、审计结果和仍需在选定 venue 后完成的模板适配。

- [ ] **Step 5: 最终提交**

```bash
git add docs/current_experiment_report_zh.md docs/formal_paper_task_status_zh.md results/paper_evidence/claim_audit.json results/paper_evidence/manuscript_audit.json
git commit -m "paper: finalize CCF-A Chinese draft"
```

- [ ] **Step 6: 交付 DOCX**

最终回复只交付 `/Users/xiaoyan_zhu/Desktop/agent-db/outputs/TxnMem_CCF-A中文论文初稿.docx`，说明正文已按系统论文主线重写并通过逐页视觉、a11y、claim、artifact 和全量测试验收；不链接内部 PNG/PDF QA 文件。
