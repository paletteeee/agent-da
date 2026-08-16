# TxnMem CCF-A 中文初稿：state-verified DOCX QA

## 交付物与可复现性

- 最终外部交付物：`<external-output-dir>/TxnMem_CCF-A中文论文初稿_state_verified.docx`
- SHA-256：`07e8495f72ee1adf68bef026143d8dce39e12bb098c917ee5720b00f0692a118`
- 最终页数：27（`page-1.png` 至 `page-27.png`）。
- 内容计数：图 6；表 8；参考文献 32（连续 `[R01]`—`[R32]`）；Heading 1/2/3 为 12/18/5。
- 两次独立正式构建均产生上述 SHA-256，`cmp -s` 返回 0；外部交付文件与第二次构建逐字节相同。

所有正式构建均由 `scripts/build_txnmem_ccfa_docx.py` 完成，输出与栅格缓存位于工作树外的临时目录。DOCX 中的 SVG 图由构建器以完整画布栅格化；构建器在确认精确尺寸 PNG 已写入后终止自身隔离的 headless-browser 进程组，避免该浏览器环境在截图后继续存活而影响构建完成性。

隐私安全的复现命令（占位符不编码本机路径或 runtime 版本）：

```text
<bundled-python> scripts/build_txnmem_ccfa_docx.py --root . \
  --output <build-1>/TxnMem_CCF-A中文论文初稿_state_verified.docx \
  --raster-cache <build-1>/raster-cache
<bundled-python> scripts/build_txnmem_ccfa_docx.py --root . \
  --output <build-2>/TxnMem_CCF-A中文论文初稿_state_verified.docx \
  --raster-cache <build-2>/raster-cache
shasum -a 256 <build-1>/TxnMem_CCF-A中文论文初稿_state_verified.docx
shasum -a 256 <build-2>/TxnMem_CCF-A中文论文初稿_state_verified.docx
cmp -s <build-1>/TxnMem_CCF-A中文论文初稿_state_verified.docx \
  <build-2>/TxnMem_CCF-A中文论文初稿_state_verified.docx

TXNMEM_CODEX_DEPS=<bundled-runtime> TXNMEM_RENDERER=<render-docx.py> \
  scripts/render_docx_with_bundled_libs.sh \
  <build-2>/TxnMem_CCF-A中文论文初稿_state_verified.docx \
  --output_dir <render-dir> --emit_pdf
```

## 结构、隐私、关系与可访问性审计

对第二次构建的精确字节执行以下审计，全部通过：

```text
unzip -t <final-docx>
  -> No errors detected in compressed data.

heading_audit.py <final-docx>
  -> Heading 1/2/3 = 12/18/5
section_audit.py <final-docx>
  -> 1 个 Letter 纵向 section，四边均 1.00 in，独立页眉/页脚
images_audit.py <final-docx>
  -> 6 个 inline 图；5.50 in 宽，比例为 2.60/2.94/2.46/2.65/2.85/3.08 in
table_geometry.py <final-docx>
  -> 8/8 表的 tblW、tblInd、tblGrid 与 tcW 一致
style_lint.py --json <style-report.json> <final-docx>
  -> 208 个受控直接 run 格式、190 个受控直接段落格式；Calibri 5,073 字符
a11y_audit.py --out_json <a11y-report.json> <final-docx>
  -> high=0 medium=0 low=0
```

受控直接格式来自 CJK 字体、题名/图题、表格和匿名稿排版；章节层级由 Heading 审计单独验证。附录的两张紧凑表仍保留 9 pt 表头、至少 8.75 pt 表体、`w:tblHeader` 和每行 `w:cantSplit`；表 7 改为 `1440/1440/6480` DXA 三列以减少标识符无意义折行，总宽仍为 9360 DXA。

额外 package/OOXML 检查结果：图 6、表 8、参考文献 32；无 `rsid`、`w:ins`/`w:del`/移动修订、`comments.xml`、`people.xml` 或 `docProps/custom.xml`；核心属性无 creator/lastModifiedBy；无外部或绝对 relationship target；正文无仓库路径、`file:` URI、作者注释或未展开的 FIG/TABLE/CLAIM 标记。所有 32 条参考文献连续且有序。

## 渲染与逐页人工检查

第二次构建的精确字节使用仓库 wrapper 渲染为 PDF 与 PNG。最终 render 目录保留 27 个 PNG，供独立复核。每页在原始分辨率打开；检查 CJK 字形、裁切/重叠、标题孤行、表格行拆分、图可读性、页眉/页码、旧边界文本、参考文献和空页。

| 页 | 检查结果 |
| --- | --- |
| 1 | 中英文题名、匿名稿、摘要、关键词与页码完整；无缺字或裁切。 |
| 2 | 引言与贡献列表连续；无标题孤行。 |
| 3 | 背景正文与层级标题完整。 |
| 4 | 图 1、题注和表 1 起始同页；图表可读。 |
| 5 | 表 1 续页有重复表头，行未拆分。 |
| 6 | 模型正文和形式化文本无裁切。 |
| 7 | 不变量列表完整，行距和项目符号正常。 |
| 8 | 图 2 与题注同页，架构图文字清晰。 |
| 9 | 4.1 节正文自然起始；留白来自不可拆分图题组，无孤立题注。 |
| 10 | 图 3、题注与 COMMIT 伪代码可读。 |
| 11 | commit/repair 正文无重叠。 |
| 12 | 图 4、题注与 REPAIR 伪代码完整。 |
| 13 | 实现章节及 5.3 衔接正常。 |
| 14 | 实现边界与评估开头无异常空白。 |
| 15 | 表 2、表 3 的列宽、边界和字体可读。 |
| 16 | 表 4 完整，随后 RQ1 标题不孤立。 |
| 17 | 图 5、题注和表 5 清晰，未裁切。 |
| 18 | RQ2/RQ3 正文无裁切。 |
| 19 | RQ4 与诊断说明无重叠。 |
| 20 | 图 6、题注和表 6 起始同页。 |
| 21 | 表 6 续页有重复表头，后续 RQ5 正常衔接。 |
| 22 | 讨论与相关工作标题、正文正常。 |
| 23 | 结论与相关工作收束无标题孤行。 |
| 24 | 参考文献 [R01]—[R12] 悬挂缩进正常。 |
| 25 | 参考文献 [R13]—[R27] 完整，无截断。 |
| 26 | [R28]—[R32]、附录和表 7 起始完整。 |
| 27 | 表 7 续页、表 8 与附录解释同页；无孤立末页或空白页。 |

首次渲染发现第 28 页仅包含附录解释段落。修复仅修改构建器：为该解释段落创建可读的 9.5 pt 单倍行距样式，并重平衡表 7 的固定列宽；新增回归测试锁定这两个布局约束。修复后重新两次构建、哈希比较并渲染，最终为 27 页；第 25—27 页及相邻内容已重新逐页检查。

## 最终测试

```text
python3 -m unittest tests.test_txnmem_ccfa_docx \
  tests.test_txnmem_manuscript_audit tests.test_txnmem_paper_projection
  -> Ran 39 tests in 1.862s
  -> OK
```

正式 DOCX 不纳入 Git；临时 build/render 目录不纳入 Git；外部交付物只复制经视觉核准的第二次构建字节。
