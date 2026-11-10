# TxnMem CCF-A 中文初稿：state-verified DOCX QA

## 最终交付物与可复现性

- 最终外部交付物：`<external-output-dir>/TxnMem_CCF-A中文论文初稿_state_verified.docx`
- SHA-256：`d5fa35b3f1312fff5c7e3e64c8ff26999d15818b3a62162d0be2ae82d9a5152f`
- 最终页数：26（`page-1.png` 至 `page-26.png`）。
- 内容计数：图 6；表 8；参考文献 32（连续 `[R01]`—`[R32]`）；Heading 1/2/3 为 12/18/5。
- 两次独立正式构建均产生上述 SHA-256，`cmp -s` 返回 0；外部交付文件与第二次构建逐字节相同。

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

## Final-review URL 协议可见性修复（TDD）

RED：在构建器仍将每条参考文献写成单一 run 时，新增回归
`test_long_reference_urls_have_reader_safe_protocol_and_path_runs` 失败：R27/R28
正文虽含完整 URL，但协议/主机没有独立、可控的换行表示，预期 URL run 序列与实际空序列不符。

```text
python3 -m unittest \
  tests.test_txnmem_ccfa_docx.TxnMemCcfaDocxTests.test_long_reference_urls_have_reader_safe_protocol_and_path_runs
Ran 1 test in 0.527s
FAILED (failures=1)
```

GREEN：参考文献段落仍保留逐字符完全一致的 catalog 文本，但 URL 按“协议+主机/”及后续路径段
构造成确定性 run 序列。R27/R28 的首个 URL run 均为 `https://www.usenix.org/`；后续只在
`conference/`、`osdi14/`、`technical-sessions/`、`presentation/` 等有意义的路径边界获得
换行机会。完整 URL、参考文献顺序和复制语义不变；未缩小参考文献字号或调整页边距。

```text
python3 -m unittest \
  tests.test_txnmem_ccfa_docx.TxnMemCcfaDocxTests.test_long_reference_urls_have_reader_safe_protocol_and_path_runs \
  tests.test_txnmem_ccfa_docx.TxnMemCcfaDocxTests.test_references_are_complete_and_stably_ordered
Ran 2 tests in 0.475s
OK
```

## Fix round 1 修复与 TDD 证据

RED：未修改构建器时，新增的三项回归测试均失败：DOCX 含独立的 `[`、`]` 段落；截断 PNG 仅凭 IHDR 尺寸被接受；表 4/6/7 仍使用导致无意义断词的旧列宽。

GREEN：构建器现在显式解析 `\[`…`\]` display-math block，丢弃界定符并将公式作为一个居中段落输出；PNG cache/raster gate 要求格式、尺寸、`Image.verify()` 和完整像素载入均成功；表 4、6、7 分别使用 `1600/2700/3150/1910`、`1800/2800/3000/1760`、`2300/2300/4760` DXA 固定列宽。表 6 只为 `Qwen + Qdrant + Neo4j` 加入语义断点；表 7 只在下划线边界加入展示断点，去除换行即可恢复原 active claim/public evidence ID。未全局缩小字体或页边距。

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
  -> 209 个受控直接 run 格式、191 个受控直接段落格式；Calibri 5,202 字符
a11y_audit.py --out_json <a11y-report.json> <final-docx>
  -> high=0 medium=0 low=0
```

额外 package/OOXML 检查：图 6、表 8、参考文献 32；无 `rsid`、`w:ins`/`w:del`/移动修订、`comments.xml`、`people.xml` 或 `docProps/custom.xml`；核心属性无 creator/lastModifiedBy；无外部或绝对 relationship target；无独立 display-math 界定符。R27/R28 的完整段落文本与 catalog 逐字符相同，首个 URL run 均为完整 `https://www.usenix.org/`。`privacy_scrub.py` 报告 0 个待移除的 rsid/core/custom/relationship 项；其重打包输出不作为正式交付物。

## 渲染与人工检查

第二次构建的精确字节使用仓库 wrapper 渲染为 PDF 与 26 个 PNG。Final-review 在原始分辨率逐页打开并检查全部参考文献/附录页及相邻页 22—26；R27/R28 的完整 `https://` 协议清晰可见，且无新增裁切、重叠、空白页或表格回归。

| 页 | Final-review 检查结果 |
| --- | --- |
| 22 | Related Work 收尾与相邻正文连续，无裁切、重叠或标题孤行。 |
| 23 | 结论、参考文献标题及 R01—R07 完整，悬挂缩进与页码正常。 |
| 24 | R08—R22 及其 URL 完整可读，无协议或行首裁切。 |
| 25 | R23—R32 完整；R27 显示 `https://www.usenix.org/.../yuan`，R28 显示 `https://www.usenix.org/.../zheng_mai`，两者协议均从 `h` 开始且未裁切；表 7 起始完整。 |
| 26 | 表 7 续页重复表头，表 8 与附录解释完整；无表格行拆分、裁切或空白终页。 |

此前 Fix round 1 还在原始分辨率检查了第 4—6、15—17、19—22、25—26 页：公式、图 1/5/6、表 1/4/5/6/7/8、CJK 字形、标题衔接和语义换行均无异常。本轮 URL run 边界没有改变总页数；仍不存在第 27 页。

## 最终测试

```text
python3 -m unittest tests.test_txnmem_ccfa_docx \
  tests.test_txnmem_manuscript_audit tests.test_txnmem_paper_projection
Ran 43 tests in 1.859s
OK
```

正式 DOCX 不纳入 Git；临时 build/render 目录不纳入 Git；外部交付物只复制经审计和视觉核准的第二次构建字节。
