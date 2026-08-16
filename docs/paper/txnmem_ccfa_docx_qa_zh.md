# TxnMem CCF-A 中文初稿：state-verified DOCX QA

## Fix round 1 交付物与可复现性

- 最终外部交付物：`<external-output-dir>/TxnMem_CCF-A中文论文初稿_state_verified.docx`
- SHA-256：`5587f65047ae47ff55c98a69ae773831e7caa9daf53e881a963ac99117e0eb9d`
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

## 修复与 TDD 证据

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

额外 package/OOXML 检查：图 6、表 8、参考文献 32；无 `rsid`、`w:ins`/`w:del`/移动修订、`comments.xml`、`people.xml` 或 `docProps/custom.xml`；核心属性无 creator/lastModifiedBy；无外部或绝对 relationship target；无独立 display-math 界定符。`privacy_scrub.py` 报告 0 个待移除的 rsid/core/custom/relationship 项；其重打包输出不作为正式交付物。

## 渲染与人工检查

第二次构建的精确字节使用仓库 wrapper 渲染为 PDF 与 26 个 PNG。Fix round 1 按变更影响范围在原始分辨率打开并检查第 4—6、15—17、19—22、25—26 页：CJK 字形、裁切/重叠、标题孤行、表格行拆分、图可读性、页眉/页码、参考文献和空页均无异常。

| 页 | Fix round 1 检查结果 |
| --- | --- |
| 4—6 | 图 1、表 1 续页和形式模型相邻正文完整；第 5 页只有一个居中公式段落，无 `[`/`]` 独立段落。 |
| 15—17 | 表 4 跨 15—16 页，`受控 simulator` 保持完整；表 5、图 5 和后续 RQ 标题无裁切或孤立。 |
| 19—22 | 图 6、表 6 和 RQ5/讨论衔接正常；`AppWorld` 未断为 `AppWorl/d`，`Qwen + / Qdrant + / Neo4j` 在语义边界换行。 |
| 25—26 | 参考文献尾部、表 7、表 8 和附录解释完整；claim/evidence ID 仅在下划线边界换行，表 7 有重复表头，末页无空白或裁切。 |

本轮因公式和表格布局重排而从 27 页变为 26 页；不存在第 27 页。

## 最终测试

```text
python3 -m unittest tests.test_txnmem_ccfa_docx \
  tests.test_txnmem_manuscript_audit tests.test_txnmem_paper_projection
Ran 42 tests in 1.791s
OK
```

正式 DOCX 不纳入 Git；临时 build/render 目录不纳入 Git；外部交付物只复制经审计和视觉核准的第二次构建字节。
