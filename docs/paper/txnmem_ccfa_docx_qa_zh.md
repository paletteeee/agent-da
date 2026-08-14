# TxnMem CCF-A 中文初稿：DOCX 渲染、可访问性与隐私 QA

## 交付物与可复现边界

- 最终 DOCX（已脱敏）：`<output-dir>/TxnMem_CCF-A中文论文初稿.docx`
- SHA-256：`6673155ad304ab39f59d6455a1c6ff546f6459735f00dacdc31b617819227714`
- 文件大小：`1,163,774` bytes
- 最终页数：27（最终渲染 PNG 的 `page-1.png` 至 `page-27.png`）
- 内容计数：标题样式为 Heading 1/2/3 = 12/18/4；表格 8；图片/图 6；参考文献 32（严格连续的 `[R01]`—`[R32]`）。
- 渲染器：工作树中的 `scripts/render_docx_with_bundled_libs.sh`，通过 `TXNMEM_CODEX_DEPS=<runtime>` 和 `TXNMEM_RENDERER=<documents-skill>/render_docx.py` 注入版本无关的 bundled runtime。渲染结论只覆盖该 LibreOffice/系统字体环境；shell 中没有 `fc-match`，因此不对其他机器的字体替代作额外保证。

渲染目录（均在 Git 工作树外）：

- 首次基线（含 PDF）：`<output-dir>/TxnMem_CCF-A中文论文初稿_render_v1`
- 迭代目录：`<output-dir>/TxnMem_CCF-A中文论文初稿_render_v2`、`..._v3`、`..._v4`、`..._v5`、`..._v6`、`..._v7`、`..._v8`
- 当前最终、已脱敏 DOCX 的 PDF/PNG（由上述 SHA-256 文件直接渲染）：工作树外 `<temp-dir>/render_final_v6`

配置中的 `outputs/TxnMem_CCF-A中文论文初稿.docx` 是 repo-relative 默认逻辑路径；测试始终改写到临时目录。正式发布命令必须显式传入 `<external-output-dir>/TxnMem_CCF-A中文论文初稿.docx`。本次连续两次正式构建 hash 相同，外部交付文件与第二次构建逐字节相同；工作树 QA 副本在验收后删除，不作为隐式仓库依赖。

## 逐页人工渲染检查

所有页面均以 100%/原始细节检查，而非 contact sheet。每页均检查 CJK 字形、裁切/重叠、页眉页脚和页码；出现图、表、伪代码或参考文献时增加对应检查。

### 首次检查：render_v1（28 页）

| 页 | 已检查内容与结论 |
| --- | --- |
| 1 | 中文题名、英文题名、匿名稿、摘要、关键词和第 1 页页码可读；无缺字。 |
| 2 | 引言正文和项目符号；无标题孤行。 |
| 3 | 背景段落与层级标题；无重叠。 |
| 4 | 图 1、题注、表 1 开始；图中文字与表格边界可读。 |
| 5 | 表 1 续页、表头与后续模型正文；无裁切。 |
| 6 | 系统模型正文与公式式文本；无 CJK 缺字。 |
| 7 | 不变量列表和设计章节起始；无标题孤行。 |
| 8 | 图 2 架构图留在本页，但题注未随图出现（发现隔离题注问题）。 |
| 9 | 图 2 题注单独出现在页首，随后正文；记录为必须修复的问题。 |
| 10 | 图 3、题注和 COMMIT 伪代码；图形清晰度偏低，需检查 SVG 栅格化。 |
| 11 | Policy-Consistent Commit 与 repair 正文；无重叠。 |
| 12 | 图 4、题注和 REPAIR 伪代码；图形清晰度偏低，需检查 SVG 栅格化。 |
| 13 | 实现章节；无标题孤行。 |
| 14 | 实现/评估衔接；无异常空白。 |
| 15 | 表 2、表 3；边界完整，无溢出。 |
| 16 | 表 4；表头和单元格内容可读。 |
| 17 | 图 5、题注、表 5；无裁切。 |
| 18 | RQ2/RQ3 正文；无重叠。 |
| 19 | RQ4 正文；无异常空白。 |
| 20 | 图 6、题注、表 6 开始；无裁切。 |
| 21 | 表 6 续页及讨论章节；续页表头可见。 |
| 22 | 讨论与相关工作；无标题孤行。 |
| 23 | 结论、参考文献开始；悬挂缩进正常。 |
| 24 | 参考文献 [R03]–[R15]；悬挂缩进正常。 |
| 25 | 参考文献 [R16]–[R30]；悬挂缩进正常。 |
| 26 | 参考文献结尾、附录、表 7 开始；表格无裁切。 |
| 27 | 表 7/表 8 及结尾说明；末页内容过少的风险被记录。 |
| 28 | 仅残留稀疏结尾内容；记录为必须消除的孤立末页。 |

### 最终检查：render_final_v6（27 页，当前已脱敏 DOCX，SHA-256 `6673155a…714`）

| 页 | 已检查内容与结论 |
| --- | --- |
| 1 | 题名、匿名稿、摘要、关键词；中英文字体完整，页码正确。 |
| 2 | 引言与贡献列表；正文/项目符号无重叠。 |
| 3 | 背景章节层级；无标题孤行。 |
| 4 | 图 1、题注、表 1 开始；图与题注同页，表格边界完整。 |
| 5 | 表 1 续页且重复表头；完整行未被拆分。 |
| 6 | 模型正文和公式式文本；无缺字/裁切。 |
| 7 | 不变量；列表和页面流正常。 |
| 8 | 图 2 架构图与题注同页；图中文字可读、长宽比正确。 |
| 9 | 设计正文；受控留白来自前页不可拆分图组，无孤立题注。 |
| 10 | 图 3、题注和 COMMIT 伪代码同页；均完整可读。 |
| 11 | commit/repair 正文；无重叠。 |
| 12 | 图 4、题注和 REPAIR 伪代码同页；无裁切。 |
| 13 | 实现章节；标题和段落流正常。 |
| 14 | 实现边界与评估开头；无异常空白。 |
| 15 | 表 2、表 3；列宽、边界与字体可读。 |
| 16 | 表 4；无溢出，标题不孤立。 |
| 17 | 图 5、题注、表 5；图中文字和单元格均可读。 |
| 18 | RQ2/RQ3；无裁切。 |
| 19 | RQ4；无重叠。 |
| 20 | 图 6、题注、表 6 开始；图题同页、比例正确。 |
| 21 | 表 6 续页且重复表头；无行拆分，随后章节衔接正常。 |
| 22 | 讨论与相关工作；标题/正文正常。 |
| 23 | 结论、参考文献开头；参考文献悬挂缩进正常。 |
| 24 | 参考文献 [R03]–[R15]；无截断。 |
| 25 | 参考文献 [R16]–[R30]；无截断。 |
| 26 | [R31]–[R32]、附录及表 7 开始；public evidence ID 取代仓库路径，9 pt 紧凑表格可读。 |
| 27 | 表 7 结尾、表 8 和附录说明同页；没有空页或只含孤立内容的页面，附录以自然尾留白结束。 |

最终逐页检查未发现裁切、重叠、缺失字形、坏表格、孤立题注或异常空白；没有空页或只含孤立内容的页面，最后一页的自然尾留白未隐藏内容。六张图均为全画布栅格，文档内有效文字尺寸可读。

## 问题、源端修复与回归保护

1. SVG 截图以 1× 内容放入 2× 画布，图中文字缩小。构建器改用 SVG 的逻辑宽高配合 `SVG_RASTER_SCALE`，并将缓存版本升级；新增测试要求非白内容占栅格宽高至少 85%。
2. 图 2 在第 8 页、题注在第 9 页。图片段落改为 `keep_with_next`，题注显式取消通用 Caption 的 `keep_with_next` 链，避免后续标题反向挤走图题组；新增图题相邻的 OOXML 测试。
3. 表 1 曾在页边界拆开一行。所有表行写入 `w:cantSplit`，并由测试覆盖；所有表头保持 `w:tblHeader`。
4. 表 7/表 8 导致孤立的第 28 页。仅将这两个附录表的上下内边距、行距和文字降到仍可读的 9 pt，并压缩题注间距；最终为 27 页且最后说明与表 8 同页。
5. 独立审查发现 `word/settings.xml`、`word/styles.xml` 和 `word/stylesWithEffects.xml` 仍含 Word rsid 会话标识。构建器现扫描全部 XML part，删除 local-name 为 `rsids`/`rsidRoot`/`rsid` 的元素及所有 `rsid*` 属性；未改变的 XML 字节保持原样，避免重序列化 `[Content_Types].xml` 造成 LibreOffice 包兼容性问题。最终 OOXML 审计为 0 个 `rsid` 字符串。
6. 表 7 曾把 `artifact_path` 原样投影给读者。现优先显示 artifact payload 的 `evidence_id`，否则生成稳定的 `E-<claim_id>`；仓库内 claim ledger 仍保留路径供作者审计，读者 DOCX 不再含 `results/`、本机绝对路径或 `file:` URI。

所有修复均在 `scripts/build_txnmem_ccfa_docx.py` 中完成，未手工编辑 DOCX。

## 结构、样式、表格与引用审计

使用 bundled Python 对最终 DOCX 执行：

```text
heading_audit.py <docx>                 -> Heading 1/2/3 = 12/18/4
section_audit.py <docx>                 -> 1 个纵向 Letter 章节，四边 1.00 in；页眉/页脚独立
images_audit.py <docx>                  -> 6 个 inline 图，均为 5.50 in 宽
style_lint.py --json ... <docx>         -> 208 个直接 run 格式、190 个直接段落格式
table_geometry.py <docx>                -> 8/8 表的 tblW、tblGrid 与 tcW 全部一致
```

样式 lint 的直接格式来自受控构建器的 CJK 字体、图题、表头/表体与匿名稿排版；其“heading-like”示例是题名及表格单元格，而非漏用 Heading 样式的正文层级。标题审计和逐页渲染确认章节顺序为题名/摘要、1–9、参考文献、附录；不存在 orphan heading。

额外 OOXML 审计确认：8/8 表首行重复、8/8 表所有行带 `w:cantSplit`；全部 32 条参考文献连续有序；无 `comments.xml`、`people.xml`、评论标记或 `w:ins`/`w:del`/移动修订标记。

## 隐私与可访问性

脱敏命令：

```text
python scripts/build_txnmem_ccfa_docx.py --root . --output <output-dir>/TxnMem_CCF-A中文论文初稿.docx
```

构建器直接生成已匿名化的最终包；最终核心属性不含 creator/lastModifiedBy，且无 custom properties、批注、人员或修订部分。保留的题名、`Anonymous manuscript` 主题和关键词是论文公开内容，不含个人标识。严格 rsid 清理由构建器的全 XML package sanitation 完成，最终 OOXML 审计确认所有 XML part 的 `rsid` 字符串数为 0。测试只在临时目录构建，不触碰 repo-relative 默认路径或外部交付物。正式命令连续运行两次，两个 SHA-256 均为 `6673155ad304ab39f59d6455a1c6ff546f6459735f00dacdc31b617819227714`，且外部交付物与第二次构建逐字节相同。`render_final_v6` 由这些精确字节直接渲染；后续 a11y、隐私和哈希检查均为只读。

最终 a11y 命令及结果：

```text
a11y_audit.py --out_json <output-dir>/TxnMem_CCF-A中文论文初稿_a11y.json <final-docx>
# high=0 medium=0 low=0
```

JSON 路径：`<output-dir>/TxnMem_CCF-A中文论文初稿_a11y.json`。六张图均保留有意义的中文替代文字，分别说明：地址—订单风险时间线、TxnMem 架构、提交协议、来源闭包修复、受控套件结果条形图和五层证据链。

## 测试与剩余模板工作

```text
PYTHONPATH=src:scripts <bundled-python> -m unittest tests.test_txnmem_ccfa_docx -v
# Ran 18 tests ... OK

PYTHONPYCACHEPREFIX=<temp-dir>/pycache PYTHONPATH=src:scripts TMPDIR=<temp-dir>/outputs <bundled-python> -m unittest discover -s tests -p 'test*.py'
# 工作树：Ran 346 tests ... OK (skipped=3)
# index-derived clean archive：Ran 346 tests ... OK (skipped=4；额外 skip 为无 .git metadata 的 Git-range 集成扫描)
```

尚未执行、且只能在拿到目标会议信息后进行的工作：将内容导入官方 CCF-A/目标会议模板、按模板处理作者/匿名审稿页、版心和 bibliography 样式，以及在最终投稿环境中再次渲染。当前 DOCX 不声称等同于某一尚未提供的 venue 模板。
