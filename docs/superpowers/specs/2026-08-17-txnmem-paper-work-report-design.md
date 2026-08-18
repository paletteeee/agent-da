# TxnMem 论文工作与实验总报告设计

## 目标与受众

生成一份面向作者、导师和内部评审的中文技术总报告，回答五个问题：论文研究什么、创新在哪里、系统如何实现、每组实验为什么做、数据从哪里来以及规模多大。报告只使用 `configs/paper_claims.json` 中的 active claims 和当前未作废证据，不把历史结果、projection replay 或 workflow reward 扩大解释为 memory accuracy。

## 交付物

- 可版本化源稿：`docs/txnmem_paper_work_and_experiment_report_zh.md`。
- 确定性构建器：`scripts/build_txnmem_paper_work_report_docx.py`。
- 文档 QA 记录：`docs/paper/txnmem_paper_work_report_qa_zh.md`。
- 外部 DOCX：`<external-output-dir>/TxnMem_论文工作与实验总报告.docx`。

## 内容结构

1. 执行摘要：一句话问题、三项系统创新、三项方法创新和主要证据。
2. 研究问题与论文定位：说明共享 memory 是受策略约束的派生状态，而非单纯检索缓存。
3. 创新点：Agent Memory Transaction、Policy-Consistent Commit、Provenance-Driven Repair，以及独立 reference semantics、因果 schedule、mutation/witness 和分层证据链。
4. 实现：确定性 core/reference simulator、原生 event contract、SQLite、Qdrant、Neo4j、Toxiproxy、Qwen2.5-7B 与公开 runtime 边界。
5. 数据：8 个合成 workload family、50 seeds、400 instances、2,000 variant rows；公开 benchmark native/runtime 数据；trace-grounded projection；真实服务重复和跨主机运行。
6. 实验矩阵：按 RQ1--RQ5 解释目的、变量、判定器、数据量、主要结果和不能支持的结论。
7. 创新与实验闭环：逐项映射每个创新由哪些实验验证。
8. 证据治理：15 条 active claims、163/163 assertions、supersession、哈希、DOCX QA 和测试。
9. 局限与投稿状态：明确当前非目标和剩余 venue 模板/作者修订工作。

## 视觉与版式

- 采用 `standard_business_brief`：US Letter、四边 1 英寸、Calibri 11 pt、中文 eastAsia 字体 Hiragino Sans GB、正文 1.10 倍行距、段后 6 pt。
- 首页采用 `editorial_cover`，但不使用大面积装饰或表格布局；保留标题、说明、日期和审计状态。
- Heading 1/2/3 分别为 16/13/12 pt，颜色依次为 `#2E74B5`、`#2E74B5`、`#1F4D78`。
- 表格固定 9360 DXA，`tblInd=120`，表头 `#F2F4F7`，按内容设置列宽并允许行自动增长。
- 复用 `architecture.svg`、`commit_protocol.svg`、`provenance_repair.svg`、`controlled_results.svg` 和 `evidence_layers.svg`，仅在图比文字更能说明关系时出现。

## 数据与正确性约束

- 所有正式数值必须能追溯到 active artifact；已 superseded artifact 只可解释历史，不可承担结论。
- 统计单位严格区分 instance、variant row、task episode、conversation、native event、服务 repetition 和跨主机 repetition。
- τ-bench reward 不称为 memory accuracy；AppWorld/LoCoMo 只作固定条件下的描述性证据；AppWorld projection 不称为 native memory ground truth。
- Toxiproxy 5×30 只支持被测单机五场景的 readback-confirmed complete-or-absent 结果，不支持一般分布式事务、跨主机容错、可用性、线性一致性或生产延迟。
- 跨主机 v8 只支持 1 Agent-worker host 到 1 model-server host 的三次独立 attested 运行；货币成本因没有明确费率而不计算。

## 验收标准

- 源稿覆盖用户要求的“做了什么、创新点、如何通过实验验证、实验目的、数据来源和数据量”。
- 自动测试检查关键样本量、结果、边界措辞和 active evidence path。
- 构建两次 SHA-256 一致；DOCX 隐私、结构、表格几何和可访问性审计通过。
- 完整渲染为 PNG，并逐页在 100% 视图检查无裁切、重叠、坏表格、缺字或异常分页。
