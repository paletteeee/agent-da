# TxnMem CCF-A 中文论文初稿设计

## 1. 目标与定位

本轮产出一篇可继续迭代、但在论证完整性上达到 CCF-A 系统论文工作稿标准的中文论文初稿。论文按 OSDI/SOSP 风格组织，不预先套用某一届会议模板；先生成可编辑的单栏 DOCX，内容量约等于 12--14 页英文双栏正文，并保留完整图表、参考文献和必要附录。

论文采用匿名工作稿形式，不填作者与单位。目标读者是熟悉分布式系统、数据库和 LLM Agent 的系统研究者。文章必须把“系统正确性”与“检索质量/任务成功率”分开，不把公开 benchmark 的 workflow reward、QA F1 或 native event 数误写成 TxnMem memory accuracy。

## 2. 中心命题

全文只围绕一个中心命题展开：

> 当多 Agent 将 memory 作为共享、可派生和可传播的系统状态时，仅有语义检索和元数据过滤不足以保证正确性；共享 memory 还需要事务边界、提交时策略重验证，以及基于 provenance 的失效闭包修复。

这一定义把 TxnMem 与单 Agent 长期记忆、RAG 检索优化、普通向量数据库和仅提供访问过滤的 memory service 区分开。论文的核心评价对象是可检查的系统不变量，不是生成文本的主观质量。

## 3. 贡献声明

正文将贡献压缩为四项，避免把工程步骤逐项包装为贡献：

1. 定义多 Agent 共享记忆的操作模型，以及 atomicity、commit authorization、scope safety、supersession consistency、provenance closure 和 recovery consistency 六类可检查不变量。
2. 提出由 Agent Memory Transaction、Policy-Consistent Commit 和 Provenance-Driven Repair 组成的策略感知事务型共享记忆运行时。
3. 构建 TxnMemBench：使用独立 serial reference semantics、因果 failure schedule、differential oracle、mutation testing、coverage 和前缀最小 witness 验证系统正确性。
4. 在受控 workload、真实 Qwen2.5-7B tool loop、三个公开 Agent runtime、真实 Qdrant/Neo4j/Toxiproxy 服务和跨主机模型负载上建立分层证据链。

## 4. 论文结构

### 4.1 摘要

摘要控制在 450--650 个中文字符，按“问题--洞见--设计--方法--关键结果--边界”展开。只保留最能支持中心命题的数字：400 个受控实例/2,000 条结果、完整 TxnMem 0/400 目标违规与 400/400 oracle match、四类最小 mutant witness，以及真实 Qwen2.5-7B 接入。Toxiproxy 旧矩阵仅作为 fault/response-path 观察，不进入摘要的原子性结论；AppWorld/LoCoMo 的弱增益、详细 token 数和跨主机拓扑限制同样不进入摘要。

### 4.2 引言

引言使用一个贯穿全文的订单/地址协作案例，依次展示半更新、commit 前撤权和源记忆失效后的派生污染。随后说明现有 memory 工作主要优化“记住什么、如何召回”，而本文研究“多 Agent 共享状态在失败和策略变化下是否仍正确”。引言末尾列出四项贡献，并用一段话明确 claim boundary。

### 4.3 背景与动机

区分四类系统：单 Agent 长期记忆、RAG/向量检索、治理/访问控制 memory、数据库事务与 provenance。用需求表说明它们分别覆盖 semantic retrieval、shared writes、commit-time policy、derived-state repair 和 fault-aware validation 的哪些部分。该节不做大段文献罗列，差距必须直接导向 TxnMem 的设计需求。

### 4.4 模型与正确性

定义系统状态 `F=(A,M,P,T,G)`、memory object 字段、操作集合和策略版本。用简洁的状态转换与不变量描述代替仅有自然语言的概念说明。明确 reference semantics 与被测 TxnMem 独立；对于 commit-boundary crash，oracle 允许多个合法线性化结果。

### 4.5 TxnMem 设计

按三个机制组织，每节包含：威胁场景、设计目标、状态/算法、失败处理和当前原型边界。

- Agent Memory Transaction：统一 write、supersede、derive、propagate 与 provenance edge 的提交边界。
- Policy-Consistent Commit：记录 begin policy version，并在 commit 点对 read/write/propagation 集合执行最新策略重验证。
- Provenance-Driven Repair：将来源 DAG 作为可执行修复索引，对 revoked、superseded 和 corrected 事件执行失效、stale、重算或传播撤销。

正文给出 commit 和 repair 两段伪代码。redact、scope downgrade 和内容级事实重判只作为未实现扩展，不写成当前系统能力。

### 4.6 实现

说明确定性核心、event contract、failure controller、可替换 backend、SQLite 与 VectorGraphMemoryBackend、Qwen structured tool loop，以及 Qdrant/Neo4j/Toxiproxy 接线。实现节提供代码规模和模块边界，但不把测试数量本身作为系统贡献。

### 4.7 评估

评估按问题组织，而不是按产物目录罗列：

- RQ1 正确性：三个核心机制是否分别阻止目标违规？使用 8 workload × 50 seed × 5 variant 的 controlled suite。
- RQ2 测试有效性：因果 schedule 是否优于随机 schedule，benchmark 是否能杀死目标 mutant，并给出最小 witness？
- RQ3 真实模型与公开 runtime：真实 Qwen tool call 能否生成合法 event，TxnMem 是否能在 τ-bench/AppWorld/LoCoMo runtime 边界执行？
- RQ4 真实服务故障：请求经过 Toxiproxy 时记录了哪些 fault/response 路径，Qwen+Qdrant+Neo4j E2E 是否闭环？故障后双存储一致性只由新的 state-verified rerun 回答。
- RQ5 外部相关性：synthetic workload 与 trace-grounded holdout 的联合分布是否匹配，结果对 generator 校准意味着什么？

正文主结果使用 controlled suite、schedule/mutation、真实故障和 Qwen native evidence。τ-bench 的 reward、AppWorld 0/20→1/20、LoCoMo +0.00162 以及 realism mismatch 放在“外部有效性与负结果”小节，以诚实的描述性证据呈现，不作为 TxnMem 优越性的 headline。

跨主机 v8 结果只证明 1 个 Agent-worker host 到 1 个 model-server host 的三次独立 attested 运行。它不承担多主机 Agent workers、跨主机 memory backend、连续 30 分钟 tunnel 或生产 latency 结论。

### 4.8 讨论、局限性与相关工作

讨论节回答 synthetic benchmark 的说服力来源、确定性 policy 的语义范围、内容级正确性与 provenance closure 的区别，以及当前部署边界。相关工作按 Agent memory、governed memory/access control、transaction/provenance 和 distributed-system testing 四组比较，明确 TxnMem 的组合创新，而不是声称每个底层机制本身首次出现。

### 4.9 结论与附录

结论只回扣中心命题、三个机制和最强证据，不重复完整实验清单。附录保留复现配置、claim ledger 说明、额外表格和详细 workload schema；正文不塞入 artifact hash、完整命令和审计历史。

## 5. 图表计划

正文包含以下七幅图，优先使用矢量图或高分辨率生成图：

1. 动机时间线：read/derive/write 期间发生 crash、revoke 和 source invalidation 时的三类错误。
2. TxnMem 架构：Agent API、Transaction Manager、Policy Engine、Memory Store、Provenance Repair Engine 及其数据流。
3. commit 状态机或时序图：begin、buffer、revalidate、commit/abort。
4. provenance repair 示例：chain、branch、supersession 与 descendant closure。
5. controlled suite 主结果：五个 variant 的 violation count/oracle match 对比。
6. 分层证据图：controlled、native model、public runtime、real services 与 cross-host 的能力和 claim boundary。
7. provenance scalability：以相同颜色表示 100、1,000 和 10,000 节点规模，在五档并发下并列展示成功操作吞吐（含 whole-repetition bootstrap 95% CI）与 p99 尾延迟；两面板均使用对数纵轴。

正文表格共七张：需求差距、系统不变量、workload family、实验设置、主结果、真实/公开 runtime 结果和 provenance-performance v10；附录另含 claim ledger 与 workload schema 两张表。长命令、hash、逐任务状态和更细粒度统计移入附录或仓库报告。

## 6. 数字与证据规则

所有正式数字必须来自 `configs/paper_claims.json` 所覆盖的 active evidence，或在加入正文前先扩展 ledger 并通过 fail-closed audit。旧 backend timing、v6/v7 cross-host 和历史 status 文件不得重新承担当前结论。

数字表达遵循以下规则：

- controlled suite 的分母始终为 400 instances 或 2,000 variant rows，不混用。
- AppWorld 始终使用全 20 task 分母；6 个 execution failure 不从分母删除。
- LoCoMo 三次 repetition 只作描述性结果，不声称统计显著。
- native event count 只表示过程记录，不充当 benchmark 样本量。
- Toxiproxy 旧结果只写成单机 proxy/fault-response 路径观察；明确未独立核验故障后双存储状态，也不写成 atomicity、latency、production 2PC 或 availability。
- E2E timing 与 cross-host load 均标记 `production_latency_claim=false`。
- realism test 的拒绝结果解释为 generator 仍需校准，不解释为分布等价。

## 7. 文献与引用

参考文献目标为 30--45 篇，以正式发表论文、官方 proceedings 和原论文 arXiv 为主。每条题名、作者、年份和 venue 在写入前核验；不保留无法确认的引用。正文使用数字编号，相关工作必须同时包含直接相邻工作和方法来源，包括 Agent memory、τ-bench/AppWorld/LoCoMo、transaction/serializability、provenance/lineage、访问控制和系统故障注入。

## 8. 文档设计

DOCX 采用 `narrative_proposal` 设计预设的克制学术变体：单栏、匿名、可编辑、以黑色正文和深蓝标题建立层级。标题页不做商业报告式封面；第一页直接包含中英文标题、匿名标记、中文摘要和关键词。正文使用真实 Heading/Caption/List 样式、显式表格几何、连续页码和统一图表题注。

目标文件为 `<output-dir>/TxnMem_CCF-A中文论文初稿.docx`。同时在仓库保存可版本控制的中文正文源和可重复生成脚本。最终交付只提供 DOCX；PDF 和逐页 PNG 仅用于内部视觉 QA。

## 9. 验收标准

1. 论文包含摘要、引言、背景、系统模型、设计、实现、评估、讨论/局限、相关工作、结论和参考文献，章节之间有明确论证链。
2. 摘要与引言不把弱外部结果包装成主要性能收益；贡献声明与实际实现一致。
3. 每个关键数字均可追溯到 active claim artifact，claim audit 为 0 findings。
4. 不出现未填写占位符、虚构引用、未实现能力的完成式表述或被 supersede 的结果。
5. 图表题注完整，表格几何、标题层级、列表、页码和引用格式一致。
6. DOCX 成功渲染为逐页 PNG，所有页面在 100% 检查下无裁切、重叠、缺字、坏表格或异常空白。
7. accessibility audit 的 high/medium/low findings 均为 0；最终文档通过隐私元数据清理。
8. 仓库全量测试、paper claim audit、artifact audit 和文档结构审计通过，生成脚本与正文源被提交到 Git。
