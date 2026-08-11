# TxnMem 参考文献核验审计

## 范围与方法

本审计对应 `configs/txnmem_paper_references.json`（32 条，稳定编号 `R01`--`R32`）。每条以 `source_class` 声明核验边界，并以 `verified_source` 保存该类唯一的 primary source：`arxiv_preprint` 只把 arXiv 的题名、作者、提交年份和 arXiv 编号写入 catalog；`arxiv_record_with_journal_reference` 仅在该记录明确给出 journal reference 时保留正式 venue（R11）；`official_proceedings`、`official_publication` 和 `doi_landing` 则分别指向正式 proceedings、官方出版页或 DOI landing page。未使用 DBLP、Semantic Scholar、新闻、博客或课程材料来确认题名、作者、年份或 venue。编号按工作稿预计首次出现的顺序预分配；新增文献应追加编号，不能重排既有 ID。

现有 worktree 中尚无 `docs/paper/txnmem_ccfa_draft_zh.md`，因此没有待删除或待更正的正文书目条目。后续写作只能使用本 catalog；若某工作稿引文的 venue/年份与 catalog 不一致，应以本 catalog 及其 primary source 为准。

## 分组、相关性与比较边界

| 组别 | 已核验条目 | 对 TxnMem 的相关性 | 不可据此推出的结论 |
| --- | --- | --- | --- |
| Agent memory | R01--R03、R07、R10 | 覆盖分层/外部记忆、经历记录、反思文本和多会话记忆的 agent 侧设计动机。 | 这些工作不是共享可串行化 memory store；不能把检索或反思称为 commit、撤销或依赖修复。 |
| RAG / long-term memory | R04--R07 | 覆盖可检索非参数记忆、检索时机与生成中的 attribution 需求。 | RAG 的 passage retrieval 不提供 TxnMem 的并发写入、策略版本检查、abort 或 stale/repair 语义。 |
| Agent benchmarks | R08--R10 | τ-bench 的状态目标、AppWorld 的 state-based tests、LoCoMo 的长程记忆任务分别限定评估语境。 | 它们是 workload/evaluation，不是事务协议的正确性证明；不可将 benchmark 分数外推为生产可靠性。 |
| Transactions / serializability | R11--R16 | 说明 isolation、可串行化、可用性权衡及跨副本事务的经典语义背景。 | TxnMem 不是通用 DBMS 或 Spanner 的复现；论文只主张已实现 memory-object 操作和 reference oracle 范围内的不变量。 |
| Provenance / lineage | R17--R21 | 说明 why/where provenance、lineage 和 semiring 表示的来源。 | 数据库 provenance 本身不规定 LLM agent 的 derived-state invalidation、repair 选择或策略判定；这些是 TxnMem 的明确设计/实验边界。 |
| Access control | R22--R25 | 为 scope、RBAC/ABAC 与 commit-time policy 的概念边界提供来源。 | 这些工作不验证自然语言 agent 的授权意图，也不等价于 TxnMem 的 policy-version validation；redact 与 scope downgrade 仍是扩展。 |
| Failure injection / model checking | R26--R32 | 覆盖状态空间探索、崩溃一致性、数据库 ACID 故障测试、分布式部分失败与自动 fault injection。 | 其 bug-finding 成果不能成为 TxnMem 已发现漏洞或性能优势的证据；TxnMem 只报告其受控 schedule 和 fault-aware validation 结果。 |

## 覆盖缺口与排除项

- catalog 刻意没有把任何 benchmark、RAG、provenance 或 access-control 工作写成 TxnMem 的端到端基线；它们回答的问题不同。
- 早期数据库与 provenance 文献中，部分出版社页面当前无法在本执行环境直接展开。为避免二手转录，catalog 保留 DOI landing page 而非重定向后的第三方镜像；其字段仅在 DOI 来源确认后可更改。
- 未纳入无法从允许的 primary source 确认题名、完整作者、年份和 venue 的候选项，包括来源不明的 agent-memory 产品页、非正式 benchmark fork、博客汇总与二手 survey citation。
- R20 采用 Widom 的原始 Trio 章节版本，而非将系统名称误写成未经核验的 conference 论文。若正文需要 Trio 的特定 VLDB 版本，应先新增独立 primary-source 核验记录。
- 后续若引入更近的 agent memory、policy engine 或 distributed model-checking 文献，应先追加完整 catalog 条目、审计其比较边界，再在正文中引用。

## 使用规则

1. 正文引用写为 `[Rxx]`，并且只引用本 catalog 中已核验的 ID。
2. `url` 与 `verified_source` 当前相同，并受 `source_class` 的 HTTPS primary-host 规则约束；arXiv source 不能支撑的正式 conference/journal venue 不写入 catalog，不以搜索结果页或二手 bibliography 代替。
3. 任何“优于”“支持”“保证”的表述必须回到 TxnMem 的 evidence map 与 claim boundary，不从本相关工作 catalog 推导实验事实。
