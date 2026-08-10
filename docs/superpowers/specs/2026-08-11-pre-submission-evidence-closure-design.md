# 投稿前六项证据闭环设计

**日期：** 2026-08-11  
**状态：** 已确认；用户要求依次完成全部六项  
**范围：** 只补齐正式投稿所需的可审计证据，不扩大为生产级性能或生产级多主机一致性主张。

## 1. 目标

关闭当前论文中的六个硬缺口：

1. 将 controlled suite 的统计口径统一为仓库中实际可复算的 400 个实例、2,000 条 variant 结果。
2. 让 Toxiproxy 真正进入 Qdrant/Neo4j 请求路径，并证明每个非 normal 场景的 trigger 和 toxic 均实际生效。
3. 提交可审计的 τ-bench 50-task 聚合结果。
4. 提交可审计的 5-task Qwen2.5-7B + Qdrant + Neo4j 端到端聚合结果。
5. 为四个主要 mutant/ablation 发布经过前缀收缩的最小反例。
6. 建立“论文主张 → 聚合字段 → 运行命令 → manifest/hash → Git commit”的一致性审计，并标记已过时状态文件。

## 2. 证据边界

- Ground truth 继续来自独立 reference semantics，不由 TxnMem 自己生成 expected outcome。
- 只提交脱敏聚合、任务标识、版本、摘要和哈希；不提交原始用户内容、完整 prompt、凭据或模型权重。
- backend-only 与端到端延迟均标记 `production_latency_claim=false`。
- 单机 Qdrant/Neo4j/Toxiproxy 结果不表述为跨主机数据库实验。
- τ-bench reward 只作为官方 evaluator 输出，不表述为 memory accuracy。
- 远程结果只有在 task manifest、模型 revision、服务版本、运行命令和 artifact hash 齐全时才进入正式论文。

## 3. 统一 controlled suite 口径

`results/final_controlled` 是正式受控实验的唯一统计源。审计器直接读取 JSONL/CSV，而不是接受手工填写的样本数。

验收值：

- 8 个 workload family × 50 seeds = 400 instances；
- 5 个 variants = 2,000 variant rows；
- TxnMem：0/400 违规、400/400 oracle match；
- Naive：350/400 违规；
- TxnMem-NoTxn：200/400；
- TxnMem-NoPolicyCommit：50/400；
- TxnMem-NoRepair：100/400。

论文、中文实验报告、任务状态和证据清单必须引用同一组字段；任何 160/800 旧口径都必须被删除或明确标为历史试运行。

## 4. 真实 Toxiproxy 故障路径

### 4.1 请求路径

真实服务运行时建立两个代理：

- `txnmem-qdrant`：host listen port → `qdrant:6333`；
- `txnmem-neo4j`：host listen port → `neo4j:7687`。

`VectorGraphMemoryBackend` 只能连接代理端口；不能在故障矩阵运行时直连服务端口。Toxiproxy management API 只负责代理和 toxic 生命周期。

### 4.2 触发与证据

backend factory 必须消费 `FaultScenario`。一个 scenario-aware wrapper 在每个 Qdrant write 或 Neo4j commit 前调用 `observe(service, operation)`；命中指定 request ordinal 后：

1. 通过 management API 安装唯一命名 toxic；
2. 记录 trigger、ordinal、proxy、toxic 类型和 API 响应；
3. 执行真实请求；
4. 按 recovery action 清理 toxic，并在需要时重试或终止；
5. 收集 canonical event、rollback、partial commit 和 retry 证据。

非 normal 场景若没有 `trigger_fired=true`、`toxic_installed=true` 和 `proxy_path_verified=true`，整行标为 `evidence_valid=false`，不得写成已完成故障实验。

### 4.3 故障语义

- `delay`：请求成功但延迟显著高于 normal；
- `timeout`：首次请求失败，按场景终止或恢复；
- `connection_drop`：首次请求连接被重置；
- `retry_success`：首次请求失败，清理 toxic 后一次重试成功；
- 所有场景必须满足 `partial_commit_count=0`。

## 5. 两个缺失的远程聚合

### 5.1 τ-bench 50-task

聚合文件至少包含：固定 task manifest 及 SHA-256、50 个唯一 task ID、运行时/模型身份、每任务状态、native event 数、evaluator availability、reward、重试归并规则、总和/均值以及源 artifact 哈希。聚合器必须拒绝重复 task、缺失 task 或未归并 retry 的输入。

### 5.2 5-task Qwen + Qdrant + Neo4j E2E

聚合文件至少包含：5 个唯一 task ID、5/5 completion、模型 revision、vLLM build、Qdrant/Neo4j 版本、backend health、每任务 wall time、mean/P50、canonical event 数、oracle/contract 状态、运行命令、manifest hash 和源 artifact hash。服务不健康或直连/代理边界不清楚时不得生成 `status=complete`。

如果远端已有原始输出，先验证再聚合；若缺失则使用固定 manifest 重跑。所有凭据仅通过交互式 SSH/现有安全配置使用，不写入仓库或日志。

## 6. 最小 mutant witnesses

从完整 controlled suite 中寻找每个 mutant 的失败实例，并复用 `find_minimal_counterexample` 做确定性的 operation-prefix shrinking。至少覆盖：

| mutant | 对应机制 |
|---|---|
| `partial_commit` | 原子提交 |
| `remove_commit_revalidation` | commit-time policy revalidation |
| `disable_provenance_traversal` | provenance closure/repair |
| `bypass_scope_check` | scope isolation |

每个 witness 保存 mutant、source instance ID/hash、原始/最小 operation count、保留操作、failure schedule、reference expected outcome、observed violation/oracle mismatch 和 shrink trace。结果必须可由 CLI 单独 replay，并验证再删去任一末尾操作后不再重现同一失败。

## 7. Claim–artifact 一致性审计

新增机器可读 claim ledger。每条正式论文实验主张包含：

- 稳定 claim ID 和论文位置；
- artifact 相对路径、格式和 JSON pointer/CSV 派生规则；
- 期望值或允许范围；
- 生成命令；
- task/config manifest 路径与 SHA-256；
- artifact SHA-256；
- 产生该证据的 Git commit（当前提交可使用可解析的 `HEAD`/生成提交字段，并在最终证据提交后固化）；
- claim boundary。

审计命令必须在以下情况非零退出：文件缺失、字段缺失、计数不一致、哈希不匹配、正式 claim 指向 superseded artifact、非 normal 故障未实际触发、远程聚合 task 数不完整。

历史状态文件不删除。通过机器可读 supersession index 标明其替代文件、替代原因和日期；论文与当前报告不得再把它们作为现状来源。

## 8. 文档同步与最终验证

实现和实验通过后，同步：

- `docs/current_experiment_report_zh.md`；
- `docs/formal_paper_task_status_zh.md`；
- 论文构建脚本及 `outputs/TxnMem_论文初稿.docx`；
- 必要的证据索引与复现命令。

最终验证包括：全量单元测试、artifact audit、claim audit、所有新聚合的严格 replay、Git diff/status 检查、DOCX→PNG/PDF 渲染、逐页视觉检查和 accessibility 审计。远端 Git push 不属于这六项证据闭环；仓库没有 remote URL 时只做本地 commit，并将 push 明确记录为外部配置阻塞。

## 9. 完成判定

六项全部完成的必要条件是：

1. 每项都有 tracked artifact；
2. 每项有自动化测试或严格审计；
3. 两个远程聚合可以从 tracked manifest 与命令追溯；
4. Toxiproxy 结果包含真实 trigger/toxic/proxy-path 证据；
5. 论文数字与 claim ledger 一致；
6. 文档视觉 QA 和全量测试通过；
7. 所有新增内容已本地 Git commit，且未纳入凭据或未脱敏数据。
