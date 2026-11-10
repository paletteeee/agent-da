# TxnMem 公开 Benchmark 与真实 Backend 证据扩展设计

**日期：** 2026-08-05

**目标：** 在不把 projection replay 或 smoke 结果冒充为原生 memory ground truth 的前提下，完成三类公开 benchmark 的大规模 native memory 采样、官方任务/QA 评测、真实向量与图 backend、可重复网络故障和端到端性能实验。

## 1. 范围与不可越过的边界

本阶段包含四个相互依赖但可分别验收的证据层：

1. **Native benchmark sampling：** 使用官方 τ-bench、AppWorld 和 LoCoMo 输入，由 Qwen2.5-7B structured tool loop 实际产生 Agent 行为，接入每个 episode 独立的持久化 memory backend，并记录 canonical memory events。
2. **Official evaluation：** 保留各 benchmark 官方 task/reward/QA evaluator 的原始结果；TxnMem reference oracle、event contract 和 provenance closure 是独立指标，不能替代官方分数。
3. **Real backend and fault evidence：** 使用 Qdrant HTTP 作为向量存储、Neo4j Bolt 作为 provenance graph 存储、Toxiproxy 作为可控网络故障层，比较 SQLite、向量 backend、图 backend 和组合 backend。
4. **Performance evidence：** 分离 backend-only 与 model-in-the-loop 结果，报告 p50/p95/p99、吞吐、错误率、重试和一致性，不作生产 SLA 或生产吞吐声明。

以下内容不属于本阶段的可声称结论：

- 公开 benchmark 原生提供 TxnMem transaction、policy churn、failure schedule 或 provenance ground truth；
- projection replay 的 oracle match 等于 benchmark task accuracy；
- Qwen2.5-7B 的小规模或单一模型结果等于通用 Agent 能力；
- 远程实验的吞吐、延迟或成本等于生产部署性能；
- 没有 remote URL 时执行 Git push。

## 2. 统一数据流

```text
official task/conversation
        │
        ▼
fixed manifest + task-level split + model/tool loop
        │
        ├── official evaluator result
        ├── canonical native memory events
        ├── independent TxnMem reference oracle
        └── backend/fault/timing metadata
                         │
                         ▼
             sanitized aggregate + confidence intervals
```

每一个 task 或 conversation 是统计单位。API 行数、memory event 行数和 token 数只作为过程指标，不能扩大样本量。原始 prompt、model response、工具参数、对话内容、memory payload、SQLite/Qdrant/Neo4j 数据库文件只保留在远端 `/data/txnmem` 下的结果目录；仓库只提交脱敏 aggregate、manifest hash、环境信息和失败分类。

## 3. 公开 benchmark native sampling

### 3.1 固定样本和运行配置

主实验使用以下固定清单：

| Benchmark | Primary episodes | 运行方式 | 主要官方指标 |
|---|---:|---|---|
| τ-bench airline | 50 tasks | official airline runtime + scripted user + Qwen2.5-7B | official reward、task completion |
| AppWorld | 20 tasks，按可用 app schema 分层 | official environment + task-specific API docs + Qwen2.5-7B | official `task_completed()` |
| LoCoMo | 10 conversations 全量 | contextual Agent runtime + Qwen2.5-7B | official QA evaluator |

所有 primary run 固定 `seed=17`、temperature、max steps、model identifier、runtime commit、task manifest hash 和 evaluator version。若需要重复性实验，使用相同 manifest 的三个独立 seed 作为 secondary run，不能把重复 episode 当作新的 benchmark task 数量。

### 3.2 Native memory contract

每个 episode 创建独立 backend namespace，并通过统一接口产生：

- `memory_read` / `memory_search`；
- `memory_write`；
- `memory_derive`，必须携带实际读取的 source IDs；
- `memory_propagate`，必须携带实际传播的 source IDs；
- `memory_supersede`、`invalidate`、`policy_change` 和 `policy_revoke`。

事件先经过 canonical validator，再写入持久化 backend 和远端 raw trace。reference executor 只接受事件、policy timeline、failure schedule 和初始状态，独立计算允许 outcome；它不读取模型 response，也不调用 TxnMem variant 生成 expected outcome。

### 3.3 正式统计

对 task/conversation-level 指标计算 95% Wilson 区间；对多分类 failure 使用 task-level counts 和显式分母；对 backend timing 使用固定 workload 的 bootstrap percentile interval。结果至少包含：

- official success/reward/QA score；
- native event contract success；
- independent oracle match；
- provenance closure、policy violation、partial commit、retry/error；
- operation count、transaction size、provenance depth、branch factor、policy transition rate；
- model/runtime/backend/fault condition、manifest hash 和 evaluator availability。

官方 evaluator 缺失、凭据缺失或版本不兼容时，runner 必须退出为 `blocked` 并写出依赖名、检查命令、错误类别和下一步；不得返回空分数或静默使用 projection replay。

## 4. AppWorld success rate 与 LoCoMo QA evaluator

### 4.1 AppWorld

AppWorld run 分成 baseline 和 tuned 两个固定配置，输入 task 清单相同。tuned 配置只允许改变以下可审计因素：官方 task reset 顺序、task 使用的 app schema/API docs、tool-call 参数适配、终止条件和重试上限。不得修改官方 evaluator 或 task state。

每个 task 结束后调用官方 `task_completed()`，同时保存 TxnMem contract/oracle 结果。结果表同时给出 baseline、tuned、official success 和 TxnMem oracle；若 tuned 仍为零，报告原因分类而不是用 oracle match 代替 success rate。

### 4.2 LoCoMo

LoCoMo adapter 将官方 conversation、session 顺序和 QA annotations 传给可执行 contextual Agent。QA evaluator 通过官方仓库入口或其官方可导入模块调用，答案、evaluator prompt 和外部 LLM 依赖不写入 Git。若官方 QA evaluator 要求外部模型凭据，运行器先做 availability check；缺失时只生成 `blocked_official_qa_evaluator.json`，保留 native event 和 TxnMem oracle 作为不同证据层。

QA 统计以 conversation/question 为单位，并额外报告 conversation-level completion；不能把 session summary 投影写入事件当作官方 QA ground truth。

## 5. 真实向量/图 backend

### 5.1 服务拓扑

远程 `/data/txnmem/services/` 下运行三个固定服务：

- Qdrant HTTP：保存 memory embedding、metadata 和 namespace；
- Neo4j Bolt：保存 `Memory` 节点及 `DERIVED_FROM`、`PROPAGATED_FROM`、`SUPERSEDES` 关系；
- Toxiproxy：作为 TxnMem client 到 Qdrant/Neo4j 的网络入口，提供 delay、timeout、connection drop 和 retry 条件。

服务版本、镜像 digest、端口、启动参数和健康检查结果写入 `environment.json`。若远端没有可用 Docker 或服务无法健康启动，运行器必须输出明确 blocked report，不得用 SQLite 结果替换真实 backend 结果。

### 5.2 Backend adapter

新增 benchmark-neutral `VectorGraphMemoryBackend`，内部执行以下顺序：

1. 将 memory metadata 和 embedding 写入 Qdrant；
2. 将 provenance 节点/边写入 Neo4j；
3. 只有两个存储操作都成功后才产生 committed canonical event；
4. 失败时按 request id 和 idempotency key 重试，最终失败必须产生 abort/error 记录，并由 reference oracle 检查不存在 partial commit 或 provenance closure violation。

SQLite backend 作为本地持久化基线，不能被标记为 vector/graph backend。所有 adapter 都使用相同的 `MemoryBackend` 事件接口，以便比较语义结果而不是比较不同事件格式。

## 6. 网络故障与端到端性能矩阵

### 6.1 故障条件

固定四类条件并使用 trigger-based controller：

1. normal；
2. write/derive 完成后注入短延迟；
3. commit 前后注入 timeout 或 connection drop；
4. 首次请求失败、第二次请求重试成功。

每个条件固定 fault seed、触发请求序号、服务名和恢复动作。验收条件是：无 partial commit、retry 具备幂等性、最终 provenance closure 与 oracle 一致，或明确 abort 且原因可定位。

### 6.2 性能条件

Backend-only workload 使用 50、200、1000 个 memory events，分别覆盖 read/search、write、derive、propagate、supersede 和 commit。每个条件至少 30 次重复，先预热再采样；模型端到端 workload 使用固定 native task manifest，单独记录 model latency 和 backend latency。

报告：

- backend operation p50/p95/p99 latency；
- transaction end-to-end p50/p95/p99；
- completed transactions/sec；
- timeout、retry、abort、partial-commit 计数；
- Qdrant/Neo4j request count、payload bytes 和 CPU/memory/GPU environment；
- model-included 与 backend-only 的差异。

## 7. 验收与交付

代码验收：

- 本地单元测试覆盖 manifest split、evaluator blocked 状态、canonical event、backend idempotency 和 fault controller；
- 远程服务 health check、单任务 smoke 和 full batch 均输出机器可读 JSON；
- 三个 benchmark 的 native run、官方评测、TxnMem oracle、统计区间和失败分类可由固定命令复现；
- 性能结果明确带 `production_latency_claim: false`；
- DOCX 内容若因结果更新发生变化，使用现有 bundled LibreOffice wrapper 重新渲染并逐页检查。

论文验收：

- 公开 benchmark 结果使用 “native workflow with instrumented memory backend” 或等价限定措辞；
- projection replay、native workflow smoke、native memory event 和 official accuracy 分栏呈现；
- AppWorld 与 LoCoMo evaluator 的不可用状态不能被隐藏；
- 真实 backend 实验单列 service version、fault matrix 和性能边界；
- Git 本地 commit 完成后，只有在用户提供 remote URL 和 push 权限时执行 `git remote add`/`git push`。
