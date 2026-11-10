# 真实 Agent 跨事件事务与恢复实验设计

日期：2026-08-17  
状态：已获用户确认，等待实现计划

## 1. 目标

本工作在真实 Qwen2.5-7B 工具循环中实现 task-scoped durable memory transaction，使一个 Agent task 内的多次 `read`、`write`、`derive`、`propagate`、`supersede` 与 `invalidate` 操作可以在统一事务边界内执行。实验通过真实进程 `SIGKILL`、提交时撤权、来源失效和 Qdrant/Neo4j 分阶段写入故障，验证经过 TxnMem gateway 的 memory 状态在恢复后属于独立 reference semantics 允许的完整提交或完整不可见结果。

本设计回答论文中的核心证据缺口：现有确定性 TxnMem core 已实现事务、提交重验证和 provenance repair，但真实模型 gateway 当前直接分派每个 memory 事件。新增路径必须证明真实模型产生的多事件工具调用可以进入持久化事务协调器，而不把模型提示词遵循能力当作事务语义。

## 2. 明确不在范围内的内容

- 不回滚 τ-bench、AppWorld 或其他 benchmark API 已产生的外部世界副作用。
- 不声称 Qdrant 或 Neo4j 原生支持统一的两阶段提交。
- 不保护绕过 TxnMem gateway 的直接后端读取。
- 不实现协调者选举、跨协调者共识或任意网络分区下的通用分布式事务。
- 不把任务成功率、模型回答质量或公开 benchmark reward 解释为 memory transaction correctness。
- 不让模型自行决定是否调用 `begin`、`commit` 或 `abort`；事务边界由系统自动管理。

## 3. 设计选择

### 3.1 每个 Agent task 自动对应一个事务

`run_real_agent` 在 task 开始时自动创建事务。模型产生的 mutating memory tool call 被缓冲；模型返回无工具调用的最终回答时，gateway 自动提交。模型协议错误、未知工具、达到最大步数、显式故障注入或提交检查失败均触发自动 abort。

该边界避免把“模型是否记得提交”与“系统是否具备事务能力”混为一谈。任务级事务只覆盖 memory 副作用，benchmark 工具仍按各自 runtime 语义直接执行。

### 3.2 持久化事务协调器，而非内存缓冲或完整 2PC

实现采用本地 SQLite transaction journal 作为单一持久化决议源。Qdrant 对象和 Neo4j 节点/边携带 `txn_id` 与 pending 元数据；TxnMem gateway 的读取路径查询 journal 决议，只公开 `COMMITTED` 事务的数据。这样，后端可以在 commit 决议前暂时存在不完整 pending 状态，但它们不会进入默认可见集合。

该方案能经受真实进程退出与重启，同时保持论文主张为“task-scoped durable transaction through the TxnMem gateway”。它不扩展为 Qdrant/Neo4j 的通用分布式事务协议。

### 3.3 真实生成、配对重放、live 子集验证

Qwen2.5-7B 先真实生成多事件 tool trace。每条固定 trace 随后分别在 `DirectDispatch` 和 `TaskTransaction` 条件下重放，以消除模型输出变化造成的混杂。最后再在两个 gateway 上运行 live Qwen 子集，证明事务路径不是纯离线 replay。

## 4. 架构

```text
Qwen2.5-7B Agent
        |
        | multiple memory tool calls
        v
TaskTransactionGateway
        |-- TransactionCoordinator
        |-- TransactionJournal (SQLite)
        |-- Read/Write/Provenance Buffer
        |-- Policy Revalidation
        |-- Visibility Filter
        `-- RecoveryWorker
                 |
                 |-- Qdrant
                 `-- Neo4j
```

### 4.1 `TaskTransactionGateway`

该 gateway 包装现有 `NativeMemoryToolGateway`，保持相同的模型可见 memory tool schema。模型不看到额外的事务控制工具。gateway 为每次 task 分配稳定 `txn_id`，将读取路由到 committed state 加本事务 pending buffer，将写入转换为 journal intent。

`run_real_agent` 增加显式 `transaction_mode`：

- `direct`：保持当前逐事件分派行为；
- `task`：使用 task-scoped transaction；
- 默认值保持现有行为，避免旧实验被静默改写。

### 4.2 `TransactionJournal`

SQLite journal 至少包含以下逻辑表：

- `transactions`：`txn_id`、`task_id`、actor、begin policy version、状态、唯一决议、创建/更新时间；
- `intents`：稳定序号、操作类型、规范化参数、payload hash、来源与 supersession 信息；
- `read_set`：读取对象、观察版本与 scope；
- `transaction_events`：prepare、stage、verify、decision、cleanup 与 recovery 事件；
- `backend_receipts`：Qdrant/Neo4j 的幂等键、写入阶段和操作后回读摘要。

事务状态机为：

```text
ACTIVE -> PREPARED -> COMMITTED
                   `-> ABORTED
```

`COMMITTED` 与 `ABORTED` 都是终态。数据库约束必须禁止一个事务产生两个决议，也禁止 committed 后转为 aborted。

### 4.3 Backend staging 与可见性过滤

Qdrant payload、Neo4j Memory 节点及 provenance/supersession 边均记录 `txn_id`。提交决议前，它们是 pending 数据。所有 TxnMem 默认读取和搜索执行以下过滤：

1. 查询本事务的 pending buffer，以支持 read-your-writes；
2. 查询后端候选；
3. 根据 journal 批量判定其 `txn_id` 是否 committed；
4. 丢弃未决、aborted 或未知事务产生的候选。

直接绕过 gateway 的数据库查询不在保证范围内。实验会同时记录 raw backend state 和 gateway-visible state，以区分“用户不可见的 orphan pending 数据”与真正的 partial visibility。

## 5. 操作语义

### 5.1 读取

`memory_read` 和 `memory_search` 只返回：

- 已提交事务产生的 active 对象；
- 当前事务自己的 pending 对象。

读取记录对象版本、scope 与 provenance status，供提交时重验证。其他活动事务的 pending 对象不可见。

### 5.2 写入与派生

`memory_write`、`memory_derive`、`memory_propagate`、`memory_supersede` 和事务内 `memory_invalidate` 先被规范化并写入 journal intent。模型收到 pending acknowledgement，使后续工具调用可以继续，但默认检索不会对其他事务公开这些对象。

derive/propagate intent 必须保留模型实际指定的直接来源；provenance 边与对象本体属于同一事务写集。supersession 的新旧对象关系也必须随事务共同决议。

事务内 `memory_invalidate` 在 prepare 前通过反向索引展开已记录 descendants，并把这些后继的 invalidation intent 纳入同一写集。若来源由事务外部在 task 执行期间失效，提交检查必须发现 read/source version 已变化并整体 abort；系统不能让新派生对象在失效来源之后提交。

### 5.3 自动提交

当模型返回最终回答时，gateway 依次执行：

1. 冻结 intent 和 read set；
2. 验证 read version、来源存在性、scope 与图结构；
3. 读取最新 policy version，并重验证 read/write/derive/propagate/supersede/invalidate；
4. 将 journal 状态原子推进到 `PREPARED`；
5. 使用稳定幂等键将全部对象写入 Qdrant pending state；
6. 将节点、provenance 边和 supersession 边写入 Neo4j pending state；
7. 对两个后端进行 operation-after readback，验证完整写集已持久化；
8. 只有 readback 完整且无歧义时，才在 journal 中原子写入唯一 `COMMITTED` 决议；
9. 返回 committed 结果并记录 transaction event。

步骤 8 是 gateway 可见性的线性化点。由于所有 backend stage 已在此之前完成，commit 后的 gateway 查询不会观察到缺失对象或边。

### 5.4 Abort 与清理

任一决议前错误使事务进入 `ABORTED`。aborted 数据即使尚未从后端删除，也因 visibility filter 而不可见。cleanup worker 使用幂等删除清除 Qdrant/Neo4j pending 数据，并以 raw readback 区分：

- `aborted_clean`：pending 数据已删除；
- `aborted_invisible_orphan`：仍有不可见 pending 数据，需要后续清理；
- `unknown`：后端无法完成确定性读取。

orphan pending 不计为 atomicity violation，但单独报告为清理可靠性指标。

### 5.5 恢复

新进程启动后扫描非终态事务：

- `ACTIVE`：没有提交决议，写入 abort 并清理；
- `PREPARED`：没有 commit 决议，写入 abort 并清理；
- `COMMITTED`：验证所有 stage 数据存在，必要时幂等补齐 event/receipt，然后返回 committed；
- `ABORTED`：继续幂等清理。

恢复重复执行必须产生相同的规范化状态哈希。任何 ambiguous backend response 都必须通过 raw readback 决定，不能仅凭客户端异常或 runner summary 推断成功。

## 6. 实验对象

### 6.1 主 workload family

冻结四类真实模型任务，每类 10 个，共 40 个唯一 task：

1. `atomic_multi_write`：一次任务更新多个相关对象；
2. `policy_revoke_before_commit`：开始时允许，提交前撤销；
3. `provenance_chain_branch`：显式 read、derive 和 propagate；
4. `supersession_mixed`：写入新版本、取代旧版本并更新派生对象。

正式 trace 必须来自真实 Qwen2.5-7B 工具循环。每条 task 预期产生至少两个 mutating memory event，并保留实际来源。模型不满足 contract 的 episode 仍生成固定 case row，分类为 `model_contract_failure`，保留在正式总分母中，但不进入“满足多事件前提后的事务正确性”条件分母；正式运行不得删除失败 episode 后重新计算成功率。

### 6.2 对照条件

- `DirectDispatch`：当前 `NativeMemoryToolGateway`，每次 memory mutation 立即写入；
- `TaskTransaction`：新增 task-scoped durable transaction gateway。

两条件使用相同模型 revision、prompt、tool schema、task manifest、初始数据、policy schedule 和 failure schedule。

## 7. 故障矩阵

### 7.1 两条件配对比较

每条 trace 在以下四种场景下分别运行 `DirectDispatch` 与 `TaskTransaction`：

1. `normal`：无故障；
2. `kill_after_first_mutation`：第一条 mutating memory event 后真实 kill worker；
3. `revoke_before_task_commit`：最后一次 memory mutation 后、task 决议前撤销写权限；
4. `invalidate_source_before_commit`：derive/propagate 后、task 决议前使来源失效。

### 7.2 TaskTransaction commit-phase 恢复

以下故障点只测试 `TaskTransaction`：

1. `kill_after_prepare`；
2. `kill_after_qdrant_stage`；
3. `kill_after_neo4j_stage_before_decision`；
4. `kill_after_commit_decision_before_response`。

故障由父进程观察持久化 phase event 后向 worker 发送 `SIGKILL`。不得使用捕获异常替代真实进程终止。随后必须启动新的 recovery process，不能在原 worker 内继续执行。

## 8. 数据规模

### 8.1 确定性 trace replay

- 40 trace × 4 配对场景 × 2 条件 = 320 case；
- 40 trace × 4 TaskTransaction 恢复故障 = 160 case；
- 共 480 个确定性 case。

### 8.2 真实 Qdrant/Neo4j

- 4 个配对场景 × 30 repetition × 2 条件 = 240 observation；
- 4 个 TaskTransaction 恢复故障 × 30 repetition = 120 observation；
- 共 360 次真实服务观测。

正式 manifest 在四个 workload family 间平衡轮换 task，固定 seed、故障触发序号和后端 namespace。每次 repetition 使用唯一 memory ID，避免跨重复污染。

### 8.3 Live Qwen 子集

- 每类 5 个 task × 2 条件 × 3 个固定 seed = 120 episode。

live 运行保留模型协议失败、未产生足够 mutating event、max-step、tool failure、policy denial 和 recovery failure 等全部 episode。

### 8.4 公开 runtime 补充 smoke

τ-bench/AppWorld 只用于确认 task transaction gateway 可以与公开 runtime 并存。它们不作为主事务正确性分母，因为公开任务不保证产生多事件 memory write，且外部 API 副作用不属于 memory transaction。

## 9. Ground truth 与分类

独立 `reference executor` 根据冻结 trace、policy event 和 failure schedule 产生允许的可观察结果集合。它不调用 `TaskTransactionGateway`、journal、backend staging 或 recovery 实现。

每次恢复后同时审计：

- journal 最终状态和唯一决议；
- gateway 默认可见对象；
- raw Qdrant object/payload；
- raw Neo4j node、provenance edge 和 supersession edge；
- commit/abort/recovery event log；
- 第二次 recovery 后的规范化状态哈希。

每个 case 归入以下互斥结果之一：

- `committed_complete`；
- `aborted_clean`；
- `aborted_invisible_orphan`；
- `partial_visible`；
- `committed_incomplete`；
- `policy_violation`；
- `provenance_violation`；
- `model_contract_failure`；
- `unknown`。

## 10. 指标与成功标准

主要指标：

- atomicity violation rate；
- policy violation rate；
- provenance violation rate；
- independent oracle match；
- recovery convergence rate；
- pending orphan rate；
- live Qwen contract success；
- task completion、工具调用数与 endpoint-reported token usage。

`TaskTransaction` 的正式成功标准：

- `partial_visible = 0`；
- `committed_incomplete = 0`；
- `policy_violation = 0`；
- `provenance_violation = 0`；
- `unknown = 0`；
- 所有有效 case 与独立 oracle 一致；
- 第二次 recovery 不改变规范化状态哈希；
- 每个事务最多一个终态决议。

模型未满足多事件 contract 的 live episode 不用于伪造事务结果，也不从总分母删除。论文同时报告全部 live episode 和满足机制前提的条件分母。

## 11. 统计分析

- `DirectDispatch` 与 `TaskTransaction` 按同一 trace 和 schedule 配对；
- 二元违规结果报告配对差值、exact McNemar 检验和置信区间；
- 每种 failure schedule 独立报告分母与结果；
- 按 workload family 分层，防止某一 family 支配聚合结果；
- deterministic replay、真实服务 repetition 和 live Qwen episode 不混合为同一成功率；
- 对 0 次违规同时报告确切分母，不把“未观察到”写成无限范围保证。

## 12. 实现边界

建议新增：

- `src/txnmem_task_transaction.py`：journal、coordinator、gateway 与 state machine；
- `src/txnmem_transaction_recovery.py`：恢复和清理；
- `src/txnmem_transaction_experiment.py`：trace replay、process harness 和正式聚合；
- `configs/real_agent_transaction_tasks.json`：冻结多事件 task；
- `configs/real_agent_transaction_schedules.json`：故障矩阵；
- 对 `src/txnmem_real_agent.py` 增加显式 `transaction_mode`；
- 对 `src/txnmem_vector_graph_backend.py` 增加 transaction stage、raw readback 和 cleanup seam；
- 对 `src/txnmem_failure_controller.py` 增加 commit-phase 语义触发点；
- 对 CLI 增加 trace capture、paired replay、recovery batch 和 live batch 子命令。

现有 direct gateway 和旧实验命令必须保持行为兼容。不得让新事务模式成为隐式默认值，从而重写历史 evidence。

## 13. 测试策略

### 13.1 单元测试

- 合法/非法状态转换；
- committed/aborted 决议唯一性；
- read-your-writes 与隔离其他 pending transaction；
- commit-time policy revalidation；
- provenance/supersession intent 与对象共同决议；
- recovery 幂等性和状态哈希稳定性；
- ambiguous response 必须 readback，无法读取时分类为 unknown。

### 13.2 Fake backend 故障测试

对 Qdrant/Neo4j 每个 stage 注入：成功、操作前失败、提交后响应丢失、读取失败、清理失败。测试必须区分 visible correctness 与 invisible orphan cleanup。

### 13.3 子进程测试

父进程等待 journal phase event 后发送 `SIGKILL`，确认原 worker 确实退出，再启动独立 recovery process。每个 commit phase 均有自动化回归测试。

### 13.4 差分 oracle 测试

将模型真实事件转换为 reference history；候选结果必须属于 reference 允许集合。reference 代码不得导入 transaction gateway 或 recovery 模块。

### 13.5 真实服务 smoke 与正式 batch

正式 batch 前每个场景先运行一次 smoke，并要求 journal、proxy/phase、Qdrant、Neo4j 和 oracle evidence 齐全。任何缺失 evidence 的 repetition fail-closed 为 unknown，不得进入成功计数。

## 14. 正式产物

- 冻结 task、trace、failure 和 live manifest；
- 每个 case 的脱敏 journal、event、进程退出码和 raw/gateway readback 摘要；
- `paired_results.jsonl`；
- `recovery_results.jsonl`；
- `live_qwen_results.jsonl`；
- `aggregate.json`；
- model/backend/environment attestation；
- oracle audit、claim ledger、source hash 与精确复现命令；
- 违规率对比图、恢复状态分类图、commit-phase 恢复图。

原始 prompt、完整 tool arguments、账号凭据和未脱敏业务值不进入公开 aggregate。正式 manifest 使用稳定 task ID、hash 和统计单位，重试不能从正式分母中删除原始失败 episode。

## 15. 论文主张边界

允许的主张：

> 在固定 Qwen2.5-7B、多事件 task manifest、TxnMem gateway 和被测故障调度下，task-scoped durable memory transactions 在真实进程终止及恢复后得到 oracle-confirmed complete-or-invisible 状态。

不允许据此声称：

- 任意 Agent 外部工具副作用可以回滚；
- Qdrant/Neo4j 获得原生统一事务；
- 绕过 gateway 的直接查询满足相同隔离；
- 系统提供一般分布式事务、线性一致性、availability 或跨协调者容错；
- 固定 manifest 的 0 次违规等于生产环境永不违规。

## 16. 完成定义

本功能只有在以下条件同时成立时才算完成：

1. 事务模式在真实 Qwen tool loop 中可执行；
2. 四个进程 kill phase 均由独立 recovery process 收敛；
3. 480 个确定性 case、360 次真实服务观测和 120 个 live episode 使用冻结 manifest 完成或按失败状态保留；
4. 独立 oracle、raw backend readback、gateway visibility 和 journal 决议均被正式聚合器复算；
5. 全量单元/集成测试、artifact audit、claim audit 和隐私扫描通过；
6. 论文只消费 active artifact，并保留本设计的准确 claim boundary。
