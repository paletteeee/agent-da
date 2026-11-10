# 多智能体共享记忆正确性实验设计

日期：2026-09-11  
状态：已完成对话设计，等待用户审阅书面规格

## 1. 目标与论文主张

本实验扩展 TxnMem 的受控正确性证据，使同一实验实例中至少有两个智能体共同访问同一个记忆空间，并让它们的事务、读取、写入、权限变化和来源依赖操作发生真实交错。实验不再把 `agent_1` 与 `agent_2` 当作可互换的单一执行者名称，而是把它们建模为同时存在、拥有独立事务和独立权限的操作主体。

实验通过完整 TxnMem、普通直接写入和三个机制消融版本的配对比较，检验以下主张：

> 在本文定义的共享记忆操作模型和故障条件下，TxnMem 通过事务隔离与原子提交、提交时权限重检以及跨智能体来源依赖修复，保证多智能体共同读写共享记忆时的事务、权限和来源一致性。

## 2. 主张边界与非目标

本工作支撑的是“已定义模型和已覆盖故障调度内的正确性”，不是对任意分布式系统、任意网络分区或任意智能体行为的无限制证明。

本实验不做以下主张：

- 不声称枚举了任意长度程序的所有可能并发执行；穷举只覆盖冻结的小型见证程序。
- 不声称五十个种子可以代替形式化证明；它们用于覆盖较复杂但有限的参数组合。
- 不把两个相同名称标签轮流执行解释为多智能体并发；同一实例必须至少出现两个不同 `agent_id`。
- 不要求两个智能体由不同大模型驱动；受控实验验证运行时语义，真实模型实验只验证事件接口。
- 不把真实存储补充实验解释为任意进程崩溃下的通用分布式事务协议。
- 不删除超时、未知状态、服务错误或失败重复；它们全部保留在正式分母中。

## 3. 总体架构

实验沿用现有规范化 workload、五个系统版本、独立 reference executor 和 invariant checker，但把主模拟器从单一全局事务状态改造成按 `txn_id` 分离的多事务执行器。

```text
每个智能体的程序序列
        |
        v
交错调度器 -----> 冻结的全局操作序列与故障调度
        |                         |
        |                         +-----> 独立参照执行器
        v
TxnMem / 四个对照版本                    |
        |                                |
        +---------------> 差分比较器 <---+
                              |
                              v
                  正确性条件、最小错误见证和统计汇总
```

### 3.1 多智能体实例

每个实例包含：

- 两个固定存在的主体 `agent_1` 和 `agent_2`；
- 每个主体自己的程序顺序；
- 每条操作的 `agent_id`、`txn_id`、`op_id` 和全局 `step`；
- 按主体、动作和 scope 区分的权限；
- 显式的崩溃、撤权和来源失效调度；
- 用于复现的种子、实际采样参数和结构签名。

小型实例由调度器枚举所有保持每个智能体内部程序顺序的合法交错。压力实例由种子选择一条合法交错，同时改变结构参数。

### 3.2 多事务执行状态

执行器维护一个 committed memory store 和一个按 `txn_id` 索引的事务表。每个事务分别保存：

- 所属 `agent_id`；
- 开始时观察到的权限版本；
- 待提交记忆写集；
- 待提交 provenance edge 和 supersession 更新；
- 已读取 committed memory 的版本与 scope；
- `active`、`committed` 或 `aborted` 状态。

一次 commit 只能读取并发布相同 `txn_id` 的写集。任何事务都不能清空、提交或读取另一个事务的 pending write。

### 3.3 独立参照语义

`txnmem_reference.py` 继续作为独立参照实现，不导入主模拟器或 invariant checker。给定同一个冻结的全局操作序列和故障调度，它计算允许的最终可观察状态，包括：

- 每个事务的最终状态；
- 正式提交和可见的 memory ID；
- 每次读取实际可见的 memory ID；
- invalid、superseded 和 active 状态；
- provenance edge；
- 被拒绝的主体、动作和原因。

故障阶段明确区分 `before_linearize` 与 `after_linearize`。前者只能整体中止，后者必须恢复为完整提交，避免把不精确的崩溃点作为任意结果解释。

### 3.4 真实存储补充

真实存储实验使用两个并发客户端，通过同一个多事务协调器访问现有 Qdrant 与 Neo4j 适配器。完整 TxnMem 在 commit 临界区内执行权限与来源重检，并把同一事务的后端写入作为一批操作跟踪；任一后端操作失败时对该事务已执行的写入进行补偿，并以 operation-after readback 判定最终持久状态。

普通直接写入条件逐条发送相同操作，不使用事务缓冲、提交重检或传递修复。两个条件使用相同初始状态、操作内容、故障位置和唯一 namespace。

真实存储层只补充验证并发客户端和真实 Qdrant/Neo4j 状态，不替代受控层对所有交错和机制消融的判定。

## 4. 操作语义

### 4.1 读取与事务隔离

事务可以读取 committed active memory 和自己的 pending write，从而满足 read-your-writes。其他事务的 pending write 一律不可见。每次读取记录 `reader_agent_id`、`txn_id`、`memory_id`、decision 和 reason code，使跨智能体脏读可以直接审计，而不是只从最终状态推断。

### 4.2 原子提交与独立事务

完整 TxnMem 的 commit 在一个线性化点一次发布本事务全部写入、来源边和 supersession 更新。提交前崩溃时本事务全部不可见；线性化后崩溃时恢复结果必须完整可见。已经提交的另一个事务不受影响，仍处于 active 的另一个事务也不能被误提交。

### 4.3 并发更新

读取 committed memory 时记录其版本。两个事务读取同一旧版本并产生竞争更新时，先提交者更新版本；后提交者在 commit 重检中发现 read version 或 supersession target 已变化并整体中止。实验不允许两个竞争的新版本同时处于 active。

### 4.4 按主体重检权限

权限状态以 `(agent_id, action, scope)` 为键。撤销 `agent_1` 的写权限不得影响 `agent_2`。commit 使用事务所属主体、最新权限版本和完整写集重新检查；任一动作不再授权时，整个事务中止。scope 检查同时覆盖 search 和 get-by-ID，防止直接编号读取绕过范围限制。

### 4.5 跨智能体来源一致性

provenance edge 不受创建者身份限制。`agent_1` 创建来源、`agent_2` 创建派生内容后，来源失效必须沿 committed provenance graph 传递到所有后继。若来源在派生事务 commit 之前失效，commit 重检必须发现 source status/version 变化并整体中止，不得发布基于失效来源的新内容。

## 5. 受控实验族

### 5.1 未提交内容的跨智能体可见性

`agent_1` 在事务中写入记忆；`agent_2` 在提交前后分别读取。检查提交前不可见、提交后在权限允许时可见，以及本事务 read-your-writes 正常。

### 5.2 交错事务的独立原子性

两个智能体各写一组记忆，操作相互交错。调度一个事务提交、另一个事务在提交前或提交阶段故障。检查每个事务分别满足全有或全无，且 commit 不会发布其他事务的写集。

### 5.3 同一记忆的竞争更新

两个智能体读取同一旧版本并分别创建竞争的新版本。检查只有一个更新可以成为 active，后提交事务因版本或 supersession 冲突中止。

### 5.4 主体特定的权限和 scope

两个事务开始后只撤销一个智能体的一项权限。检查被撤权事务整体中止，未被撤权事务正常提交；同时检查不同 scope 的 search 和 get-by-ID 都不会泄露记忆。

### 5.5 跨智能体来源修复

一个智能体创建根来源，另一个智能体创建链式或分支式派生内容。根来源失效后检查所有后继失效；来源在派生事务 commit 前失效时检查派生事务整体中止。

### 5.6 混合压力

同一实例组合两个交错事务、多条写入、选择性撤权、来源状态变化和提交附近崩溃，检查三个机制组合后仍满足全部条件。

## 6. 交错覆盖与种子设计

### 6.1 穷举微型见证

每个核心机制至少冻结一个短程序。调度器枚举所有保持各智能体程序内顺序的线性扩展。每条线性扩展单独写入结果，不用随机抽样代替。报告总交错数、去重后的结构数、oracle mismatch 和最短失败前缀。

### 6.2 五十个结构不同的压力实例

每个实验族使用种子 `0..49`。种子实际控制：

- 每个事务的一至四条写入；
- 两个智能体操作的合法交错；
- 被撤权的主体、动作与触发位置；
- 遭遇故障的事务和故障阶段；
- provenance depth 一至四；
- branch factor 一至三；
- 来源创建者与派生创建者；
- 并发更新的先提交者。

生成器为每个实例计算排除 seed、instance ID、memory ID 后的规范化结构签名。同一实验族的五十个签名必须唯一；不唯一时生成命令失败，不得把重复编号计为不同场景。每个实际参数值和签名随实例保存。

六个实验族共产生 300 个 seeded instance。每个实例在五个版本上运行，得到 1500 条配对 observation。穷举交错结果另行报告，不混入五十种子分母。

## 7. 对照版本

所有版本运行完全相同的实例：

- `Naive`：直接写入、无统一权限提交检查、无传递修复；
- `TxnMem-NoTxn`：关闭事务隔离与原子发布；
- `TxnMem-NoPolicyCommit`：保留事务，但关闭 commit-time policy revalidation；
- `TxnMem-NoRepair`：保留事务和权限重检，但关闭 transitive provenance repair；
- `TxnMem`：启用完整机制。

多智能体重构不得静默改变旧单智能体实验的输入和预期输出。旧受控套件作为兼容性回归测试保留。

## 8. 正确性条件和判定

多智能体实验使用以下可直接审计的条件：

1. `no_dirty_read`：不能读取其他事务的 pending memory。
2. `per_txn_atomicity`：每个事务的写集只能完整可见或完整不可见。
3. `no_cross_txn_flush`：提交一个事务不能发布另一个事务的写集。
4. `commit_authorization_by_actor`：提交内容全部满足提交时该主体的最新权限。
5. `scope_safety_by_actor`：search 与 get-by-ID 都不能越过主体获准的 scope。
6. `conflict_consistency`：竞争更新不能产生多个同时 active 的版本。
7. `cross_agent_provenance_closure`：失效来源不存在 active transitive descendant，与创建者身份无关。
8. `recovery_consistency`：恢复状态属于明确故障阶段对应的允许结果。

每条 observation 同时接受两种判定：invariant checker 输出具体违规类型；independent oracle 比较完整可观察状态。两者不得共享最终状态生成代码。

## 9. 真实存储实验矩阵

正式补充实验包含四个代表场景：

1. 两个并发事务写入不同对象，其中一个在 Qdrant/Neo4j 分阶段写入时故障；
2. 两个并发事务中只撤销一个主体的写权限；
3. 一个主体创建来源、另一个创建派生链，随后根来源失效；
4. 并发写入、来源失效和一次服务故障组成的混合场景。

每个场景运行 30 次，分别运行 `Naive` 与完整 `TxnMem`，共 240 条 observation。每次使用唯一 namespace，保留线程实际开始顺序、commit linearization order、故障代理证据和两个后端的 operation-after state。

最终状态分为互斥类别：

- `committed_complete`；
- `aborted_clean`；
- `partial_visible`；
- `policy_violation`；
- `provenance_violation`；
- `unknown`。

`unknown` 保留在分母中，不能改写为成功。

## 10. 成功标准

只有同时满足以下条件，论文才能启用第 1 节的主张：

- 完整 TxnMem 在全部穷举交错中为零 invariant violation，且 100% 匹配 independent oracle；
- 完整 TxnMem 在 300 个 seeded instance 中为零 invariant violation，且 100% 匹配 independent oracle；
- 每个机制消融至少存在一个由对应实验族产生的最小失败见证；
- `TxnMem-NoTxn` 在事务族中暴露脏读、部分提交或跨事务误提交之一；
- `TxnMem-NoPolicyCommit` 在选择性撤权族中产生未授权提交；
- `TxnMem-NoRepair` 在跨智能体来源族中留下 active descendant；
- 真实存储 TxnMem 条件的 `partial_visible`、`policy_violation`、`provenance_violation` 和 `unknown` 均为零；
- 所有正式输入、结果、汇总和报告经过 schema、哈希、计数和路径审计；
- 同一命令重新运行三次产生相同的规范化输入哈希和结果汇总哈希。

零违规结果同时报告精确计数和 95% Wilson 区间，不把有限样本中的 0 次错误解释为真实世界错误概率严格等于零。

如果任一成功标准未满足，保留全部失败结果，主张降级为已实际观察到的范围，不修改数据或排除失败场景来迎合结论。

## 11. 输出与论文证据

受控实验写入 `results/multi_agent_controlled_v1/`：

- `data/generated_instances.jsonl`：全部 seeded instance；
- `data/exhaustive_interleavings.jsonl`：全部穷举交错；
- `results/experiment_results.csv`：五版本配对结果；
- `results/exhaustive_results.csv`：穷举结果；
- `results/summary.json`：分实验族、版本和违规类型汇总；
- `results/coverage.json`：场景、参数、交错和条件覆盖；
- `results/minimal_witnesses.json`：各消融最短失败前缀；
- `results/reproducibility.json`：三次运行的规范化哈希；
- `results/claim_status.json`：逐条成功标准与主张是否可启用。

真实存储实验写入 `results/multi_agent_real_storage_v1/`，包含正式 observation、服务版本、故障路由证据、持久状态分类和审计报告。不得写入服务密码、可路由地址或原始业务内容。

## 12. 测试与错误处理

实现遵循 RED–GREEN–REFACTOR。测试先覆盖：

- 每个事务拥有独立写集和状态；
- 其他事务 pending memory 不可见；
- commit 只发布相同 `txn_id`；
- 选择性撤权只影响目标主体；
- read version 冲突使后提交事务中止；
- provenance repair 跨作者传递；
- 五十个结构签名唯一；
- reference executor 不导入主执行器；
- 每种消融能被目标场景杀死；
- 命令行生成完整、可审计的结果目录。

未知事务、事务主体与操作主体不一致、重复终态决议、缺少 scope、无效来源、重复结构签名和真实服务无法确定状态都必须显式失败或分类为 `unknown`，不得静默继续。

实现完成后运行聚焦测试、全量单元测试、三次受控复现、正式受控实验和真实存储矩阵。任何完成或论文主张都以这些新鲜运行结果为依据。
