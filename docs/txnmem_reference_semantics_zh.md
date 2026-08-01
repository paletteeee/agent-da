# TxnMem 独立 Reference Semantics（v0.1）

本文档定义 TxnMemBench 的独立 reference semantics。它是后续
`reference executor` 的唯一语义依据，用来从 workload、policy、failure
schedule 和实际操作序列计算 ground truth。

本规范不是 TxnMem simulator 的实现说明，也不调用 TxnMem 的代码。特别是：

1. workload generator 只生成输入场景，不生成 `expected_outcome`；
2. reference executor 不导入、不调用 TxnMem simulator、invariant checker
   或其内部 repair 函数；
3. TxnMem 和其他 baseline 都只与 reference executor 的输出比较；
4. reference executor 使用确定性的状态转移规则，必要时输出“允许结果集合”，
   而不是为了配合某个实现而选择一个结果。

## 1. 输入、输出与独立性边界

### 1.1 Reference executor 的输入

reference executor 读取一个 workload instance 的以下字段：

```text
workload
seed
initial_memory
initial_policy
operations
failure_schedule
parameters
```

其中：

- `initial_memory` 是初始状态，不包含由本次 workload 运行产生的 memory；
- `initial_policy` 是带版本号的策略状态；
- `operations` 是待执行的真实操作序列；
- `failure_schedule` 是在操作边界或显式触发点注入的事件；
- `parameters` 只控制 workload 的规模和结构，例如 transaction size、
  provenance depth、branch factor 和 merge 数量。

当前 schema 中的 `expected_outcome` 可以暂时保留以兼容旧 JSONL，但在新的
数据构造流程中必须为空、删除，或由 reference executor 运行后再写入。它不能
作为 reference executor 的输入，也不能参与语义判断。

### 1.2 Reference executor 的输出

reference executor 输出一个 oracle record：

```json
{
  "oracle_version": "0.1",
  "allowed_outcomes": [
    {
      "txn_states": {"txn_001": "aborted"},
      "committed_memory_ids": [],
      "visible_memory_ids": ["m0"],
      "invalid_memory_ids": [],
      "superseded_memory_ids": [],
      "provenance_edges": [],
      "policy_version": 2,
      "invariants": {
        "atomicity": true,
        "commit_authorization": true,
        "no_invalid_visibility": true,
        "provenance_closure": true
      }
    }
  ],
  "safety_invariants": {
    "atomicity": true,
    "commit_authorization": true,
    "no_invalid_visibility": true,
    "supersession_consistency": true,
    "provenance_closure": true
  },
  "event_trace": [],
  "minimal_counterexample": null
}
```

`allowed_outcomes` 是一个集合。只有在 failure schedule 对原子边界的观察点
本身不够精确时，集合才可以包含多个结果。例如，`crash at commit` 若没有
说明 crash 发生在 commit linearization 之前还是之后，则允许 `abort` 和
`commit` 两个原子结果；不允许出现 partial commit。

## 2. 状态模型

reference state 表示为：

```text
S = (M, P, T, G, F, E)
```

### 2.1 Memory state `M`

每个 memory object 至少包含：

```text
MemoryObject = {
  id,
  value,
  scope,
  status,
  owner_txn,
  policy_version,
  supersedes_id,
  created_by,
  committed_at
}
```

`status` 取以下值：

- `pending`：只存在于未提交 transaction 的写集合中，对普通查询不可见；
- `active`：已提交且可被当前策略访问；
- `superseded`：被新版本替代，不再是该 logical memory 的当前版本；
- `invalid`：由于来源失效、策略要求或显式 invalidate 而不可继续使用。

`pending` 对象不属于 committed state。事务 abort 后，pending 对象和它的
provenance 边全部消失；事务 commit 后，所有相关对象和边同时变为可见的
committed state。

### 2.2 Policy state `P`

策略是带版本号的状态：

```text
Policy = {
  version,
  rules
}
```

每一次 policy change 都原子地生成一个更大的 `version`。每条 rule 至少描述：

```text
Rule = {
  principal,
  action,       // read | search | write | derive | propagate | supersede
  scope,
  effect        // allow | deny
}
```

未被明确允许的 action 默认拒绝。`scope` 检查必须使用资源的真实 scope，不能
因为资源出现在某个 search 结果中就自动获得更宽权限。

### 2.3 Transaction state `T`

```text
Transaction = {
  id,
  principal,
  begin_policy_version,
  status,          // active | committed | aborted
  read_set,
  write_set,
  derive_set,
  supersession_set,
  pending_edges
}
```

reference semantics 的 transaction 是原子的：一次 transaction 的 committed
write、derived memory、supersession 更新和 provenance edges 要么全部可见，要么
全部不可见。

`begin_policy_version` 只用于记录事务开始时观察到的版本；它不能替代
commit-time policy validation。commit 时必须重新检查当前 `P.version` 和所有
受影响资源的权限。

### 2.4 Provenance graph `G`

`G` 是有向图，边方向为“来源 → 派生结果”：

```text
Edge = {
  source_id,
  derived_id,
  relation,       // read_derive | propagate | supersede
  operation_id,
  txn_id,
  committed_at
}
```

只有真实的 `read → derive → write/propagate` 操作可以产生
`read_derive` 或 `propagate` 边；`supersede` 只能产生明确标记为
`relation = supersede` 的替代关系。generator 不得为了让图满足某个期望答案
而事后补边。

边必须满足：

- source 和 derived object 都存在，或 source 是显式记录的外部输入；
- `source_id != derived_id`；
- 不产生有向环；
- 同一 `(source_id, derived_id, operation_id, relation)` 不重复；
- 未提交 transaction 的 pending edge 不进入 committed graph。

## 3. 操作语义

下面的规则使用 `S --op--> S'` 表示一次操作产生状态转移。所有拒绝均产生
明确的 `denied` event，不得静默改变状态。

### 3.1 `begin_txn`

```text
begin_txn(t, principal)
```

创建 `T[t]`：

```text
status = active
begin_policy_version = P.version
read_set = write_set = derive_set = supersession_set = {}
pending_edges = {}
```

begin 不锁定未来的 policy。事务开始后发生的 revoke 或 policy version
change 对 commit validation 生效。

### 3.2 `read`

```text
read(t, memory_id)
```

reference executor 按以下顺序判断：

1. memory 必须处于 `active`；
2. 当前 policy 必须允许该 principal 对该 memory 的 `read`；
3. memory 的 scope 必须与 rule 的 scope 相交或满足定义的包含关系。

成功读取时，将 `memory_id` 加入 `read_set`，并记录 read event。若 memory 为
`superseded` 或 `invalid`，默认读取失败；若 policy 不允许，读取结果也必须是
不可见。read 失败不能产生 provenance edge。

### 3.3 `search`

```text
search(t, query, scope)
```

search 先按 query 和 scope 得到候选集合，再对每个候选独立执行与 `read`
相同的 authorization 和 status 检查。返回集合只能包含当前可见的 `active`
memory。

因此，“允许 search”不等价于“允许读取所有 memory”，search 结果也不能成为
scope bypass 的依据。返回结果中实际被后续 derive 使用的 object，仍须记录
单独的 read event。

### 3.4 `get_by_id`

```text
get_by_id(t, memory_id)
```

`get_by_id` 与 `read` 使用相同的 policy、scope 和 status 检查。知道 ID 不会
绕过授权，也不会绕过 invalid/superseded 检查。

### 3.5 `write`

```text
write(t, memory_id, value, scope)
```

write 首先检查当前 policy 是否允许 `write`，然后把新对象放入 `write_set`
并标记为 `pending`。在 commit linearization 前，它不能被其他 transaction 或
普通 read/search 看到。

如果 `memory_id` 是新 ID，reference executor 记录新对象；如果它是已有对象的
更新，则记录一个新的 version object，不原地覆盖已提交对象。这样可以保留
旧版本的 provenance 和 supersession 关系。

### 3.6 `derive`

```text
derive(t, source_ids, output_id, value, scope)
```

derive 的前置条件是：

1. 对每个 `source_id` 的 read 已成功，或该 source 是显式允许的外部输入；
2. 当前 policy 允许 `derive`；
3. 输出 scope 满足 policy 的 scope restriction；
4. `output_id` 不与现有 active object 冲突，除非该操作明确表示 version update。

成功后创建 pending output，并对每个真实使用的 source 记录待提交的
`read_derive` edge。derive 不得直接引用 generator 提供的任意 DAG 来“补足”
输入；边集合必须由本操作的 `source_ids` 产生。

### 3.7 `propagate`

```text
propagate(t, source_id, output_id, target_scope)
```

propagate 要求 source 可读、当前 policy 允许 `propagate`，且 target scope
满足跨 scope 传播规则。它可以写入一个新的 pending output，也可以向已有的
derived object 添加一次传播事件；两种情况都必须记录真实的 operation 和
待提交 provenance edge。

### 3.8 `supersede`

```text
supersede(t, old_id, new_id)
```

supersede 必须与 new version 的写入属于同一个 atomic commit。commit 成功后：

```text
old.status = superseded
new.status = active
```

commit 失败或 crash before linearization 时：

```text
old.status 保持不变
new 不进入 committed state
```

一个 transaction 不能只把旧对象标记为 superseded 而不提交新对象。旧版本
的历史 provenance 仍保留，但普通 retrieval 只返回当前 active version。

### 3.9 `invalidate` 与 `repair`

```text
invalidate(memory_id, reason)
repair(memory_id | root_set)
```

`invalidate` 将目标 object 标记为 `invalid`。随后对 provenance graph 计算
committed descendant closure：

```text
Desc(root) = {x | root ->* x in G}
```

在 repair 完成后，所有依赖 invalid source 的 active descendant 都必须被标记为
`invalid` 或显式 `stale`；本 v0.1 只使用 `invalid`，因此 oracle 中不得再把它们
列入 `visible_memory_ids`。

repair 必须满足：

- 传递闭包，而非只处理一跳 child；
- 与 provenance graph 的 edge direction 一致；
- 幂等：重复 repair 不改变最终集合；
- 不创建新的 provenance edge；
- 不会把 invalid object 自动恢复为 active；如需恢复，必须有新的 derive/write
  transaction。

为了避免把实现细节混入 ground truth，v0.1 将一次 `invalidate + repair`
视为 reference-level atomic macro。后续若研究 repair crash，再扩展为显式的
incremental repair semantics，并把未完成 repair 的状态加入
`allowed_outcomes`。

## 4. Commit、Abort 与 Failure Semantics

### 4.1 Commit 的两个阶段

每个 `commit(t)` 分为：

```text
validate(t, current_state)
linearize(t)
```

`validate` 至少检查：

1. transaction 仍为 `active`；
2. 当前 policy 允许所有 read/write/derive/propagate/supersede action；
3. read set 中的 source 没有在本次依赖关系下失效或被不允许地替换；
4. write scope 和 target scope 合法；
5. supersession 的 old/new 前置条件成立；
6. pending provenance edges 合法且不会产生 cycle；
7. 本次 transaction 不会使任何已提交的 invariant 失效。

只有全部检查通过，`linearize` 才能一次性提交：

- `write_set` 中的对象变为 `active`；
- derived objects 变为 `active`；
- supersession status 一起更新；
- pending provenance edges 一起写入 `G`；
- transaction 状态变为 `committed`。

任意一项失败，transaction 变为 `aborted`，所有 pending object 和 pending edge
丢弃。reference semantics 不允许 partial commit。

### 4.2 Policy change

```text
policy_change(new_rules)
```

policy change 在一个事件边界原子生效，并令 `P.version := P.version + 1`。
已经提交的 memory 不会因为 policy change 自动被删除；但从该事件之后开始：

- 新的 read/search/get_by_id 必须按新 policy 判断；
- 尚未 linearize 的 transaction 必须按新 policy revalidate；
- 被撤销权限保护的 transaction 不能仅凭旧的
  `begin_policy_version` commit。

### 4.3 Crash

crash 是控制流事件，不是随机地改变 memory state。reference semantics 只允许
以下两类结果：

| crash 位置 | reference 结果 |
|---|---|
| `before_validate`、`during_validate`、`before_linearize` | transaction abort；无本次 committed write/edge |
| `after_linearize`、`after_commit` | transaction committed；所有 commit effects 保留 |

如果 schedule 只写 `target = commit`，却没有说明 linearization 位置，oracle
必须返回两个 allowed outcomes（abort 和 commit），但两个 outcome 都必须满足
atomicity。不得用 TxnMem 的某一次执行结果替代这个语义集合。

### 4.4 Delay、revoke 与 invalidate 的触发点

failure schedule 的具体生成方式可以由 workload 层决定，但 reference 层只接受
已经解析的事件：

```text
event = {
  trigger,
  action,
  target,
  phase
}
```

例如：

```json
{
  "trigger": {"after_operation": "op_write_001"},
  "action": "crash",
  "target": "txn_001",
  "phase": "before_linearize"
}
```

`delay` 只改变后续事件的顺序，不直接改变状态；`revoke` 生成新的 policy
version；`invalidate` 和 `repair` 按第 3.9 节执行。随机 schedule 只能作为
baseline，不能成为 oracle 的语义来源。

## 5. Serial reference 与并发 schedule 的关系

reference executor 采用确定性的事件线性化模型：每个 operation、policy
change 和 failure event 都有一个唯一的 event position。对于有明确
`phase` 的 crash，状态转移是单一结果；对于 commit 边界不明确的 crash，返回
允许结果集合。

在 micro-witness 中，可以枚举所有满足因果约束的 interleaving。每一个
interleaving 都独立运行同一套语义规则，得到一个 oracle outcome。多个
interleaving 都被 schedule 允许时，最终 oracle 是这些结果的并集，而不是由
TxnMem 自己挑选其中一个结果。

这使得比较可以区分：

- implementation 产生了语义允许的结果；
- implementation 产生了 partial commit、越权可见性或 stale visibility；
- implementation 只是在未指定的 crash boundary 上选择了另一个允许结果。

## 6. Oracle 的比较规则

baseline 的最终状态 `R` 通过以下规则与 reference oracle 比较。

### 6.1 Safety：必须满足

以下性质必须在每一个允许结果中成立：

1. **Atomicity**：一次 transaction 的 committed effects 全有或全无；
2. **Commit authorization**：commit 使用当前 policy，而不是只使用 begin-time
   policy；
3. **Scope safety**：任何返回或写入都不超出授权 scope；
4. **No invalid visibility**：invalid、stale 和 superseded object 不得作为
   当前 active memory 返回；
5. **Supersession consistency**：旧版本被替代时，新版本必须同时成为 active；
6. **Provenance closure**：invalid source 的 committed descendants 不得继续
   以 active/visible 状态存在；
7. **Graph validity**：provenance graph 无环、无孤立的伪造 edge、无重复 edge。

### 6.2 Exact fields：在语义确定时必须一致

当 `allowed_outcomes` 只有一个元素时，以下字段必须逐项一致：

```text
txn_states
committed_memory_ids
visible_memory_ids
invalid_memory_ids
superseded_memory_ids
provenance_edges
policy_version
```

当 `allowed_outcomes` 有多个元素时，implementation 的结果必须属于该集合；
同时仍必须满足所有 safety invariants。

### 6.3 Liveness：单独报告，不混入 safety

在无 crash、事件最终执行、并且 policy 最终允许操作的条件下，reference
executor 可以报告 liveness expectation，例如“可提交 transaction 最终应
committed”或“repair 最终覆盖全部 descendant”。crash、永久 deny 或无限 delay
下不把 liveness 失败误报成 safety 失败。

## 7. W1–W8 的语义映射

下面是当前核心 workload family 应使用的语义目标。它们是 reference
executor 的计算结果约束，不是 generator 中手写的 `expected_outcome`。

| workload | reference 重点 | 成功/失败的语义条件 |
|---|---|---|
| W1 Atomic Multi-Write | 多写原子性 | commit 前 crash 时所有本次 write 都不可见；不得只留下第一条 write |
| W2 Crash During Commit | commit boundary | before-linearize 只能 abort；after-linearize 只能 commit；未标 phase 时允许结果集合 |
| W3 Revoke Before Commit | commit-time policy | write 后 revoke，若 revoke 先于 linearize，则 commit 必须 abort |
| W4 Scope Bypass | 查询和写入授权 | search 允许不代表 get_by_id/read 或 write 允许；越权结果不可见 |
| W5 Supersession Consistency | version replacement | old/new 的替代关系必须原子；不能出现“old 已失效但 new 未提交” |
| W6 Provenance Chain Repair | 传递 repair | root 失效后，所有 committed descendants 都不可见；只修一跳属于错误 |
| W7 Branch/Merge Repair | 多分支和合并 | 任一必要 source 失效都使依赖它的 derived object 失效；merge 不能漏修 |
| W8 Policy/Repair Interaction | policy 与 repair 的组合 | repair 不能绕过当前 policy；被拒绝的 rederive 不得恢复为 active |

### 7.1 最小实例示例

#### W1

```text
begin(txn_001)
write(txn_001, m1)
write(txn_001, m2)
crash(txn_001, before_linearize)
```

唯一 oracle 结果：`txn_001 = aborted`，`m1` 和 `m2` 均不在
`committed_memory_ids` 或 `visible_memory_ids` 中。

#### W3

```text
begin(txn_001, policy_version=1)
write(txn_001, m1)
policy_change(version=2, deny write)
commit(txn_001)
```

由于 policy version 2 在 linearization 前生效，唯一 oracle 结果是 abort；旧的
begin-time version 1 不能授权该 commit。

#### W6

```text
initial: m0
derive(m0 -> m1)
derive(m1 -> m2)
derive(m2 -> m3)
invalidate(m0)
repair(m0)
```

reference executor 计算 `Desc(m0) = {m1, m2, m3}`，并要求这三个 object 都不在
最终 visible set 中。这个结果来自真实 derive events 和 graph closure，不来自
generator 事先填写的任意 `provenance_edges`。

## 8. Generator 与旧 schema 的迁移约束

为避免一次性破坏旧数据，迁移可以分两步：

### 阶段 A：兼容读取

- 保留旧 `expected_outcome` 字段，但 reference executor 忽略它；
- 将旧 `provenance_edges` 视为 legacy metadata，只用于审计和对比；
- 新生成的 instance 额外记录每个 derive/propagate operation 的 source IDs；
- 用 reference executor 生成新的 oracle record，并报告 legacy edges 与实际
  event-derived edges 的差异。

### 阶段 B：正式 schema

- 从 instance 输入中删除 `expected_outcome`；
- 将 `provenance_edges` 改为 executor 输出，或改名为
  `initial_provenance_edges`，仅表示初始外部图；
- 在 operation record 中明确 `operation_id`、`txn_id`、`source_ids`、
  `output_id`、`scope` 和 `policy_version_observed`；
- 将 `oracle_version` 和 reference executor 的版本写入结果文件；
- 保存 `event_trace`，使每个 committed edge 都能追溯到真实 operation。

## 9. 必须保留的审计信息

reference executor 每次运行至少记录：

```text
event_id
operation_id
txn_id
event_type
pre_state_digest
post_state_digest
policy_version
decision              // allowed | denied | committed | aborted | invalidated
reason_codes
affected_memory_ids
affected_edge_ids
```

`reason_codes` 使用稳定字符串，例如：

```text
POLICY_REVOKED
SCOPE_DENIED
SOURCE_INVALID
SOURCE_NOT_READ
COMMIT_PRECONDITION_FAILED
CRASH_BEFORE_LINEARIZE
CRASH_AFTER_LINEARIZE
SUPERSESSION_TARGET_MISSING
PROVENANCE_CYCLE
```

这些记录用于生成最小反例、coverage 和 differential-oracle 报告，不用于让
TxnMem 反向修改 oracle。

## 10. 本规范的验收标准

完成“定义独立 reference semantics”这一项的最低标准是：

- 本文档明确规定 state、operation、policy version、commit linearization、
  crash、invalidate/repair、supersession 和 provenance 的语义；
- 明确说明 `expected_outcome` 不是 reference executor 的输入；
- 明确说明 provenance edge 必须由真实 operation 产生；
- 对 W1、W3、W4、W5、W6 至少有可判定的最小示例；
- 对 commit 边界不明确的 crash 使用 allowed outcome set，而不是借用某个
  implementation 的执行结果；
- 下一步可以直接据此实现一个不依赖 TxnMem simulator 的 reference executor。

下一项工作是实现 reference executor，并为 W1/W3/W6 编写 differential-oracle
测试；在此之前不应继续扩大 W7/W8 的 workload 数量。

## 11. 当前 pilot 与本规范的已知差异

本节不是 reference semantics 的例外，而是迁移时必须显式处理的差异：

1. 当前 `txnmem_simulator.py` 在执行某个 operation 之前处理同一个 `step` 的
   failure events。因此旧 W1 的 `step = 2` 实际对应“第一个 write 执行之前”，
   不能直接解释为“完成第一条 write 后 crash”。新 schedule 应改为显式的
   `after_operation: op_write_001` 触发器。
2. 当前 W2 只有 `target = commit`，没有
   `before_linearize/after_linearize` phase。它只能对应一个未精化的 crash
   boundary；reference oracle 应输出允许结果集合，或在 workload 迁移时补充
   phase，而不能沿用旧的单一 abort 标签。
3. 当前 W6/W7 的 `provenance_edges` 由 generator 预先写入，且 operation 只有
   `invalidate`。这可以作为 legacy replay 输入，但不满足“由真实
   read/derive/propagate 操作产生 provenance”的正式要求。正式版本需要记录
   derive/propagate operations，再由 reference executor 计算 graph 和 repair
   closure。
4. 当前 schema 强制要求 `expected_outcome`，这只是兼容旧数据的结构约束，不是
   语义约束。迁移完成后应将它移出 instance input，并由 oracle output 取代。

因此，现阶段不应宣称旧 JSONL 已经具有独立 ground truth；它只能被标记为
`legacy pilot`。只有经过 reference executor 重放、并保存 oracle version 和
event trace 的记录，才能进入正式 TxnMemBench 评测集。
