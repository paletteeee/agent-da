# TxnMem：面向多 Agent 共享记忆的策略感知事务运行时

匿名稿

# 摘要

多 Agent 系统中的记忆一旦被多个执行者读取、写入、传播和派生，就不再只是为语言模型提供上下文的检索缓存，而成为会影响后续行动的共享状态，并直接约束其他 Agent 的行动选择、外部工具调用与协作分工安排。本文的洞见是：相似度检索、对象级访问过滤或开始时的权限检查，均不能单独保证该状态在故障、撤权和来源更新后的正确性。为此，本文提出 TxnMem，一个将 Agent Memory Transaction、Policy-Consistent Commit 与 Provenance-Driven Repair 结合的策略感知事务运行时：前者把写入、取代、派生和传播纳入同一提交边界，中者在提交点按照最新策略重验证，后者沿来源依赖闭包使受影响对象退出默认可见集合。我们以独立的串行 reference semantics、因果故障调度和差分 oracle 检查可观察历史，而非以生成文本质量替代系统正确性；判定器把策略变更、来源撤销和崩溃恢复显式编码为状态转换，以区分系统保证的合法可见性与模型输出本身的内容质量，并使错误表现为可定位的不变量违反而非主观案例分析。受控套件覆盖 400 个实例和 2,000 条变体结果，完整 TxnMem 未出现目标违规且与 oracle 一致；四类主要缺陷均有前缀最小见证；真实服务故障注入未观察到部分提交，并已接入真实 Qwen 的逐事件工具循环。<!-- TXNMEM-AUTHOR-ANNOTATIONS:BEGIN -->[[CLAIM:controlled_correctness_400x5]] deterministic controlled simulator evidence against an independent reference semantics; not a public-task accuracy claim [[CLAIM:minimal_mutant_witnesses_4]] one deterministic operation-prefix-minimal witness per major mutant; minimality is with respect to suffix removal [[CLAIM:toxiproxy_fault_matrix_5x30]] single-host real-service fault injection through Toxiproxy; not production availability, distributed 2PC, or production latency [[CLAIM:native_qwen_repetitions_5x10]] mechanism-level model tool-loop and differential-oracle evidence; not end-user task success or production quality<!-- TXNMEM-AUTHOR-ANNOTATIONS:END -->

本文的结论边界同样明确：事务提交、abort 与 invalidation-only repair 在确定性 TxnMem core/reference simulator 中实现并验证；真实 Qwen 与公开 runtime 适配器只提供逐事件接线，不提供 transactional tool execution；`stale`、受权重算和更丰富 repair 策略是形式目标中的未来扩展，内容层事实裁决、redact、scope downgrade、分布式两阶段提交或生产延迟同样不是现有能力。

关键词：多 Agent 系统；共享记忆；事务；访问策略；数据来源；故障验证

# 1 引言

设想一个订单协作流程。客服 Agent 先读取用户的新地址，物流 Agent 据此生成发货建议，订单 Agent 再把地址、建议和订单更新写回共享记忆。若进程在几项写入中间崩溃，后继 Agent 可能同时看见新地址和旧订单，或只看见没有来源的建议；这不是检索排名问题，而是一次逻辑更新被部分公开的问题。若用户在推理期间撤回物流对该订单的授权，而系统只在开始时检查策略，旧授权仍可在提交点留下新的可传播记忆。若地址源记录随后被纠正，已经由它派生的发货建议和下游副本仍被检索，则错误会沿协作链继续发挥作用。

这三个时刻对应同一个系统问题：多 Agent memory 是共享、可派生且可传播的状态，正确性必须覆盖提交边界、策略版本和来源闭包。现有 Agent memory 常把重点放在长期保存、摘要、反思或上下文调度 [R01][R02][R03]；检索增强模型和向量索引则主要回答如何找回相关内容 [R04][R05][R06][R07]。这些能力是有用的底座，却没有规定多项写入何时共同可见、策略在执行期间变化时谁能提交、或某一来源失效后哪些派生对象必须退出可用集合。传统事务定义了隔离和序列化的语言 [R11][R12][R13]，provenance 研究提供了记录与解释来源的形式工具 [R17][R18][R19][R21]，但两者尚未直接给出面向 Agent 读、写、派生、取代和传播的统一运行时语义。

TxnMem 因而不试图宣称一种更好的文本记忆或更高的公开任务准确率。它研究更窄、也更可检验的问题：给定显式策略和操作历史，系统是否只公开合法事务，并在源对象失效时恢复可检索状态。为避免实现与判定器共享同一错误，本文把被测运行时与独立 serial reference semantics 分开实现；对于贴近提交边界的崩溃，判定器保留多个可合法线性化的观察结果。受控验证、真实模型工具循环、公开 runtime 接线和真实服务故障注入构成分层证据，而不是互相替代的单一指标。

本文作出以下四项贡献：

- 定义多 Agent 共享记忆的操作模型，以及 atomicity、commit authorization、scope safety、supersession consistency、provenance closure 和 recovery consistency 六类可检查不变量。

- 提出由 Agent Memory Transaction、Policy-Consistent Commit 和 Provenance-Driven Repair 组成的策略感知事务型共享记忆运行时。

- 构建 TxnMemBench：使用独立 serial reference semantics、因果 failure schedule、differential oracle、mutation testing、coverage 和前缀最小 witness 验证系统正确性。

- 在受控 workload、真实 Qwen tool loop、三个公开 Agent runtime、真实 Qdrant/Neo4j/Toxiproxy 服务和跨主机模型负载上建立分层证据链。

本文不将上述接线或外部 workflow 指标改写成 end-user 成功率、memory accuracy 或 production quality。后文首先定位问题与设计需求，继而给出形式模型、三个机制及其原型边界；评估、讨论和相关工作在本工作稿中保留与前文衔接的实质性叙述，以便后续将已审计证据扩展为完整结果章节。

# 2 背景与动机

## 2.1 从“召回内容”到“维护共享状态”

单 Agent 长期记忆通常把会话事实、反思或任务经验压缩为可再利用的上下文 [R01][R02][R03]。RAG 与外部检索系统将语料或索引作为模型的非参数知识源，重点是相关性、覆盖面和生成辅助 [R04][R05][R06][R07]。在这些场景中，一条记录即使过期，影响也往往体现为下一次回答质量下降。

协作式 Agent 则使 memory 参与行动链：一个 Agent 的写入成为另一个 Agent 的计划输入，派生结论可以被传播到新的 scope，旧对象可被更新对象取代。对象由此带有状态转换和可见性后果。仅靠 metadata filter 可以拒绝某次检索，却无法说明缓冲的写集是否原子公开，也无法清理已经从失效源导出的结果。访问控制的保护矩阵、RBAC 与 ABAC 为主体、对象和动作之间的授权关系提供了成熟抽象 [R22][R23][R24][R25]；TxnMem 的不同之处在于，策略约束不仅在 API 入口发生，而且在事务提交和派生修复时参与状态转换。

`[[FIG:motivation_timeline]]`

图：地址—订单协作中的三类风险。时间线依次标出写入中崩溃导致的半更新、提交前撤权导致的旧授权提交，以及地址源记录更新后仍被派生建议引用的来源污染。

## 2.2 设计需求与现有能力的缺口

表中的“覆盖”表示一类工作是否把相应要求作为端到端语义的一部分，而非是否可以被工程上拼接为近似实现。Agent memory 与 RAG 支撑语义检索，却不定义共享写集和提交时授权 [R01][R02][R04][R07]；访问控制可限制对象访问，却不要求来源闭包修复 [R22][R23][R25]；事务和 provenance 分别提供原子历史与来源描述，但没有将策略变化、Agent 派生和修复动作联成同一 memory runtime [R11][R12][R17][R21]。故障感知验证工作说明，真实系统错误需要在可控扰动下被系统暴露 [R26][R27][R28][R29][R31][R32]，这也是 TxnMemBench 将 fault-aware validation 纳入需求而非事后压力测试的原因。

`[[TABLE:requirements_gap]]`

| 系统类别 | 语义检索 | 共享写入 | 提交时策略 | 派生状态修复 | 故障感知验证 | 本文的缺口判断 |
| --- | --- | --- | --- | --- | --- | --- |
| Agent memory | 强 | 未定义 | 通常为入口过滤 | 通常为被动记录 | 非核心 | 长期记忆不等于共享状态正确性 [R01][R02][R03] |
| RAG 与向量检索 | 强 | 非核心 | metadata 过滤 | 无来源闭包语义 | 非核心 | 检索相关性不能给出原子提交 [R04][R05][R06][R07] |
| 访问控制 memory 服务 | 可组合 | 未定义 | 可授权单个动作 | 无派生修复义务 | 非核心 | 授权本身不足以处理执行期间策略变化 [R22][R23][R24][R25] |
| 事务与 provenance 系统 | 非语义目标 | 强 | 普通数据授权 | 可记录来源 | 可借鉴 | 缺少 Agent 派生、传播与 scope 语义 [R11][R12][R17][R18][R19][R21] |
| 故障测试方法 | 非目标 | 取决于对象 | 取决于对象 | 取决于模型 | 强 | 需要把故障调度绑定到 memory 不变量 [R26][R27][R28][R29][R30][R31][R32] |

由此导出五项需求：memory 操作必须有语义检索之上的共享写入边界；授权必须在最新策略下重验证；派生对象需要可执行的来源依赖；修复必须改变可检索状态，而不止追加审计日志；验证必须把 crash、撤权和失效插入可重放的操作历史。TxnMem 以这五项需求组织接口和 oracle，而不是以单一 embedding、模型或存储后端定义自身。

# 3 系统模型与正确性

## 3.1 状态、对象与操作

系统状态写为 \(F=(A,M,P,T,G)\)。\(A\) 是 Agent 集合；\(M\) 是 memory object 集合；\(P\) 是带版本的策略；\(T\) 是活动及已决事务；\(G\) 是对象间的有向 provenance 图。一个对象 \(m\in M\) 记为

\[
m=(id, tenant, owner, scope, key, value, status, version, policy, supersedes, provenance).
\]

其中 `scope` 描述可见主体与用途边界，`supersedes` 指向被取代对象，`provenance` 是直接来源集合。形式目标为 `status` 保留 `valid`、`invalid`、`stale` 三种状态；当前确定性 TxnMem core/reference simulator 实现并检查 `valid` 到 `invalid` 的闭包转换，`stale` 状态与重算不在当前实现中。内容字段可以是文本、结构化值或向量索引键；TxnMem 不以内容相等性判断事实真伪，而以对象、策略与依赖关系定义运行时可观察语义。

Agent 通过 `begin`、`read`、`write`、`derive`、`supersede`、`propagate`、`revoke`、`commit`、`abort` 与 `repair` 改变状态。`read` 产生 read set 记录；`write` 仅在事务缓冲区构造新对象；`derive` 同时创建候选对象及从其直接来源出发的边；`supersede` 记录新旧版本关系；`propagate` 记录目标 scope；`revoke` 产生新策略版本或对象失效事件。事务 \(t\in T\) 至少携带 begin 时观察到的策略版本、read set、buffered write set、候选 provenance 边和决议状态。

策略版本是全序递增的不可变快照。策略判定记为 \(allow(p,a,op,m,s)\)，含义为在策略 \(p\) 下 Agent \(a\) 是否可在 scope \(s\) 对对象 \(m\) 执行 \(op\)。开始时检查只能快速拒绝明显非法请求；决定对象是否公开的是提交点对当前策略版本的重验证。该区分使“开始时合法、提交时已撤权”成为可表达且可测试的状态，而非实现中的偶然竞态。

## 3.2 合法线性化与独立 reference semantics

一次执行产生带调用、响应、故障、策略变化和修复事件的历史 \(H\)。若存在全序 \(L\)，保持每个已完成操作的实时先后关系，并且从初始状态按 \(L\) 逐步执行操作后得到的可见对象、策略版本和 provenance 图等价于观察结果，则称 \(L\) 是 \(H\) 的合法线性化。事务在 \(L\) 中要么以一个原子 `commit` 步出现，要么不产生其缓冲写入的可见效果；`abort` 只留下审计事件，不留下用户可检索的半更新。

被测 TxnMem 的实现与判定器使用独立的 serial reference semantics。reference semantics 不调用 TxnMem 的事务管理、存储适配器或修复遍历代码，而以简化的纯状态转换重放操作、策略和依赖。这种独立性将“实现自洽”与“满足所声明语义”区分开来，并允许差分 oracle 对同一历史比较不同实现变体。

崩溃若落在提交边界，外部观察并不总能确定它发生在原子决议之前还是之后。因此 reference semantics 为该边界产生一组合法结果：未决事务可线性化为未提交，或可线性化为完整提交；任何包含其写集真子集的状态都不在该集合中。恢复检查比较的是被观测状态是否属于这组结果，而不是强迫故障调度选择某个不可观测的内部瞬间。该定义既避免把崩溃恢复误判为错误，也明确禁止 partial commit。

## 3.3 不变量

以下六类不变量共同定义 TxnMem 的正确性目标；每条均对任意可达状态和任意可观察历史成立。

- **I-atomicity。** 已决提交事务的全部写入、取代关系、传播记录与 provenance 边共同可见；未提交或 abort 事务的任何缓冲副作用均不可见。

- **I-commit authorization。** 对每个提交事务，其 read、write、derive、supersede 与 propagate 集合在提交时使用的当前策略版本下均获准；若任一必需授权被撤销，该事务必须 abort。

- **I-scope safety。** 对象只有在当前策略允许的 scope 内才能被读取、按标识获取、检索、传播或作为派生来源；不同 API 路径不得形成绕过。

- **I-supersession consistency。** 已取代对象与替代对象的双向关系一致；被取代对象不会以 `valid` 身份作为默认的当前事实返回。形式目标可将其后继转为 `stale` 并重算；当前确定性实现采用更保守的 invalidation-only 投影。

- **I-provenance closure。** 当源对象撤销、失效或被纠正时，沿 \(G\) 可达且语义依赖该源的对象不能继续以不受影响的 `valid` 身份参与默认检索。当前确定性实现以 descendants 的 invalidation-only 闭包满足这一安全性质；`stale`、受权重算和新来源 repair 是不削弱该性质的未来扩展。

- **I-recovery consistency。** crash 与恢复后的持久状态必须等价于某个合法线性化结果；特别地，提交边界只允许完整提交或完整未提交这两类结果，不允许其写集的真子集。

这些不变量不裁定一条语言模型生成的自然语言是否客观为真。它们裁定的是：在给定对象和显式策略的前提下，系统是否以一致、受权且可修复的方式处理共享状态。这一边界使由内容级事实判定引入的模型不确定性不被误计为事务正确性。

# 4 TxnMem 设计

本节先给出 TxnMem 的目标语义。确定性 TxnMem core/reference simulator 实现并验证事务缓冲、提交重验证、abort 和 descendants 的 invalidation-only 闭包；原生 Qwen、公开 runtime 与真实后端适配器则只记录并直接分派独立事件。TxnMem 将 Agent API、Transaction Manager、Policy Engine、Memory Store 和 Provenance Repair Engine 分开。API 把动作编译为事件 contract；Transaction Manager 保存缓冲状态并决定提交；Policy Engine 提供版本化授权；Memory Store 持久化对象和索引；Repair Engine 维护反向来源索引并执行闭包更新。各组件共享事件标识和决议记录，而不共享隐式的模型推理状态。

`[[FIG:architecture]]`

图：TxnMem 的目标组件边界。确定性 core 中，事务管理器在提交点调用策略引擎并原子写入 Memory Store；来源修复引擎从反向索引遍历受影响后继并产生 invalidation。原生适配器只将工具调用转换为独立事件；stale 与重算属于未来 repair 扩展。

## 4.1 Agent Memory Transaction

**威胁。** 单个 Agent task 往往同时更新基础事实、取代关系、派生结论和传播副本。若这些变化分别落盘，crash 或异常会把中间状态暴露给其他 Agent；若 provenance 边晚于对象写入，修复器也无法识别真实依赖。

**目标。** 将一个 task 的共享记忆副作用收束为单一可决边界：其他事务只能看见决议前的状态或全部决议后的状态；每个派生对象的直接来源与对象本体一并成为提交单元。

**算法与状态。** `begin` 创建带快照策略版本的事务。后续 `read` 记录对象版本与访问理由，`write`、`derive`、`supersede` 和 `propagate` 只写入事务缓冲区。提交器先冻结缓冲区，再检查读对象仍可作为来源、候选对象键没有冲突、取代关系无环且所有 provenance 边指向存在的对象或同批候选对象。通过后，它一次性持久化对象、状态索引、双向取代关系与来源边，并写入 commit record。

**失败处理。** 在决议前发生错误或注入 crash 时，事务被标记为 abort，缓冲区被丢弃；恢复过程依据 commit record 重放完整提交，或清理未决缓冲，而不尝试把其中一部分对象解释成成功结果。该规则将存储适配器的异常转换为明确的事务状态，而非由调用者猜测哪些写入已经成功。

**原型边界。** abort 与原子提交路径在确定性 TxnMem core/reference simulator 中实现并验证。原生后端和 Qwen 适配器不暴露这一事务协议，而是直接分派单个事件；它们不宣称跨独立服务的分布式两阶段提交，也不将网络层的任意不确定响应自动解释为内容正确性。

`[[FIG:commit_protocol]]`

图：提交协议。事务从 begin 进入 buffer；commit 请求冻结读写集，使用当前策略版本重验证，随后在原子持久化与 abort 之间决议。崩溃恢复只接受完整提交或完整未提交的合法观察结果。

```text
procedure COMMIT(tx):
    freeze(tx)
    p <- latest_policy()
    ops <- {tx.read_set, tx.write_set, tx.derived_writes,
            tx.supersession_writes, tx.propagations}
    if not validate_versions(tx.read_set):
        return ABORT(tx, "stale read")
    if not authorize(p, tx.actor, ops):
        return ABORT(tx, "policy denied")
    if not validate_graph(tx.provenance_edges, tx.supersession_writes):
        return ABORT(tx, "invalid dependency")
    atomically persist(tx.write_set, tx.derived_writes, tx.provenance_edges,
                       tx.supersession_writes, tx.propagations, commit_record(tx, p))
    return COMMITTED
```

## 4.2 Policy-Consistent Commit

**威胁。** 授权在长推理或多工具调用期间可能改变。仅在 begin 检查可使 Agent 带着过期权限把新的对象、派生结论或传播副本提交给当前已无权访问的 scope。

**目标。** 以提交时的当前策略版本决定事务的可见性，并把 read、write、derive、supersede 和 propagation 的授权视为同一决定的组成部分。这样，策略变化不是只影响下一次请求的配置更新，而成为阻断旧事务公开效果的线性化事件。

**算法与状态。** 事务保留 begin 版本以便审计与快速路径判断，但 `commit` 总是取得 latest policy。对 read set 的重验证检查既有来源是否仍可被该 Agent 使用；对 write、derived write 和 supersession write 检查目标对象与 scope；对 propagation 检查源、目的与传播动作。若版本未变，检查仍以当前快照执行；若版本改变，输出记录区分“策略漂移后仍合法”和“策略撤回导致拒绝”，以便 failure controller 生成可解释事件。

**失败处理。** 任一集合不能通过当前策略即整体 abort。已提交对象的后续撤权并不回写为伪造的成功提交，而是产生撤权事件并交给 repair 路径处理；这把 commit-time authorization 与 post-commit invalidation 分离，避免在一个决议内混淆授权判断和依赖清理。

**原型边界。** 确定性 TxnMem core/reference simulator 的策略判定覆盖读取、写入、派生、取代、传播与提交重验证。原生逐事件适配器可记录动作级策略事件，但没有 `begin`、`commit` 或 `abort` 工具事件，因而不提供 transactional tool execution。redact 与 scope downgrade 是可能的治理扩展，但当前没有被实现或声称为自动恢复动作；内容层的事实裁决同样不属于 Policy Engine。

## 4.3 Provenance-Driven Repair

**威胁。** 撤权、纠错或 supersession 不会自然删除已派生的结论。若系统只修改源对象，默认检索仍可能返回下游对象；若修复只扫描正向对象集合，则分支传播和深链依赖容易遗漏。

**目标。** 将 provenance 从审计元数据转为执行索引：任一源事件均能沿反向边找到语义依赖者并将其退出默认可见集合。目标语义可进一步按失效类型把对象变为过期或由允许的输入重算，但这不是当前原型路径。

**算法与状态。** Memory Store 保存正向来源边和反向依赖索引。当前 repair 从种子对象出发执行有序遍历，并为每个后继记录处理原因；无论来源事件是撤权、失效、取代还是更正，当前实现均将受影响 descendants 置为 `invalid`，使默认读取与检索跳过该对象。传播副本被作为同一依赖图中的后继，而不是特殊的无来源缓存。形式目标中的 `stale` 与受权重算需要新增状态转换和替代来源判定，当前尚未实现。

**失败处理。** repair 本身的异常不会把已处理后继重新标为 `valid`；当前实现保留 `invalid` 状态和 repair event，因此保守不可用优于保守可见。环检测和已访问集合保证异常图输入不会导致无限遍历，并把图错误显式记录为失败事件。重试式受权重算尚不在当前路径中。

**原型边界。** 当前确定性原型实现 descendants 的 invalidation-only repair 与传播后继失效。`stale/recompute 是未来扩展`：它们需要受权替代来源、重算调度和新 provenance 边的实现及验证。原型不执行 redact，不主动缩小 scope，也不调用语言模型做内容级事实裁决；当没有受权且可验证的替代来源时，当前动作是保留不可用状态而不是编造修复结果。

`[[FIG:provenance_repair]]`

图：来源修复示例。一个地址源对象分叉到发货建议和跨 Agent 传播副本；源对象被撤销、取代或更正时，当前反向索引找到所有后继并将其置为 invalid。以新来源重算并恢复 valid 是形式目标，不是当前实现。

```text
procedure REPAIR(seed, reason):
    frontier <- reverse_dependents(seed)
    visited <- empty_set()
    while frontier is not empty:
        obj <- pop(frontier)
        if obj in visited:
            continue
        add(visited, obj)
        mark_invalid(obj, reason)
        append(frontier, reverse_dependents(obj))
    record_repair(seed, reason, visited)
```

# 5 实现

## 5.1 模块边界与事件 contract

确定性 TxnMem core/reference simulator 承担事务语义：它维护 buffered read/write state、依赖边和决议，并以独立 reference semantics 重放同一 history。Policy Engine 返回可审计的允许或拒绝；Invariant Checker 在执行后检查前述六类性质。failure controller 在指定操作边界注入 crash、撤权、延迟或来源失效，并把触发点写入该确定性 history；在这一核心中，错误路径以 abort 或 invalidation-only repair 结束，再由独立 reference semantics 判断可观察结果。

原生 event contract 是另一条适配路径。它只序列化逐个 memory event 与 policy event，不包含 `begin`、`commit` 或 `abort` 事件；Qwen gateway 和公开 runtime gateway 也逐个 memory event 直接分派。因而模型工具循环的证据说明结构化工具调用能进入受检验的事件接口，并不说明原生工具执行具有 Transaction Manager 的缓冲或提交语义。

## 5.2 存储与后端适配

SQLite 后端为确定性 core 提供本地对象和 provenance 索引，适合差分执行与最小见证重放。VectorGraphMemoryBackend 将原生对象检索和依赖图分别映射到向量与图服务接口；该适配路径按单个事件立即写入，并不复用 deterministic core 的事务缓冲或 commit record。这样，语义检索可以服务于候选对象发现，但原生适配本身不构成 commit-time revalidation 的实现。

真实服务接线用于验证模型、服务和 evaluator 能沿事件接口闭环，并在故障场景记录预期结果；它不把后端调用时间包装为生产性能，也不证明原生后端提供事务协调。跨独立服务的原子提交、恢复决议和两阶段提交均不在当前后端适配范围内。

## 5.3 Qwen 工具循环与原型边界

Qwen 工具循环通过结构化工具调用产生逐个 memory event；模型输出经 contract 校验后由 gateway 直接分派。它可以记录读取、写入、派生、取代、传播与策略事件，但当前没有 `begin`、`commit` 或 `abort` 工具事件，也不把多个工具调用缓冲为一笔事务。原生 Qwen/public-runtime 接线因此是 integration evidence，不是 transactional tool execution。模型仍只提出操作及其内容，不会因 gateway 接纳事件而使内容被判定为事实。

当前可验证的实现边界是：确定性 TxnMem core/reference simulator 覆盖事务 abort、策略提交检查和 descendants 的 invalidation-only repair；原生适配器覆盖逐个 event 的记录、校验与直接分派。`stale/recompute 是未来扩展`，并需新增状态转换和受权替代来源验证。redact、scope downgrade、内容级事实裁决、分布式两阶段提交和生产延迟均在本实现边界之外。这个边界避免把治理设想、未来部署能力或公开 benchmark workflow 指标混入已实现的正确性主张。

# 6 评估

评估将按机制问题组织：受控 history 检查六类不变量和独立 oracle 一致性；因果调度与最小见证检查测试方法是否能稳定暴露缺陷；模型工具循环和公开 runtime 只验证 event contract 与 workflow 接线；真实服务故障注入检查原子边界是否在后端异常时仍成立。每层使用各自的统计单位，任何 workflow reward、问答分数或时延均不被解释为 memory accuracy。

本文还将把 synthetic workload 与 trace-grounded adaptation 的联合分布诊断作为校准信息，而非真实性等价证明。结果章节将在不改变上述边界的前提下呈现完整分母、负结果和每项证据的可审计出处。

# 7 讨论与局限性

TxnMemBench 的说服力来自机制可控、oracle 独立和不变量可检查，而不来自把合成 history 宣称为真实用户行为的统计替身。真实 runtime 接线补充了工具、事件和服务边界的可执行性，却不能消除 workload 建模选择。尤其是，policy 的确定性判定假设策略已经被外部系统明确给出；自然语言政策解释、主体身份归因和内容真实性仍需要各自的治理与验证机制。

来源闭包也不是事实纠错的充分条件：它能确保已声明依赖不会在源失效后静默保留为默认可用，但不能发现未记录的隐式依赖。未来系统可增加更丰富的 provenance 捕获、人工复核和跨服务协调；这些方向不改变本文对当前原型能力的保守描述。

# 8 相关工作

Agent memory 工作通过层级记忆、反思和长期状态改善上下文使用 [R01][R02][R03]，RAG 工作通过检索扩展模型知识 [R04][R05][R06][R07]；TxnMem 在其上处理共享写集与可检查的状态转换。事务研究为合法历史和隔离提供基础语言 [R11][R12][R13][R14][R15][R16]，provenance 研究描述数据由何而来 [R17][R18][R19][R20][R21]；本文将二者连接到策略变化后的 Agent 派生修复。

访问控制模型关注主体对对象和操作的授权 [R22][R23][R24][R25]，故障注入与系统测试工作强调以受控执行暴露恢复和部分失败错误 [R26][R27][R28][R29][R30][R31][R32]。TxnMem 的定位不是替代这些工作，而是在多 Agent shared memory 的单一运行时中同时落实提交授权、来源闭包和故障可检验性。

# 9 结论

当 memory 成为多 Agent 的共享状态时，正确性问题从“是否召回相关内容”扩展为“哪些状态能够合法公开、何时仍被授权、以及失效如何传播”。TxnMem 以事务边界、提交时策略重验证和来源驱动修复回答这三个问题，并将其落实为独立 oracle 可检查的不变量。本文坚持将该系统正确性主张与生成质量、公开 workflow 成功率和生产部署能力分离；后续评估将以同一边界补全证据链。

# 参考文献

本文仅使用已核验目录中的条目。Agent memory 与检索方向包括 [R01] 至 [R07]；公开 Agent 任务背景包括 [R08] 至 [R10]；事务与隔离语义包括 [R11] 至 [R16]；provenance 与 lineage 包括 [R17] 至 [R21]；访问控制包括 [R22] 至 [R25]；故障注入和可靠性测试包括 [R26] 至 [R32]。完整作者、题名、年份、venue 与原始链接以 `configs/txnmem_paper_references.json` 的已核验 catalog 为准，渲染阶段从该 catalog 生成书目，避免人工转写引入未核验元数据。

# 附录

<!-- TXNMEM-AUTHOR-ANNOTATIONS:BEGIN -->
[[CLAIM:causal_schedule_vs_random]] schedule detection in the controlled simulator; random baseline consists of ten seeded schedules per instance

[[CLAIM:tau_bench_native_50]] official τ-bench workflow reward with native memory events; reward is not memory accuracy

[[CLAIM:appworld_prompt_profile_pair]] descriptive 20-task paired result; tuned token total is an observed lower bound and no population-significance claim is made

[[CLAIM:locomo_prompt_profile_repetitions]] three fixed paired repetitions; descriptive effect only, with no population-significance or universal-improvement claim

[[CLAIM:qwen_vector_graph_e2e_5]] single-host five-task end-to-end smoke including model, services, and evaluator; not production latency

[[CLAIM:cross_host_model_load_v8]] three independently attested Agent-client-to-model-server repetitions; not multi-host Agent workers, one continuous 30-minute tunnel, or production latency

[[CLAIM:joint_realism_tau]] joint-distribution diagnostic on a held-out trace split; rejection indicates mismatch and does not establish equivalence

[[CLAIM:joint_realism_locomo]] joint-distribution diagnostic with only two held-out conversations; low-power and not evidence of equivalence

[[CLAIM:appworld_projection_regeneration]] method/URL-only trace-grounded projection from official API calls; not native Agent memory ground truth

[[CLAIM:joint_realism_appworld]] joint-distribution diagnostic over a redacted projection with two held-out tasks; not native memory ground truth or distributional equivalence
<!-- TXNMEM-AUTHOR-ANNOTATIONS:END -->

## B. 评估记录的解释规则

每条 evaluation record 应保留 workload、操作 history、策略变化、故障触发、对象决议和 oracle 判定。受控实例、公开 task、会话、native event、服务重复与跨主机 repetition 是不同统计单位，不能在同一比率或平均值中混用。任何将来加入的正式数值均须具有 active claim、artifact、命令、输入清单与对应 boundary；未覆盖的数值宁可不进入正文，也不以近似描述替代审计。
