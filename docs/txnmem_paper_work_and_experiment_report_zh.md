# TxnMem 论文工作与实验总报告

副标题：面向多 Agent 共享记忆的策略感知事务运行时

报告日期：2026-08-17

报告范围：论文问题、系统创新、数据构建、实验设计、主要结果、证据边界与投稿状态

# 执行摘要

TxnMem 研究的不是“如何让语言模型记得更多”，而是当 memory 被多个 Agent 共同读取、写入、派生和传播后，系统如何保证只有合法且一致的状态进入默认可见集合。论文聚焦三个容易被传统检索式 memory 忽略的系统问题：多项写入中途崩溃会不会留下半提交状态；事务执行期间策略发生变化时，旧授权能不能继续提交；来源被撤销、取代或更正后，依赖它的派生记忆是否仍会被检索和传播。

论文提出三项相互配合的运行时机制：Agent Memory Transaction 把 read、write、derive、supersede 和 propagate 放入事务边界；Policy-Consistent Commit 在提交点按最新策略版本重验证；Provenance-Driven Repair 沿真实操作产生的来源依赖闭包执行 invalidation-only repair。与这三项系统创新配套，论文还建立了独立 serial reference semantics、因果 failure schedule、differential oracle、mutation testing、前缀最小反例和分层证据链，避免由 TxnMem 自己给自己生成 ground truth。

最强正确性证据来自受控 simulator：8 个 workload family、50 个 seed 生成 400 个 instance，每个 instance 在 5 个系统变体上运行，共形成 2,000 条 variant row。完整 TxnMem 在 400 个 instance 上目标违规为 0，且 400/400 与独立 oracle 一致；四个删减或朴素对照分别出现 350、200、50 和 100 个违规 instance。因果调度在 400 个 case 上检测率为 0.875，随机基线在 4,000 个 case 上为 0.750；mutation matrix 覆盖 350 个 case，杀死 300 个，并为四类主要缺陷生成了可重放的前缀最小 witness。

外部和工程证据按层解释，而不替代受控 oracle。Qwen2.5-7B 原生工具循环完成 50 个 task episode 和 110 个 native event；τ-bench 官方 runtime 汇总 50 个唯一 task 和 497 个 native event；AppWorld 使用 20 个配对 task；LoCoMo 完成 3 次固定配对重复、每次 1,986 个问题。真实 Qdrant/Neo4j/Toxiproxy 实验执行 5 个场景 × 30 次重复 = 150 次观测，得到 90 次 complete、60 次 absent、0 次 partial 和 0 次 unknown。跨主机模型负载完成 3 次独立 attested repetition、1,632/1,632 contract success，并由 endpoint 报告 3,251,506 tokens。

报告的结论边界与结果同等重要：τ-bench reward 不是 memory accuracy；AppWorld 和 LoCoMo 只是固定条件下的描述性结果；AppWorld projection 不是原生 Agent memory ground truth；真实服务五场景不支持一般分布式事务、跨主机容错、可用性、线性一致性或生产延迟；跨主机模型负载不是多主机 Agent workers，且因为没有明确费率，货币成本未计算。

[[FIG:evidence_layers]]

图 1. TxnMem 的分层证据结构：受控语义证据、真实模型接线、公开 runtime、真实服务和跨主机模型服务分别回答不同问题。

# 1. 论文研究问题与定位

## 1.1 为什么共享 memory 是系统状态问题

单 Agent memory 或 RAG 通常以召回相关内容为中心；记录过期时，主要后果是回答质量下降。多 Agent 协作改变了这一性质：一个 Agent 写入的地址、订单、计划或权限判断会成为另一个 Agent 的行动输入，派生结果还可能传播到新的 scope。此时 memory 不再只是模型上下文，而是影响工具调用、协作分工和外部世界操作的共享状态。

论文用三个风险场景说明这一转变。第一，多项写入中间发生 crash，后继 Agent 可能看见新地址和旧订单的混合状态。第二，事务开始后用户撤回授权，如果系统只在入口检查策略，旧授权仍可能在提交时留下新记忆。第三，源记录被更正或取代后，已经由它派生的建议和传播副本仍然可见，错误会沿协作链继续发挥作用。

## 1.2 论文回答的核心问题

TxnMem 将问题压缩为三条可验证主张：

1. 一次逻辑 memory 更新的多个结果应共同公开或共同不公开。
2. 提交是否合法应由提交时的最新策略决定，而不是只由事务开始时的授权决定。
3. 来源失效后，所有已记录的下游依赖对象都应退出默认可见集合。

论文没有把模型生成内容的真实性、自然语言政策解释或检索相关性混入这三条系统保证。模型负责提出操作和内容，TxnMem 负责判断这些操作构成的状态转换是否合法、一致和可追踪。

## 1.3 与传统方向的差异

RAG 和向量数据库解决“找什么”；访问控制解决“谁在当前请求中可以访问什么”；事务系统解决“哪些写入共同提交”；provenance 解决“对象从哪里来”。TxnMem 的组合点是把 Agent memory 的读写、策略版本、scope、supersession 和 provenance 依赖放入同一可检查 history，并在提交与修复两个关键时刻改变可见状态。

[[FIG:architecture]]

图 2. TxnMem 架构：Transaction Manager、Policy Engine、Provenance Manager、Invariant Checker 与独立 reference semantics 共同构成受控语义核心；模型和公开 runtime 通过逐事件 contract 接入。

# 2. 主要创新点

## 2.1 创新一：Agent Memory Transaction

Agent Memory Transaction 为共享 memory 定义显式的 begin、read、write、derive、supersede、propagate、commit 和 abort 语义。未决写集在提交前不进入默认可见集合；crash 或策略拒绝会把事务收束为 abort，而不是让一部分对象提前公开。该机制把“多次工具调用”提升为“一个逻辑状态更新”，从而可以检查 atomicity 和 recovery consistency。

该机制的创新不在于重新发明一般数据库事务，而在于把 Agent 特有的 derive、propagate、scope 和 supersession 纳入同一 memory transaction 模型，并明确区分确定性 core 中的事务语义与原生 gateway 的逐事件 direct dispatch。

## 2.2 创新二：Policy-Consistent Commit

TxnMem 不把入口处的授权结果永久缓存为提交许可。事务记录其观察到的 policy version；提交时，Policy Engine 按最新版本重新检查主体、动作、对象和 scope。如果在事务执行期间发生撤权或策略变更，旧授权不能产生新的已提交记忆。

这项机制把 policy change 变成 transaction history 中的状态转换，而不是外围配置事件。它直接支持 commit authorization 和 scope safety 两类不变量，并使“事务开始时允许、提交时已不允许”的时间窗口成为可重放的测试对象。

[[FIG:commit_protocol]]

图 3. 提交流程：验证写集与依赖、读取最新策略版本、重验证授权、原子公开或 abort，并记录可审计决议。

## 2.3 创新三：Provenance-Driven Repair

Provenance 图不是任意生成的 DAG，而由实际语义事件产生：read 记录依赖输入，derive 产生新对象与来源边，write 或 propagate 公开结果，supersede 记录替代关系。当源对象被撤销、取代或更正时，Provenance Manager 通过反向索引遍历所有 descendants，并把它们标记为 invalid，使其退出默认检索结果。

当前实现采用 invalidation-only repair；它证明来源闭包可以被执行，而不仅是被记录。受权重算、`stale` 状态、redact、scope downgrade 和人工复核需要新的状态转换，因此被明确列为未来扩展。

[[FIG:provenance_repair]]

图 4. 来源闭包修复：源对象失效后，沿分支和传播边找到全部后继并使其退出默认可见集合。

## 2.4 创新四：独立 reference semantics 与 differential oracle

论文将被测 TxnMem simulator 与 reference executor 分开实现。reference semantics 接收相同的初始对象、策略、操作 history 和 failure schedule，产生一个或多个允许的可观察结果；候选实现只要不属于该允许集合，就被 differential oracle 判定为不一致。对于提交边界附近的 crash，oracle 可以保留多个合法线性化，而不是强行规定唯一实现细节。

这一设计避免 TxnMem 自己生成 expected outcome。ground truth 来自独立 serial semantics 和显式不变量，包括 atomicity、commit authorization、scope safety、supersession consistency、provenance closure 和 recovery consistency。

## 2.5 创新五：因果调度、mutation 与最小反例

failure schedule 采用“触发条件 → 注入动作”，例如“第一条 write 后 crash”、“policy version 改变后 commit”和“来源失效后继续 search”，而不是只随机选一个时间点。benchmark 同时保留随机 schedule baseline，用于回答因果调度是否更有效。

Mutation testing 故意移除事务缓冲、提交重验证、provenance traversal 或 scope 检查。系统随后缩减触发目标违规的 operation prefix，并验证去掉最后一步后不再复现，从而得到可以人工理解和自动重放的 witness。这使 benchmark 不仅给出失败率，还能说明失败为什么发生。

# 3. 系统与工程实现

## 3.1 确定性语义核心

确定性 core 维护 memory objects、transaction state、policy versions、supersession 和 provenance edges。TxnMem simulator 执行候选实现；reference executor 串行重放同一 history；Invariant Checker 检查六类性质；differential evaluator 比较可观察结果。受控实验的事务、abort、commit-time revalidation 和 invalidation-only repair 都发生在这一层。

## 3.2 原生事件 contract 与模型 gateway

真实 Qwen 和公开 benchmark runtime 使用另一条接入路径。模型通过结构化工具调用产生 read、write、derive、supersede、propagate 和 policy event，gateway 校验格式后逐事件分派。这个 event contract 可以证明模型输出能进入受检验接口并被记录，但当前不包含 begin、commit 或 abort，也不会把多个工具调用缓冲为一笔事务。因此它是 integration evidence，不是 transactional tool execution。

## 3.3 存储后端

SQLite 用于确定性 replay 和 per-task native memory，便于隔离与复现。VectorGraphMemoryBackend 将对象检索映射到 Qdrant，将来源依赖映射到 Neo4j。真实服务实验使用 Qdrant 1.11.5、Neo4j 5.22.0 和 Toxiproxy 2.5.0；每次操作后都读取两个唯一 memory ID 在向量与图后端中的状态，以区分 complete、absent、partial 和 unknown。

## 3.4 模型与运行环境

真实模型使用 Qwen2.5-7B-Instruct，模型 revision 为 `7b44fc9c...26b4`，服务端为 vLLM 0.8.5.post1。公开 runtime 包括 τ-bench、AppWorld 和 LoCoMo。跨主机实验只把 Agent client 与 model server 分布到两个 host，不把 Qdrant/Neo4j 迁移到远端，也不声称多主机 Agent worker 集群。

# 4. 数据构建、来源与规模

## 4.1 数据分层原则

论文将数据分成四层，分别承担不同作用：受控 synthetic history 用于系统正确性；最小 witness 用于解释缺陷；公开 benchmark native/runtime 数据用于外部接线；trace-grounded adaptation 用于诊断 synthetic generator 与观测 trace 的分布差异。不同层的统计单位不能混合为一个成功率。

| 数据层 | 数据如何产生 | 统计单位 | 数据量 | 主要用途 |
| --- | --- | --- | --- | --- |
| TxnMemBench synthetic | 8 类语义生成规则 × 50 seeds | instance、variant row | 400 instance、2,000 row | 检查事务、策略和来源不变量 |
| Mutation/witness | 对四项核心逻辑注入实现缺陷并缩减 history | mutant、operation prefix | 350 mutation case、4 witness | 检查 benchmark 能否暴露并解释缺陷 |
| Native model/runtime | Qwen 工具循环与官方 evaluator 实际执行 | task episode、conversation、native event | Qwen 50 episode/110 event；τ 50 task/497 event；AppWorld 20 task；LoCoMo 3 repetition | 检查模型和公开 runtime 接线 |
| Trace-grounded realism | 从 τ/LoCoMo 公开 trace 与 AppWorld 官方 API calls 提取六维特征 | episode、projection event | τ 175 episode/920 event；LoCoMo 10 conversation/272 event；AppWorld 5 task/380 event | 检查 synthetic 与 held-out trace 的联合分布失配 |
| 真实服务 | Qdrant/Neo4j/Toxiproxy 的确定性故障矩阵 | scenario repetition | 5×30=150 observation | 检查 proxy、响应和双存储 readback |
| 跨主机模型服务 | 1 client host 到 1 model host 的独立 attested 运行 | repetition、attempt、request、token | 3 repetition、1,632 attempt、3,672 request | 检查跨主机模型服务拓扑和 token accounting |

## 4.2 TxnMemBench synthetic 数据如何生成

生成配置包含 8 个 workload family：`atomic_multi_write`、`crash_during_commit`、`revoke_before_commit`、`scope_bypass`、`supersession_consistency`、`provenance_chain_repair`、`provenance_branch_repair` 和 `mixed_stress`。每个 family 使用 50 个 seed，共得到 400 个 instance。参数范围为 transaction size 1--4、provenance depth 1--4、branch factor 1--3、policy churn 0--2、concurrency 1--3。

每条 instance 不是一段自由文本，而是一个可执行 JSON history，包含初始 memory objects、主体与 scope、带版本策略、操作序列、failure schedule 和 provenance edges。provenance 结构由 read→derive→write/propagate 语义生成，并通过 depth、branch factor、merge 和 supersession 参数控制；不是在运行结束后任意补边。

每个 instance 在 TxnMem、Naive、TxnMem-NoTxn、TxnMem-NoPolicyCommit 和 TxnMem-NoRepair 五个变体上执行，因此 400 个 instance 形成 2,000 条 variant row。正式来源为 `results/paper_evidence/controlled_suite.json`，它从 `generated_instances.jsonl` 与完整结果 CSV 重新计算规模和结果，而不是使用手填总数。

## 4.3 公开 benchmark 数据如何获得

τ-bench native 数据来自 airline test split 的官方 runtime 和 workflow evaluator。正式聚合保留 50 个唯一 task、50/50 evaluator available 和 497 个 native event；两个网络错误 task 使用固定 retry manifest 合并，不能把 retry 或失败 episode从分母中删除。正式 artifact 为 `results/submission_evidence/tau_bench_50/aggregate.json`。

AppWorld native comparison 使用同一 20-task manifest、相同模型和相同可见工具集合，分别运行 baseline 与 tuned prompting。官方判定来自 TestTracker success 和 task-completed protocol；tuned 条件的 6 个 execution failure 仍保留在 20-task 分母内。用于 realism 的另一份 AppWorld 数据来自官方 `ground_truth/api_calls.json`，只保留 method 和 URL，删除 request data 和原始值，5 个 task 共 380 个 projection event。它是 trace-grounded projection，不是原生 Agent memory ground truth。

LoCoMo native QA 使用 10 个 conversation 和官方 QA evaluator。baseline/tuned 各运行 3 次，seeds 为 17、1017 和 2017；每次 1,986 个问题，因此每个条件累计 5,958 个问题级评分。realism 数据另外从 10 个官方 conversation summary 提取 272 个事件。

## 4.4 真实服务与跨主机数据如何获得

真实后端矩阵使用 normal、delay、timeout、connection_drop 和 retry_success 五个场景，每个场景重复 30 次。每次 workload 含两个事件，并为两个唯一 memory ID 记录 Qdrant 与 Neo4j 的操作后 readback、Toxiproxy toxic 安装/清理、trigger ordinal、proxy route、客户端响应、retry 和 abort 决议。

正式 artifact 为 `results/submission_evidence/toxiproxy_state_verified_30/aggregate.json`。

跨主机 v8 使用三个彼此不重叠的 UTC interval 和三条独立 tunnel。每次运行 68 cycles、544 attempts、约 600 秒，configured concurrency=4，observed peak=4。聚合器校验模型调用前后的 listener ownership、ControlMaster 连续性、loopback forwarding、host identity 区分和 endpoint/transport failure analysis。

# 5. 实验设计与实验矩阵

## 5.1 五个研究问题

- RQ1：三个核心机制是否阻止目标违规？
- RQ2：因果 failure schedule、mutation testing 和最小 witness 是否能暴露目标缺陷？
- RQ3：真实模型和三个公开 runtime 能否接入逐事件 memory contract？
- RQ4：真实 Qdrant/Neo4j 在受控故障后的持久状态是什么？
- RQ5：synthetic workload 与 trace-grounded holdout 的六维联合分布是否匹配？

## 5.2 判定器分工

| 判定层 | 判定对象 | 能回答什么 | 不能替代什么 |
| --- | --- | --- | --- |
| Independent reference semantics | 受控可观察 history | 合法提交、可见性、abort、repair | 公开任务质量 |
| Invariant checker | 六类系统性质 | 违规类型与触发路径 | 内容事实真伪 |
| Event contract | 单个 native memory event | 格式、路由与模型接线 | 多事件事务语义 |
| Official evaluator | τ/AppWorld/LoCoMo workflow | 官方 reward、assertion 或 QA F1 | memory accuracy 和事务正确性 |
| Service readback validator | proxy、response、Qdrant/Neo4j state | 被测五场景的 complete/absent/partial/unknown | 一般分布式事务与生产可用性 |
| Cross-host attestation | tunnel、listener、request usage | client-to-model-server 拓扑和 token accounting | 多主机 Agent workers 与跨主机 memory backend |

## 5.3 变量与对照

受控实验的主要自变量是系统变体、workload family 和 schedule；因变量是目标违规数、oracle match、schedule detection 和 mutant kill。公开 runtime 的比较只在固定 manifest/condition 内进行。真实服务实验固定 workload 和 repetition，只改变故障场景；跨主机实验固定模型、并发、时长和拓扑，重复三次以获得独立 attestation。

# 6. 各项实验、目的与结果

## 6.1 实验 A：受控正确性与消融（RQ1）

目的：验证完整 TxnMem 是否满足独立 reference semantics，并确定三个机制分别阻止哪类违规。

数据：8 workload family × 50 seeds = 400 instance；5 variants = 2,000 variant row。

结果：

| 变体 | 目标违规 instance | Oracle 一致 instance | 解释 |
| --- | ---: | ---: | --- |
| TxnMem | 0/400 | 400/400 | 三项机制共同生效 |
| Naive | 350/400 | 50/400 | 直接公开事件，缺少事务、提交重验证和修复 |
| TxnMem-NoTxn | 200/400 | 200/400 | crash 和多写可泄露 partial update |
| TxnMem-NoPolicyCommit | 50/400 | 350/400 | 开始后撤权仍可能提交 |
| TxnMem-NoRepair | 100/400 | 300/400 | 依赖失效源的对象残留可见 |

[[FIG:controlled_results]]

图 5. 完整 TxnMem 与四个对照在受控 400-instance 套件上的目标违规数量。

结论：在确定性 core/reference simulator 边界内，三项机制分别对应原子性、提交授权和 provenance closure；该结果不是公开 benchmark accuracy。

## 6.2 实验 B：因果 schedule 对随机 schedule（RQ2）

目的：验证触发条件与注入动作对齐是否比随机时间点更容易命中会改变可观察 history 的边界。

数据：因果 schedule 400 case；随机基线为每个 instance 10 个带 seed 的 schedule，共 4,000 case。

结果：因果调度检测率为 0.875，随机基线为 0.750。差值表示受控 simulator 对预设目标违规的检测能力，不是生产故障发生概率。

## 6.3 实验 C：Mutation matrix 与前缀最小 witness（RQ2）

目的：检验 benchmark 是否能杀死已知关键缺陷，并把失败缩减成可理解反例。

数据：350 个 variant-instance mutation case；四个主要 mutant。

结果：300/350 case 被杀死，kill rate 为 0.8571428571。四个 prefix-minimal witness 为：partial commit 2 步、remove commit revalidation 1 步、disable provenance traversal 6 步、bypass scope check 1 步。每个 witness 都经过“完整前缀重放成功、去掉最后一步不再复现”的验证。

## 6.4 实验 D：Qwen2.5-7B 原生事件循环（RQ3）

目的：验证真实模型能否稳定生成结构化 memory event，并进入 event contract 和 differential oracle。

数据：5 次 repetition，每次 10 个 task，共 50 个 task episode、110 个 native event。

结果：50/50 contract success、50/50 oracle match、0 evaluation error；预期机制事件中包含 5 个 injected crash 和 5 个 policy denied。该实验只证明逐事件工具循环接线，不证明模型工具调用具有事务缓冲和 commit 语义，也不代表终端任务成功率。

正式 artifact 为 `results/remaining_tasks/native_repetitions5/repetition_report.json`。

## 6.5 实验 E：τ-bench native runtime（RQ3）

目的：验证 TxnMem event 接口能在官方 tool-agent workflow 中运行，并记录 native memory events。

数据：50 个唯一 task、50/50 evaluator available、497 个 native event。

结果：official reward sum=15，mean=0.3000；48 个 task 状态 completed、2 个 failed，另有 2 个 max-steps 和 2 个 no-events failure code 计数。reward 不是 memory accuracy，也不能替代独立 oracle。

正式 artifact 为 `results/submission_evidence/tau_bench_50/aggregate.json`。

## 6.6 实验 F：AppWorld baseline/tuned 配对（RQ3）

目的：在工具集合逐 task 一致的条件下，检查更强 prompting/tool strategy 是否改善官方 workflow 结果。

数据：20 个配对 task、112 个官方 assertions。baseline token usage 为 517,564；tuned 观测 token 为 2,171,632，但有两次响应缺少 usage，因此只是下界。

结果：baseline 0/20 official success、17/112 assertion；tuned 1/20 success、53/112 assertion。13 个 task 改善、7 个不变、0 个回退；tuned 有 4 个 unauthorized-tool 和 2 个 model-HTTP failure，共 6 个 execution failure。由于 n=20 且存在失败，结果只能作固定条件下的描述性比较，不支持总体显著性主张。

正式 artifact 为 `results/prompt_profile_formal_v4/appworld_prompt_comparison.json`。

## 6.7 实验 G：LoCoMo paired QA repetition（RQ3）

目的：检查更强 Agent prompting 在长期对话 QA 上是否产生稳定描述性变化。

数据：baseline/tuned 各 3 次配对 repetition；每次 10 conversation、每次 1,986 个问题，单个条件累计 5,958 个问题级评分。

结果：baseline mean F1 分别为 0.136459、0.141139、0.137486；tuned 为 0.135513、0.144819、0.139602。三次 paired delta 为 -0.000946、+0.003681、+0.002116，平均 +0.001617，标准差 0.001921；token 精确增加 40,539。由于只有 3 次 repetition、增益很小且一次回退，不能声称总体显著或普适提升。

正式 artifact 为 `results/prompt_profile_formal_v4/locomo_prompt_comparison.json`。

## 6.8 实验 H：真实 Qdrant/Neo4j/Toxiproxy 状态核验（RQ4）

目的：不只证明 toxic 被安装，而是验证客户端响应和两个真实后端的操作后持久状态是否共同自洽。

数据：normal、delay、timeout、connection_drop、retry_success 五个场景，每场景 30 次，共 150 次观测；每次读取两个唯一 memory ID 在 Qdrant 和 Neo4j 中的状态。

结果：normal、delay、retry_success 各 30 次 complete，共 90；timeout 和 connection_drop 各 30 次 absent，共 60；partial 0/150、unknown 0/150；retry_success 30/30。所有场景均通过 proxy path、trigger、toxic lifecycle、response 和 readback 的重新计算验证。

backend-only 两事件诊断为 p50 25.748 ms、p95 32.029 ms、p99 42.234 ms、吞吐 76.256 operations/s，但 `production_latency_claim=false`。该证据只支持被测 workload 与五个单机场景的 readback-confirmed complete-or-absent 结果，不支持一般分布式事务、跨主机容错、可用性、线性一致性或生产延迟。

正式 artifact 为 `results/submission_evidence/toxiproxy_state_verified_30/aggregate.json`。

## 6.9 实验 I：Qwen + Qdrant + Neo4j 端到端 smoke（RQ4）

目的：验证模型、真实向量/图后端和 evaluator 能形成单机端到端闭环。

数据：5 个 task、30 个 native event。

结果：5/5 completed、5/5 evaluator available；mean 18,851.6 ms、P50 15,497.0 ms。该实验是 single-host smoke，不是生产延迟或跨服务事务协调证据。

正式 artifact 为 `results/submission_evidence/qwen_vector_graph_e2e_5/aggregate.json`。

## 6.10 实验 J：跨主机模型负载与 token accounting

目的：验证 Agent client 到远端 model server 的独立网络拓扑、并发工具循环、预期故障机制和 endpoint usage 统计。

数据：3 次独立 attested repetition；合计 204 cycles、1,632 attempts、1,811.068 秒；每次 concurrency=4。endpoint 报告 3,672/3,672 request usage，其中 prompt 2,935,703、completion 315,803、total 3,251,506 tokens。

结果：1,632/1,632 contract success；1,224 completed attempts；408 runner-level failures 全部是 workload 预期机制，包括 204 injected crash 和 204 policy denied；endpoint/transport 相关失败为 0。拓扑为 1 个 Agent-worker host 和 1 个 model-server host，三条 tunnel 相互独立。

该结果不是多主机 Agent workers，不是单一连续 30 分钟 tunnel，不包含跨主机 Qdrant/Neo4j，也不构成生产延迟。没有显式 pricing rate，因此货币成本未计算。

正式 artifact 为 `results/cross_host_model_load_formal_v8_aggregate/results/model_load_repetition_summary.json`。

## 6.11 实验 K：Synthetic 与 trace-grounded joint realism（RQ5）

目的：检查 synthetic generator 在 operation count、transaction size、policy change rate、provenance depth、branch factor 和 agent count 六维联合空间中是否接近 held-out trace；使用 standardized RBF random-feature MMD permutation test。

数据与结果：

| 来源 | 原始 trace | Calibration/Holdout | MMD² | p 值 | 解释 |
| --- | --- | --- | ---: | ---: | --- |
| τ-bench | 175 episode、920 event | 141/34 episode；744/176 event | 0.181979 | 0.001 | 拒绝联合分布相同；holdout 为 34 |
| LoCoMo | 10 conversation、272 event | 8/2；213/59 event | 1.941886 | 0.0005 | holdout 仅 2，低功效且不稳定 |
| AppWorld projection | 5 task、380 event | 3/2；164/216 event | 1.220967 | 0.0005 | method/URL-only projection，不是 native memory GT |

三个结果都说明当前 synthetic generator 与观测 holdout 存在失配，必须继续校准；它们不支持分布等价。这里报告负结果本身是一项方法贡献：synthetic 数据的说服力来自独立语义和缺陷揭示能力，而不是假装它已经复刻真实行为分布。

# 7. 创新点如何由实验闭环验证

| 创新点 | 主要实验 | 成功判据 | 已获得证据 | 仍不能声称 |
| --- | --- | --- | --- | --- |
| Agent Memory Transaction | RQ1 消融、partial-commit witness、真实服务 abort/readback | 完整实现无 partial update；删去事务后出现目标违规 | TxnMem 0/400；NoTxn 200/400；partial witness 2 步；真实后端 partial 0/150 | 原生模型工具调用已具备跨事件事务 |
| Policy-Consistent Commit | NoPolicyCommit 消融、revalidation witness、policy-denied model load | 策略变化后的旧授权不能提交 | NoPolicyCommit 50/400；witness 1 步；跨主机 204 个预期 policy denied | 自然语言政策解释或身份归因正确 |
| Provenance-Driven Repair | NoRepair 消融、provenance witness、chain/branch workload | 失效源的 descendants 退出默认可见集合 | NoRepair 100/400；witness 6 步；provenance family 覆盖闭环 | 内容事实真伪、隐式依赖或受权重算 |
| 独立 ground truth | TxnMem/reference differential suite | 完整实现结果属于 reference 允许集合 | 400/400 oracle match；50/50 Qwen oracle match | public evaluator 可以替代系统 oracle |
| 因果 schedule 与 mutation | causal/random baseline、350-case matrix、4 witnesses | 比随机调度更敏感且能杀死核心 mutant | 0.875 vs 0.750；300/350；4/4 prefix-minimal | 这些比率等于生产缺陷率 |
| 分层外部证据 | τ/AppWorld/LoCoMo、E2E、Toxiproxy、cross-host、realism | 每层使用匹配的 evaluator 和分母 | 公开 runtime、真实模型/服务和负 realism 结果均有 active artifact | workflow reward、QA F1 或服务 readback 等于 memory accuracy |

这一映射说明论文不是先提出机制，再用一个总分笼统佐证。每项创新都至少由一组受控消融或 mutant 证明因果作用，再由模型、runtime 或真实服务实验验证其工程接线边界。外部实验增加生态可信度，但不会覆盖或替换独立语义证据。

# 8. 证据治理与可复现性

## 8.1 Claim ledger

论文维护 15 条 active claim，每条绑定当前 artifact、字段断言、运行命令、manifest、source commit 和 claim boundary。最终 claim audit 检查 163/163 个字段断言，finding 为 0；4 个旧 artifact 通过 supersession index 保留为历史审计对象，但不再承担正文结论。

active claim 覆盖 controlled correctness、causal schedule、mutation matrix、Qwen repetitions、τ-bench、AppWorld、LoCoMo、Toxiproxy、真实后端 E2E、cross-host、三个 realism/投影结果和 minimal witnesses。任何新数字进入论文前都必须先进入 ledger 并通过 fail-closed audit。

## 8.2 防止证据污染

原始 prompt、tool arguments、账号凭据、完整对话和 AppWorld request data 不进入公开证据。远端环境只提交脱敏 aggregate、模型/服务版本、source hash、host identity hash 和统计。DOCX 构建会移除作者元数据、Word 修订标识、批注、修订和绝对工作站路径。

## 8.3 可复现性与文档状态

状态核验分支在合并前和合并后均通过 362 项单元测试；最终论文的 claim、artifact、manuscript 和路径审计均为 0 findings。中文论文初稿由确定性构建器生成，最终版本为 26 页、6 图、8 表、32 条参考文献，可访问性 high/medium/low 均为 0，并经过整分支与范围复审。

最终论文证据的核心哈希包括：controlled claim ledger `c45def3...b9b5`、state-verified Toxiproxy aggregate `04de2a3...bcad`、原始 backend result `2ec4db6...0220`、最终中文论文 DOCX `d5fa35b...a5152f`。这些哈希用于识别精确证据字节，不用于扩大 claim boundary。

# 9. 结论边界、局限与投稿状态

## 9.1 当前可以正式声称的内容

1. 在独立 reference semantics 和 400-instance 受控套件中，完整 TxnMem 未出现目标违规并与 oracle 一致；三项机制的删减都会产生对应违规。
2. 因果 schedule、mutation matrix 和四个最小 witness 能检测并解释预设的关键实现缺陷。
3. Qwen2.5-7B、τ-bench、AppWorld 和 LoCoMo 已接入逐事件 memory/runtime 路径，并保留各自官方 evaluator 与失败分母。
4. Qdrant/Neo4j/Toxiproxy 五场景状态核验得到 90 complete、60 absent、0 partial、0 unknown，且每次都进行了双存储 readback。
5. 跨主机 v8 证明了 1 个 Agent client host 到 1 个 model server host 的三次独立 attested 运行和完整 endpoint token accounting。
6. 三组 joint realism 诊断显示 synthetic 与 held-out trace 存在分布失配，论文据此把 generator 校准列为后续工作，而不是声称分布等价。

## 9.2 当前不能声称的内容

- 原生 Qwen 或公开 runtime 已实现跨多工具调用的 Transaction Manager。
- τ-bench reward、AppWorld success/assertion 或 LoCoMo F1 等同于 memory accuracy。
- AppWorld projection 是公开 benchmark 提供的原生 memory transaction/provenance ground truth。
- 五个单机 Toxiproxy 场景证明一般跨服务事务、线性一致性、availability 或跨主机 fault tolerance。
- backend-only 或 5-task E2E 时延代表 production latency。
- 三次 cross-host client-to-server 运行等同于多主机 Agent worker 集群或连续 30 分钟 tunnel。
- TxnMem 能判断生成内容真假、自动理解自然语言政策或修复未记录的隐式依赖。

## 9.3 投稿状态

论文的系统实现、受控 benchmark、真实模型与公开 runtime 接线、真实服务状态核验、跨主机模型负载、claim audit、正文同步和 DOCX 视觉 QA 已完成。代码和论文证据已合并到本地 `main`。

剩余工作属于投稿工程而不是实验 blocker：选择具体 CCF-A venue；转换到官方 LaTeX/Word 模板；完成英文稿、作者与基金信息、伦理/数据开放声明、参考文献格式和正常作者修订；配置 Git remote 后再决定是否公开代码与数据。若未来扩大主张，则需要另外完成多主机 Agent workers、跨主机 Qdrant/Neo4j、连续 tunnel、更多 AppWorld/LoCoMo repetitions 和有明确费率的成本核算。

# 附录 A：正式实验一览

| 实验 | 正式数据规模 | 主要结果 | Active artifact |
| --- | --- | --- | --- |
| Controlled correctness | 400 instance、2,000 row | TxnMem 0 violation、400 oracle match | `results/paper_evidence/controlled_suite.json` |
| Causal vs random | 400 vs 4,000 case | 0.875 vs 0.750 | `results/final_controlled/results/schedule_baseline.json` |
| Mutation matrix | 350 case | 300 killed、rate 0.8571 | `results/final_controlled/reproducibility_report.json` |
| Minimal witnesses | 4 mutant | 4/4 prefix-minimal | `results/final_controlled/results/minimal_mutant_witnesses.json` |
| Qwen native repetitions | 50 episode、110 event | 50/50 contract、50/50 oracle | `results/remaining_tasks/native_repetitions5/repetition_report.json` |
| τ-bench native | 50 task、497 event | reward 15、mean 0.3 | `results/submission_evidence/tau_bench_50/aggregate.json` |
| AppWorld paired | 20 task、112 assertion | 0→1 success；17→53 assertions | `results/prompt_profile_formal_v4/appworld_prompt_comparison.json` |
| LoCoMo paired | 3×1,986 question | mean F1 delta +0.001617 | `results/prompt_profile_formal_v4/locomo_prompt_comparison.json` |
| Toxiproxy state verified | 5×30=150 observation | 90 complete、60 absent、0 partial/unknown | `results/submission_evidence/toxiproxy_state_verified_30/aggregate.json` |
| Qwen vector/graph E2E | 5 task、30 event | 5/5 completed | `results/submission_evidence/qwen_vector_graph_e2e_5/aggregate.json` |
| Cross-host v8 | 3 run、1,632 attempt | 1,632/1,632 contract；3,251,506 tokens | `results/cross_host_model_load_formal_v8_aggregate/results/model_load_repetition_summary.json` |
| τ realism | 400 synthetic vs 34 holdout | MMD² 0.181979、p=0.001 | `results/joint_realism/tau_bench/results/trace_realism.json` |
| LoCoMo realism | 400 vs 2 holdout | MMD² 1.941886、p=0.0005 | `results/joint_realism/locomo/results/trace_realism.json` |
| AppWorld projection | 5 task、380 event | method/URL-only regenerated | `results/appworld_projection_regenerated/projection_inventory.json` |
| AppWorld realism | 400 vs 2 holdout | MMD² 1.220967、p=0.0005 | `results/appworld_projection_regenerated/results/trace_realism.json` |

# 附录 B：如何阅读这些结果

`workload family` 是一类生成规则，`seed` 是该规则下的随机化输入，`instance` 是一条受控 history，`variant row` 是一个 instance 与一个系统变体的组合。`task episode`、`conversation`、`native event`、服务 `repetition`、跨主机 `attempt` 和 token 是不同统计单位，不能直接相加或写成同一成功率。

受控 simulator 的 oracle 一致性回答系统语义；官方 evaluator 回答 benchmark workflow；event contract 回答接线；service readback 回答被测持久状态；realism test 回答观测分布失配。只有在判定器、分母和 claim boundary 同时匹配时，一个实验结果才可以进入论文主张。
