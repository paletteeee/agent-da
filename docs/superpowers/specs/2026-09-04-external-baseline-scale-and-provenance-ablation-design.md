# TxnMem 外部基线扩规模与 Provenance 性能对照设计

## 1. 目标

本设计补齐两项论文证据：

1. 将五个外部基线从 80 个验证实例扩展到 TxnMemBench 正式 400 实例，并以互斥运行状态、统一 oracle 和 Wilson 95% 置信区间报告结果。
2. 在三个代表性规模/并发条件下比较完整 TxnMem 与 memory-only/no-provenance-maintenance 对照，估计 provenance 维护路径的增量延迟与吞吐开销。

两项实验必须使用版本化输入、隔离命名空间、固定依赖、逐次原始记录、严格聚合和哈希绑定。诊断、smoke 或不合格运行不得进入正式论文结果。

## 2. 范围与非目标

### 2.1 范围

- 外部基线：AppendOnly、LastWriteWins、MetadataFiltered、Mem0、LangGraph Store。
- 正式输入：8 个 workload family × 50 seeds = 400 个实例。
- 性能对照：完整 TxnMem 与 `MemoryOnly-NoProvenance`。
- 性能条件：`(100, 1)`、`(1000, 4)`、`(10000, 16)`，分别表示 provenance 图节点数与并发度。
- 每个 variant/cell 运行 30 个 repetition；2 × 3 × 30 = 180 个 repetition。
- 统计：Wilson 95% CI、whole-repetition bootstrap 95% CI、p50/p95/p99、吞吐和配对增量。

### 2.2 非目标

- 不重新运行或覆盖 v10 完整 15-cell 正式矩阵。
- 不把 `TxnMem-NoRepair` 当作 provenance 总开销对照；该变体仍可能维护来源边。
- 不声称生产延迟、线性扩展、跨主机存储容错或一般分布式事务保证。
- 不把 unsupported mapping 或 runtime error 计为正确性违规。
- 不把 capability absence 从正确性分母中移除；只要运行成功并产生规范化可观察状态，就由同一 oracle 判定。

## 3. 方案选择

### 3.1 外部基线

复用现有统一适配器、规范化历史和独立 oracle，扩大 manifest 而不改变 workload 内容或适配语义。每个实例使用由正式 run identity、variant、workload、seed 和 repetition 派生的唯一命名空间。

选择 Wilson 区间而不是逐行 bootstrap 作为二项正确性指标的主区间，因为违规与 oracle match 均为二项结果，且不同方案可能因 unsupported mapping 获得不同有效分母。逐 workload 与总体区间均使用各自 correctness-included 分母。

### 3.2 Provenance 性能对照

采用 `MemoryOnly-NoProvenance` 作为单一轻量对照。它保留与完整 TxnMem 相同的 memory CRUD、真实服务客户端、事务外围、输入对象、并发调度和 repetition 生命周期，但关闭：

- provenance edge 持久化；
- provenance adjacency/index 维护；
- 主动 provenance traversal/repair。

为了保持可解释性，配对增量只对两种 variant 都执行的共同操作集合计算：read、write、derive 和 propagate 的 memory-side 部分。完整 TxnMem 的 traversal 延迟另行绝对报告，不把对照中的 no-op 当作零延迟样本。总体 wall-clock 与吞吐仍按各 variant 的实际完整 repetition 报告，并明确两种 workload 语义不同，因此总体差异是机制包成本，而非单操作严格等价微基准。

三个 cell 覆盖小规模串行、中规模适度并发和大规模高并发。每个 cell 内使用相同 repetition seeds 和相同操作输入形成配对；执行顺序按固定种子交错，避免始终先运行某个 variant。

## 4. 外部基线运行协议

### 4.1 输入资格

正式 manifest 必须满足：

- 400 个唯一 `instance_id`；
- 8 个 workload family；
- 每类恰好 50 个唯一 seed；
- manifest SHA-256 与受控正式套件绑定；
- 无重复、缺失或额外实例；
- oracle 文件与实例 manifest 有独立哈希。

### 4.2 依赖与能力记录

每个外部方案记录：

- 方案名称和精确版本；
- Python 版本与锁文件哈希；
- 后端类型和配置；
- 原生接口能力矩阵；
- 适配器 source commit；
- 运行命令、环境身份和开始/结束时间。

适配器不得添加 TxnMem 的事务缓冲、提交时策略重验证或来源闭包遍历。

### 4.3 互斥状态分类

每次尝试必须且只能产生一个 `run_status`：

- `success`：接口执行完成并产生规范化可观察状态。
- `unsupported_mapping`：数据集操作不能可靠映射到原生接口，执行前或受控执行边界内明确拒绝。
- `runtime_error`：安装、初始化、API、存储访问或未预期执行失败。

`capability_absent` 是独立布尔观察字段，不是第四种 `run_status`。它表示接口运行成功，但缺少基准要求的原生语义。此类行保持 `run_status=success`、进入 correctness denominator，并由 oracle 判定。

聚合展示四个论文概念：能力缺失、映射不支持、运行错误、正确性违规；底层字段仍保持互斥运行状态加正交能力标志，避免一行被重复计数。

### 4.4 正确性统计

对每个 variant 和 workload 报告：

- attempted；
- successful；
- correctness_included；
- excluded；
- capability_absent_observed；
- unsupported_mapping；
- runtime_error；
- violation_count / violation_rate；
- oracle_match_count / oracle_match_rate；
- Wilson 95% CI 的 estimate、lower、upper、numerator、denominator。

必须满足计数恒等式：

```text
attempted = successful + unsupported_mapping + runtime_error
correctness_included = successful
excluded = unsupported_mapping + runtime_error
0 <= capability_absent_observed <= successful
0 <= violation_count <= correctness_included
0 <= oracle_match_count <= correctness_included
```

若某组分母为 0，区间必须显式标记 unavailable，不生成伪 0%。

## 5. Provenance 对照运行协议

### 5.1 正式矩阵

| graph nodes | concurrency | variants | repetitions/variant |
|---:|---:|---|---:|
| 100 | 1 | TxnMem, MemoryOnly-NoProvenance | 30 |
| 1,000 | 4 | TxnMem, MemoryOnly-NoProvenance | 30 |
| 10,000 | 16 | TxnMem, MemoryOnly-NoProvenance | 30 |

每个 repetition 使用唯一命名空间，结束后验证隔离并按既有正式生命周期清理。一个 repetition 的超时不得取消、污染或重用另一个 repetition 的命名空间。

### 5.2 操作与样本

- 两个 variant 使用相同输入对象、seed、并发调度描述和 memory-side 操作序列。
- `derive` 与 `propagate` 在对照中仍完成对应 memory 对象写入，但不持久化 provenance edge。
- `traverse` 仅在完整 TxnMem 中执行和计时；对照不生成 traversal latency 样本。
- 每个样本记录 variant、cell、repetition、operation、elapsed_ns、success、timeout、error category 和 namespace。
- repetition 汇总记录总 wall-clock、共同操作计数、完整机制额外 traversal 计数、吞吐和最终状态检查。

### 5.3 统计方法

每个 variant/cell 报告：

- repetition eligibility；
- operation sample count；
- p50/p95/p99 latency（ms）；
- total wall-clock；
- throughput ops/s；
- timeout/error/exclusion count；
- whole-repetition bootstrap 95% CI，10,000 次重采样，固定 seed。

配对比较按相同 cell 和 repetition seed 计算：

```text
latency_overhead_pct = (TxnMem - MemoryOnly) / MemoryOnly * 100
throughput_change_pct = (TxnMem - MemoryOnly) / MemoryOnly * 100
```

配对指标使用 repetition 级统计量进行 bootstrap，不把同一 repetition 内的操作样本误当作独立重复。分母为零、配对缺失或任一侧不合格时，该 pair 不进入增量估计，并报告缺失原因。

### 5.4 正确性和隔离伴随检查

完整 TxnMem 必须验证：

- memory 对象和 provenance edge 数量符合预期；
- traversal 返回集合与生成图一致；
- 无 partial provenance update；
- 无跨 repetition/cell 可见对象；
- timeout 后不存在未分类残余状态。

MemoryOnly 对照必须验证：

- memory 对象操作完成；
- provenance edge、adjacency/index 和 repair side effect 均不存在；
- 对照行为没有静默回退到完整 TxnMem。

任一资格检查失败时，repetition fail closed，不进入正式性能统计。

## 6. 产物结构

两项实验使用不同根目录：

```text
results/external_baselines_scale_400/
  manifest.json
  environment.json
  capabilities.json
  results.csv
  errors.jsonl
  summary.json

results/provenance_ablation_v10/
  manifest.json
  environment.json
  samples.csv
  repetitions.jsonl
  aggregate.json
  errors.jsonl
```

正式发布采用 candidate → validate → promote 流程。聚合器必须从逐行产物复算计数、区间和哈希，不信任 runner 自报 summary。已存在的非空正式目录不得覆盖。

## 7. 失败处理

- 单实例或单 repetition 错误必须保留归因，不能被重试结果静默覆盖。
- 允许通过显式 retry manifest 补跑失败项；最终聚合保留原始失败与选择规则。
- 外部依赖不可用归为 runtime error，不替代为模拟实现。
- unsupported mapping 必须由版本化能力规则支持，不能事后根据结果好坏决定。
- 性能 timeout 必须保留信号、边界、隔离和清理证据。
- 若正式环境、拓扑、依赖或输入哈希不一致，整批 candidate 不得 promote。

## 8. 测试策略

所有行为变更采用测试先行：

1. 外部 runner：400 实例 manifest、唯一命名空间、互斥状态和 capability flag。
2. 外部聚合：计数恒等式、零分母、Wilson 区间、workload/variant 分组和原始行复算。
3. 对照 backend：memory CRUD 保留、provenance edge/traversal/repair 禁用且无静默回退。
4. 配对聚合：共同操作集合、缺失 pair、零分母、whole-repetition bootstrap 和确定性 seed。
5. 正式门禁：现有目录拒绝覆盖、输入/环境哈希、candidate promotion、timeout 隔离和清理。
6. 小规模 smoke：每种外部适配器至少覆盖一个成功、能力缺失、映射不支持或运行错误路径；性能对照覆盖两个 variant 的小图单 repetition。

正式运行前必须通过相关 focused tests、完整测试套件、真实依赖 smoke 和拓扑资格检查。

## 9. 验收标准

### 9.1 外部基线

- 五个方案各有 400 次 attempted，共 2,000 次。
- 每一行状态唯一，四类论文统计可从底层字段无歧义复算。
- correctness denominator 只排除 unsupported mapping 与 runtime error。
- 所有总体和 workload 级二项比例提供 Wilson 95% CI 或明确 unavailable。
- manifest、环境、能力矩阵、逐行结果、错误和聚合全部存在并通过哈希审计。

### 9.2 Provenance 对照

- 3 个 cell × 2 个 variant × 30 repetitions = 180 个 attempted repetitions。
- 完整 TxnMem 与对照的共同操作输入可逐 repetition 配对。
- 完整机制报告 traversal 绝对成本；共同操作和总体成本的比较口径明确分开。
- 报告 p50/p95/p99、吞吐、配对增量和 whole-repetition bootstrap 95% CI。
- timeout、error、exclusion、正确性和隔离检查独立报告。
- 结果只支持被测真实后端、三个代表性 cell 和正式拓扑，不外推为生产性能。

## 10. 论文使用边界

外部基线结果支持相同 TxnMemBench 工作负载下的可观察正确性比较，不证明第三方系统存在安全漏洞。能力缺失应写为原生接口未提供目标语义。

性能对照支持估计被测条件下 provenance 维护机制包的增量成本。由于 `MemoryOnly-NoProvenance` 不执行 traversal，论文必须把 traversal 的绝对延迟与共同操作的配对开销分开，不能把不存在的对照 traversal 视为零延迟，也不能声称所有 provenance 功能的逐操作因果开销均已隔离。
