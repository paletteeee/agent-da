# TxnMem 投稿证据补量设计

## 目标与范围

本轮工作补齐四类证据，而不是机械增加重复行：

1. 将受控实验扩展为 8 个 workload family、每类 200 个确定性参数化实例、5 个系统变体，共 1,600 个实例和 8,000 条变体结果，并生成按 family 分层的饱和曲线与置信区间。
2. 使用冻结的官方数据边界运行 τ-bench retail/test 115、AppWorld test_normal 168 个配对任务，以及 LoCoMo 10 个对话的 5 次配对重复；LongMemEval-S cleaned 500 作为独立补充，不与前三项混称。
3. 将 AppWorld realism 扩展到至少 50 个独立任务 family，并对 LoCoMo 全部 10 个 conversation 做 leave-one-conversation-out 与 conversation-cluster bootstrap。
4. 在真实 Qdrant/Neo4j 上完成 3 个 provenance graph 规模 × 5 个并发度 × 每格 30 次重复的性能矩阵，并将 backend-only、模型端到端和跨主机证据分开报告。

本轮不把 legacy τ-bench 0.1.0 称为 τ³-bench，不把 AppWorld reference API projection 称为原生 memory ground truth，不把 LoCoMo 的问题数当作独立对话数，也不把同机服务结果称为一般分布式事务或生产延迟。

## 子项目一：受控正确性与饱和曲线

### 正确性门

当前提交中的 reference semantics 已要求 `supersede` 权限，但 `supersession_consistency` workload 未提供该权限，导致当前代码重放与历史 artifact 不一致。首先新增覆盖 8 个 workload 的 differential regression test，并在 workload 中显式声明合法 supersede policy。修复后必须满足：

- 1,600 个 TxnMem 实例均无 invariant violation；
- 1,600/1,600 与独立 reference oracle 匹配；
- 两次独立生成的文件 hash 一致；
- 历史 50-seed口径可由当前代码重放，并明确记录是否因参数化升级而作废。

### 参数化而非伪重复

`configs/workload_families.yaml` 中的范围必须真正驱动生成器。对每个 `(family, seed)` 使用稳定的 SHA-256 派生随机流，确定性采样事务大小、provenance depth、branch factor、policy churn 和 concurrency。不同 family 只消费与其语义有关的参数；生成器同时输出 `semantic_parameters` 和去除标识符后的 `semantic_fingerprint`。

扩量报告必须给出每个 family 的 unique fingerprint 数、参数覆盖率和组合覆盖率。饱和曲线使用嵌套 seed 前缀 `10/25/50/100/150/200`，分别报告每个变体的违规发现率、oracle match rate 和 Wilson 95% 区间。区间仅描述被测参数空间，不能解释为生产违规概率。

### 产物

- `configs/controlled_scale_200.json`
- `results/final_controlled_200/data/generated_instances.jsonl`
- `results/final_controlled_200/data/reference_oracles.jsonl`
- `results/final_controlled_200/results/experiment_results.csv`
- `results/final_controlled_200/results/saturation.json`
- `results/final_controlled_200/results/diversity.json`
- `results/final_controlled_200/results/figures/saturation.svg`
- 绑定配置 hash、源码 commit、seed 集和 variant 集的 `run_manifest.json`

## 子项目二：公开 benchmark 扩量

### 冻结边界

所有 manifest 记录 benchmark code commit、data version、split 文件 SHA-256、模型 revision、模型服务 build、seed、任务顺序和 manifest hash。长任务支持 shard、resume 和 fail-closed merge；失败任务仍保留在分母中。

### τ-bench

使用 legacy τ-bench 0.1.0 的 retail/test 115 个任务、官方环境状态和 reward evaluator，继续采用已审计的 scripted user boundary。论文表述限定为“legacy τ-bench retail/test workflow evidence”。如果官方 LLM user simulator 未接入，不声称标准 τ-bench pass^k。

### AppWorld

manifest 必须读取官方 `data/datasets/test_normal.txt`，精确得到 168 个 Test-N ID，禁止按目录排序截断。baseline 与 tuned 使用同一 manifest、模型、tool strategy、max steps 和 evaluator，仅 prompt profile 不同。每臂分片运行并独立保存，merge 时拒绝缺失、重复、跨 split 或条件 fingerprint 不一致。

### LoCoMo

重新运行 baseline/tuned 各 5 次，固定种子 `17/1017/2017/3017/4017`。摄入方式改为按 session 流式处理完整历史，记录总 session、总字符、已摄入 session/字符和覆盖率；不再用 24,000 字符头尾截断冒充 full-history。官方 QA evaluator 保持独立，按 conversation 聚类报告不确定性。

### LongMemEval

只接入官方 LongMemEval-S cleaned 500，不接 LongMemEval-M。每个问题隔离 memory namespace，按 session 流式摄入，导出官方要求的 `{question_id, hypothesis}` JSONL，并记录 evidence-session retrieval recall。若缺少官方 GPT-4o judge 凭据，正式产物只报告 deterministic retrieval 指标和 `official_qa_status=blocked`，不得用本地模型分数冒充官方 QA；这不阻塞前三个已预注册公开 benchmark 的完成状态。

## 子项目三：realism 与分层统计

新增通用 group-aware resampling：

- `leave_one_group_out(records, group_key)` 保证每个 group 恰好一次 holdout；
- `cluster_bootstrap_interval` 以完整 conversation/task family 为重采样单位；
- 每个 fold 的 calibration 只能使用其余 group，并实际生成该 fold 的 synthetic suite；
- 输出 group ID 的哈希、每组 feature 数和 zero-event denominator，不保存敏感 payload。

AppWorld 从 Test-N 中按固定 seed 选择至少 50 个独立 task family，生成 method/URL-only projection，并将 50 个 family 全部作为外部评估单位；另取不重叠 calibration family。LoCoMo 使用全部 10 个 conversation 做 10-fold LOO，不再把 8/2 单次拆分作为主要 realism 证据。

## 子项目四：真实后端性能与拓扑

### 性能矩阵

固定矩阵如下：

- graph node 数：`100/1000/10000`；
- concurrency：`1/2/4/8/16`；
- 每格 30 次独立 repetition，共 450 个 repetition；
- 分层 DAG 由固定 seed 生成，记录 node/edge count、graph hash 和 namespace；
- workload mix 固定为 read/search/derive/invalidate-repair，并记录每操作 latency、success、retry；
- 吞吐只以成功操作为分子；
- 报告 p50/p95/p99、成功吞吐及 repetition-cluster bootstrap 95% CI。

每次 repetition 使用唯一 namespace，预加载后核验 node/edge 数，结束后抽样读回 provenance closure。`partial` 或 `unknown` 状态不允许被汇总为成功。性能运行记录 CPU、内存、磁盘、Qdrant/Neo4j/Toxiproxy version、并发峰值和是否存在外部共租负载；存在无法隔离的共租负载时，结果只作为诊断，不进入正式曲线。

### 跨主机

沿用现有 v8 listener ownership 和 transport attestation，将 model endpoint 与 Qdrant/Neo4j endpoint 分开记录。若只有一个 Agent host 和一个 model/backend host，只声称该双主机拓扑；没有三个独立 host 就不声称 multi-host Agent scaling。local 与 cross-host 必须使用同一 workload/config hash。

## 数据、隐私和 Git 策略

原始 prompt、工具参数、业务值和完整 benchmark trace 只保留在授权服务器，不提交 Git。Git 只保存官方任务 ID、哈希、脱敏 per-task feature、聚合统计、失败分类和可重放配置。受控 synthetic instances 与独立 reference oracle 允许版本化，但 artifact audit 使用精确 allowlist，并继续扫描敏感键和值。

新增结果先写入版本化目录，不覆盖历史 artifact。只有通过 claim ledger、artifact audit、确定性复跑和完整测试后，才更新正文 active claim；无法完成的外部 evaluator 或拓扑保持显式 blocked，不伪造替代结果。

## 验收标准

1. 受控实验：1,600 instances、8,000 rows、TxnMem 0 violation、1,600 oracle match、非平凡 semantic fingerprint 覆盖、两次 hash 一致。
2. τ-bench：115 个唯一 retail/test任务、115 个 evaluator-available 结果、无任务丢失。
3. AppWorld：168 个唯一 Test-N任务，每臂168个官方 evaluator结果，配对条件 fingerprint 一致。
4. LoCoMo：10 conversations × 5 seeds × 2 profiles，完整 session-stream coverage，conversation-cluster interval 可重算。
5. LongMemEval-S：500个隔离问题完成 ingestion/retrieval/hypothesis导出；官方 QA 状态如实记录。
6. Realism：AppWorld至少50个独立 family；LoCoMo恰好10个无泄漏 LOO fold。
7. 性能：15个矩阵 cell × 30 repetitions；每格指标、CI、环境和状态验证完整。
8. 工程：新增单测先失败后通过；全量测试、claim audit、artifact audit均无未解释失败；新增代码和脱敏结果提交并推送到独立分支。

