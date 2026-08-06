# TxnMem 当前实验报告

更新时间：2026-08-07  
模型：Qwen2.5-7B-Instruct  
实验仓库：`remote_staging/txnmem`  
远端实验目录：`/data/txnmem_run_20260806`

## 1. 报告口径

本报告只使用仓库中可审计的脱敏 aggregate 结果。TxnMem 的 expected outcome 由独立 serial reference executor 计算，不由 TxnMem、LLM 或 workload generator 生成。公开 benchmark 的结果分成四层：官方 evaluator、native memory event contract、TxnMem 独立 oracle、backend consistency/performance；四者不互相替代。

τ-bench、AppWorld 和 LoCoMo 的 native 运行证明的是官方 workflow/runtime 边界可以接入 TxnMem memory backend，不等于这些 benchmark 原生提供了 memory transaction 或 provenance ground truth。因此本文不把它们命名为公开 benchmark 的原生 memory accuracy。

## 2. Controlled correctness suite

TxnMemBench 覆盖 8 个 workload family、160 个实例、5 个系统变体和 800 条 variant-level 结果。核心 workload 包括 atomic multi-write、crash during commit、policy revoke、scope bypass、supersession consistency、provenance chain repair、provenance branch repair 和 mixed stress。

在当前受控实例中，TxnMem 的目标违规数为 0/160，TxnMem oracle match 为 160/160。对照结果为：Naive 140/160 违规，TxnMem-NoTxn 80/160，TxnMem-NoPolicyCommit 20/160，TxnMem-NoRepair 40/160。该结果支持三个机制判断：事务边界防止 partial update，commit-time policy revalidation 防止 stale authorization，provenance repair 防止失效源对象继续污染派生对象。

## 3. Failure schedule、coverage 与 mutation

failure schedule 使用“触发条件 → 注入动作”形式，而不是只使用独立随机故障。已覆盖 write 后 crash、commit 边界 crash、policy revoke、network drop、timeout、connection drop 和 retry-success 等路径。

- 因果 schedule：400 个 case，目标违规检测率 0.875。
- random schedule baseline：4,000 个 case，检测率 0.750。
- schedule 结论：因果 schedule 的检测效率高于随机故障 baseline，但该比较不是对真实生产故障分布的估计。
- distributed protocol smoke：4 类 schedule，5/5 invariant coverage，无 minimal counterexample。
- mutation testing：kill rate 0.8571；已覆盖 NoTxn、NoPolicyCommit、NoRepair 等实现缺陷。

独立 reference coverage 覆盖 atomicity、commit authorization、provenance closure、recovery consistency、scope safety 和 supersession consistency，coverage rate 为 1.0。

证据路径：`results/final_controlled/results/coverage.json`、`results/final_controlled/results/schedule_baseline.json`、`results/final_controlled/results/mutation_report.json`、`results/remaining_tasks/distributed_protocol/results/process_protocol.json`。

## 4. Qwen2.5-7B native-agent 实验

固定 native task manifest 重复运行 5 次，每次 10 个 task，共 50 个 task episode、110 个 native events、0 个 evaluation error。5 次重复均为 10/10 contract success 和 10/10 TxnMem oracle match；contract success 与 oracle match 的 95% Wilson 区间均为 `[0.929, 1.000]`。预期的 `injected_crash` 与 `policy_denied` 失败也按设计出现。

该实验验证真实模型 tool loop、canonical event contract、failure injection 和 independent differential evaluation 可执行；它不是用户任务成功率或生产质量结论。

证据路径：`results/remaining_tasks/native_repetitions5/repetition_report.json`。

## 5. 三类公开 benchmark native/runtime evidence

### 5.1 τ-bench

官方 runtime 完成 50-task batch，最终合并 497 个 native events，50/50 evaluator available，reward sum 为 15，mean reward 为 0.3000。2 个 task 达到 max steps，1 个 task 没有 memory event，另 1 个长请求在 timeout retry 后完成但仍无 memory event。τ-bench reward 不是 memory accuracy，不能把 reward 直接解释为 TxnMem 的任务成功率。

### 5.2 AppWorld

官方 runtime 完成 20-task batch，20/20 evaluator available，记录 49 个 native events，官方 `task_completed()` 为 0/20，112 个官方断言中通过 17 个。实验使用 DB snapshot 和 task-specific app/API allowlist。0/20 是当前 Agent/tool strategy 的官方任务结果，不是 TxnMem oracle；后续仍需做 baseline/tuned prompting 对比。

### 5.3 LoCoMo native contextual Agent

10 个 conversation 全部完成 native contextual ingestion，记录 44 个 events，TxnMem differential match 为 9/10。该结果是 native workflow/runtime evidence，不是 LoCoMo 原生 memory transaction ground truth。

### 5.4 LoCoMo paired memory QA

同一 SQLite memory backend 先完成 conversation ingestion，再由 backend retrieval 提供 QA 上下文。10/10 ingestion 完成，记录 331 个 native events，1,986 个问题，官方 evaluator 为 `locomo.task_eval.evaluation.eval_question_answering`，mean F1 为 `0.16424`。

各类别 mean F1 为：category 1 `0.10417`、category 2 `0.05387`、category 3 `0.10724`、category 4 `0.10132`、category 5 `0.41256`。直接全文上下文 QA 的 mean F1 为 `0.32220`；它与 paired memory QA 不是同一条件下的 ablation，不能将二者差异单独归因于 TxnMem。

证据路径：`results/locomo_paired_full_retrieval/locomo_paired_summary.json`。

## 6. 真实 vector/graph backend 与网络故障

`VectorGraphMemoryBackend` 已在单机真实 Qdrant 1.11.5、Neo4j 5.22.0 和 Toxiproxy 环境中完成 direct service smoke。30 次重复 backend-only 写入性能结果如下：

| workload | repetition | p50 ms | p95 ms | p99 ms | throughput ops/s | errors | partial commit |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 50 events | 30 | 381.259 | 423.219 | 465.135 | 130.337 | 0 | 0 |
| 200 events | 30 | 1,794.536 | 2,058.061 | 2,083.128 | 109.537 | 0 | 0 |
| 1000 events | 30 | 15,787.283 | 20,770.839 | 20,947.253 | 62.764 | 0 | 0 |

真实 Toxiproxy fault matrix 覆盖 normal、Qdrant delay、Qdrant timeout、Neo4j connection drop 和 retry-success，所有已执行场景均无 partial commit。上述 backend timing 是单机服务测量，统一标记 `production_latency_claim=false`，不能推出生产吞吐、跨主机一致性或生产级 2PC 结论。

证据路径：`results/real_backend_performance_reps30_v2/results/backend_performance.json`。

另有 5 个 τ-bench task 的 Qwen2.5-7B + Qdrant + Neo4j 端到端 smoke：5/5 completed，mean 17,879.4 ms，P50 15,351.6 ms。该结果包含模型、backend 和 evaluator 的端到端开销，不应当被当作 backend-only latency。

## 7. Synthetic 与 trace-grounded realism

当前正式 realism 统计使用 400 个 synthetic instances、固定 seed 17、2,000 次 bootstrap；τ-bench 使用 175 个 trace-grounded episode，LoCoMo 使用 10 个 conversation。方法是 feature-wise bootstrap mean/mean-difference intervals，不是高维 joint test。

- τ-bench 平均相对 feature absolute difference：0.225。
- LoCoMo 平均相对 feature absolute difference：0.437。
- τ-bench operation count：synthetic mean 4.25，trace mean 7.257，mean difference 3.007，95% bootstrap interval `[2.385, 3.626]`。
- LoCoMo operation count：synthetic mean 4.25，trace mean 29.2，mean difference 24.95，95% bootstrap interval `[21.928, 27.493]`。

结果说明当前 generator 低估长对话的 operation/transaction size；它支持校准方向，不支持宣称 synthetic 与真实 trace 已经联合分布一致。AppWorld projection 的原始事件文件没有在本地重新生成，不能把现有 AppWorld projection 统计升级为 native memory ground truth。

证据路径：`results/official_trace_runs/tau_bench_joint_bootstrap/results/trace_realism.json`、`results/official_trace_runs/locomo_joint_bootstrap/results/trace_realism.json`。

## 8. 可复现性与文档 QA

- 全量单元测试：157 tests，3 个依赖缺失项 skipped，其他测试通过。
- 本地 process concurrency smoke：2 workers、3 operations、线性化序号完整，无未确认 operation。
- DOCX 初稿已生成 20 页 PNG/PDF 并逐页视觉检查；accessibility audit 为 high=0、medium=0、low=0。
- 本地 Git 已保存当前实现和 aggregate 结果；最近一次真实 backend 结果提交为 `f5de22e`。

论文初稿：`/Users/xiaoyan_zhu/Desktop/agent-db/outputs/TxnMem_论文初稿.docx`。

## 9. 当前未完成与下一步实验

以下项目仍不能标记为完成：

1. LoCoMo paired QA 的多次 repetition，以及与更强 Agent/tool prompting 的同条件比较。
2. AppWorld baseline/tuned agent/tool strategy 对比；当前官方 success 为 0/20。
3. 多 Agent 并发、跨主机网络、长周期运行和真实模型 token cost 统计。当前已有单机 backend performance，但没有跨主机和成本计量证据。
4. 高维 joint realism test，以及 AppWorld projection 原始事件的重新生成。
5. Git 远端推送。当前仓库没有 remote URL，不能安全执行 push。

继续实验时必须保持以下顺序：先固定 manifest 和 evaluator，再运行 baseline/tuned 对比；先保留 raw trace 在远端，只同步脱敏 aggregate；最后按 task/conversation 统计，而不是按 event 行数扩大样本量。
