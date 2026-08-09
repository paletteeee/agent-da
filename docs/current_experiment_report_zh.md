# TxnMem 当前实验报告

更新时间：2026-08-10
模型：Qwen2.5-7B-Instruct  
实验仓库：`remote_staging/txnmem`  

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

同一 20-task manifest、Qwen2.5-7B revision、generation 参数、官方 evaluator、`instruction_inferred` 工具策略和逐任务模型可见工具集合下，完成 baseline/tuned 配对。20/20 任务的工具数量及工具名 SHA-256 摘要完全匹配，condition fingerprint 相同。

- baseline：20/20 execution completed，官方 success 为 0/20，官方断言通过 17/112；40/40 模型响应含 usage，共 517,564 tokens。
- tuned：官方 success 为 1/20，官方断言通过 53/112；13 个任务改善、7 个不变、0 个回退。14/20 execution completed；4 个任务因模型调用未授权工具名失败，2 个长工具链任务因 model HTTP error 失败。116 次请求中 114 次返回 usage，观测到 2,171,632 tokens，因此该 token 总量及相对 baseline 的增量只能写成下界。

该结果表明当前 tuned prompt + trusted preflight 在这 20 个任务上改善了官方断言覆盖，并首次获得 1 个官方成功；样本量小且 tuned 存在 6 个 execution failure，不能声称通用显著提升，也不能把 AppWorld 结果解释为 TxnMem memory accuracy。

证据路径：`results/prompt_profile_formal_v4/appworld_baseline/native_batch_summary.json`、`results/prompt_profile_formal_v4/appworld_tuned/native_batch_summary.json`、`results/prompt_profile_formal_v4/appworld_prompt_comparison.json`。

### 5.3 LoCoMo native contextual Agent

10 个 conversation 全部完成 native contextual ingestion，记录 44 个 events，TxnMem differential match 为 9/10。该结果是 native workflow/runtime evidence，不是 LoCoMo 原生 memory transaction ground truth。

### 5.4 LoCoMo paired memory QA

同一 SQLite memory backend 先完成 conversation ingestion，再由 backend retrieval 提供 QA 上下文。10/10 ingestion 完成，记录 331 个 native events，1,986 个问题，官方 evaluator 为 `locomo.task_eval.evaluation.eval_question_answering`，mean F1 为 `0.16424`。

各类别 mean F1 为：category 1 `0.10417`、category 2 `0.05387`、category 3 `0.10724`、category 4 `0.10132`、category 5 `0.41256`。直接全文上下文 QA 的 mean F1 为 `0.32220`；它与 paired memory QA 不是同一条件下的 ablation，不能将二者差异单独归因于 TxnMem。

证据路径：`results/locomo_paired_full_retrieval/locomo_paired_summary.json`。

在固定 condition fingerprint、相同 10 个 conversation、每次 1,986 个问题和 seeds `[17, 1017, 2017]` 下，进一步完成 baseline/tuned 各 3 次 paired repetition。baseline F1 分别为 `0.13646/0.14114/0.13749`，均值 `0.13836`；tuned F1 分别为 `0.13551/0.14482/0.13960`，均值 `0.13998`。逐 repetition 差值为 `-0.00095/+0.00368/+0.00212`，平均差值 `+0.00162`，总体标准差 `0.00192`。baseline 使用 481,410 tokens，tuned 使用 521,949 tokens，精确增加 40,539 tokens。

这只是 3 次固定配对重复的描述性结果：更强提示带来的平均 F1 增益很小且一次回退，不能据此声称总体显著优于 baseline。

证据路径：`results/prompt_profile_formal_v4/locomo_baseline/locomo_paired_repetition_summary.json`、`results/prompt_profile_formal_v4/locomo_tuned/locomo_paired_repetition_summary.json`、`results/prompt_profile_formal_v4/locomo_prompt_comparison.json`。

## 6. 真实 vector/graph backend、网络故障与跨主机模型负载

`VectorGraphMemoryBackend` 已在单机真实 Qdrant 1.11.5、Neo4j 5.22.0 和 Toxiproxy 环境中完成 direct service smoke。30 次重复 backend-only 写入性能结果如下：

| workload | repetition | p50 ms | p95 ms | p99 ms | throughput ops/s | errors | partial commit |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 50 events | 30 | 381.259 | 423.219 | 465.135 | 130.337 | 0 | 0 |
| 200 events | 30 | 1,794.536 | 2,058.061 | 2,083.128 | 109.537 | 0 | 0 |
| 1000 events | 30 | 15,787.283 | 20,770.839 | 20,947.253 | 62.764 | 0 | 0 |

真实 Toxiproxy fault matrix 覆盖 normal、Qdrant delay、Qdrant timeout、Neo4j connection drop 和 retry-success，所有已执行场景均无 partial commit。上述 backend timing 是单机服务测量，统一标记 `production_latency_claim=false`，不能推出生产吞吐、跨主机一致性或生产级 2PC 结论。

证据路径：`results/real_backend_performance_reps30_v2/results/backend_performance.json`。

另有 5 个 τ-bench task 的 Qwen2.5-7B + Qdrant + Neo4j 端到端 smoke：5/5 completed，mean 17,879.4 ms，P50 15,351.6 ms。该结果包含模型、backend 和 evaluator 的端到端开销，不应当被当作 backend-only latency。

在 `cross_host_client_server` 范围内，Qwen2.5-7B-Instruct（revision `7b44…26b4`，vLLM `0.8.5.post1`）完成 3 次独立 attested 运行：每次 68 cycles、544 attempts，elapsed 分别为 `605.798544333`、`604.362563375`、`605.420804708` 秒；合计 204 cycles、1,632 attempts、`1,815.581912416` 秒。三个 UTC interval 不重叠，distinct tunnel process count 为 3。每次 configured concurrency=4，observed peak=4。

全部 1,632/1,632 attempts 达到 contract success；其中 1,224 个为 completed attempts，408 个 runner-level failures 全部是 workload 预期机制（204 `injected_crash`、204 `policy_denied`），不是模型或 endpoint/transport 错误。三份 endpoint/transport analysis 合计为 0 相关失败。endpoint 精确报告 3,672/3,672 request usage：prompt `2,935,706`、completion `315,828`、total `3,251,534` tokens。

拓扑为 1 个 Agent-worker host 加 1 个 model-server host；ControlMaster same-session/PID binding 已验证、host identities distinct、`cross_host_network_claim=true`。这支持跨主机 client-to-model-server 负载实验的机制证据，但不等于生产级多主机 Agent workers、单一连续 30 分钟 tunnel、跨主机 Qdrant/Neo4j，亦不构成生产 latency 结论（`production_latency_claim=false`）。`cross_host_multi_agent_workers_claim=false`、`single_continuous_tunnel_claim=false`；没有显式 pricing rate，货币成本未计算。

早期 v6 的三次运行虽模型、usage 与 topology 工件无错，但 strict aggregator 因 UTC 与 `perf_counter` 不一致而拒绝，根因是 macOS idle sleep 时 `mach_absolute_time` 暂停；因此 v6 不作为正式结果。以 `caffeinate` 重跑 v7 后，三次 clock difference 分别为 `0.000663`、`0.000359`、`0.007711` 秒，均显著低于 1% tolerance。

证据路径：`results/cross_host_model_load_formal_v7_aggregate/results/model_load_repetition_summary.json`、`results/cross_host_model_load_formal_v7_rep1/results/model_load_summary.json`、`results/cross_host_model_load_formal_v7_rep2/results/model_load_summary.json`、`results/cross_host_model_load_formal_v7_rep3/results/model_load_summary.json`、`results/cross_host_model_load_formal_v7_rep1/results/endpoint_transport_failure_analysis.json`、`results/cross_host_model_load_formal_v7_rep2/results/endpoint_transport_failure_analysis.json`、`results/cross_host_model_load_formal_v7_rep3/results/endpoint_transport_failure_analysis.json`。

## 7. Synthetic 与 trace-grounded realism

正式 realism 统计使用 400 个 synthetic instances，并在 `operation_count`、`transaction_size`、`policy_change_rate`、`provenance_depth`、`branch_factor` 和 `agent_count` 六维联合空间执行 standardized RBF random-feature MMD permutation test；calibration/train 与 holdout/test 按 episode 隔离。

- τ-bench：141 个 calibration、34 个 holdout episode；平均相对 feature absolute difference 为 `0.22590`，MMD²=`0.18198`、p=`0.001`（999 permutations）。
- LoCoMo：8 个 calibration、2 个 holdout conversation；平均相对差异为 `0.43952`，MMD²=`1.94189`、p=`0.0005`（1,999 permutations）。holdout n=2，推断低功效且不稳定。
- AppWorld：从官方 `ground_truth/api_calls.json` 重新生成 5 个 task 的 380 条 method/URL-only 脱敏原始 projection event，3 个用于 calibration、2 个用于 holdout；平均相对差异为 `0.43071`，MMD²=`1.22097`、p=`0.0005`（1,999 permutations）。holdout n=2，同样只能作为诊断证据。

这些结果拒绝“当前 synthetic 与 holdout trace 的六维联合分布相同”这一零假设，说明 generator 仍需校准；它们不支持分布等价。AppWorld projection 是从官方 API call provenance 重新生成的 trace-grounded adaptation，不是 AppWorld 原生 memory ground truth。

证据路径：`results/joint_realism/tau_bench/results/trace_realism.json`、`results/joint_realism/locomo/results/trace_realism.json`、`results/appworld_projection_regenerated/projection_inventory.json`、`results/appworld_projection_regenerated/results/trace_realism.json`。

## 8. 可复现性与文档 QA

- 全量单元测试：242 tests，3 个 skipped，0 failures。
- 本地 process concurrency smoke：2 workers、3 operations、线性化序号完整，无未确认 operation。
- DOCX 初稿已生成 20 页 PNG/PDF 并逐页视觉检查；accessibility audit 为 high=0、medium=0、low=0。
- artifact audit：0 findings。
- 最近代码提交为 `15b8e69`。v7 results 仍为未提交工件，本报告不将其表述为已在 Git 中保存。

## 9. 当前状态、claim boundary 与后续工作

当前计划中的四个实验组均已完成：LoCoMo paired baseline/tuned 3 次重复、AppWorld baseline/tuned 配对、τ/LoCoMo/AppWorld joint realism，以及 attested cross-host model load。它们的已知限制应作为 claim boundary，而不是被误写为未完成实验：

1. LoCoMo 只有 3 次描述性 paired repetition，平均 F1 增益很小且有一次回退；不能作统计显著性或普适改进结论。
2. AppWorld tuned 有 6 个 execution failure，且 n=20；token 总量为观测下界，不能作总体显著性结论。
3. joint realism 显示 synthetic 与 holdout trace 存在分布差异，特别是 LoCoMo/AppWorld holdout n=2；该结果不支持等价性。
4. cross-host 证据仅覆盖 1 Agent-worker host 与 1 model-server host 的三次独立运行；不包含生产级多主机 Agent workers、连续 30 分钟 tunnel 或跨主机 Qdrant/Neo4j。货币成本未计算，原因是没有显式 pricing rate。
5. Git 远端推送仍是唯一明确外部阻塞：`git remote -v` 为空，必须由用户提供 remote URL 后才能安全 push。

未来 production-grade 扩展应固定 manifest/evaluator 并保持脱敏 aggregate 口径：增加多主机 Agent workers、连续 tunnel、跨主机 Qdrant/Neo4j 与有明确定价率的成本核算；这些是 future work，不是当前已完成的主张。
