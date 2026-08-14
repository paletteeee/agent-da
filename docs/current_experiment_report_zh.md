# TxnMem 当前实验报告

更新时间：2026-08-12
模型：Qwen2.5-7B-Instruct  
实验仓库：`remote_staging/txnmem`  

## 1. 报告口径

本报告只使用仓库中可审计的脱敏 aggregate 结果。TxnMem 的 expected outcome 由独立 serial reference executor 计算，不由 TxnMem、LLM 或 workload generator 生成。公开 benchmark 与服务证据分成四层：官方 evaluator、native memory event contract、TxnMem 独立 oracle、proxy/fault-response observation；四者不互相替代。

τ-bench、AppWorld 和 LoCoMo 的 native 运行证明的是官方 workflow/runtime 边界可以接入 TxnMem memory backend，不等于这些 benchmark 原生提供了 memory transaction 或 provenance ground truth。因此本文不把它们命名为公开 benchmark 的原生 memory accuracy。

## 2. Controlled correctness suite

TxnMemBench 覆盖 8 个 workload family、50 个 seed、400 个实例、5 个系统变体和 2,000 条 variant-level 结果。核心 workload 包括 atomic multi-write、crash during commit、policy revoke、scope bypass、supersession consistency、provenance chain repair、provenance branch repair 和 mixed stress。上述计数由 `results/paper_evidence/controlled_suite.json` 从 `generated_instances.jsonl` 与完整 CSV 笛卡尔积直接复算，不采用手工填写的样本总数。

在当前受控实例中，TxnMem 的目标违规数为 0/400，TxnMem oracle match 为 400/400。对照结果为：Naive 350/400 违规，TxnMem-NoTxn 200/400，TxnMem-NoPolicyCommit 50/400，TxnMem-NoRepair 100/400。该结果支持三个机制判断：事务边界防止 partial update，commit-time policy revalidation 防止 stale authorization，provenance repair 防止失效源对象继续污染派生对象。

## 3. Failure schedule、coverage 与 mutation

failure schedule 使用“触发条件 → 注入动作”形式，而不是只使用独立随机故障。已覆盖 write 后 crash、commit 边界 crash、policy revoke、network drop、timeout、connection drop 和 retry-success 等路径。

- 因果 schedule：400 个 case，目标违规检测率 0.875。
- random schedule baseline：4,000 个 case，检测率 0.750。
- schedule 结论：因果 schedule 的检测效率高于随机故障 baseline，但该比较不是对真实生产故障分布的估计。
- distributed protocol smoke：4 类 schedule，5/5 invariant coverage，无 minimal counterexample。
- mutation testing：kill rate 0.8571；已覆盖 NoTxn、NoPolicyCommit、NoRepair 等实现缺陷。
- minimal mutant witness：`partial_commit`、`remove_commit_revalidation`、`disable_provenance_traversal`、`bypass_scope_check` 四类 mutant 均已从 400 个实例中找到可重放的最短 operation prefix；最小前缀长度分别为 2、1、6、1，删除最后一个操作后均不再复现同一目标违规。

独立 reference coverage 覆盖 atomicity、commit authorization、provenance closure、recovery consistency、scope safety 和 supersession consistency，coverage rate 为 1.0。

证据路径：`results/final_controlled/results/coverage.json`、`results/final_controlled/results/schedule_baseline.json`、`results/final_controlled/results/mutation_report.json`、`results/final_controlled/results/minimal_mutant_witnesses.json`、`results/remaining_tasks/distributed_protocol/results/process_protocol.json`。

## 4. Qwen2.5-7B native-agent 实验

固定 native task manifest 重复运行 5 次，每次 10 个 task，共 50 个 task episode、110 个 native events、0 个 evaluation error。5 次重复均为 10/10 contract success 和 10/10 TxnMem oracle match；contract success 与 oracle match 的 95% Wilson 区间均为 `[0.929, 1.000]`。预期的 `injected_crash` 与 `policy_denied` 失败也按设计出现。

该实验验证真实模型 tool loop、canonical event contract、failure injection 和 independent differential evaluation 可执行；它不是用户任务成功率或生产质量结论。

证据路径：`results/remaining_tasks/native_repetitions5/repetition_report.json`。

## 5. 三类公开 benchmark native/runtime evidence

### 5.1 τ-bench

官方 runtime 完成 50-task batch，最终合并 497 个 native events，50/50 evaluator available，reward sum 为 15，mean reward 为 0.3000。2 个 task 达到 max steps，1 个 task 没有 memory event，另 1 个长请求在 timeout retry 后完成但仍无 memory event。τ-bench reward 不是 memory accuracy，不能把 reward 直接解释为 TxnMem 的任务成功率。

该聚合记录 50 个唯一 task ID、retry 归并规则、manifest/hash、Qwen2.5-7B revision、vLLM build、官方 evaluator 状态、运行命令、源 artifact hash 和运行时连续性 attestation。证据路径：`results/submission_evidence/tau_bench_50/aggregate.json`、`results/submission_evidence/tau_bench_50/runtime_attestation.json`。

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

`VectorGraphMemoryBackend` 的旧单机 Qdrant 1.11.5、Neo4j 5.22.0 和 Toxiproxy 2.5.0 矩阵记录了 fault/response 路径。客户端只连接两个代理 listen port，Toxiproxy management API 在指定 Qdrant `write` 或 Neo4j `commit` ordinal 前安装 toxic。normal、delay、timeout、connection-drop 和 retry-success 各执行 30 次；四个非 normal 场景的 `trigger_fired`、`toxic_installed`、`proxy_path_verified` 均为 30/30。旧 runner 没有在 toxic 清除后独立读取 Qdrant 与 Neo4j 状态，因此这些数据不是双存储持久状态、原子性、可用性或延迟证据。

- normal 与 delay 均记录 30/30 success response。
- timeout 与 connection-drop 均记录 30/30 client error/abort response。
- retry-success 记录 30/30 首次故障、清除 toxic 后 30/30 单次重试成功 response。

旧的 `results/real_backend_performance_reps30_v2/results/backend_performance.json` 没有证明请求实际穿过已激活的 toxic，已在 supersession index 中标为历史结果，不再承担正式故障或性能结论。当前 active aggregate 为 `results/submission_evidence/toxiproxy_faults_30/aggregate.json`，只投影 proxy/fault-response 字段；新的 verifier 会在恢复后分别读取 Qdrant 与 Neo4j，将状态分类为 complete/absent/partial/unknown，并在读取失败或缺失时 fail closed。由于本会话没有可用 Docker/真实服务，state-verified 5×30 尚未运行，仍是投稿阻塞实验。

另有 5 个 τ-bench task 的 Qwen2.5-7B + Qdrant + Neo4j 端到端 smoke：5/5 completed，记录 30 个 native events，mean 18,851.6 ms，P50 15,497.0 ms。模型 revision 为 `7b44…26b4`，vLLM 为 `0.8.5.post1`，运行时健康检查确认 Qdrant 1.11.5 与 Neo4j 5.22.0 可用。该结果包含模型、backend 和 evaluator 的端到端开销，不应当被当作 backend-only 或生产 latency。证据路径：`results/submission_evidence/qwen_vector_graph_e2e_5/aggregate.json`。

在 `cross_host_client_server` 范围内，Qwen2.5-7B-Instruct（revision `7b44…26b4`，vLLM `0.8.5.post1`）完成 3 次独立 attested v8 运行：每次 68 cycles、544 attempts，elapsed 分别为 `604.165115041`、`603.328782334`、`603.574593500` 秒；合计 204 cycles、1,632 attempts、`1,811.068490875` 秒。三个 UTC interval 不重叠，distinct tunnel process count 为 3。每次 configured concurrency=4，observed peak=4。

全部 1,632/1,632 attempts 达到 contract success；其中 1,224 个为 completed attempts，408 个 runner-level failures 全部是 workload 预期机制（204 `injected_crash`、204 `policy_denied`），不是模型或 endpoint/transport 错误。三份 endpoint/transport analysis 合计为 0 相关失败。endpoint 精确报告 3,672/3,672 request usage：prompt `2,935,703`、completion `315,803`、total `3,251,506` tokens。

拓扑为 1 个 Agent-worker host 加 1 个 model-server host；每次运行均完成模型调用前的 preflight 与结束时的 final PID-owned listener 校验、ControlMaster PID pre/post check、严格 loopback forwarding 校验和拓扑连续性校验，host identities distinct、`cross_host_network_claim=true`。这支持跨主机 client-to-model-server 负载实验的机制证据，但不等于生产级多主机 Agent workers、单一连续 30 分钟 tunnel、跨主机 Qdrant/Neo4j，亦不构成生产 latency 结论（`production_latency_claim=false`）。`cross_host_multi_agent_workers_claim=false`、`single_continuous_tunnel_claim=false`；没有显式 pricing rate，货币成本未计算。

早期 v6 预正式运行因 UTC 与 `perf_counter` 的时间证据不一致而被 strict aggregator 拒绝，因此 v6 不计入正式结果。v7 虽通过当时的聚合规则，但最终安全审查发现其尚未证明 SSH 隧道进程在每次运行前后持续拥有模型端点监听器，故 v7 仅保留为修复前审计历史，不再承担最终跨主机结论。代码提交 `4669a01` 加入 preflight/final listener ownership、ControlMaster 连续性、严格 forwarding/UTC 校验和固定 `3 × 600` 秒正式聚合要求后，采用防休眠执行重跑 v8；三次 UTC wall-clock 与内部 elapsed 的差值分别为 `0.002323041`、`0.009159334`、`0.039579500` 秒，均低于 1% tolerance，且 UTC offset 均为 0。

证据路径：`results/cross_host_model_load_formal_v8_aggregate/results/model_load_repetition_summary.json`、`results/cross_host_model_load_formal_v8_rep1/results/model_load_summary.json`、`results/cross_host_model_load_formal_v8_rep2/results/model_load_summary.json`、`results/cross_host_model_load_formal_v8_rep3/results/model_load_summary.json`、`results/cross_host_model_load_formal_v8_rep1/results/endpoint_transport_failure_analysis.json`、`results/cross_host_model_load_formal_v8_rep2/results/endpoint_transport_failure_analysis.json`、`results/cross_host_model_load_formal_v8_rep3/results/endpoint_transport_failure_analysis.json`。v7 作废说明见 `results/cross_host_model_load_formal_v7_aggregate/SUPERSEDED.md`。

## 7. Synthetic 与 trace-grounded realism

正式 realism 统计使用 400 个 synthetic instances，并在 `operation_count`、`transaction_size`、`policy_change_rate`、`provenance_depth`、`branch_factor` 和 `agent_count` 六维联合空间执行 standardized RBF random-feature MMD permutation test；calibration/train 与 holdout/test 按 episode 隔离。

- τ-bench：141 个 calibration、34 个 holdout episode；平均相对 feature absolute difference 为 `0.22590`，MMD²=`0.18198`、p=`0.001`（999 permutations）。
- LoCoMo：8 个 calibration、2 个 holdout conversation；平均相对差异为 `0.43952`，MMD²=`1.94189`、p=`0.0005`（1,999 permutations）。holdout n=2，推断低功效且不稳定。
- AppWorld：从官方 `ground_truth/api_calls.json` 重新生成 5 个 task 的 380 条 method/URL-only 脱敏原始 projection event，3 个用于 calibration、2 个用于 holdout；平均相对差异为 `0.43071`，MMD²=`1.22097`、p=`0.0005`（1,999 permutations）。holdout n=2，同样只能作为诊断证据。

这些结果拒绝“当前 synthetic 与 holdout trace 的六维联合分布相同”这一零假设，说明 generator 仍需校准；它们不支持分布等价。AppWorld projection 是从官方 API call provenance 重新生成的 trace-grounded adaptation，不是 AppWorld 原生 memory ground truth。

证据路径：`results/joint_realism/tau_bench/results/trace_realism.json`、`results/joint_realism/locomo/results/trace_realism.json`、`results/appworld_projection_regenerated/projection_inventory.json`、`results/appworld_projection_regenerated/results/trace_realism.json`。

## 8. 可复现性与文档 QA

- 工作树全量单元测试：346 tests，3 个 skipped（仅可选 runtime 未安装），0 failures；index-derived clean archive 亦为 346 tests、4 个 skipped（额外一个 skip 是 archive 不含 `.git` metadata，无法执行 Git-range 集成扫描）、0 failures。两者均以 bundled Python、`<temp-dir>` pycache/TMPDIR 和临时输出运行，不写入仓库或交付 DOCX。
- 本地 process concurrency smoke：2 workers、3 operations、线性化序号完整，无未确认 operation。
- 最终中文 CCF-A 初稿由正式命令直接写入 `<external-output-dir>/TxnMem_CCF-A中文论文初稿.docx`，SHA-256 为 `6673155ad304ab39f59d6455a1c6ff546f6459735f00dacdc31b617819227714`。连续两次正式构建逐字节一致，最终外部交付物与第二次构建相同；工作树中的 `outputs/...` 仅作可重建的临时 QA 副本，验收后删除。该文件对应工作树外精确 render `render_final_v6`：27 页 PNG 与 27 页 PDF；文档含 6 张图、8 张表、32 条已核验参考文献，accessibility audit 为 high=0、medium=0、low=0。
- artifact audit：0 findings。
- claim audit：15 条 active claim、131 个字段断言、0 findings，ledger digest 为 `596ef06eaf6a107e27c82fda1a9c520a9c064c5049e5e8be3af114ad126d664f`；manuscript audit 亦为 0 findings。每条正式数字关联 artifact/hash、运行命令、manifest/hash、source commit 与 claim boundary。历史状态和旧故障结果由 `results/paper_evidence/supersession_index.json` 标记，不再作为当前结论来源。
- 最终 OOXML 隐私闭环：无 rsid、批注/人员部分、追踪修订、custom properties、creator、lastModifiedBy、company 或绝对工作站路径；新 Git diff 与交付包的凭据模式扫描均为 0。上述最终 hash 在全量测试、审计和只读核验前后均保持一致，后续未重写 DOCX。
- 最终 attestation 代码修复提交为 `4669a01`；脱敏 v8 aggregate、per-repetition summaries、endpoint/transport analyses 与 v7 作废标记已由本地结果提交 `9785a48` 保存。

## 9. 当前状态、claim boundary 与后续工作

本分支已完成 controlled 400/2,000 统一口径、τ-bench 50-task 严格聚合、5-task Qwen+Qdrant+Neo4j E2E 聚合、四类前缀最小 mutant witness，以及覆盖正式实验数字的 claim ledger/supersession audit。Toxiproxy 旧数据仅保留 fault/response-path 观察，state-verified 5×30 尚未完成，因此当前是 CCF-A 质量工作稿而非 submission-ready evidence：

1. LoCoMo 只有 3 次描述性 paired repetition，平均 F1 增益很小且有一次回退；不能作统计显著性或普适改进结论。
2. AppWorld tuned 有 6 个 execution failure，且 n=20；token 总量为观测下界，不能作总体显著性结论。
3. joint realism 显示 synthetic 与 holdout trace 存在分布差异，特别是 LoCoMo/AppWorld holdout n=2；该结果不支持等价性。
4. cross-host v8 证据仅覆盖 1 Agent-worker host 与 1 model-server host 的三次独立运行；不包含生产级多主机 Agent workers、连续 30 分钟 tunnel 或跨主机 Qdrant/Neo4j。货币成本未计算，原因是没有显式 pricing rate。
5. Manuscript readiness blockers 是 state-verified Qdrant/Neo4j/Toxiproxy 5×30 rerun、选定 venue 后的模板适配，以及正常作者修订。

Repository operations note：仓库未配置 remote，本任务未执行 push；该状态不属于 manuscript readiness blocker。

未来 production-grade 扩展应固定 manifest/evaluator 并保持脱敏 aggregate 口径：增加多主机 Agent workers、连续 tunnel、跨主机 Qdrant/Neo4j 与有明确定价率的成本核算；这些是 future work，不是当前已完成的主张。
