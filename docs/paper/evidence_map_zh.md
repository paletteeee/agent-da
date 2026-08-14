# TxnMem 正文证据索引

本索引只供作者核对正文；正文数字必须由 `configs/paper_claims.json` 中的 active claim 覆盖，并保留对应 claim boundary，不将本文件直接粘贴进论文。

## RQ1：三个核心机制是否阻止目标违规？

| 可使用 claim | 正文可使用数字 | artifact | 统计单位 | claim boundary | 预定章节 |
| --- | --- | --- | --- | --- | --- |
| `controlled_correctness_400x5` | 8 workload family、50 seed、400 instance、5 variant、2000 variant row；TxnMem 0 violation、400 oracle match；Naive 350、NoTxn 200、NoPolicyCommit 50、NoRepair 100 violation | `results/paper_evidence/controlled_suite.json` | instance、variant row | deterministic controlled simulator evidence against an independent reference semantics; not a public-task accuracy claim | 6 评估 / RQ1 |

## RQ2：测试方法是否揭示目标缺陷？

| 可使用 claim | 正文可使用数字 | artifact | 统计单位 | claim boundary | 预定章节 |
| --- | --- | --- | --- | --- | --- |
| `causal_schedule_vs_random` | causal 400 case、0.875 detection rate；random 4000 case、0.75 detection rate | `results/final_controlled/results/schedule_baseline.json` | schedule case | schedule detection in the controlled simulator; random baseline consists of ten seeded schedules per instance | 6 评估 / RQ2 |
| `minimal_mutant_witnesses_4` | 400 source instance、4 mutant、4 prefix-minimal witness | `results/final_controlled/results/minimal_mutant_witnesses.json` | mutant、witness | one deterministic operation-prefix-minimal witness per major mutant; minimality is with respect to suffix removal | 6 评估 / RQ2 |

## RQ3：真实模型和公开 runtime 能否接入？

| 可使用 claim | 正文可使用数字 | artifact | 统计单位 | claim boundary | 预定章节 |
| --- | --- | --- | --- | --- | --- |
| `native_qwen_repetitions_5x10` | 5 repetition、50 task、110 native event、0 evaluation error、50 contract success、50 oracle match | `results/remaining_tasks/native_repetitions5/repetition_report.json` | repetition、task episode、native event | mechanism-level model tool-loop and differential-oracle evidence; not end-user task success or production quality | 6 评估 / RQ3 |
| `tau_bench_native_50` | 50 task、50 evaluator-available task、497 native event、15.0 reward sum、0.3 reward mean | `results/submission_evidence/tau_bench_50/aggregate.json` | task、native event、official reward | official τ-bench workflow reward with native memory events; reward is not memory accuracy | 6 评估 / RQ3 |
| `appworld_prompt_profile_pair` | 20 paired task、0/1 official success、17/53 of 112 official assertions、13 improved、0 regressed、517564/2171632 observed tokens | `results/prompt_profile_formal_v4/appworld_prompt_comparison.json` | paired task、official assertion、observed token | descriptive 20-task paired result; tuned token total is an observed lower bound and no population-significance claim is made | 6 评估 / 外部有效性 |
| `locomo_prompt_profile_repetitions` | 3 paired repetition、1986 question per repetition、0.0016169580043333333 mean F1 delta、0.001921336312006298 delta std、40539 token delta | `results/prompt_profile_formal_v4/locomo_prompt_comparison.json` | repetition、question、token | three fixed paired repetitions; descriptive effect only, with no population-significance or universal-improvement claim | 6 评估 / 外部有效性 |

## RQ4：真实服务故障下记录了哪些路径？

| 可使用 claim | 正文可使用数字 | artifact | 统计单位 | claim boundary | 预定章节 |
| --- | --- | --- | --- | --- | --- |
| `toxiproxy_fault_matrix_5x30` | 5 scenario、30 repetition per scenario；四个 non-normal 场景的 trigger/toxic/proxy-path 均为 30/30，并记录客户端 success/error/abort/retry 响应 | `results/submission_evidence/toxiproxy_faults_30/aggregate.json` | fault scenario、repetition | single-host proxy/fault-response observations; post-fault Qdrant/Neo4j persistent state was not independently verified; not atomicity/availability/latency evidence | 6 评估 / RQ4 |
| `qwen_vector_graph_e2e_5` | 5 task、5 completed、30 native event；mean 18851.635056734085 ms、P50 15496.98719382286 ms | `results/submission_evidence/qwen_vector_graph_e2e_5/aggregate.json` | task、native event、end-to-end latency | single-host five-task end-to-end smoke including model, services, and evaluator; not production latency | 6 评估 / RQ4 |
| `cross_host_model_load_v8` | 3 repetition、204 completed cycle、1632 attempt/contract success、3251506 token | `results/cross_host_model_load_formal_v8_aggregate/results/model_load_repetition_summary.json` | attested repetition、cycle、attempt、token | three independently attested Agent-client-to-model-server repetitions; not multi-host Agent workers, one continuous 30-minute tunnel, or production latency | 6 评估 / RQ4 |

## RQ5：synthetic workload 与 trace-grounded holdout 是否匹配？

| 可使用 claim | 正文可使用数字 | artifact | 统计单位 | claim boundary | 预定章节 |
| --- | --- | --- | --- | --- | --- |
| `joint_realism_tau` | 400 synthetic、34 trace、MMD statistic 0.18197906075782866、p=0.001、999 permutation | `results/joint_realism/tau_bench/results/trace_realism.json` | synthetic instance、held-out trace、permutation | joint-distribution diagnostic on a held-out trace split; rejection indicates mismatch and does not establish equivalence | 6 评估 / RQ5 |
| `joint_realism_locomo` | 400 synthetic、2 trace、MMD statistic 1.9418861405587229、p=0.0005、1999 permutation | `results/joint_realism/locomo/results/trace_realism.json` | synthetic instance、held-out conversation、permutation | joint-distribution diagnostic with only two held-out conversations; low-power and not evidence of equivalence | 6 评估 / RQ5 |
| `appworld_projection_regeneration` | 5 task、380 event | `results/appworld_projection_regenerated/projection_inventory.json` | projected task、projection event | method/URL-only trace-grounded projection from official API calls; not native Agent memory ground truth | 6 评估 / RQ5 |
| `joint_realism_appworld` | 400 synthetic、2 trace、MMD statistic 1.2209670111356792、p=0.0005、1999 permutation | `results/appworld_projection_regenerated/results/trace_realism.json` | synthetic instance、held-out projected task、permutation | joint-distribution diagnostic over a redacted projection with two held-out tasks; not native memory ground truth or distributional equivalence | 6 评估 / RQ5 |
