# TxnMem 原剩余实验任务完成状态报告

更新时间：2026-08-10。本文按原剩余五项任务记录状态；所有证据路径均相对仓库根目录。模型为 Qwen2.5-7B-Instruct，本文不报告原始 prompt、tool payload、SSH 身份或主机名。

## 1. LoCoMo paired baseline/tuned repetitions — DONE_WITH_CONCERNS

在相同 10 个 conversation、每次 1,986 个问题、seeds `[17, 1017, 2017]` 与固定 condition fingerprint 下，baseline/tuned 各完成 3 次 paired repetition。baseline mean F1 为 `0.13836`，tuned 为 `0.13998`，delta 为 `+0.00162`；tuned tokens 比 baseline 精确增加 `40,539`。这是描述性配对结果；增益很小且一次 repetition 回退，不作统计显著性或普适改进结论。

证据：`results/prompt_profile_formal_v4/locomo_baseline/locomo_paired_repetition_summary.json`、`results/prompt_profile_formal_v4/locomo_tuned/locomo_paired_repetition_summary.json`、`results/prompt_profile_formal_v4/locomo_prompt_comparison.json`。

## 2. AppWorld baseline/tuned 配对 — DONE_WITH_CONCERNS

同一 20-task manifest、模型 revision、generation 参数、官方 evaluator、`instruction_inferred` 工具策略与逐任务工具集合 attestation 下完成配对。baseline 为 0/20 official success、17/112 assertions、517,564 exact tokens；tuned 为 1/20、53/112、2,171,632 observed token lower bound，且有 6 个 execution failures。该比较不能被表述为 TxnMem memory accuracy 或总体显著改进。

证据：`results/prompt_profile_formal_v4/appworld_baseline/native_batch_summary.json`、`results/prompt_profile_formal_v4/appworld_tuned/native_batch_summary.json`、`results/prompt_profile_formal_v4/appworld_prompt_comparison.json`。

## 3. τ/LoCoMo/AppWorld joint realism — DONE_WITH_CONCERNS

已完成 400 synthetic instances 的六维 standardized RBF random-feature MMD permutation test，并按 episode 分隔 calibration/holdout。结果显示各 trace 与当前 synthetic generator 存在分布差异，不能主张等价性；LoCoMo 与 AppWorld 的 holdout 均为 2，推断低功效。AppWorld 的输入是从官方 API-call provenance 生成的 method/URL-only 脱敏 projection，不是原生 memory ground truth。

证据：`results/joint_realism/tau_bench/results/trace_realism.json`、`results/joint_realism/locomo/results/trace_realism.json`、`results/appworld_projection_regenerated/projection_inventory.json`、`results/appworld_projection_regenerated/results/trace_realism.json`。

## 4. Attested cross-host model load — DONE_WITH_CONCERNS

Qwen2.5-7B-Instruct（revision `7b44…26b4`，vLLM `0.8.5.post1`）完成 3 次 independently attested `cross_host_client_server` repetition。每次 68 cycles、544 attempts，elapsed 为 `605.798544333`、`604.362563375`、`605.420804708` 秒；合计 204 cycles、1,632 attempts、`1,815.581912416` 秒。三次 UTC intervals non-overlapping、distinct tunnel process count=3；configured concurrency=4，observed peak=4。

总 contract success 为 1,632/1,632；completed attempts 1,224；408 个 runner-level failures 都是预期 workload 机制（204 `injected_crash`、204 `policy_denied`），不得算作模型错误。三份 endpoint/transport analysis 合计 0 相关失败。endpoint 精确报告 request/usage=3,672/3,672，prompt=2,935,706、completion=315,828、total=3,251,534 tokens。

拓扑为 1 Agent-worker host + 1 model-server host，ControlMaster same-session/PID binding 已验证且 host identities distinct，`cross_host_network_claim=true`。边界：`production_latency_claim=false`、`cross_host_multi_agent_workers_claim=false`、`single_continuous_tunnel_claim=false`；未覆盖生产级多主机 Agent workers、连续 30 分钟 tunnel 或跨主机 Qdrant/Neo4j。没有显式 pricing rate，货币成本未计算。

初始 v6 三次的模型、usage 与 topology 工件无错，但 strict aggregator 因 UTC 与 `perf_counter` 不一致而拒绝；根因是 macOS idle sleep 期间 `mach_absolute_time` 暂停。经 `caffeinate` 重跑后，v7 clock diff 为 `0.000663`、`0.000359`、`0.007711` 秒，均远低于 1% tolerance；v6 不作为正式结果。

证据：`results/cross_host_model_load_formal_v7_aggregate/results/model_load_repetition_summary.json`、`results/cross_host_model_load_formal_v7_rep1/results/model_load_summary.json`、`results/cross_host_model_load_formal_v7_rep2/results/model_load_summary.json`、`results/cross_host_model_load_formal_v7_rep3/results/model_load_summary.json`、`results/cross_host_model_load_formal_v7_rep1/results/endpoint_transport_failure_analysis.json`、`results/cross_host_model_load_formal_v7_rep2/results/endpoint_transport_failure_analysis.json`、`results/cross_host_model_load_formal_v7_rep3/results/endpoint_transport_failure_analysis.json`。

## 5. Git remote push — BLOCKED

`git remote -v` 为空。没有用户提供的 remote URL，不能安全添加 remote 或 push；这是当前唯一明确的外部阻塞。

## 验证与自审

- fresh full test：242 tests，3 skipped，0 failures。
- 对 7 个 v7 JSON 运行 `python3 -m json.tool` 均成功。
- artifact audit：0 findings。
- 最近代码提交为 `15b8e69`；v7 results 仍未提交，本文不声称其已在 Git 中。
- 未使用统计显著性或生产级结论；预期 `injected_crash`/`policy_denied` 未当作模型错误。
