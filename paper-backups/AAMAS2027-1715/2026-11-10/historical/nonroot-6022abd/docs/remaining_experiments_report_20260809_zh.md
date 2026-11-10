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

Qwen2.5-7B-Instruct（revision `7b44…26b4`，vLLM `0.8.5.post1`）在最终 attestation 修复后完成 3 次 independently attested v8 `cross_host_client_server` repetition。每次 68 cycles、544 attempts，elapsed 为 `604.165115041`、`603.328782334`、`603.574593500` 秒；合计 204 cycles、1,632 attempts、`1,811.068490875` 秒。三次 UTC intervals non-overlapping、distinct tunnel process count=3；configured concurrency=4，observed peak=4。

总 contract success 为 1,632/1,632；completed attempts 1,224；408 个 runner-level failures 都是预期 workload 机制（204 `injected_crash`、204 `policy_denied`），不得算作模型错误。三份 endpoint/transport analysis 合计 0 相关失败。endpoint 精确报告 request/usage=3,672/3,672，prompt=2,935,703、completion=315,803、total=3,251,506 tokens。

拓扑为 1 Agent-worker host + 1 model-server host，每次均通过模型调用前 preflight 与结束时 final PID-owned listener 校验、ControlMaster PID pre/post check、严格 loopback forwarding 校验及拓扑连续性校验，host identities distinct，`cross_host_network_claim=true`。边界：`production_latency_claim=false`、`cross_host_multi_agent_workers_claim=false`、`single_continuous_tunnel_claim=false`；未覆盖生产级多主机 Agent workers、连续 30 分钟 tunnel 或跨主机 Qdrant/Neo4j。没有显式 pricing rate，货币成本未计算。

初始 v6 预正式运行因 UTC 与 `perf_counter` 的时间证据不一致而被 strict aggregator 拒绝，v6 不作为正式结果。v7 虽通过当时聚合规则，但最终安全审查发现监听器归属与连续性证明不足，因此只保留为修复前审计历史。代码提交 `4669a01` 完成 listener ownership、ControlMaster continuity、严格 forwarding/UTC 和固定 `3 × 600` 秒聚合约束后重跑 v8；三次 clock diff 为 `0.002323041`、`0.009159334`、`0.039579500` 秒，均低于 1% tolerance，UTC offset 均为 0。

证据：`results/cross_host_model_load_formal_v8_aggregate/results/model_load_repetition_summary.json`、`results/cross_host_model_load_formal_v8_rep1/results/model_load_summary.json`、`results/cross_host_model_load_formal_v8_rep2/results/model_load_summary.json`、`results/cross_host_model_load_formal_v8_rep3/results/model_load_summary.json`、`results/cross_host_model_load_formal_v8_rep1/results/endpoint_transport_failure_analysis.json`、`results/cross_host_model_load_formal_v8_rep2/results/endpoint_transport_failure_analysis.json`、`results/cross_host_model_load_formal_v8_rep3/results/endpoint_transport_failure_analysis.json`。v7 作废说明见 `results/cross_host_model_load_formal_v7_aggregate/SUPERSEDED.md`。

## 5. Git remote push — BLOCKED

`git remote -v` 为空。没有用户提供的 remote URL，不能安全添加 remote 或 push；这是当前唯一明确的外部阻塞。

## 验证与自审

- fresh full test：250 tests，3 skipped，0 failures。
- 对 7 个 v8 JSON 运行严格 JSON 解析均成功。
- artifact audit：0 findings。
- 最终 attestation 代码修复提交为 `4669a01`；脱敏 v8 aggregate、per-repetition summaries、endpoint/transport analyses 与 v7 作废标记已由本地结果提交 `9785a48` 保存。
- 未使用统计显著性或生产级结论；预期 `injected_crash`/`policy_denied` 未当作模型错误。
