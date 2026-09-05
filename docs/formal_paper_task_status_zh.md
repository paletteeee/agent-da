# TxnMem 正式论文任务状态

更新时间：2026-09-05。以下状态区分“代码/实验接口已完成”“已完成正式 batch”“仍不足以支撑更宽结论”，避免把五场景状态回读误写成一般分布式事务保证。

| 序号 | 正式任务 | 状态 | 已完成证据 | 仍需补强 |
|---:|---|---|---|---|
| 1 | 独立 reference semantics / ground truth | 已完成 | `src/txnmem_reference.py`、differential evaluator、相关单元测试；oracle 不由 TxnMem 生成；正式口径为 400 instances、2,000 variant rows，TxnMem 0/400 违规且 400/400 oracle match | 增加更大规模独立实现交叉核对 |
| 2 | 因果 failure schedule、coverage、最小反例 | 已完成 | trigger-based schedule、schedule baseline、coverage JSON；四类 mutant 均有可重放的 operation-prefix-minimal witness，去掉最后一步后不再复现目标违规 | 在真实多 Agent workflow 上扩大 schedule 组合 |
| 3 | 语义生成 provenance graph | 已完成（含真实服务 smoke） | derive/read/write/propagate 事件 contract、chain/branch/merge/supersession workload 与 repair matrix；SQLite 与 `VectorGraphMemoryBackend`；真实 Qdrant 1.11.5/Neo4j 5.22.0 direct service smoke | 尚非生产级跨主机部署；需要扩大真实多 Agent 事件日志 |
| 4 | mutation testing 与 differential oracle | 已完成 | NoTxn、NoPolicyCommit、NoRepair、scope bypass 等 mutant/ablation 及 kill/evaluation 报告；`partial_commit`、`remove_commit_revalidation`、`disable_provenance_traversal`、`bypass_scope_check` 四个最小 witness 已固化 | 对真实 backend 注入更丰富实现缺陷 |
| 5 | synthetic 与公开 trace 的 realism/holdout 分析 | 已完成（含高维 joint test） | 400 synthetic instances；按 episode 拆分 calibration/holdout；六维 standardized RBF random-feature MMD permutation test；τ-bench 141/34、LoCoMo 8/2；AppWorld 从 5 个官方 task 重新生成 380 条 method/URL-only 脱敏 projection events 并按 3/2 拆分 | LoCoMo/AppWorld holdout 均只有 2 个 episode，joint-test 低功效；结果显示分布差异，不支持等价性 |
| 6 | Qwen2.5-7B 真实模型实验 | 已完成（机制层、真实 backend E2E 与跨主机 client-to-server 负载） | GPU endpoint；5×10 repetition；官方 runtime native batch；5 个 τ-bench + Qdrant/Neo4j E2E 为 5/5 completed、30 events、mean 18,851.6 ms、P50 15,497.0 ms；v8 完成 3 次独立 attested cross-host 运行，合计 204 cycles、1,632/1,632 contract success、3,251,506 endpoint-reported tokens，模型 revision `7b44…26b4`、vLLM `0.8.5.post1` | 非生产 latency 结论；无显式 pricing rate，货币成本未计算；不是多主机 Agent workers |
| 7 | 并发、跨进程、协议故障 | 已完成（单机五场景状态核验） | 4 类确定性 protocol schedule、5/5 invariant coverage；真实 Qdrant/Neo4j/Toxiproxy state-verified 5×30 已完成，逐 repetition 回读双存储并 fail-closed 分类 complete/absent/partial/unknown | 仅覆盖被测 workload 和五个单机场景；不是一般分布式事务或跨主机容错证据 |
| 8 | τ-bench native runtime | 已完成（50-task batch + retry 合并） | official runtime；50 个唯一 task ID；最终合并 497 events、50/50 evaluator available、reward sum 15、mean 0.3000；manifest/hash、模型/runtime identity、retry 归并、source artifact hash 与运行命令均已固化 | 官方 reward 非 accuracy，需避免将失败 episode 写成任务成功或 memory 结论 |
| 9 | AppWorld native runtime | 已完成（20-task baseline/tuned 配对） | 同 manifest/condition/tool attestation；baseline 0/20 success、17/112 assertions、517,564 exact tokens；tuned 1/20、53/112，13 task 改善、7 不变；20/20 evaluator available | tuned 有 4 个 unauthorized-tool 和 2 个 model-HTTP execution failure；2,171,632 observed tokens 为下界；n=20 不支持总体显著性声称 |
| 10 | LoCoMo executable Agent | 已完成（10-conversation native + paired official QA + 3 repetitions） | Qwen2.5-7B native contextual batch；paired QA baseline/tuned 各 3 次、每次 1,986 问题、相同 seeds/condition；baseline mean F1=0.13836，tuned=0.13998，平均差值 +0.00162；token 精确增加 40,539 | 仅 3 次描述性 paired repetition，增益很小且一次回退，不能声称总体显著提升 |
| 11 | 论文初稿与结果同步 | 已同步 state-verified 与 provenance performance 结果 | 正文、7 图、9 表、32 条参考文献和确定性 DOCX 构建链均保留窄 claim boundary；图 7 同时呈现节点规模 × 并发的吞吐、95% CI 与 p99 尾延迟 | 选定 venue 后模板适配和正常作者修订 |
| 12 | Claim ledger 与历史结果作废 | 已完成 | 17 条 active claim、176 个字段断言；Toxiproxy 与 provenance performance 均绑定严格 limitation；claim/artifact/manuscript audit 均要求 current bytes | 新增论文数字时必须同步更新 ledger 并重跑 fail-closed audit |

## State-verified backend 证据摘要

正式结果为 5 个场景 × 30 次重复 = 150 次观测：完整回读 90/90、缺失回读 60/60、`partial` 0/150、`unknown` 0/150，且 `retry_success` 30/30。每次重复在操作后针对两个唯一 memory ID 分别读取 Qdrant 与 Neo4j；严格聚合器从原始 repetition evidence 重新计算 state/proxy/response facts，不信任 runner summary flags。

正式 artifact 为 `results/submission_evidence/toxiproxy_state_verified_30/aggregate.json`，SHA-256 为 `04de2a3c7da3b8c2dcda06d88afdb18e6f224d8f0e1fcaae4847f1277b3bbcad`，source raw SHA-256 为 `2ec4db6df575a61b8c203777e0c552bc061e9af44f6f1c8cfb3ec719a3800220`。backend-only 两事件诊断为 p50 25.748 ms、p95 32.029 ms、p99 42.234 ms、吞吐 76.256 operations/s，且 `production_latency_claim=false`。

精确 claim boundary 为：single-host real Qdrant/Neo4j with deterministic Toxiproxy fault injection and post-operation readback for the tested workload and five scenarios; not general distributed transactions, cross-host fault tolerance, availability, linearizability, or production latency。换言之，被测补偿与故障路径在这五个场景中得到 readback-confirmed complete-or-absent 结果；TxnMem 不是一般分布式事务协议，该证据不支持跨主机容错、可用性、线性一致性或生产延迟结论。

## 论文目前可以正式声称的内容

1. TxnMemBench 的 400-instance/2,000-row controlled correctness suite、独立 oracle、failure coverage、四类最小 mutant witness、mutation/differential evaluation 和 distributed protocol smoke 已闭环。
2. Qwen2.5-7B 已验证真实模型 tool loop、事件 contract、failure injection 和 reference differential evaluation；50 个 task episode 的结果可作为机制层证据。
3. τ-bench、AppWorld、LoCoMo 的官方 runtime 边界已可执行，并已在 Qwen2.5-7B 上接入 per-task SQLite memory backend；LoCoMo 已完成同条件 paired repetitions，AppWorld 已完成工具集合逐任务证明一致的 baseline/tuned 配对，joint realism 已完成并显示分布差异。当前只能写成 native workflow/runtime evidence 或 trace-grounded adaptation/replay，不能写成三套公开 benchmark 的原生 memory accuracy。
4. Qwen2.5-7B v8 在修复监听器归属与 ControlMaster 连续性校验后完成 3 次 independently attested cross-host client-to-model-server repetition；合计 1,632/1,632 contract success，408 个 runner-level failures 均为预期 `injected_crash`/`policy_denied` workload 机制，3 份 endpoint/transport analysis 均为 0 相关失败。v7 仅作为修复前审计历史保留，不承担最终结论。
5. 单机状态核验矩阵支持五个被测场景的 proxy/response/readback 结果；它不证明一般 cross-service transaction，不能据此主张跨主机 fault tolerance、availability、linearizability 或 production latency。5-task E2E 仍只作为单机闭环 smoke。

完整汇总报告见 `docs/current_experiment_report_zh.md`。

## 已完成结果的限制与 future work

- τ-bench 50-task 主 batch + 2 个网络错误 task retry 已完成最终合并：50/50 evaluator available，497 native events；2 max-step、1 no-events、1 retry 后 no-events，不能把这些 episode 写成任务成功。
- 单机 Qdrant/Neo4j/Toxiproxy 5×30 已完成状态核验；其边界仍限定为被测 workload 与五场景的 complete-or-absent 回读，不外推到通用跨服务事务语义。
- LoCoMo paired baseline/tuned 各 3 次 repetition 已完成，但平均 F1 只增加 0.00162 且一次回退；这是描述性证据，不是总体显著性结论。直接全文上下文 QA 的 0.3222 F1 仍不能与不同条件的旧 paired run 直接归因比较。
- AppWorld tuned 将官方 success 从 0/20 提至 1/20、断言从 17/112 提至 53/112，但有 6 个 execution failure，token usage 也有 2 次响应缺失；必须按全 20 task 分母报告，并将 tuned token 总量标为观测下界。
- joint realism 已完成，但 τ/LoCoMo/AppWorld 的结果显示分布差异；LoCoMo/AppWorld holdout 均为 2，不支持分布等价性或强推断。
- v8 的跨主机范围为 1 Agent-worker host + 1 model-server host、3 条独立 tunnel；生产级多主机 Agent workers、单一连续 30 分钟 tunnel、跨主机 Qdrant/Neo4j 和有明确定价率的成本核算均为 future work/claim boundary，不能伪装成已完成。
- Claim audit 当前口径为 17 条正式主张和 176 个字段断言；`results/remaining_tasks/final_status.json`、`results/remaining_tasks/production_evidence_status.json` 和旧 backend fault artifact 仅保留作历史审计，不得再被正文引用为当前状态。
- 本分支已闭环 state-verified backend 实验；venue 模板适配与正常作者修订仍待完成。

## Manuscript readiness blockers

1. 选定 venue 后完成匿名与版式模板适配。
2. 完成正常作者修订。

## Repository operations note

仓库未配置 remote，且本任务未执行 push；这是仓库操作状态，不属于 manuscript readiness blocker。
