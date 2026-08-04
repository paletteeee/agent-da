# 官方 trace-grounded adaptation/replay 结果

本轮首次使用官方公开数据/运行环境做了实际 replay。为避免概念混淆，以下结果称为 **trace-grounded adaptation/replay**，不称为原生 memory 数据集：三个来源都没有直接提供 TxnMem 所需的 memory transaction、policy churn、failure schedule 或 provenance ground truth。

## 运行结果

| 来源 | 原始输入 | 适配 instance | canonical event | holdout | projection |
|---|---:|---:|---:|---:|---|
| τ-bench historical gpt-4o airline | 200 trials | 175 | 920 | 160/40 records | `tau_api_tool_call` + `tau_policy_guideline` |
| LoCoMo official `locomo10.json` | 10 conversations | 10 | 272 | 8/2 episodes | `locomo_session_summary` |
| AppWorld official train reference-solution logs | 5 tasks / 380 API calls | 5 | 380 | 235/145 API records | `appworld_api_call` |

对应产物：

- `results/official_trace_runs/tau_bench/`
- `results/official_trace_runs/locomo/`
- `results/official_trace_runs/appworld/`
- `results/performance.json`

三组 `trace_realism.json` 都报告 `trace_grounded_status: trace_supplied`。holdout 按 task/trial/sample episode 划分，没有把同一任务的记录拆到 train 和 holdout 两侧。

由于三类公开日志没有 TxnMem transaction boundary，适配层为没有显式
`begin_txn/commit` 的 episode 加入一个明确的 episode-level replay envelope。
因此 replay engine 实际执行的 operation 数分别为 1,270、292 和 390；上表的
canonical event 数仍只统计来源日志投影出的事件，不把这两个适配操作冒充为
来源数据。带 envelope 后，完整 `TxnMem` variant 在三组 `trace_replay.csv`
中均与独立 reference executor 匹配；AppWorld 的 Naive/NoTxn 由于把同一
transaction 内的写入提前暴露给后续读取而出现预期的 `visible_memory_ids`
差异。这是 replay 语义的 sanity check，不是三套 benchmark 的原生正确率。

## 解释边界

- τ-bench 的历史数据是 tool/API trajectory；本项目将显式工具调用投影为 workflow memory read/search/write，并保留 projection 标记。
- LoCoMo 的 session summary 是对话记忆材料，不是 Agent backend 写入日志；本项目将每个 session summary 投影为 temporal memory write。
- AppWorld 日志来自官方 train task 的 reference solution 执行，验证了 AppWorld 环境和 API log 入口，但不是独立 LLM Agent 采样；原始 API `data` 中可能包含凭据的字段没有写入仓库产物。
- 这些 projection 只用于校准 operation count、transaction size 和 temporal update 形态，不能作为 policy/provenance ground truth。

仓库另提供 `examples/native_memory_agent.py` 作为 connector contract 示例：它通过
`InstrumentedMemoryBackend` 实际调用 write/read/derive，并由 canonical validator
检查事件。该 fixture 用于证明 provenance 来源可以在产生事件时记录；它与本节的
公开数据 projection 分开，也不把公开数据改称为 native memory ground truth。

对于真实模型实验，仓库现在提供 `txnmem_model_protocol.py`、
`txnmem_real_agent.py`、`txnmem_real_experiment.py` 和
`txnmem_failure_controller.py`：它们可以连接 OpenAI-compatible GPU endpoint，
运行结构化 memory tool loop，按 event trigger 注入 crash/policy revoke/invalidate，
再由独立 reference executor 评测。LoCoMo 另外配置了
`locomo_agent_runtime` contextual wrapper；远程 Qwen2.5-7B smoke 结果保存在
`/data/txnmem/results/locomo_native_smoke_final2/`，产生 11 个
`real_model_native` events、完成 1/1 task，且 independent oracle match。该结果是
单样本环境验证，不是 LoCoMo QA 正确率，也不能把本节三组 projection replay 改称
为原生 memory 数据集。

## 数据来源

- τ-bench historical trajectories：<https://github.com/sierra-research/tau-bench>
- LoCoMo official data：<https://github.com/snap-research/locomo>
- AppWorld official package/data：<https://github.com/StonyBrookNLP/appworld>

原始数据保存在本地 `external_data/` 或临时目录，已加入 `.gitignore`，不随仓库提交。
