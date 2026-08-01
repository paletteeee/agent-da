# 八项补强的实现状态

本轮把八项补强拆成了可以在当前仓库内独立验证的代码接口，并将外部数据/运行环境依赖显式保留。

| 项目 | 当前实现 | 证据/边界 |
|---|---|---|
| 1. τ-bench/AppWorld/LoCoMo trace-grounded replay | `txnmem_trace_pipeline.py`，支持 JSON/JSONL、按 episode 构造 instance、重放和 realism 输出；已实际运行三类官方公开输入 | 已完成 workflow/API projection smoke replay：τ-bench 175 instance/920 event，LoCoMo 10/272，AppWorld 5/380；不是原生 memory ground truth |
| 2. benchmark-specific adapter | `txnmem_bench_adapters.py`：`tau-bench`、`appworld`、`locomo`、`normalized` | 只转换显式 tool/API/memory event；普通对话会被跳过 |
| 3. 真实 Agent memory trace 采集 | `txnmem_backend.py`：`InstrumentedMemoryBackend`、`AgentReplayRunner`、`run_agent` 注入点；`txnmem_event_contract.py` 提供 canonical validator；`examples/native_memory_agent.py` 提供 native 调用示例 | 已完成 connector 边界和 deterministic fixture；实际 LLM/线上 memory backend 仍需接入，示例不等于真实 Agent trace |
| 4. trace 校准与 holdout | `txnmem_realism.py` 的 `calibrate_config`、`calibrated_suite`、`split_holdout` | 按完整 episode 划分，校准只影响 synthetic config，不改变 oracle |
| 5. 并发/micro-witness | `txnmem_interleavings.py` 穷举保持各 agent 局部顺序的所有 linearization；`txnmem_concurrency.py` 提供线程锁 harness；`txnmem_distributed.py` 和 `process-smoke` CLI 提供跨进程 owner-linearization smoke harness | 已完成小规模线程与跨进程 smoke harness；仍不等同于生产级跨进程/分布式事务 |
| 6. incremental repair failure | `txnmem_repair.py` 的 `incremental_repair` 和 `repair_failure_matrix` | 明确定义 crash-after-k repair steps，并报告 unsafe active descendants |
| 7. backend/Agent workflow | `AgentReplayRunner` 接受 callable Agent policy 和可替换 backend；native event contract 要求实际 derive/propagate 调用携带来源 ID | 当前自带 backend 与示例是 deterministic instrumented fixture，不冒充线上 memory backend/LLM 实验 |
| 8. 论文同步 | 根目录脚本/初稿同步 controlled suite、三类官方 replay、local timing 和边界 | 已完成初稿同步；DOCX 仍需完成渲染条件允许时的视觉 QA |

## 建议的真实运行顺序

1. 已完成三类官方输入的 workflow/API projection replay，并完成 canonical event contract/native fixture；下一步是让实际 Agent memory backend 输出该 contract。
2. 已保留非敏感的 `trace_replay.csv`、`trace_realism.json` 和 calibration/performance JSON；原始输入及含内容的 instance 文件不提交仓库。
3. 已按 task/trial/sample episode 做 holdout；下一步只用原生 memory train episodes 校准，再在 holdout 上报告分布差异。
4. 已完成小规模 interleaving、线程锁和跨进程 owner-linearization smoke harness；下一步是从真实 trace 生成每个 agent 的局部 operation sequence，并运行完整 linearization 检查。
5. 仍需使用真实 backend/LLM 运行 Agent，再对 backend event log 与 reference executor 做 differential comparison；还需补充分布式/跨进程故障与端到端性能。
