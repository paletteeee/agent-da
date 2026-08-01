# 八项补强的实现状态

本轮把八项补强拆成了可以在当前仓库内独立验证的代码接口，并将外部数据/运行环境依赖显式保留。

| 项目 | 当前实现 | 证据/边界 |
|---|---|---|
| 1. τ-bench/AppWorld/LoCoMo trace-grounded replay | `txnmem_trace_pipeline.py`，支持 JSON/JSONL、按 episode 构造 instance、重放和 realism 输出 | 当前目录没有三套数据的原始 trace，因此未声称已完成真实 benchmark 运行 |
| 2. benchmark-specific adapter | `txnmem_bench_adapters.py`：`tau-bench`、`appworld`、`locomo`、`normalized` | 只转换显式 tool/API/memory event；普通对话会被跳过 |
| 3. 真实 Agent memory trace 采集 | `txnmem_backend.py`：`InstrumentedMemoryBackend`、`AgentReplayRunner`、`run_agent` 注入点 | 可接入实际 Agent/backend；仓库内没有可授权的真实 Agent 服务或运行凭据 |
| 4. trace 校准与 holdout | `txnmem_realism.py` 的 `calibrate_config`、`calibrated_suite`、`split_holdout` | 按完整 episode 划分，校准只影响 synthetic config，不改变 oracle |
| 5. 并发/micro-witness | `txnmem_interleavings.py` 穷举保持各 agent 局部顺序的所有 linearization | 这是小规模穷举串行化，不等同于真实多线程/分布式运行；真实运行仍需 backend harness |
| 6. incremental repair failure | `txnmem_repair.py` 的 `incremental_repair` 和 `repair_failure_matrix` | 明确定义 crash-after-k repair steps，并报告 unsafe active descendants |
| 7. backend/Agent workflow | `AgentReplayRunner` 接受 callable Agent policy 和可替换 backend | 当前自带 backend 是 deterministic instrumented fixture，不冒充线上 memory backend/LLM 实验 |
| 8. 论文同步 | 根目录脚本/初稿会同步新的代码、指标和上述边界 | 发布前仍应把真实三套 trace 的结果填入同一套表格 |

## 建议的真实运行顺序

1. 分别导出 τ-bench、AppWorld、LoCoMo 的 Agent trajectory/tool/API log，并让 Agent memory backend 输出本仓库的 canonical event contract。
2. 用 `trace-replay` 为每个数据集单独运行，保留 `trace_grounded_instances.jsonl`、`trace_replay.csv`、`trace_realism.json`。
3. 只用 train episodes 调用校准；holdout episodes 只用于最终报告，不能回流到 workload 参数。
4. 对小型真实 trace 生成每个 agent 的局部 operation sequence，再用 `micro_witness_report` 做完整 linearization 检查。
5. 使用真实 backend 运行 Agent，再对 backend event log 与 reference executor 做 differential comparison。
