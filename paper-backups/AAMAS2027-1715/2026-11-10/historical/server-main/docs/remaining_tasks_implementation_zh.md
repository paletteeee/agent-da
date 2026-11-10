# 八项补强的实现状态

本轮把八项补强拆成了可以在当前仓库内独立验证的代码接口，并将外部数据/运行环境依赖显式保留。

| 项目 | 当前实现 | 证据/边界 |
|---|---|---|
| 1. τ-bench/AppWorld/LoCoMo trace-grounded replay | `txnmem_trace_pipeline.py`，支持 JSON/JSONL、按 episode 构造 instance、重放和 realism 输出；已实际运行三类官方公开输入 | 已完成 workflow/API projection smoke replay：τ-bench 175 instance/920 event，LoCoMo 10/272，AppWorld 5/380；不是原生 memory ground truth |
| 2. benchmark-specific adapter | `txnmem_bench_adapters.py`：`tau-bench`、`appworld`、`locomo`、`normalized` | 只转换显式 tool/API/memory event；普通对话会被跳过 |
| 3. 真实 Agent memory trace 采集 | `txnmem_model_protocol.py` 的 OpenAI-compatible client、`txnmem_real_agent.py` 的 structured tool loop、`txnmem_event_contract.py` validator、`examples/real_model_smoke.py` 入口 | 已完成可接入真实 endpoint 的采集框架和 offline fixture；尚未在 GPU 上运行真实模型，尚无真实 native trace 结果 |
| 4. trace 校准与 holdout | `txnmem_realism.py` 的 `calibrate_config`、`calibrated_suite`、`split_holdout` | 按完整 episode 划分，校准只影响 synthetic config，不改变 oracle |
| 5. 并发/micro-witness | `txnmem_interleavings.py` 穷举保持各 agent 局部顺序的所有 linearization；`txnmem_concurrency.py` 提供线程锁 harness；`txnmem_distributed.py` 和 `process-smoke` CLI 提供跨进程 owner-linearization smoke harness；`txnmem_failure_controller.py` 提供 native trigger-based injection | 已完成小规模线程、跨进程 smoke 和真实 Agent 触发器；仍不等同于生产级跨进程/分布式事务 |
| 6. incremental repair failure | `txnmem_repair.py` 的 `incremental_repair` 和 `repair_failure_matrix` | 明确定义 crash-after-k repair steps，并报告 unsafe active descendants |
| 7. backend/Agent workflow | `AgentReplayRunner` 与 `run_real_agent` 接受可替换 model/backend；`real_model_tasks.json` 固定 task、seed、temperature 和 failure schedule | 真实 endpoint 的 tool calling、task evaluator 和 native trace 采集尚未实际运行；offline fixture 不冒充线上 LLM 实验 |
| 8. 论文同步 | 根目录脚本/初稿同步 controlled suite、三类官方 replay、local timing 和边界 | 已完成初稿同步；DOCX 仍需完成渲染条件允许时的视觉 QA |

## 建议的真实运行顺序

1. 已完成三类官方输入的 workflow/API projection replay，并完成 canonical event contract、真实 endpoint client、tool loop 和 native fixture；下一步是在 GPU endpoint 上采集真实 Agent memory trace。
2. 已保留非敏感的 `trace_replay.csv`、`trace_realism.json` 和 calibration/performance JSON；原始输入及含内容的 instance 文件不提交仓库。
3. 已按 task/trial/sample episode 做 holdout；下一步只用原生 memory train episodes 校准，再在 holdout 上报告分布差异。
4. 已完成小规模 interleaving、线程锁、跨进程 owner-linearization smoke harness 和 trigger-based controller；下一步是从真实 trace 生成每个 agent 的局部 operation sequence，并运行完整 linearization 检查。
5. 仍需在远程 GPU 上启动 OpenAI-compatible model endpoint，运行 smoke/pilot/holdout，收集 native event log，并对 backend event log 与 reference executor 做 differential comparison；还需补充分布式/跨进程故障与端到端性能。

## 当前真实模型实验入口

本地可先运行无模型依赖的协议 fixture：

```bash
python3 examples/real_model_smoke.py \
  --manifest configs/real_model_smoke.json \
  --offline-fixture \
  --out-dir results/real_model_fixture
```

真实 GPU endpoint 使用：

```bash
python3 examples/real_model_smoke.py \
  --manifest configs/real_model_tasks.json \
  --endpoint http://GPU_HOST:8000/v1 \
  --model MODEL_ID \
  --out-dir results/real_model_run
```

当前仓库只提交脱敏 aggregate summary；原始 prompt、tool arguments 和 native
event trace 保存在本地运行目录，不作为公开数据集提交。
