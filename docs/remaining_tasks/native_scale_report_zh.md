# 公开 benchmark 大规模 native sampling 状态

更新时间：2026-08-05。

本轮已完成可复现执行层：

- `build_native_scale_manifest()` 固定 seed、task-level split、source hash 和 manifest hash；推荐主样本为 τ-bench 50、AppWorld 20、LoCoMo 10。
- `benchmark-native-batch` 保留 official evaluator、native event contract 和 TxnMem reference oracle 三个独立字段。
- official evaluator 缺失时返回 `blocked`，不会把 oracle match 计为官方成功。
- `scripts/run_native_scale.sh` 和 `scripts/run_remote_evidence.sh` 提供远端 preflight、批量运行与脱敏 aggregate 入口。

正式大规模结果仍未形成。本地环境缺少 `tau_bench`、`appworld` 和 LoCoMo QA evaluator；本轮对配置的远程 GPU 主机执行 SSH 检查时连接被远端关闭，因此没有启动 50/20/10 primary batch。已有 τ-bench/AppWorld/LoCoMo native smoke 和 SQLite-backed smoke 继续作为独立的小规模证据，不升级为正式 benchmark accuracy。

正式运行完成后必须至少提交：task/conversation-level official score、95% 区间、native event contract、独立 oracle、failure classification、manifest/runtime hash，以及 `raw_reports_committed: false`。
