# 真实 vector/graph backend 与故障性能状态

更新时间：2026-08-05。

本轮已完成可测试的实现层：

- `VectorGraphMemoryBackend`：Qdrant HTTP + Neo4j Bolt，canonical event 只在两个存储都成功后提交。
- graph failure compensation、request idempotency、reopen/read、provenance edge、invalidation 和 health/metrics 接口。
- `ToxiproxyFaultController`：按 service、operation、request ordinal 触发 delay、timeout、connection drop。
- `backend-performance`：50/200/1000 events、p50/p95/p99、吞吐、错误/重试/partial-commit 统计；所有报告固定 `production_latency_claim: false`。
- fake-client、故障控制器、CLI smoke 已通过本地测试。

真实服务实验仍未形成。当前本地环境没有 Docker；远程 GPU 主机 SSH 检查被远端关闭，因此尚未启动 Qdrant、Neo4j、Toxiproxy，也没有产生真实网络/服务延迟结果。任何现有 deterministic serial replay timing 都不能替代本实验。

下一次远端运行必须先通过 `scripts/run_real_backend_smoke.sh` 的 health check、write/read/derive/reopen 和 zero partial-commit 验收，再运行故障矩阵与端到端模型比较。
