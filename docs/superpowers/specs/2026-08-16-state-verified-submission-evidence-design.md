# State-verified real-backend submission evidence design

日期：2026-08-16  
状态：用户已批准推荐方案

## 1. 目标与边界

本设计把真实 Qdrant、Neo4j 与 Toxiproxy 上完成的五场景、每场景 30 次实验升级为论文的主要 real-backend fault evidence。新证据必须证明：每次操作结束后，两个持久化后端均被重新读取，结果只能是完整提交或完整缺失，不能出现 partial 或 unknown。

该证据只支持已测试 workload 和故障触发点上的双后端原子结果，不外推为通用分布式事务、线性一致性、跨主机容错、生产可用性或生产延迟证据。性能数字继续标记 `production_latency_claim=false`。

## 2. 方案选择

采用“新证据成为唯一主证据，旧证据保留但明确 superseded”的方案。

- 采用方案：创建新的 state-verified aggregate；旧 proxy-only aggregate、原始 source summary 和提交历史不删除，通过 `SUPERSEDED.md` 与 supersession index 指向新证据。
- 不采用原地覆盖：原地修改旧 aggregate 会破坏审计链，无法解释旧论文文本和历史提交为何只声明 fault-path observation。
- 不采用两份同等有效证据并列：这会让 claim audit 和论文作者难以判断哪个边界具有权威性，并可能继续误用旧的“未核验持久状态”结论。

## 3. 工件布局

正式工件采用以下布局：

- 原始正式结果：`results/real_backend_faults_state_verified_30_v2/results/backend_performance.json`
- 运行环境证明：`results/submission_evidence/toxiproxy_state_verified_30/environment_attestation.json`
- 新聚合结果：`results/submission_evidence/toxiproxy_state_verified_30/aggregate.json`
- 旧证据作废说明：`results/submission_evidence/toxiproxy_faults_30/SUPERSEDED.md`
- 全局映射：`results/paper_evidence/supersession_index.json`

远端 `run.log` 和 PID 只用于运行诊断，不进入论文证据包。退出码、正式结果 SHA-256、运行命令、源码提交和镜像 digest 进入环境证明。

## 4. 输入验证契约

聚合器对输入 fail closed，并重新计算结论，不直接信任顶层布尔值。

### 4.1 基础条件

- `backend` 必须为 `vector-graph`。
- Qdrant 和 Neo4j 的 healthcheck 必须可用且包含版本。
- fault matrix 必须且只能包含 `normal`、`delay`、`timeout`、`connection_drop`、`retry_success` 五个场景。
- 每个场景必须正好有 30 个 repetition，且 repetition evidence、fault evidence、proxy-path evidence 和状态核验记录的数量一致。
- 触发、toxic 安装/清理、fault observation、成功、失败、回滚和重试计数必须由逐 repetition 记录重新计算并与汇总字段相符。

### 4.2 持久状态条件

聚合器从唯一的 performance workload row 读取正整数 `workload_events`。每个 repetition 的 `persistent_state_verifications` 必须满足：

- 包含恰好 `workload_events` 个唯一 memory ID；
- 每个 memory 均包含 Qdrant 与 Neo4j 的独立读取结果，且两者 `read_ok=true`；
- repetition 分类和所有 item 分类一致；
- `normal`、`delay`、`retry_success` 必须分类为 `complete`，两个后端均 `present=true` 且 `matches=true`；
- `timeout`、`connection_drop` 必须分类为 `absent`，两个后端均 `present=false`；
- 任意 `partial`、`unknown`、读取失败、后端不一致、重复 memory ID 或计数不一致均拒绝聚合。

聚合器重新计算各场景的 complete/absent/partial/unknown 计数，并要求总数为 30、partial 为 0、unknown 为 0。输入中的 `all_scenarios_state_verified` 和 `all_observed_states_consistent` 仍须为 true，但不能代替上述重算。

### 4.3 环境证明条件

环境证明必须包含并验证：

- 执行范围为 single-host real services；
- 运行退出码为 0；
- 正式结果 SHA-256 与实际输入文件一致；
- 完整的 40 位 Git `source_commit`，本次运行对应已固定提交 `33a334dc7c4e6d2e0250bb54cd25f0e2f080ed5d`；
- 脱敏运行命令，不含 password、token、secret 或 credential；
- Python、Docker、Docker Compose 和内核版本；
- Qdrant、Neo4j、Toxiproxy 的服务版本、固定 tag、image ID/content digest、实际拉取来源；
- 容器端口边界：Qdrant 6333 和 Neo4j 7687 不直接发布到主机，客户端数据路径经过 Toxiproxy；
- 环境证明采集时间及主机身份哈希，不保存 SSH 密码。

服务版本必须同时与结果文件的 backend health 匹配。digest、source commit 或结果 hash 不匹配时必须拒绝聚合。

## 5. 聚合输出

新输出使用独立 evidence ID `toxiproxy_state_verified_30`，状态为 `complete_state_verified_fault_observations`，schema version 升为 2。输出至少包含：

- 五场景、每场景 30 次和总计 150 次；
- proxy/fault/trigger/toxic 逐场景计数；
- complete/absent/partial/unknown 逐场景计数；
- `all_scenarios_evidence_valid=true`；
- `all_scenarios_state_verified=true`；
- `all_observed_states_consistent=true`；
- 后端版本、Toxiproxy 版本和三个 image digest；
- source commit、运行命令、环境证明 hash、原始结果路径与 hash；
- 明确的 claim boundary 和 `production_latency_claim=false`。

聚合文件保持紧凑，不复制 150 份逐 repetition 明细；逐次证据保留在原始正式结果中并通过 SHA-256 绑定。

## 6. Supersession 与论文声明

旧 `toxiproxy_fault_path_30` 证据继续保留历史内容，但标记为 superseded，不再作为当前论文主张来源。claim audit 应将旧 evidence ID 用于 post-fault state、atomic outcome 或 rollback consistency 声明视为错误。

论文可声明：在五个预定义场景、每场景 30 次、每次两个 memory event 的单机真实服务实验中，150/150 次 post-operation readback 得到完整提交或完整缺失，未观察到 partial 或 unknown；这是一项受控场景证据。

论文不得声明：TxnMem 已证明任意并发/崩溃下的通用原子性、跨主机容错、生产 SLA，或该单机 backend-only 延迟可代表生产系统。

## 7. 数据流与失败处理

数据流为：远端正式结果与退出码 → 环境证明及 hash 绑定 → 严格聚合器重算 → supersession/claim audit → Markdown 与 DOCX 论文表格。

任一输入缺失或不一致时，聚合命令非零退出且不生成“完成”状态的 aggregate。已有有效 aggregate 不应被失败运行静默覆盖；生成过程先写临时文件，验证成功后再替换目标。

## 8. 测试策略

实现采用 TDD，至少覆盖：

- 真实结构的正向 5×30 fixture；
- 缺少顶层状态标志；
- repetition 数、状态核验数或 workload item 数不匹配；
- partial、unknown、读取失败、双后端存在性/内容不一致；
- 场景期望状态错误，例如 timeout 被分类为 complete；
- proxy、trigger、toxic 或 retry 计数不一致；
- 结果 hash、source commit、image digest、服务版本或退出码不匹配；
- 运行命令含敏感字段；
- superseded 证据被用于新状态主张；
- 新 aggregate、claim audit、manuscript audit 和完整测试套件的端到端验证。

## 9. 实施顺序

1. 扩展聚合器及 CLI 的 environment-attestation 参数和严格状态验证。
2. 生成并验证环境证明及新 aggregate。
3. 标记旧证据 superseded，更新全局 supersession index 与 claim audit。
4. 更新中文论文 Markdown、实验报告、表格和证据路径。
5. 重新生成 DOCX，渲染 PNG，完成逐页视觉 QA。
6. 运行本地与干净归档测试、论文审计和 artifact hash 检查后提交 Git。
