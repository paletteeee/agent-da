# TxnMem 正式性能实验 v6：可观测进度、快速失败与受控重启设计

## 状态与关系

- 状态：已批准采用推荐方案 A，待书面规范复核。
- 日期：2026-08-28。
- 适用范围：真实 Qdrant、Neo4j、Toxiproxy 上的 provenance backend performance 正式矩阵。
- 上位设计：`2026-08-18-evidence-scale-up-design.md` 与 `2026-08-23-formal-proxy-attribution-and-ingress-design.md`。
- 本文只修复运行可观测性、失败时机、Neo4j 超时和进程生命周期，不改变预注册的 `3 graph sizes × 5 concurrencies × 30 repetitions` 矩阵，也不将中止的 v5 数据升级为正式证据。

## 背景与已确认事实

v5 正式运行在最后一个矩阵 cell 长时间没有生成 completion。安全中断后的只读诊断证明：

- 前 14/15 个 cell 已在真实后端完成，共 420 个 repetition；
- 第 15 个 cell 是最大图规模与最高并发组合；
- 该 cell 有部分 repetition 留下后端状态，但没有形成连续、完整、可发布的结果；
- candidate 目录为空，因为当前实现把所有 cell report 保存在 runner 进程内存中，只在矩阵全部完成后一次性发布；
- runner 和 controller 在采样窗口内均处于睡眠状态，未观察到 CPU、读或写进展；
- collector 退出后，使用独立 session 启动的 runner 变成孤儿进程，网络 guard 也需要额外清理；
- 私有状态文件中的退出码不能替代 completion receipt，v5 没有 completion，因而不能 promotion。

上述证据支持“外部 I/O 等待加上失败过晚”的诊断，但不足以宣称数据库内部发生了经典死锁。实现层面的直接原因有四个：

1. `_Neo4jBoltClient` 没有消费已配置的 `request_timeout_seconds`，Neo4j 连接获取和事务/查询缺少有界超时；
2. protected candidate 以 diagnostic source 方式运行，而 `run_matrix_cell` 仅在 `formal=True` 时对不合格 repetition 立即失败，导致正式候选直到最终聚合才失败；
3. 现有 `progress_callback` 只更新进程内字典，没有可由 controller 安全读取的实时快照；
4. collector 以 `start_new_session=True` 启动 runner，却没有 parent-death signal 和完整的终止清理协议。

因此 v5 是一次中止的、不可恢复为正式结果的运行。它的原始材料和服务卷只保留用于诊断，不复用其 run identity、nonce、candidate 或部分结果。

## 目标

v6 必须实现以下目标：

1. controller 能在不读取数据库、原始日志、原始 payload 或 candidate 内容的前提下，精确看到已完成的 cell、repetition 和 sample 数；
2. 每完成一个 repetition 就持久化一份单调、脱敏、可校验的进度快照；
3. 任何不符合正式资格的 repetition 在首次出现时立即失败，不再等待整个矩阵结束；
4. Qdrant 和 Neo4j 的单次后端请求都具有同一冻结配置导出的有限超时；
5. collector 被终止、崩溃或父进程消失时，runner、controller 子进程和 nftables guard 都能在有界时间内清理；
6. 保持 candidate 的原子发布、promotion 与 tamper-evident 边界，不让运行进度文件污染候选树；
7. 使用全新 v6 commit、registration、run identity、nonce 和干净服务卷重跑完整矩阵，同时保留 v5 证据。

## 非目标

- 不从 v5 的 420 个后端状态恢复或拼接正式矩阵；
- 不通过查询数据库、解析原始日志或扫描 payload 推断进度；
- 不改变 graph size、concurrency、repetition、operation mix、seed 或统计口径；
- 不升级已固定的 Qdrant、Neo4j 或 Toxiproxy 镜像；
- 不把进度快照作为论文数值或 promotion 证据；
- 不把 30 秒请求超时解释为生产环境 SLA。

## 推荐架构

采用 collector-owned 单向进度管道。runner 只发送 canonical、脱敏事件，collector 独占验证与持久化权。数据流为：

```text
run_matrix_cell
    -> experiment 内部 progress callback
    -> runner canonical JSON line writer
    -> collector-owned one-way pipe
    -> collector validator/state machine
    -> root-owned atomic progress.json
```

不采用 runner 直接写 candidate、不采用数据库计数推断、不采用原始日志 tail。这些替代方案分别会污染证据边界、读取运行数据或缺少精确的完成语义。

## 组件边界

### `_GatedCandidate`

`txnmem_provenance_execution_collector.py` 中的 `_GatedCandidate` 除现有 gate 外，新增一对仅用于进度的 pipe file descriptor：

- read end 归 collector 所有，不传给 measured child；
- write end 以 `pass_fds` 精确传给 runner；
- runner 环境仅通过保留键 `TXNMEM_PROVENANCE_PROGRESS_FD` 得到十进制 FD 编号；
- collector 启动后立即关闭自己的 write end；runner exec 后关闭所有非 allowlist FD；
- pipe 是单向的，runner 无法读取或修改 collector 已持久化的快照；
- 所有正常和异常路径都幂等关闭两端，避免 EOF 永不出现。

该保留环境键属于内部执行协议。用户提供的 env、config 或 payload 不得覆盖它；检测到冲突必须在 child 启动前 fail closed。

### runner 与 experiment 内部接口

`txnmem_provenance_runner.py` 负责：

1. 严格解析进度 FD；
2. 构造只写、逐行、UTF-8 canonical JSON emitter；
3. 将内部 progress callback 和 `require_formal_eligibility=True` 传入 `txnmem_experiment.main`；
4. 在每条完整记录后执行 flush；
5. pipe 关闭、短写、编码失败或 collector 消失时立即中止正式运行。

`txnmem_experiment.main` 增加仅供受保护 runner 使用的内部参数，不扩展普通 CLI 的用户输入面：

- `_progress_callback: Callable[[Mapping[str, Any]], None] | None`；
- `_require_formal_eligibility: bool`。

这两个值不得从普通命令行参数、配置文件或环境变量直接启用。普通 diagnostic CLI 仍保持原有容错行为；只有由 protected collector 安装并经过 source attestation 的 runner 才强制正式资格门。

### 进度事件 schema

runner 每完成一个 repetition 后发出一条 `txnmem-provenance-progress-event-v1`。exact-key closure 为：

| 字段 | 类型与约束 |
|---|---|
| `schema` | 固定字符串 `txnmem-provenance-progress-event-v1` |
| `run_binding_sha256` | 64 位小写十六进制安全绑定 hash；不是 run ID 或 nonce |
| `config_sha256` | 冻结配置的 64 位小写十六进制 hash |
| `phase` | 固定为 `measurement` |
| `cell_index` | 整数 `1..15` |
| `cell_count` | 固定整数 `15` |
| `graph_size` | 固定集合 `100, 1000, 10000` |
| `concurrency` | 固定集合 `1, 2, 4, 8, 16` |
| `repetition_index` | 整数 `1..30`，表示当前 cell 内已完成 repetition |
| `repetition_count` | 固定整数 `30` |
| `completed_repetitions` | 整数 `1..450` |
| `total_repetitions` | 固定整数 `450` |
| `completed_samples` | `completed_repetitions × 32` |
| `total_samples` | 固定整数 `14400` |
| `update_sequence` | 从 1 开始、每条恰好加 1 的整数 |
| `status` | 事件中固定为 `running` |

事件禁止出现 hostname、username、UID、PID、namespace、endpoint、IP、port、filesystem path、wall-clock timestamp、nonce、run ID、task ID、payload、数据库值、原始异常文本或原始 latency samples。

`run_binding_sha256` 由 source commit、冻结 command hash、config hash 和 collector registration 的 canonical 安全派生组成。它只用于防止跨运行串线；不能反推出私有 identity。

### collector 状态机与快照

collector 在释放 gate 前启动专用 drain thread。它按换行边界读取，逐条执行：

- UTF-8 严格解码；
- 单条记录最大 4096 bytes；
- 拒绝 NUL、空行、截断行、非 canonical JSON、重复 key、未知或缺失 key；
- 验证 hash、schema、固定矩阵维度和 exact-key closure；
- 验证 cell 顺序与预注册笛卡尔积完全一致；
- 验证 repetition 在 cell 内连续递增；
- 验证全局 repetition、sample 和 update sequence 恰好递增；
- 拒绝倒退、跳号、重复、越界、跨 cell 提前切换和完成数不一致。

验证通过后，collector 生成 `txnmem-provenance-progress-snapshot-v1`。快照包含事件中的安全字段，以及：

- `phase`: `setup | measurement | finalizing | terminal`；
- `status`: `starting | running | completed | blocked | interrupted`；
- `last_update_age_seconds`: controller 在读取/呈现时计算的非负整数，不由 runner 提供；
- `terminal_reason_class`: 终态可选的枚举值，不包含原始异常文本。

快照写在 candidate tree 之外的 controller-private run root，权限固定为 root-owned `0600`，父目录不可由 measured UID 写入。写入协议固定为：

1. 在同一目录以 `O_CREAT|O_EXCL|O_NOFOLLOW` 创建临时文件；
2. 写入 canonical JSON 和结尾换行；
3. `fsync` 文件；
4. 原子 replace 到固定 `progress.json`；
5. `fsync` 父目录；
6. 重新以 no-follow 方式核验 owner、mode、regular-file 和 link count。

快照是运行控制遥测，不进入 candidate seal、sanitized topology、统计汇总或论文 active claim。completion 后可保留一份脱敏终态快照用于运维审计，但不能替代 completion receipt。

若 drain thread、pipe 或快照校验失败，collector 必须先阻止 promotion，再终止 child 并清理 guard；不得降级成“无进度显示但继续正式运行”。

## 正式资格快速失败

当前 `formal` 同时承担“生成哪种报告”和“是否立即拒绝不合格 repetition”两种语义。v6 将它们拆开：

- `formal_requested` 继续表示普通调用方是否请求 formal report；
- `require_formal_eligibility` 单独控制每个 repetition 的即时资格门；
- protected collector 启动的 source candidate 固定为 diagnostic publication mode，但强制 `require_formal_eligibility=True`；
- 普通 diagnostic CLI 默认 `require_formal_eligibility=False`，保持调试能力。

每个 repetition 完成独立 backend verification 后，先计算原有 `eligible_for_diagnostic` 和正式资格闭包，再执行：

1. 若 `require_formal_eligibility=True` 且任一正式条件不满足，立即抛出结构化安全错误；
2. 不发送“completed repetition”进度事件；
3. collector 把终态写为 `blocked`，原因只使用冻结枚举；
4. 不运行后续 repetition 或 cell，不生成可 promotion candidate。

快速失败只改变失败时机，不放宽或新增资格条件。最终汇总必须继续独立重算全部 repetition 的正式资格，防止仅靠运行时分支绕过验证。

## Neo4j 有界超时

`request_timeout_seconds` 已由配置验证为有限正数，并传入真实 backend 层。v6 要求 `_Neo4jBoltClient` 与 Qdrant 使用同一冻结值，默认正式配置为 30 秒。

Neo4j driver 初始化与事务执行必须显式绑定：

- TCP/driver connection establishment timeout；
- connection pool acquisition timeout；
- transaction/query timeout。

实现必须使用仓库锁定 Neo4j Python driver 版本公开支持的参数或 `Query`/transaction config API，不依赖未记录私有属性。数值不得被四舍五入为无限、零或负数，也不得在 retry 层被无界重复。任一 timeout 进入现有结构化失败路径并触发 fail-fast；不把 timeout repetition 计为成功样本。

配置 attestation 新增安全字段，证明 Qdrant request timeout、Neo4j connection acquisition timeout 和 Neo4j transaction/query timeout 都源于同一 `request_timeout_seconds`。原始 endpoint 和连接字符串仍不进入脱敏产物。

## 进程生命周期与中断协议

### parent-death protection

Linux 正式 runner 在 exec 后、降权前设置 `PR_SET_PDEATHSIG=SIGTERM`，随后立即重新读取 parent PID；若 parent 已变化或不是已登记 collector，则自行退出，关闭父进程死亡信号设置与检查之间的竞态窗口。

非 Linux 单元测试环境不伪装该能力；正式 remote smoke 必须证明 Linux 路径已启用。无法设置 parent-death signal 时，正式运行 fail closed。

### collector 信号处理

collector 为 SIGTERM 和 SIGINT 安装最小处理器，只设置中断标志并唤醒主控制流。实际清理在正常控制流中按有界顺序执行：

1. 将进度终态标记为 `interrupted`；
2. 关闭 gate 与进度 write capability；
3. 向已验证的 runner process group 发送 SIGTERM；
4. 在固定 grace period 内等待 child、drain thread 与 controller monitor；
5. 仅对仍满足原始 pid/start-time/signature 绑定的残留 child 使用 SIGKILL；
6. 执行幂等 nft guard cleanup，并验证 guard 数量为零；
7. 关闭 FD、写入安全 blocked/interrupted receipt，然后退出非零。

任何清理操作都必须验证 PID 重用、process group、session 和命令签名，不能向未绑定进程发送信号。cleanup failure 是独立硬失败，禁止复用该 identity 启动下一次运行。

runner 同时安装 SIGTERM 处理：停止创建新 repetition，关闭正在管理的客户端，刷新不了的 in-memory report 直接丢弃，关闭 progress FD 后退出。由于 candidate 只在完整成功时原子发布，中断不会产生半 candidate。

## v6 存储与重启协议

v5 的 candidate root、private run root、服务卷和一次性诊断证明保持原样，不删除、不修改、不 promotion。v6 使用：

- 新 source commit；
- 新 registration commit；
- 新 run identity 与新 formal nonce；
- 新 compose project name；
- 新 Qdrant、Neo4j、Toxiproxy volume；
- 新 controller-private progress root。

旧服务容器可以在验证确属 v5 后停止，以释放固定 container name、network 和 host port，但禁止执行会删除旧 volume 的 `down -v` 或等价操作。v6 以同一 digest-pinned 镜像和冻结服务配置创建干净卷；不得复制 v5 namespace、数据库文件或 cache。

正式重启前，服务器必须从 exact registration commit 的干净 checkout 重建并重装 protected controller。不能把本地未提交文件 rsync 到 protected source 后直接运行。

## 测试先行策略

所有实现遵循 RED → GREEN。每个生产改动必须先有能在旧代码上失败的测试。

### 进度 schema 与状态机单元测试

- 接受 1 到 450 的完整 canonical 事件序列；
- 验证 15 cells、每 cell 30 repetitions、每 repetition 32 samples；
- 拒绝未知/缺失字段、错误类型、布尔伪装整数、NaN/Infinity、非小写 hash；
- 拒绝重复 JSON key、非 canonical key order/whitespace、无终止换行、截断记录和超过 4096 bytes；
- 拒绝 sequence/repetition/sample 倒退、重复、跳号和越界；
- 拒绝错误 cell 顺序、graph size 或 concurrency；
- 扫描所有事件和快照，证明敏感字段名和值无法出现；
- 验证 starting、running、completed、blocked、interrupted 的合法状态转移闭包。

### pipe 与原子快照集成测试

- child 每完成一次 repetition，collector 恰好收到一次事件并刷新快照；
- child 正常关闭时 drain thread 收到 EOF 并可 join；
- reader 提前退出、writer 短写、pipe 满、collector 消失时 runner 快速失败，不死锁；
- 原子 replace 前崩溃只保留上一份完整快照；
- symlink、非 regular file、错误 owner/mode/link count 全部 fail closed；
- progress 文件不出现在 candidate manifest、seal 或 promotion byte set 中。

### 快速失败测试

- 第一个 repetition 不合格时只执行一次，不发送 completed event，不进入第二次；
- 第 N 个 repetition 不合格时快照保持 N-1 completed，终态为 blocked；
- protected candidate 即使 publication mode 为 diagnostic 也强制正式资格；
- 普通 diagnostic CLI 仍能记录不合格 repetition 供调试；
- 最终聚合继续重算资格，mutation 删除运行时 gate 或最终 gate 都能被测试杀死。

### Neo4j timeout 测试

- 配置的有限正数精确传到 driver connection、pool acquisition 和 transaction/query timeout；
- 超时值缺失、NaN、Infinity、布尔、零或负数被拒绝；
- 模拟永不返回的 acquisition/query 在有界时间内失败；
- timeout repetition 不计成功、不发送 completed event，并触发 blocked；
- Qdrant 与 Neo4j timeout attestation 不一致时 validator 拒绝。

### 进程与 guard 清理测试

- parent-death signal 设置失败时正式 runner 不启动测量；
- collector 被 SIGTERM 后 runner 不成为 PPID 1 孤儿；
- gate 前、运行中和 finalizing 三个阶段中断都留下零 runner、零 controller、零 nft guard；
- PID 被重用或签名漂移时不误杀无关进程，同时把 cleanup 标记为硬失败；
- 连续调用 cleanup 幂等，第二次不产生新错误或扩大删除范围。

## 服务器验证门

### 小规模真实服务 smoke

完整 v6 矩阵前，使用同一 protected controller、真实 Qdrant/Neo4j/Toxiproxy 和新干净卷运行不进入论文的短 smoke。至少证明：

1. 第一个 repetition 后出现合法 `progress.json`；
2. 连续 repetition 的 sequence、completed repetitions 和 samples 单调精确增长；
3. 人工制造的后端超时在冻结上限附近失败并产生 `blocked`，不继续下一 repetition；
4. 人工制造的不合格 repetition 立即 fail-fast；
5. 运行中 SIGTERM 后 runner、controller 和 nft guard 全部为零；
6. candidate tree 未出现部分文件，v5 材料未被修改；
7. 正常 smoke completion 后原始 candidate 仍可按现有字节级 promotion 协议校验。

smoke 使用独立测试 identity 和 nonce，失败后绝不复用。只有全部门通过，才创建正式 v6 registration。

### 完整正式矩阵

v6 正式矩阵仍固定为 15 cells、450 repetitions、14,400 operation samples。运行中只允许自动化监控以下脱敏状态：

- launcher/controller 是否存活；
- completion 是否存在；
- runner 数量；
- nft guard 数量；
- `progress.json` 中通过 schema 校验的 cell/repetition/sample 完成数和更新年龄。

运行中仍禁止读取数据库、原始日志、原始 payload 或 candidate 内容。出现进度超过请求超时与有限清理窗口仍不变化时，monitor 只报告 `stale`，由 collector 的 timeout/fail-fast 负责结束；monitor 不自行查询后端解释原因。

## 成功后的既定流水线

完整矩阵成功后继续执行已有、未被本文修改的流程：

1. 对 material 做严格计数校验：15 cells、450 repetitions、14,400 samples；
2. 生成并独立复核 sanitized topology v6；
3. 测试先行注册拓扑摘要并提交、推送；
4. 服务器精确重建 registration commit 并重装 protected controller；
5. promotion 原始候选，不重跑测量；
6. 仅同步脱敏正式产物；
7. 本地逐字节复算统计与置信区间；
8. 更新 claim audit、论文、DOCX 和 PDF，完成 PNG 视觉 QA；
9. 全量测试、独立代码/证据审查；
10. 合并 main 并推送。

进度快照不能替代以上任何一步。

## 验收标准

实现与正式运行必须同时满足：

1. 进度文件在首个成功 repetition 后可见，随后严格单调更新到 `450/450` 和 `14400/14400`；
2. 任一 repetition 不符合正式资格时，在进入下一 repetition 前停止并留下脱敏 blocked 终态；
3. Neo4j connection acquisition 与 transaction/query 使用冻结的 30 秒超时，测试能证明参数真实到达 driver API；
4. SIGTERM、collector crash 和 parent death 场景都不遗留 runner、controller 或 nft guard；
5. progress pipe/file 任一协议错误都 fail closed，不能静默继续；
6. progress 文件不属于 candidate byte set，现有 seal 和 promotion 字节保持精确；
7. v5 candidate、private run material 和服务卷 hash/metadata 在 v6 完成前后保持不变；
8. v6 使用全新 identity、nonce、registration 和干净卷；
9. 正式成功产物严格包含 15 cells、450 repetitions 和 14,400 samples；
10. 专项测试、全量测试、claim audit、artifact audit、静态检查和独立审查均通过且无未解释 skip/finding。

## 安全与隐私

Git 只保存代码、测试、设计、公开配置、脱敏进度 schema、脱敏拓扑证明和聚合统计。密码、服务器地址、用户名、nonce、run identity、私有路径、原始日志、原始 payload、数据库内容、完整 namespace 和未脱敏 exception 均不得进入 Git、终端回显、论文或用户进度通知。

进度监控只回答“已完成多少”和“多久未更新”，不回答“数据库中有什么”。任何安全字段不确定时，优先停止正式运行并保留一次性失败证据，不扩大读取范围。

## 决策结论

v5 不恢复、不 promotion、不复用。v6 采用 collector-owned 单向进度 pipe、root-owned candidate 外原子快照、protected mode 正式资格快速失败、Neo4j 有界超时、Linux parent-death signal 与 collector 有界清理。先通过测试和小规模真实服务 smoke，再以全新注册身份和干净卷重跑完整正式矩阵。
