# TxnMem 正式性能实验：代理归因与 Docker 入口隔离设计

## 状态与关系

- 状态：已批准，采用推荐方案 A。
- 日期：2026-08-23。
- 适用范围：真实 Qdrant、Neo4j、Toxiproxy 的正式 backend performance 运行及其原始/脱敏拓扑证明。
- 上位设计：`2026-08-18-evidence-scale-up-design.md`。
- 本文只修正正式证据契约，不改变 3 个 graph size × 5 个 concurrency × 30 repetitions 的预注册矩阵，也不生成正式候选结果。

## 问题与证据

服务器同路径冒烟测试暴露了两个相互独立、都会使正式性能证据失效的问题。

第一，固定的 Toxiproxy 2.5.0 镜像默认没有启用 proxy metrics，`/metrics` 因而返回 404。即使启用，当前解析器仍使用不存在的 `toxiproxy_proxy_transmitted_bytes_total`，并假定删除、重建 route 会把 Prometheus CounterVec 清零。Toxiproxy 2.5.0 的官方定义实际为：

- 以 `-proxy-metrics` 显式启用代理指标；
- 两个字节计数器名是 `toxiproxy_proxy_received_bytes_total` 与 `toxiproxy_proxy_sent_bytes_total`；
- 标签集合固定为 `direction`、`proxy`、`listener`、`upstream`；
- 两类指标都是进程生命周期内单调递增的 Counter，删除并重建同标签 route 不会归零。

依据为 Toxiproxy 2.5.0 的 [METRICS.md](https://github.com/Shopify/toxiproxy/blob/v2.5.0/METRICS.md)、[API 注册逻辑](https://github.com/Shopify/toxiproxy/blob/v2.5.0/api.go) 和 [proxy collector](https://github.com/Shopify/toxiproxy/blob/v2.5.0/collectors/proxy.go)。

第二，Docker 29 的 loopback published port 由 root-owned `docker-proxy` 转发。现有 nftables output policy 允许正式 runner 访问 `127.0.0.1:19000/19001`，却拒绝 `docker-proxy` 从宿主机到 ingress bridge 内 Toxiproxy 容器地址的第二跳，因此 root 管理探针、Qdrant 数据路径和 Neo4j 数据路径都会失败。放宽整个 root bridge 流量会扩大可信边界，不能作为正式修复。

## 安全目标与威胁边界

正式运行信任已安装、只读、由 root 管理的 controller，以及 root-owned Docker daemon/docker-proxy。正式 runner 是固定非 root UID；其他非 root 宿主进程、非正式容器及未登记网络路径均不可信。

本设计必须同时证明：

1. measured child 在 gate release 之前不能发送测量流量；
2. runner 只能通过两个 loopback data port 进入 Toxiproxy；
3. Toxiproxy management port 只能由 root controller 通过 loopback 使用；
4. docker-proxy 的 bridge 第二跳仅可到达本次观测并绑定的 Toxiproxy ingress IPv4 和三个登记端口；
5. Qdrant/Neo4j backend bridge 不能被宿主机或其他转发路径直接访问；
6. 计入候选结果的 Qdrant/Neo4j 字节增量来自 guard 生效后的封闭测量窗口；
7. 任一拓扑、route、metric series、counter 或规则漂移都 fail closed，不能降级为警告。

root 本身不属于对抗者；若 root controller、Docker daemon 或内核被攻破，本证据不再成立。本文不声称抵御该类攻击。

## 方案总览

采用四个互相绑定的修正：

1. 在现有 digest-pinned Toxiproxy 2.5.0 服务上显式加入 `-proxy-metrics`，不升级镜像，也不启用无关 runtime metrics。
2. 用严格的官方 exposition 契约解析 received/sent counters，拒绝历史错误名、额外标签、缺失/重复 series 和非整数值。
3. 以“绝对单调计数器快照 + 差值”替代“route 重建后绝对为零”的错误假设，并在 guard 激活前后建立无流量边界。
4. 观测并绑定唯一 Toxiproxy ingress IPv4，只为 root-owned docker-proxy 开放该 IPv4 的登记端口；其余 bridge 流量继续拒绝。

## Toxiproxy 服务契约

Compose 保持镜像 `shopify/toxiproxy:2.5.0` 及当前 digest 不变，显式设置等价于：

```text
/toxiproxy-server -host=0.0.0.0 -proxy-metrics
```

具体 argv 以镜像 entrypoint/command 的实际组合为准，但容器 inspect 后必须证明最终 argv 同时包含原有 host 绑定和恰好一个 `-proxy-metrics`。不得依赖未固定的环境变量或镜像默认值。`-runtime-metrics` 不在本轮范围。

正式启动检查必须在创建候选目录前验证：

- `/version` 为已登记的 2.5.0；
- `/metrics` 返回 UTF-8 Prometheus text；
- 两个登记 route 均出现完整的官方 counter series；
- compose 文件 hash、镜像 manifest digest、runtime image ID 和 container argv 与证明材料一致。

## 严格 metrics 解析

每个登记 route 必须恰好出现以下四个 series：

- `received_bytes_total` × `direction=upstream`；
- `sent_bytes_total` × `direction=upstream`；
- `received_bytes_total` × `direction=downstream`；
- `sent_bytes_total` × `direction=downstream`。

因此两个 route 共恰好八个目标 series。每个 series 的标签键集合必须严格等于：

```text
direction, proxy, listener, upstream
```

标签顺序可以变化，但不得缺失、重复或增加标签。解析器必须按 Prometheus text format 处理转义，不得继续用“搜索 proxy 标签后累加任意匹配行”的宽松正则。每个 series 还必须满足：

- metric 名严格为官方 received/sent 名，`transmitted` 明确拒绝；
- `proxy` 等于登记的唯一 route 名；
- `direction` 只能是 `upstream` 或 `downstream`；
- `listener` 与 route API 归一化后的 listener 完全一致；
- `upstream` 与 route API 中的 upstream 完全一致；
- sample value 符合 Prometheus 数值语法，解析后是有限、非负、数学意义上的精确整数，且不超过 IEEE-754 可精确表达的整数上限；
- 同一 `(metric, direction, proxy, listener, upstream)` 不得重复；
- 任一登记 route 的四个 series 不完整时立即失败。

解析输出保留每个 route 的四个分量和分量总和，而不是只保留无法审计的合计。角色合计定义为四个分量之和；`toxiproxy` 合计定义为 Qdrant 与 Neo4j 角色合计之和。HELP/TYPE 行和非 proxy metric family 可以忽略，但任何以 `toxiproxy_proxy_` 开头的未知 sample family 必须拒绝，防止版本或契约漂移被静默吞掉。

## 单调 baseline 与测量窗口

正式执行顺序固定如下，measured child 在第 8 步前始终由 gate 阻塞：

1. 校验 source、runtime、compose、Docker 网络和 formal nonce，创建受保护的 controller context。
2. 删除并重建两个无 toxic 的登记 route，校验 listener/upstream/name/enabled 的精确闭包。
3. 通过两个代理完成 controller-owned Qdrant/Neo4j 健康与版本探针，并同步关闭所有探针连接。
4. 读取 route 和八个 metrics series，得到绝对计数器快照 `baseline_a`。
5. 激活 nftables guard，并验证规则 hash、bridge/subnet 绑定及正式 UID 约束。
6. guard 生效后由 root management API 再次删除并重建两个精确 route。Toxiproxy 2.5.0 的 `Proxy.Stop` 会停止 listener 并关闭现存连接；该步骤用于终止可能跨越 guard 边界的旧数据连接，但不假设 CounterVec 归零。
7. 再次读取精确 route 和 counters，得到 `baseline_b`；要求 `baseline_b == baseline_a` 且 route 语义与第 2 步一致。任一计数增长、series 变化或 route 漂移均中止运行。launch 原始证明同时记录 A、B 及其 canonical hash。
8. 写入 exclusive launch receipt 后释放 measured child gate，开始正式 repetition。
9. child 退出且 execution monitor 完成终态检查后，在 guard 仍生效时读取 `final` route/counters；随后 seal candidate。
10. 要求每个 final 分量均不小于 `baseline_b`，逐分量计算 delta；Qdrant 与 Neo4j 的角色 delta 必须分别大于零，角色合计与 Toxiproxy 总计必须严格相等。
11. 写入 exclusive completion receipt，完成脱敏验证；无论成功或失败都执行幂等 guard cleanup。

这里的正式归因窗口是 `[baseline_b, final]`。健康探针产生的流量保留在绝对 baseline 中，但其差值为零，因此不进入候选结果。管理 API 请求不经过两个 data route，不计入 proxy byte counters。

若 route re-arm 后 counters 因连接关闭而异步变化，步骤 7 失败；controller 不重试并掩盖该运行，而是清理后重新开始一个全新的 run identity。这样不会把未静止边界包装为成功证据。

## Docker ingress 最小授权

controller 从 Docker inspect 获取 Toxiproxy 在 ingress network 上的唯一 IPv4。该地址必须：

- 是规范 IPv4 host address；
- 属于已证明的 ingress RFC1918 subnet；
- 不属于 backend subnet；
- 与 ingress network endpoint ID/容器 ID 同时绑定；
- 在 Qdrant 和 Neo4j 容器的网络 attachment 中不存在；
- 在同一 ingress network 上不存在第二个非 Toxiproxy workload container。

原始 attestation 保存该地址；脱敏 attestation 只保存 canonical IPv4 的 SHA-256、membership 布尔证明及与 guard 的交叉绑定。网络规则按以下顺序建立：

1. `runner_uid -> 127.0.0.1:{19000,19001}`：允许；
2. `root -> 127.0.0.1:8474`：允许；
3. `root -> exact_toxiproxy_ingress_ipv4:{8474,19000,19001}`：允许，供 docker-proxy 第二跳；
4. runner 的其他 output：拒绝；
5. 非 root 对 management loopback：拒绝；
6. 非 runner 对 data loopback：拒绝；
7. 到 backend/ingress 两个 subnet 的其余宿主 output：拒绝；
8. 非登记 bridge/interface 进入两个 subnet 的 forward 流量：拒绝。

第 3 条不按进程名、PID 或可变 cgroup 猜测 docker-proxy，只使用已声明可信的 root UID、单一 attested destination IPv4 和三个固定 destination port。它不允许 root 访问 backend subnet，也不允许 root 通过 loopback data port 充当额外测量客户端。root 进程理论上可直接使用该精确 ingress IPv4/port 规则，因此该保证依赖“root 属于可信计算基”的已声明威胁边界；它不扩大论文声称的非 root 隔离边界。

## 证明对象与 schema 升级

本修正改变了正式证据语义，禁止在旧 schema 下偷渡新字段。实现时进行以下显式升级：

| 对象 | 当前 | 新版本 | 新绑定 |
|---|---|---|---|
| topology snapshot | `txnmem-provenance-topology-snapshot-v2` | `v3` | 结构化 proxy counter snapshot、ingress IP 证明 |
| backend isolation | `txnmem-provenance-backend-isolation-v2` | `v3` | Toxiproxy ingress IPv4 hash、endpoint/network membership |
| network guard | `txnmem-provenance-network-guard-v2` | `v3` | 同一 ingress IPv4 hash、三个精确端口、最小 root ingress allow 证明 |
| raw launch | `txnmem-provenance-execution-launch-raw-v3` | `v4` | `baseline_a`、`baseline_b`、两者 equality/hash、route re-arm 证明 |
| raw completion | `txnmem-provenance-execution-completion-raw-v4` | `v5` | final 分量 counters、逐分量 delta、最终 route closure |
| sanitized topology | `txnmem-topology-attestation-v5` | `v6` | baseline/final/delta 摘要、正增量与总和一致性、ingress 交叉绑定 |

新增 proxy counter snapshot 使用独立 schema `txnmem-provenance-proxy-counters-v1`，字段闭包固定为：schema、captured phase、两个按 role 排序的 route counter、toxiproxy total、canonical hash。每个 route counter 固定包含 role、proxy/listener/upstream、received/sent × upstream/downstream 四个绝对整数和 role total。

所有 validator 使用 exact-key equality；旧 schema 不自动升级，历史 artifact 仍由旧代码/旧 validator 解释。launch 与 completion 必须交叉绑定：source commit、command、route、backend isolation、network guard、ingress IPv4 hash、baseline B 和 candidate identity。脱敏器必须从 raw A/B/final 重新计算所有 equality、delta 和总和，不能信任预计算布尔值。

## Fail-closed 与清理

以下任一情况均不得生成可汇总的正式 candidate：

- `/metrics` 不可用、非 UTF-8 或未启用 proxy metrics；
- metric 名、标签闭包、series 数、整数类型或 route 映射不精确；
- A/B 不相等、counter 回退、final 分量小于 baseline 或任一 backend delta 为零；
- route 在 A、B、final 三个边界之间漂移；
- Toxiproxy ingress IPv4、network membership、subnet、bridge interface 或 container image 漂移；
- nft rule hash 改变、guard monitor 发现违规或 cleanup 无法证明完成；
- child 在 launch receipt 前越过 gate；
- raw/sanitized schema、field closure 或交叉 hash 不一致。

失败路径保留脱敏诊断分类，不保留密码、业务 payload、原始 benchmark trace、完整宿主地址或 formal nonce。guard、child 和临时 route 的清理必须幂等；清理失败提升为独立硬失败并阻止下一正式运行复用同一 identity。

## 测试与验收

实现遵循 RED → GREEN，至少覆盖以下 mutation：

### 静态与单元测试

- compose 缺少、重复或拼错 `-proxy-metrics` 时失败；镜像 tag/digest 漂移时失败；
- 官方 received/sent 八 series、标签任意顺序可以通过；
- `transmitted`、未知 `toxiproxy_proxy_*` family、额外/缺失/重复标签、重复/缺失 series、错误 listener/upstream、NaN/Inf/小数/负数均失败；
- 非零单调 baseline 可以通过，旧“必须为零”断言被删除；
- A/B 任一分量变化、route re-arm 漂移、final 回退、任一 backend 零 delta、角色/总和不一致均失败；
- ingress IPv4 不在 subnet、出现在错误网络、多个候选 IP、hash 不一致或 guard/backend 交叉绑定漂移均失败；
- nft policy 只允许 root 到 exact ingress IP 的三个端口，改变 UID、IP、端口、顺序或扩大 subnet allow 均被测试杀死；
- 所有旧 schema 被新 validator 明确拒绝，所有新对象 exact-key closure 生效。

### 本地工程门

- collector/topology 专项测试通过；
- 全量测试通过且无新增未解释 skip；
- claim audit 全通过；
- artifact audit 为 0；
- `py_compile`、格式/静态检查及 `git diff --check` 通过；
- 独立审查者无 Critical/Important finding。

### 服务器同路径冒烟门

只能从 exact commit 的干净 clone 安装 protected controller。正式矩阵之前必须完成一次不生成正式 candidate 的 production same-path smoke，并证明：

- Toxiproxy 2.5.0 proxy metrics 可读且八个目标 series 闭合；
- root management 成功；
- formal runner 经 loopback proxy 访问 Qdrant 和 Neo4j 成功；
- 非 runner 经两个 proxy data port 均失败；
- 宿主机直接访问 Qdrant/Neo4j backend bridge 均失败；
- 非登记 forward/bridge 路径失败；
- A/B 完全相等，正式 smoke 负载后两个 backend delta 均为正；
- guard 在负载期间稳定、终态 hash 不变且清理后 nft table 不存在；
- Docker topology、container argv、image ID/digest 与 attestation 一致。

只有该冒烟门全部通过，才允许启动预注册的 15 cells × 30 repetitions 正式矩阵。失败 smoke 的数据只属于诊断，不进入论文表格、曲线或 active claim。

## 不采用的方案

- 全局设置 Docker `userland-proxy=false`：需要修改 daemon 并重启，影响服务器上其他 workload，且不解决 metrics 单调 baseline，拒绝。
- 升级 Toxiproxy：扩大镜像与行为边界，仍不解决 docker-proxy 最小授权和 CounterVec 语义，拒绝。
- 允许所有 root 到 bridge subnet 的流量：授权面过宽，不能证明 exact ingress path，拒绝。
- 继续伪造“route 重建后计数器为零”：与官方 Counter 语义冲突，拒绝。
- 只看候选文件中预计算的 delta/布尔值：无法抵御 collector 或 artifact 篡改，必须由脱敏 validator 重算，拒绝。

## 数据与 Git 约束

Git 只保存代码、测试、设计/运行配置、脱敏证明和聚合统计。服务器私有材料包括密码、formal nonce、完整宿主/容器地址、原始 benchmark trace 和未脱敏操作 payload，均不得提交或出现在日志、commit message、论文正文中。

本设计规范单独提交。实现、服务器 smoke、正式候选和论文 claim 分别在后续可审计提交中完成，任何阶段不得用“代码已实现”替代“同路径实测已通过”。
