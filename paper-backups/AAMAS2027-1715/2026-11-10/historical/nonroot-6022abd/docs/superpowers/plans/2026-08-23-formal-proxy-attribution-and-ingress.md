# TxnMem Formal Proxy Attribution and Ingress Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复正式真实后端实验的 Toxiproxy 字节归因与 Docker 29 ingress 隔离，使同路径冒烟通过后可安全运行 3×5×30 的正式性能矩阵。

**Architecture:** 保持固定的 Toxiproxy 2.5.0 镜像，通过严格 Prometheus parser、A/B/final 单调 counter 快照和结构化 delta 建立可重算的代理归因窗口。Docker topology 提供唯一 Toxiproxy ingress IPv4，nftables 仅允许可信 root 到该地址的三个登记端口；raw v3/v4/v5 证据保存完整边界，sanitized v6 重新计算并验证所有哈希、差值和交叉绑定。

**Tech Stack:** Python 3.10.12-compatible standard library, `unittest`, Docker 29, Docker Compose, nftables, Toxiproxy 2.5.0, Qdrant 1.11.5, Neo4j 5.22, Bash, Git.

**Spec:** `docs/superpowers/specs/2026-08-23-formal-proxy-attribution-and-ingress-design.md`

## Global Constraints

- Toxiproxy 保持 `shopify/toxiproxy:2.5.0@sha256:927c797a2115a193ae3a527e5a36782b938419904ac6706ca0efa029ebea58cb`；只增加 `-proxy-metrics`，不增加 `-runtime-metrics`。
- 服务器受保护运行时必须兼容 CPython 3.10.12；不得引入第三方 Prometheus parser 依赖。
- metrics 只接受官方 `received_bytes_total` 和 `sent_bytes_total`，以及精确的 `direction/proxy/listener/upstream` 标签闭包。
- root controller、Docker daemon 和 root-owned docker-proxy 属于可信计算基；所有非 root、非正式 runner 路径均按不可信处理。
- nftables 不修改 Docker daemon 全局配置，不允许整个 root bridge subnet，只允许 exact ingress IPv4 与端口 `8474/19000/19001`。
- raw launch/completion、sanitized topology、backend isolation、network guard 和 topology snapshot 必须显式升级 schema；旧 schema 不自动转换。
- 脱敏 validator 必须从 raw A/B/final snapshots 重新计算 equality、delta 与总和，不能信任 collector 写入的布尔值或合计值。
- production same-path smoke 通过前不得创建、封存或推广正式 performance candidate。
- 密码、formal nonce、完整宿主地址、容器地址、原始操作 payload 和 benchmark trace 不进入 Git、终端报告或论文正文。

## File Map

**Create:**

- `src/txnmem_toxiproxy_metrics.py` — 无 I/O 的严格 Prometheus exposition parser、counter snapshot 与 delta validator。
- `src/txnmem_formal_smoke.py` — 受保护 controller 下的同路径网络/归因冒烟编排与脱敏报告。
- `scripts/run_formal_provenance_smoke.sh` — 调用已安装 controller 的最小环境 wrapper。
- `tests/test_txnmem_toxiproxy_metrics.py` — 官方 series、标签、数值和 mutation 测试。
- `tests/test_txnmem_formal_smoke.py` — smoke gate、拒绝路径、清理和无 candidate 测试。

**Modify:**

- `infra/real_backend/docker-compose.yml` — 显式启用 proxy metrics。
- `infra/real_backend/README.md` — 记录 metrics 与 formal smoke 边界。
- `src/txnmem_provenance_execution_collector.py` — topology v3、Docker ingress 观测、guard v3、A/B/final 时序和 raw evidence。
- `src/txnmem_topology_attestation.py` — raw launch v4、completion v5、sanitized v6 与独立重算。
- `src/txnmem_provenance_runner.py` — gate 后执行最小 Qdrant/Neo4j smoke 子命令并返回固定 receipt。
- `src/txnmem_formal_controller.py` — source-approved `smoke` action。
- `scripts/install_formal_provenance_runtime.sh` — 将 smoke wrapper 纳入 approved auxiliary closure。
- `scripts/run_cross_host_provenance_performance.sh` — 暴露 smoke action 并保持 measure/material/attest/promote 顺序。
- `tests/test_real_backend_script.py` — compose command 与 wrapper 静态/解析测试。
- `tests/test_txnmem_provenance_execution_collector.py` — Docker、nft、boundary 与 raw schema 测试。
- `tests/test_txnmem_topology_attestation.py` — sanitizer v6、exact-key、delta 与 privacy mutation 测试。
- `tests/test_txnmem_formal_controller.py` — approved export 与 smoke dispatch 测试。
- `configs/paper_claims.json`、`docs/paper/evidence_map_zh.md`、`docs/formal_paper_task_status_zh.md`、`docs/paper/txnmem_ccfa_draft_zh.md` — 仅在正式 450 repetitions 被验证后更新。

## Spec Coverage Review

- Toxiproxy 服务参数与固定镜像：Tasks 1、7。
- 官方 metrics 名称、标签闭包、数值与 series mutation：Task 1。
- A/B/final 单调窗口、post-guard route re-arm 与 gated child 时序：Tasks 4、5、6。
- exact ingress IPv4、Docker membership 与最小 root trust：Tasks 2、3、6。
- topology v3、backend v3、guard v3、launch v4、completion v5、sanitized v6：Tasks 2–5。
- fail-closed 错误路径、幂等清理与隐私：Tasks 1–6。
- 本地全量测试、claim/artifact audit 和独立代码审查：Task 6。
- exact-commit protected install、同路径 live smoke 与无 candidate 门：Task 7。
- 15 cells × 30 repetitions、独立 digest 注册和 promotion：Task 8。
- 论文曲线、证据账本、限制陈述和 DOCX/PDF 视觉核验：Task 9。
- 设计中拒绝的 Toxiproxy 升级、全局关闭 userland proxy、subnet-wide root allow 和伪零 baseline 均未出现在任何实现步骤。

---

### Task 1: Enable and strictly parse Toxiproxy 2.5 proxy metrics

**Files:**

- Create: `src/txnmem_toxiproxy_metrics.py`
- Create: `tests/test_txnmem_toxiproxy_metrics.py`
- Modify: `infra/real_backend/docker-compose.yml:44-55`
- Modify: `tests/test_real_backend_script.py:98-173`
- Modify: `src/txnmem_provenance_execution_collector.py:1-35,2820-2858`

**Interfaces:**

- Consumes: normalized route rows with exact fields `role/proxy_name/listen/upstream/enabled/toxics_count`.
- Produces: `parse_toxiproxy_byte_counters(metrics: str, *, phase: str, proxy_routes: Sequence[Mapping[str, Any]]) -> dict[str, Any]`.
- Produces: `validate_proxy_counter_snapshot(value: Any, *, expected_phase: str | None = None) -> dict[str, Any]`.
- Produces: `proxy_counter_values(value: Mapping[str, Any]) -> tuple[int, int, int, int, int, int, int, int]`, `proxy_counter_payload_sha256(value: Mapping[str, Any]) -> str` and `derive_proxy_counter_deltas(baseline: Mapping[str, Any], final: Mapping[str, Any]) -> dict[str, Any]` for Tasks 4–5.
- Raises: `ToxiproxyMetricsError(ValueError)`; collector converts it to `CollectorError` without including metric payloads.

- [ ] **Step 1: Write the failing compose tests**

Add assertions that inspect both source YAML and `docker compose config --format json`:

```python
def test_compose_explicitly_enables_only_toxiproxy_proxy_metrics(self):
    compose = (ROOT / "infra" / "real_backend" / "docker-compose.yml").read_text(
        encoding="utf-8"
    )
    self.assertEqual(compose.count("-proxy-metrics"), 1)
    self.assertNotIn("-runtime-metrics", compose)
    self.assertIn("-host=0.0.0.0", compose)
```

Extend the existing resolved-compose test with:

```python
self.assertEqual(
    services["toxiproxy"]["command"],
    ["-host=0.0.0.0", "-proxy-metrics"],
)
```

- [ ] **Step 2: Run the compose tests to verify RED**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_real_backend_script.RealBackendScriptTests.test_compose_explicitly_enables_only_toxiproxy_proxy_metrics -v
```

Expected: FAIL because the compose service has no explicit proxy-metrics command.

- [ ] **Step 3: Add the exact compose command**

Under the pinned Toxiproxy service add:

```yaml
    command:
      - -host=0.0.0.0
      - -proxy-metrics
```

Do not modify the image reference, port publications, networks or dependencies.

- [ ] **Step 4: Write the failing strict parser tests**

Create an eight-series fixture with arbitrary label order and one scientific-notation integer:

```python
ROUTES = [
    {
        "role": "qdrant",
        "proxy_name": "txnmem-qdrant",
        "listen": "0.0.0.0:19000",
        "upstream": "qdrant:6333",
        "enabled": True,
        "toxics_count": 0,
    },
    {
        "role": "neo4j",
        "proxy_name": "txnmem-neo4j",
        "listen": "0.0.0.0:19001",
        "upstream": "neo4j:7687",
        "enabled": True,
        "toxics_count": 0,
    },
]

METRICS = """
# TYPE toxiproxy_proxy_received_bytes_total counter
toxiproxy_proxy_received_bytes_total{direction="upstream",proxy="txnmem-qdrant",listener="[::]:19000",upstream="qdrant:6333"} 1.1e1
toxiproxy_proxy_sent_bytes_total{proxy="txnmem-qdrant",listener="[::]:19000",upstream="qdrant:6333",direction="upstream"} 13
toxiproxy_proxy_received_bytes_total{listener="[::]:19000",upstream="qdrant:6333",direction="downstream",proxy="txnmem-qdrant"} 17
toxiproxy_proxy_sent_bytes_total{upstream="qdrant:6333",direction="downstream",proxy="txnmem-qdrant",listener="[::]:19000"} 19
toxiproxy_proxy_received_bytes_total{direction="upstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 23
toxiproxy_proxy_sent_bytes_total{direction="upstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 29
toxiproxy_proxy_received_bytes_total{direction="downstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 31
toxiproxy_proxy_sent_bytes_total{direction="downstream",proxy="txnmem-neo4j",listener="[::]:19001",upstream="neo4j:7687"} 37
"""

def test_parses_exact_official_proxy_metric_closure(self):
    snapshot = parse_toxiproxy_byte_counters(
        METRICS, phase="baseline_a", proxy_routes=ROUTES
    )
    self.assertEqual(snapshot["schema"], "txnmem-provenance-proxy-counters-v1")
    self.assertEqual([row["total_bytes"] for row in snapshot["routes"]], [60, 120])
    self.assertEqual(snapshot["toxiproxy_total_bytes"], 180)
```

Add table-driven mutations for `transmitted`, an unknown `toxiproxy_proxy_*` family, duplicate/missing series, duplicate/extra/missing labels, wrong proxy/listener/upstream, invalid direction, negative, fractional, NaN, Inf and an integer above `2**53 - 1`.

- [ ] **Step 5: Run parser tests to verify RED**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_txnmem_toxiproxy_metrics -v
```

Expected: FAIL because `txnmem_toxiproxy_metrics` does not exist.

- [ ] **Step 6: Implement the strict parser and immutable snapshot helpers**

Use `decimal.Decimal` for exact numeric validation and a cursor-based label parser that only decodes Prometheus `\\`, `\"` and `\n` escapes. The snapshot shape is exact:

```python
PROXY_COUNTER_SCHEMA = "txnmem-provenance-proxy-counters-v1"
PROXY_COUNTER_DELTA_SCHEMA = "txnmem-provenance-proxy-counter-deltas-v1"
_PHASES = frozenset({"baseline_a", "baseline_b", "final"})
_LABEL_KEYS = frozenset({"direction", "proxy", "listener", "upstream"})
_METRIC_NAMES = frozenset(
    {
        "toxiproxy_proxy_received_bytes_total",
        "toxiproxy_proxy_sent_bytes_total",
    }
)
_MAX_EXACT_COUNTER = 2**53 - 1
```

Each route row returned by the parser must contain exactly:

```python
{
    "role": role,
    "proxy_name": proxy_name,
    "listener": normalized_route_listener,
    "upstream": upstream,
    "received_upstream_bytes": received_upstream,
    "sent_upstream_bytes": sent_upstream,
    "received_downstream_bytes": received_downstream,
    "sent_downstream_bytes": sent_downstream,
    "total_bytes": total,
}
```

Build the top-level hash before returning:

```python
document = {
    "schema": PROXY_COUNTER_SCHEMA,
    "phase": phase,
    "routes": normalized_rows,
    "toxiproxy_total_bytes": sum(row["total_bytes"] for row in normalized_rows),
}
document["snapshot_sha256"] = hashlib.sha256(
    canonical_json_bytes(document)
).hexdigest()
return document
```

`proxy_counter_values` returns the eight component integers in role order `qdrant/neo4j` and component order `received_upstream/sent_upstream/received_downstream/sent_downstream`. `proxy_counter_payload_sha256` hashes only normalized `routes` and `toxiproxy_total_bytes`, excluding phase and `snapshot_sha256`, so A and B can be compared across distinct phases. `derive_proxy_counter_deltas` returns this exact shape:

```python
{
    "schema": PROXY_COUNTER_DELTA_SCHEMA,
    "routes": [
        {
            "role": role,
            "proxy_name": proxy_name,
            "listener": listener,
            "upstream": upstream,
            "received_upstream_bytes": received_upstream_delta,
            "sent_upstream_bytes": sent_upstream_delta,
            "received_downstream_bytes": received_downstream_delta,
            "sent_downstream_bytes": sent_downstream_delta,
            "total_bytes": role_delta,
        }
        for role, proxy_name, listener, upstream,
        received_upstream_delta, sent_upstream_delta,
        received_downstream_delta, sent_downstream_delta, role_delta
        in normalized_delta_rows
    ],
    "toxiproxy_total_bytes": toxiproxy_delta,
}
```

Canonicalize metric listeners `0.0.0.0:<port>` and `[::]:<port>` to the source-registered wildcard route before comparison; reject concrete IPv4/IPv6 listeners.

- [ ] **Step 7: Replace the collector’s permissive parser with the new module**

Import and re-export `parse_toxiproxy_byte_counters` from `txnmem_toxiproxy_metrics`. Convert only the exception type:

```python
try:
    return parse_toxiproxy_byte_counters(
        text, phase=phase, proxy_routes=routes
    )
except ToxiproxyMetricsError as exc:
    raise CollectorError("formal Toxiproxy metrics are invalid") from exc
```

Delete the old received/transmitted regex implementation and its old two-series test.

- [ ] **Step 8: Verify GREEN and commit**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_txnmem_toxiproxy_metrics tests.test_real_backend_script -v
git diff --check
```

Expected: all tests PASS; Docker-dependent compose resolution may retain its existing environment-based skip.

Commit:

```bash
git add infra/real_backend/docker-compose.yml src/txnmem_toxiproxy_metrics.py \
  src/txnmem_provenance_execution_collector.py tests/test_txnmem_toxiproxy_metrics.py \
  tests/test_real_backend_script.py
git commit -m "fix: enforce official Toxiproxy proxy metrics"
```

### Task 2: Bind the exact Toxiproxy ingress endpoint into Docker topology v3

**Files:**

- Modify: `src/txnmem_provenance_execution_collector.py:77,2390-2412,3531-3825`
- Modify: `src/txnmem_topology_attestation.py:221-285,1019-1190`
- Modify: `tests/test_txnmem_provenance_execution_collector.py:899-1235`
- Modify: `tests/test_txnmem_topology_attestation.py:220-280,490-650`

**Interfaces:**

- Consumes: exact Docker inspect documents for three containers and two named networks.
- Produces: raw `txnmem-provenance-backend-isolation-v3` containing literal `toxiproxy_ingress_ipv4` only in out-of-tree raw evidence.
- Produces: sanitized `txnmem-provenance-backend-isolation-sanitized-v3` containing only `toxiproxy_ingress_ipv4_sha256` and membership booleans.
- Produces: `_collect_docker_network_guard_profile(*, toxiproxy_container: str) -> dict[str, str]` with `toxiproxy_ingress_ipv4` for Task 3.
- Produces: `_validated_backend_ipv4_by_role(containers: Mapping[str, Any], backend_network: Mapping[str, Any], ingress_network: Mapping[str, Any]) -> dict[str, str]` for Task 6 denial probes; Qdrant/Neo4j addresses remain in memory and never enter sanitized output.

- [ ] **Step 1: Extend Docker fixture data and write RED tests**

Add `EndpointID`, `IPAddress` and matching network membership data:

```python
toxiproxy_endpoint_id = "4" * 64
containers["toxiproxy"]["NetworkSettings"]["Networks"]["txnmem-ingress"].update(
    {
        "EndpointID": toxiproxy_endpoint_id,
        "IPAddress": "172.20.0.2",
        "IPPrefixLen": 16,
    }
)
ingress_network["Containers"][containers["toxiproxy"]["Id"]] = {
    "Name": "txnmem-toxiproxy",
    "EndpointID": toxiproxy_endpoint_id,
    "MacAddress": "02:42:ac:14:00:02",
    "IPv4Address": "172.20.0.2/16",
    "IPv6Address": "",
}
```

Assert the raw object contains the canonical address and hashes, while sanitizer output does not contain `172.20.0.2`. Add mutations for address outside subnet, address equal to gateway/network/broadcast, endpoint mismatch, prefix mismatch, duplicate ingress member, missing address and Qdrant/Neo4j attached to ingress.

- [ ] **Step 2: Verify RED**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_docker_backend_isolation_requires_proxy_only_ingress_network \
  tests.test_txnmem_topology_attestation.TopologyAttestationTests.test_sanitization_hashes_identity_and_binds_two_execution_phases -v
```

Expected: FAIL because v2 has no ingress endpoint identity.

- [ ] **Step 3: Implement raw Docker endpoint normalization**

In `_normalize_docker_backend_isolation`, validate both container and network views with `ipaddress.IPv4Address/IPv4Interface`. Add these raw fields:

```python
{
    "schema": "txnmem-provenance-backend-isolation-v3",
    "toxiproxy_ingress_ipv4": str(ingress_address),
    "toxiproxy_ingress_ipv4_sha256": hashlib.sha256(
        str(ingress_address).encode("utf-8")
    ).hexdigest(),
    "toxiproxy_ingress_endpoint_id_sha256": hashlib.sha256(
        endpoint_id.encode("utf-8")
    ).hexdigest(),
    "toxiproxy_ingress_membership_verified": True,
    "ingress_unique_workload_container_verified": True,
}
```

Retain every existing v2 field and exact container order. This task upgrades only the nested backend-isolation object; Task 4 upgrades the enclosing topology snapshot when structured counters are wired into the collector.

- [ ] **Step 4: Split raw and sanitized backend validators**

Add:

```python
def _validate_raw_backend_isolation(value: Any) -> dict[str, Any]:
    raw, _sanitized = _normalize_backend_isolation_pair(value)
    return raw


def _sanitize_backend_isolation(value: Any) -> dict[str, Any]:
    _raw, sanitized = _normalize_backend_isolation_pair(value)
    return sanitized
```

The sanitized result uses schema `txnmem-provenance-backend-isolation-sanitized-v3`, removes `toxiproxy_ingress_ipv4`, retains its SHA-256, and validates both membership booleans as exact `True`. Update collector imports to use `_validate_raw_backend_isolation`; reserve `_sanitize_backend_isolation` for Task 5.

- [ ] **Step 5: Produce one guard profile from the same observed endpoint**

Change `_collect_docker_network_guard_profile` to inspect the named Toxiproxy container and both networks, then return:

```python
{
    "backend_ipv4_subnet": str(backend_subnet),
    "ingress_ipv4_subnet": str(ingress_subnet),
    "backend_bridge_interface": backend_interface,
    "ingress_bridge_interface": ingress_interface,
    "toxiproxy_ingress_ipv4": raw_isolation["toxiproxy_ingress_ipv4"],
}
```

Before returning, require its hash to equal the raw backend-isolation hash produced from the same inspect batch.

Implement `_validated_backend_ipv4_by_role` from those already validated container/network attachments. It returns exact keys `qdrant/neo4j/toxiproxy_ingress`, rejects loopback/link-local/multicast/gateway/network/broadcast addresses, and is used only while the protected smoke guard is active.

- [ ] **Step 6: Verify GREEN and commit**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_txnmem_provenance_execution_collector \
  tests.test_txnmem_topology_attestation -v
git diff --check
```

Commit:

```bash
git add src/txnmem_provenance_execution_collector.py \
  src/txnmem_topology_attestation.py \
  tests/test_txnmem_provenance_execution_collector.py \
  tests/test_txnmem_topology_attestation.py
git commit -m "feat: bind exact Toxiproxy ingress identity"
```

### Task 3: Add the minimal Docker-proxy nftables trust rule and guard v3

**Files:**

- Modify: `src/txnmem_provenance_execution_collector.py:1493-1815,3531-3568,4400-4410`
- Modify: `src/txnmem_topology_attestation.py:221-239,1019-1068,1173-1190`
- Modify: `tests/test_txnmem_provenance_execution_collector.py:861-899,1160-1195`
- Modify: `tests/test_txnmem_topology_attestation.py:236-270,590-620,900-920`

**Interfaces:**

- Consumes: Task 2 guard profile with exact `toxiproxy_ingress_ipv4`.
- Produces: `_nft_guard_batch(table_name: str, *, runner_uid: int, backend_ipv4_subnet: str, ingress_ipv4_subnet: str, backend_bridge_interface: str, ingress_bridge_interface: str, toxiproxy_ingress_ipv4: str) -> str` with one narrow root ingress allow before all bridge denies.
- Produces: `_NftNetworkGuard.snapshot() -> txnmem-provenance-network-guard-v3`.
- Produces: `_validate_network_guard_backend_binding` that additionally compares `toxiproxy_ingress_ipv4_sha256`.

- [ ] **Step 1: Write the failing nft policy and mutation tests**

Call `_nft_guard_batch` with `toxiproxy_ingress_ipv4="172.20.0.2"` and assert exact order:

```python
root_ingress = (
    "meta skuid 0 ip daddr 172.20.0.2 "
    "tcp dport { 8474, 19000, 19001 } accept "
    'comment "txnmem-docker-proxy-ingress-allow"'
)
self.assertIn(root_ingress, batch)
self.assertLess(batch.index(root_ingress), batch.index("txnmem-host-bridge-deny"))
self.assertEqual(batch.count(" accept comment"), 3)
self.assertEqual(batch.count(" reject comment"), 5)
```

Add invalid address tests for IPv6, loopback, network/gateway/broadcast, an address outside ingress subnet and one inside backend subnet. Add a policy mutation proving subnet-wide root allow is absent.

- [ ] **Step 2: Verify RED**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_nft_network_guard_policy_allows_only_two_loopback_proxy_ports -v
```

Expected: FAIL because the exact root ingress rule is absent.

- [ ] **Step 3: Implement the exact destination rule**

After validating the address belongs only to the ingress subnet, insert:

```python
f"    meta skuid 0 ip daddr {ingress_address} "
"tcp dport { 8474, 19000, 19001 } accept "
"comment \"txnmem-docker-proxy-ingress-allow\"\n"
```

Place it after root loopback management allow and before runner/global deny rules. Do not add source-port, process-name, PID, cgroup or subnet-wide exceptions.

- [ ] **Step 4: Upgrade normalized nft and guard attestations to v3**

Require the new comment in `_normalize_nft_snapshot`. Add exact guard fields:

```python
{
    "schema": "txnmem-provenance-network-guard-v3",
    "allowed_root_ingress_ports": [8474, 19000, 19001],
    "root_ingress_destination_exact": True,
    "toxiproxy_ingress_ipv4_sha256": hashlib.sha256(
        self.toxiproxy_ingress_ipv4.encode("utf-8")
    ).hexdigest(),
}
```

Update `_NftNetworkGuard` to store the raw address only in memory. The snapshot and sanitized evidence retain only its hash. Include the exact address in `policy_sha256` through `_nft_guard_batch`.

- [ ] **Step 5: Add backend/guard cross-binding mutations**

In topology tests, mutate only `network_guard["toxiproxy_ingress_ipv4_sha256"]` and require `TopologyAttestationError("network guard is not bound to backend isolation")`. Also mutate the backend hash while keeping subnet/interface hashes fixed.

- [ ] **Step 6: Verify GREEN and commit**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_txnmem_provenance_execution_collector \
  tests.test_txnmem_topology_attestation -v
git diff --check
```

Commit:

```bash
git add src/txnmem_provenance_execution_collector.py \
  src/txnmem_topology_attestation.py \
  tests/test_txnmem_provenance_execution_collector.py \
  tests/test_txnmem_topology_attestation.py
git commit -m "fix: attest minimal Docker proxy ingress rule"
```

### Task 4: Replace the impossible zero baseline with A/B/final monotonic evidence

**Files:**

- Modify: `src/txnmem_provenance_execution_collector.py:2390-2818,3966-4180,4248-4616`
- Modify: `src/txnmem_topology_attestation.py:24-96,1330-1665`
- Modify: `tests/test_txnmem_provenance_execution_collector.py:112-218,470-550,1760-1935,1960-2105`
- Modify: `tests/test_txnmem_topology_attestation.py:360-560`

**Interfaces:**

- Consumes: Task 1 snapshots and Task 3 guard v3.
- Changes: `_snapshot_components(snapshot) -> tuple[roles, routes, proxy_counters, backend_isolation]`.
- Changes: `network_guard_activate() -> {network_guard, proxy_routes, proxy_counters, route_rearmed}`.
- Changes: `network_guard_finalize() -> {network_guard, proxy_routes, proxy_counters}`.
- Produces: launch v4 fields `proxy_counter_baseline_a`, `proxy_counter_baseline_b`, `proxy_route_rearm_verified`.
- Produces: completion v5 fields `proxy_counter_baseline_b_sha256`, `proxy_counter_final`, `proxy_counter_deltas`.

- [ ] **Step 1: Write RED tests for non-zero stable baselines**

Replace the zero-baseline test with snapshots whose four components are non-zero but equal across A/B:

```python
baseline_a = proxy_snapshot(
    "baseline_a",
    qdrant=(11, 13, 17, 19),
    neo4j=(23, 29, 31, 37),
)
baseline_b = proxy_snapshot(
    "baseline_b",
    qdrant=(11, 13, 17, 19),
    neo4j=(23, 29, 31, 37),
)
final = proxy_snapshot(
    "final",
    qdrant=(21, 33, 47, 69),
    neo4j=(73, 89, 101, 127),
)
```

Assert non-zero A/B is valid, any component change between A/B fails, any final component regression fails, and either backend with zero total delta fails.

- [ ] **Step 2: Verify RED**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_txnmem_provenance_execution_collector.ProvenanceExecutionCollectorTests.test_toxiproxy_attribution_baseline_requires_zero_and_exact_routes -v
```

Expected: FAIL because the current implementation requires absolute zero.

- [ ] **Step 3: Make topology v3 capture baseline A after health probes**

Set `SNAPSHOT_SCHEMA = "txnmem-provenance-topology-snapshot-v3"`, remove `proxy_counter_bytes` from raw role rows, and require the exact snapshot field set `schema/roles/proxy_routes/proxy_counters/backend_isolation`. Change `_snapshot_components` to return those four payload components after exact validation.

For `phase="before"`, enforce this order inside `collect_docker_topology_snapshot`:

```python
routes = prepare_isolated_toxiproxy_routes(
    toxiproxy_url,
    qdrant_proxy=qdrant_proxy,
    neo4j_proxy=neo4j_proxy,
)
qdrant_version, qdrant_rtt = probe_qdrant(qdrant_url)
neo4j_version, neo4j_rtt = probe_neo4j(neo4j_uri, neo4j_auth, runtime_snapshot)
proxy_counters = capture_toxiproxy_counter_snapshot(
    toxiproxy_url,
    phase="baseline_a",
    proxy_routes=routes,
)
```

Delete both zero checks. Remove `proxy_counter_bytes` from raw role rows; counters now exist only in structured snapshots.

- [ ] **Step 4: Rearm exact routes after guard activation and capture B**

Replace `_require_zero_toxiproxy_attribution_baseline` with:

```python
def _validate_toxiproxy_attribution_boundary(
    baseline_a: Mapping[str, Any],
    baseline_b: Mapping[str, Any],
    routes_a: Sequence[Mapping[str, Any]],
    routes_b: Sequence[Mapping[str, Any]],
) -> None:
    first = validate_proxy_counter_snapshot(baseline_a, expected_phase="baseline_a")
    second = validate_proxy_counter_snapshot(baseline_b, expected_phase="baseline_b")
    if proxy_counter_values(first) != proxy_counter_values(second):
        raise CollectorError("formal Toxiproxy attribution boundary was not quiescent")
    if list(routes_a) != list(routes_b):
        raise CollectorError("formal Toxiproxy routes changed at the attribution boundary")
```

The activation closure must call `network_guard.activate()`, then `prepare_isolated_toxiproxy_routes`, then capture B while the child gate is still blocked. Return exact key set:

```python
{
    "network_guard": guard_snapshot,
    "proxy_routes": routes_b,
    "proxy_counters": baseline_b,
    "route_rearmed": True,
}
```

- [ ] **Step 5: Capture final state before guard cleanup and derive deltas**

The finalization closure returns guard v3, final routes and a `phase="final"` snapshot. `_collect_execution_evidence` calls `derive_proxy_counter_deltas(baseline_b, final)` and writes the result before deactivating the guard. Require both role totals positive and the top-level total equal to their sum.

- [ ] **Step 6: Upgrade raw evidence schemas and exact field sets**

Set:

```python
RAW_LAUNCH_SCHEMA = "txnmem-provenance-execution-launch-raw-v4"
RAW_COMPLETION_SCHEMA = "txnmem-provenance-execution-completion-raw-v5"
```

Launch adds:

```python
"proxy_counter_baseline_a": baseline_a,
"proxy_counter_baseline_b": baseline_b,
"proxy_route_rearm_verified": True,
```

Completion adds:

```python
"proxy_counter_baseline_b_sha256": proxy_counter_payload_sha256(baseline_b),
"proxy_counter_final": final_counters,
"proxy_counter_deltas": proxy_deltas,
```

The completion baseline hash must bind the launch B snapshot; it is not accepted as an independent caller value.

- [ ] **Step 7: Add order assertions around the gated child**

Extend `_collect_execution_evidence` fixture callbacks to append these events:

```python
expected = [
    "topology_before",
    "guard_activate",
    "route_rearm",
    "baseline_b",
    "launch_write",
    "monitor_start",
    "gate_release",
    "child_exit",
    "monitor_finalize",
    "final_counters",
    "guard_verify",
    "guard_deactivate",
    "topology_after",
    "completion_write",
]
```

Instrument `FormalStore.write_json_exclusive` in the unit test rather than adding production event logging. Assert no candidate callback runs before `launch_write`.

- [ ] **Step 8: Verify GREEN and commit**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_txnmem_toxiproxy_metrics \
  tests.test_txnmem_provenance_execution_collector \
  tests.test_txnmem_topology_attestation -v
git diff --check
```

Commit:

```bash
git add src/txnmem_provenance_execution_collector.py \
  src/txnmem_topology_attestation.py \
  tests/test_txnmem_provenance_execution_collector.py \
  tests/test_txnmem_topology_attestation.py
git commit -m "fix: attest monotonic proxy attribution window"
```

### Task 5: Make sanitized topology v6 independently recompute attribution

**Files:**

- Modify: `src/txnmem_topology_attestation.py:24-430,1430-1815`
- Modify: `tests/test_txnmem_topology_attestation.py:360-940`
- Modify: `tests/test_txnmem_provenance_execution_collector.py:112-218`

**Interfaces:**

- Consumes: raw launch v4 and completion v5 only.
- Produces: `txnmem-topology-attestation-v6` with `proxy_counter_attribution` and sanitized backend isolation.
- Produces: `validate_registered_topology_attestation(attestation: Mapping[str, Any], *, expected_run_id_sha256: str, expected_config_sha256: str, expected_config_file_sha256: str, expected_workload_sha256: str, expected_environment_attestation_sha256: str, expected_evidence_manifest_sha256: str, expected_candidate_bundle_id: str, expected_candidate_operation_samples_sha256: str, expected_candidate_repetitions_sha256: str) -> dict[str, Any]` with v6 exact-key validation for promotion.

- [ ] **Step 1: Upgrade fixture documents and write RED mutation tests**

Construct A/B/final snapshots using Task 1 helpers. Add a sanitized top-level object with:

```python
{
    "schema": "txnmem-provenance-proxy-attribution-v1",
    "baseline_a_sha256": baseline_a["snapshot_sha256"],
    "baseline_b_sha256": baseline_b["snapshot_sha256"],
    "final_sha256": final["snapshot_sha256"],
    "boundary_values_equal": True,
    "route_rearmed": True,
    "qdrant_delta_bytes": qdrant_delta,
    "neo4j_delta_bytes": neo4j_delta,
    "toxiproxy_delta_bytes": qdrant_delta + neo4j_delta,
    "component_deltas_sha256": hashlib.sha256(
        canonical_json_bytes(raw_deltas)
    ).hexdigest(),
}
```

Mutation table must independently change every snapshot hash, one A/B component, one final component, one stored delta, the completion B hash, route order, ingress hash, membership boolean and each exact-key set.

- [ ] **Step 2: Verify RED**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_txnmem_topology_attestation -v
```

Expected: FAIL because v5 assumes launch counters are zero and reads totals from role rows.

- [ ] **Step 3: Recompute all counter facts from raw snapshots**

In `sanitize_topology_attestation`:

```python
baseline_a = validate_proxy_counter_snapshot(
    launch["proxy_counter_baseline_a"], expected_phase="baseline_a"
)
baseline_b = validate_proxy_counter_snapshot(
    launch["proxy_counter_baseline_b"], expected_phase="baseline_b"
)
final = validate_proxy_counter_snapshot(
    completion["proxy_counter_final"], expected_phase="final"
)
if proxy_counter_values(baseline_a) != proxy_counter_values(baseline_b):
    raise TopologyAttestationError("proxy attribution boundary changed")
deltas = derive_proxy_counter_deltas(baseline_b, final)
if deltas != completion["proxy_counter_deltas"]:
    raise TopologyAttestationError("proxy counter deltas were not independently derived")
```

Verify completion’s B hash against the launch snapshot and require positive Qdrant/Neo4j totals. Never copy a collector-provided equality boolean.

- [ ] **Step 4: Derive sanitized roles from structured snapshots**

For client, write zeros. For Qdrant and Neo4j, use B/final role totals and derived delta. For Toxiproxy, use top-level totals. Keep service versions, host/listener hashes and RTT values from raw roles.

- [ ] **Step 5: Sanitize the backend address and bind guard v3**

Use `_sanitize_backend_isolation` so the literal ingress IPv4 cannot enter the v6 document. Require guard and backend ingress hashes equal in both raw phases and in the final sanitized validator.

Make `_validate_sanitized_shape` call `_validate_sanitized_backend_isolation`, while collector/raw paths call `_validate_raw_backend_isolation`; a sanitized-v3 object and a raw-v3 object are never accepted interchangeably.

- [ ] **Step 6: Set v6 exact schemas and reject all legacy combinations**

Set:

```python
SANITIZED_SCHEMA = "txnmem-topology-attestation-v6"
```

The v6 top-level exact fields add `proxy_counter_attribution`. Direct validation must reject launch v3, completion v4, sanitized v5, backend v2 and guard v2. Historical files remain untouched and are not rewritten.

- [ ] **Step 7: Verify GREEN and commit**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_txnmem_topology_attestation \
  tests.test_txnmem_provenance_execution_collector \
  tests.test_txnmem_toxiproxy_metrics -v
git diff --check
```

Commit:

```bash
git add src/txnmem_topology_attestation.py \
  tests/test_txnmem_topology_attestation.py \
  tests/test_txnmem_provenance_execution_collector.py
git commit -m "feat: independently verify proxy attribution v6"
```

### Task 6: Add a protected production same-path smoke with no candidate output

**Files:**

- Create: `src/txnmem_formal_smoke.py`
- Create: `scripts/run_formal_provenance_smoke.sh`
- Create: `tests/test_txnmem_formal_smoke.py`
- Modify: `src/txnmem_provenance_runner.py:1-95`
- Modify: `src/txnmem_formal_controller.py:20-55,350-445`
- Modify: `scripts/install_formal_provenance_runtime.sh:90-155`
- Modify: `scripts/run_cross_host_provenance_performance.sh:4-70`
- Modify: `tests/test_txnmem_formal_controller.py:90-215`
- Modify: `tests/test_real_backend_script.py:60-98`
- Modify: `infra/real_backend/README.md:1-20`

**Interfaces:**

- Produces: `collect_formal_smoke(*, project_root: Path, out_path: Path, qdrant_url: str, neo4j_uri: str, toxiproxy_url: str, neo4j_password: str, _controller_context: Mapping[str, Any] | None = None) -> dict[str, Any]` with schema `txnmem-formal-provenance-smoke-v1`.
- Produces: runner mode `provenance-smoke` returning `txnmem-provenance-smoke-child-receipt-v1` through the existing completion FD after gate release.
- Produces: controller action `smoke` and wrapper `scripts/run_formal_provenance_smoke.sh OUT_JSON`.
- Guarantees: no call to `_prepare_formal_run_workspace`, `_seal_candidate_tree`, candidate material loader or promotion registry.

- [ ] **Step 1: Write RED tests for smoke action and no-candidate boundary**

Mock network/process dependencies and assert the exact event order:

```python
expected_events = [
    "source_verified",
    "topology_observed",
    "routes_prepared",
    "health_probed",
    "baseline_a",
    "child_gated",
    "guard_activated",
    "routes_rearmed",
    "baseline_b",
    "root_management_success",
    "root_data_denied",
    "direct_backend_denied",
    "forward_path_denied",
    "child_released",
    "runner_qdrant_success",
    "runner_neo4j_success",
    "final_counters",
    "guard_stable",
    "guard_removed",
    "workspace_removed",
]
self.assertEqual(events, expected_events)
self.assertFalse(any(path.name == "candidate" for path in smoke_root.rglob("*")))
```

Add failure tests for every false probe, A/B drift, zero backend delta, changed guard hash and cleanup failure. A failed smoke must not write a success report.

- [ ] **Step 2: Verify RED**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_txnmem_formal_smoke -v
```

Expected: FAIL because the smoke module does not exist.

- [ ] **Step 3: Add the gated runner smoke mode**

After the existing ready/gate protocol, accept either `provenance-performance` or `provenance-smoke`. For smoke, use the locked Neo4j runtime and exact loopback endpoints, then write:

```python
{
    "schema": "txnmem-provenance-smoke-child-receipt-v1",
    "qdrant_proxy_ok": True,
    "neo4j_proxy_ok": True,
}
```

The runner performs one Qdrant `/readyz` request and one Neo4j `RETURN 1` transaction. It returns nonzero if either fails, if arguments are duplicated, or if the receipt exceeds the existing 65536-byte limit.

- [ ] **Step 4: Implement the protected smoke orchestrator**

Use the same `_validate_formal_controller_context`, locked runtime snapshot, `_start_gated_candidate`, route helpers, Docker normalizer, `_NftNetworkGuard` and metrics helpers as formal measurement. The success report exact fields are:

```python
{
    "schema": "txnmem-formal-provenance-smoke-v1",
    "source_commit": source_commit,
    "source_manifest_sha256": source_manifest_sha256,
    "backend_isolation_sha256": backend_isolation_sha256,
    "network_guard_sha256": network_guard_sha256,
    "toxiproxy_image_manifest_digest": toxiproxy_manifest_digest,
    "toxiproxy_version": "2.5.0",
    "proxy_metrics_series_count": 8,
    "baseline_values_equal": True,
    "route_rearmed": True,
    "root_management_succeeded": True,
    "runner_qdrant_succeeded": True,
    "runner_neo4j_succeeded": True,
    "non_runner_proxy_blocked": True,
    "direct_backend_blocked": True,
    "forwarded_bridge_blocked": True,
    "qdrant_delta_positive": True,
    "neo4j_delta_positive": True,
    "guard_stable": True,
    "guard_removed": True,
    "candidate_created": False,
    "report_sha256": report_sha256,
}
```

Compute `report_sha256` over canonical JSON bytes of the exact report without the `report_sha256` field, then append that field. `validate_formal_smoke_report` repeats this operation and rejects noncanonical or extra keys.

Probe non-runner data denial with root against loopback data ports after the root management check. Probe direct backend denial with validated raw backend IPv4 addresses. Probe forward denial from an ephemeral default-bridge container using an already present pinned runtime image; prohibit registry pulls and remove the container in `finally`. The committed report contains booleans/hashes only.

- [ ] **Step 5: Add controller dispatch and approved source closure**

Map:

```python
module_name = {
    "measure": "txnmem_provenance_execution_collector",
    "smoke": "txnmem_formal_smoke",
    "material": "txnmem_experiment",
    "attest": "txnmem_topology_attestation",
    "promote": "txnmem_experiment",
}.get(action)
```

Pass `_controller_context` for both `measure` and `smoke`. Add `scripts/run_formal_provenance_smoke.sh` to `_FORMAL_AUXILIARY_PATHS` in the controller and installer, and add `src/txnmem_formal_smoke.py` to both required source sets.

- [ ] **Step 6: Add wrappers without exposing credentials**

`scripts/run_formal_provenance_smoke.sh` requires one absolute out-of-repository path, requires `TXNMEM_NEO4J_PASSWORD`, starts from `/usr/bin/env -i`, and calls:

```bash
/usr/bin/python3 -I -S -B \
  /opt/txnmem-formal-controller/txnmem_formal_controller.py \
  --project-root "$PWD" smoke --out "$smoke_out"
```

Add `smoke` to `run_cross_host_provenance_performance.sh` without changing the existing four actions.

- [ ] **Step 7: Update README boundaries**

Document that ordinary `run_real_backend_smoke.sh` remains diagnostic, while only the installed-controller smoke proves the formal ingress/attribution gate. State that neither smoke is a production latency result.

- [ ] **Step 8: Verify module, controller, scripts and full local suite**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_txnmem_formal_smoke \
  tests.test_txnmem_formal_controller \
  tests.test_real_backend_script \
  tests.test_txnmem_provenance_execution_collector \
  tests.test_txnmem_topology_attestation \
  tests.test_txnmem_toxiproxy_metrics -v
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest discover -s tests -v
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B src/txnmem_claim_audit.py audit \
  --root . --ledger configs/paper_claims.json --out /private/tmp/txnmem-proxy-claim-audit.json
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B src/txnmem_artifact_audit.py --root .
PYTHONPYCACHEPREFIX=/private/tmp/txnmem-proxy-pycache python3 -m py_compile \
  src/txnmem_toxiproxy_metrics.py src/txnmem_formal_smoke.py \
  src/txnmem_provenance_execution_collector.py src/txnmem_topology_attestation.py \
  src/txnmem_provenance_runner.py src/txnmem_formal_controller.py
git diff --check
```

Expected: full suite PASS, claim audit all claims pass, artifact audit reports zero violations, compilation succeeds, and no new unexplained skip appears.

- [ ] **Step 9: Request independent review and commit**

Review scope: compose argv, parser closure, raw/sanitized schema separation, root trust statement, rule order, A/B/final race closure, fail-closed cleanup and absence of candidate writes. Resolve every Critical/Important finding and rerun Step 8.

Commit:

```bash
git add src/txnmem_formal_smoke.py src/txnmem_provenance_runner.py \
  src/txnmem_formal_controller.py scripts/install_formal_provenance_runtime.sh \
  scripts/run_formal_provenance_smoke.sh scripts/run_cross_host_provenance_performance.sh \
  tests/test_txnmem_formal_smoke.py tests/test_txnmem_formal_controller.py \
  tests/test_real_backend_script.py infra/real_backend/README.md
git commit -m "feat: add protected provenance ingress smoke"
```

### Task 7: Deploy the exact commit and pass the live production same-path smoke

**Files:**

- Create after validation: `results/provenance_performance_formal_v3/preflight_smoke.json`
- Remote-only: a fresh clean clone, protected controller/runtime installation, private raw smoke workspace and service logs.

**Interfaces:**

- Consumes: Tasks 1–6 at one clean source commit.
- Produces: one sanitized smoke report whose `source_commit` equals the installed approved commit.
- Gate: Task 8 cannot start unless every report boolean is exact `True` except `candidate_created`, which must be exact `False`.

- [ ] **Step 1: Freeze and push the implementation commit**

Run locally:

```bash
git status --short --branch
git rev-parse HEAD
git push origin codex/evidence-scale-up
```

Require a clean worktree and equality of local HEAD and `origin/codex/evidence-scale-up`.

- [ ] **Step 2: Create a fresh exact remote clone**

On the authorized server, derive `approved_commit` from the pushed branch, clone into a new versioned directory, checkout that exact object, and require:

```bash
approved_commit=$(git ls-remote https://github.com/paletteeee/agent-da.git \
  refs/heads/codex/evidence-scale-up | awk '{print $1}')
remote_run_root="${TXNMEM_REMOTE_RUN_ROOT:?set to the approved remote run root}"
remote_clone="${remote_run_root}/evidence-scale-up-${approved_commit:0:7}"
git clone --branch codex/evidence-scale-up --single-branch \
  https://github.com/paletteeee/agent-da.git "$remote_clone"
cd "$remote_clone"
git checkout --detach "$approved_commit"
git status --porcelain
git rev-parse HEAD
git fsck --no-dangling
```

Expected: empty status, exact approved commit, no repository integrity failure. Do not reuse either historically dirty repository.

- [ ] **Step 3: Run the exact-clone server compatibility suite**

Before touching services, run the collector/topology/metrics/controller/smoke test modules and then the full test suite from the exact clone under the server’s Python 3.10.12. Require no failure and no new unexplained skip; this is the protected-server compatibility gate.

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_txnmem_toxiproxy_metrics \
  tests.test_txnmem_provenance_execution_collector \
  tests.test_txnmem_topology_attestation \
  tests.test_txnmem_formal_controller \
  tests.test_txnmem_formal_smoke -v
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest discover -s tests -v
```

- [ ] **Step 4: Recreate services and attest final Toxiproxy argv**

Run Docker Compose from the exact clone with the private Neo4j credential already present in the authorized shell. Force recreation so the new command is active. Verify container inspect reports the pinned digest and argv `-host=0.0.0.0 -proxy-metrics`, then require `/metrics` to return HTTP 200 rather than 404. Task 7 Step 6 creates routes and health traffic before requiring the exact eight series.

```bash
test -n "${TXNMEM_NEO4J_PASSWORD:?TXNMEM_NEO4J_PASSWORD is required}"
docker compose -f infra/real_backend/docker-compose.yml up -d --force-recreate
docker inspect txnmem-toxiproxy
curl --fail --silent --show-error http://127.0.0.1:8474/metrics
```

- [ ] **Step 5: Install the exact protected controller/runtime**

Run `scripts/install_formal_provenance_runtime.sh` as root with the exact clone and approved commit. Verify installed manifest source commit, file count, owner/mode closure and wheel hashes. The installed controller and manifest must not be writable by group or other.

```bash
sudo --preserve-env=TXNMEM_FORMAL_WHEEL_SOURCE \
  scripts/install_formal_provenance_runtime.sh "$remote_clone" "$approved_commit"
sudo stat -Lc '%u:%g:%a:%n' \
  /opt/txnmem-formal-controller/txnmem_formal_controller.py \
  /opt/txnmem-formal-controller/approved_source_manifest.json
```

- [ ] **Step 6: Run the protected smoke once**

Create a root-owned `0700` output directory outside Git and invoke `scripts/run_formal_provenance_smoke.sh` with its report path. Never echo the Neo4j credential. The smoke itself must test management success, runner Qdrant/Neo4j success, non-runner/data denial, direct backend denial, forward denial, A/B equality, positive deltas, stable guard and cleanup.

```bash
sudo install -d -o root -g root -m 0700 /var/lib/txnmem-formal/smoke-output
sudo --preserve-env=TXNMEM_NEO4J_PASSWORD \
  scripts/run_formal_provenance_smoke.sh \
  /var/lib/txnmem-formal/smoke-output/preflight_smoke.json
```

- [ ] **Step 7: Verify cleanup and sanitize-copy the report**

Require no matching nft table, no smoke child, no ephemeral probe container, no candidate directory and no leftover writable bootstrap export. Copy only the sanitized report to:

```text
results/provenance_performance_formal_v3/preflight_smoke.json
```

Validate `report_sha256`, source commit, image digest, series count, exact key set and all gate booleans locally. Run artifact audit before staging.

- [ ] **Step 8: Commit the smoke evidence**

```bash
git add results/provenance_performance_formal_v3/preflight_smoke.json
git commit -m "evidence: record protected proxy ingress smoke"
git push origin codex/evidence-scale-up
```

The commit message and report must contain no remote address, username, password, nonce path or raw container IP.

### Task 8: Run, attest and promote the 3×5×30 formal backend matrix

**Files:**

- Remote-only raw: identity-derived candidate, launch v4, completion v5, material and private service logs.
- Create locally after promotion: `results/provenance_performance_formal_v3/formal/**`
- Modify after independent digest review: `src/txnmem_topology_attestation.py` topology digest registry.

**Interfaces:**

- Consumes: Task 7 passing smoke and the already pre-registered remote-private formal nonce.
- Produces: 15 cells, 450 repetitions, 14,400 operation samples, sanitized topology v6, promoted formal aggregate and curve inputs.
- Gate: no aggregate includes partial/unknown repetition or failed state/readback validation.

**Failure-handling addendum (2026-08-23):** The first registered measurement
identity was consumed by a fail-closed candidate during serial preload after
Neo4j authority committed one record whose Qdrant projection did not complete.
That identity and its remote-private evidence are retained and must never be
reused or promoted. Before registering a fresh identity, freeze performance
schema v2, use the same canonical write/CAS path with fixed DAG-layer
parallelism, share the real service clients across repetitions, cache the exact
Qdrant collection readiness result only after verifying vector size/distance,
synchronize backend-local preload bookkeeping, configure only the shared
Neo4j performance client with `notifications_min_severity=OFF` so server
notification logging cannot contaminate timed operations while ordinary
clients retain the driver default, and permit one setup-only
reconciliation only after matching cross-store readback. Formal validation must
bind the layered worker limit to the frozen graph. Repetition evidence must
separately record preload method/parallelism, setup repair budget/count and
elapsed time, while measured backend and driver retries remain exactly zero.
Run real-service 100/1,000/10,000-node preload pilots before the next protected
measurement.

- [ ] **Step 1: Freeze the formal measurement source commit**

Use the clean post-smoke branch commit, reinstall the controller against that exact object, and rerun source/runtime/compose/GPU-independent preflight. Confirm the pre-registered run identity and nonce hash without printing nonce bytes.

- [ ] **Step 2: Execute measure through the installed controller**

Use the existing `run_cross_host_provenance_performance.sh measure` action with the identity-derived candidate root and out-of-tree launch/completion paths. Do not edit source, restart containers or change nft rules while the child runs.

Expected raw schemas:

```text
txnmem-provenance-execution-launch-raw-v4
txnmem-provenance-execution-completion-raw-v5
```

- [ ] **Step 3: Validate candidate material before attestation**

Run the `material` action and require:

```python
assert material["matrix_cell_count"] == 15
assert material["repetition_count"] == 450
assert material["operation_sample_count"] == 14_400
assert set(material["observed_service_versions"]) == {
    "qdrant", "neo4j", "toxiproxy"
}
```

Also require 30 successful repetition records for every graph-size/concurrency cell and exact unique namespaces.

- [ ] **Step 4: Generate and independently inspect sanitized topology v6**

Run `attest`, verify all raw hashes, the candidate seal, A/B equality, final deltas, ingress binding and cleanup facts. Independently recompute the sanitized attestation digest from canonical bytes.

- [ ] **Step 5: Register only the reviewed topology digest**

Add the exact run-hash to `FORMAL_PROVENANCE_TOPOLOGY_ATTESTATION_SHA256_BY_RUN`, write a test that accepts that exact digest and rejects a one-bit mutation, run the topology and promotion tests, then commit:

```bash
git add src/txnmem_topology_attestation.py tests/test_txnmem_topology_attestation.py
git commit -m "evidence: register reviewed formal topology v6"
git push origin codex/evidence-scale-up
```

Reinstall the protected controller from this registration commit before promotion.

- [ ] **Step 6: Promote without rerunning measurement**

Run `promote` against the sealed candidate, reviewed sanitized topology and expected bundle ID. Promotion must consume the original measured bytes and must not invoke the performance runner.

- [ ] **Step 7: Copy only sanitized formal outputs locally**

Copy aggregate JSON/CSV, sanitized per-repetition rows, figures, run manifest and topology v6. Leave operation payloads, credentials, raw launch/completion, nonce and database volumes on the server.

- [ ] **Step 8: Recompute formal statistics locally**

For every cell require 30 repetitions, success-only throughput, ordered p50/p95/p99, deterministic repetition-cluster bootstrap CI, graph/node/edge hash closure and post-run provenance readback. Compare recomputed aggregates byte-for-byte with promoted outputs.

- [ ] **Step 9: Run audits and commit formal evidence**

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_txnmem_provenance_performance \
  tests.test_txnmem_topology_attestation \
  tests.test_txnmem_claim_audit \
  tests.test_txnmem_artifact_audit -v
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B src/txnmem_artifact_audit.py --root .
git diff --check
git add results/provenance_performance_formal_v3/formal
git commit -m "data: add attested formal backend performance matrix"
git push origin codex/evidence-scale-up
```

### Task 9: Activate only supported paper claims and rebuild submission artifacts

**Files:**

- Modify: `configs/paper_claims.json`
- Modify: `docs/paper/evidence_map_zh.md`
- Modify: `docs/formal_paper_task_status_zh.md`
- Modify: `docs/paper/txnmem_ccfa_draft_zh.md`
- Modify if required by existing result routing: `scripts/build_txnmem_paper_figures.py`
- Regenerate through existing builders: formal paper DOCX/PDF/figures.

**Interfaces:**

- Consumes: promoted Task 8 outputs only.
- Produces: paper tables/curves stating single-host Docker 29, real Qdrant/Neo4j/Toxiproxy and the exact 15×30 matrix boundary.
- Prohibits: claims of cross-host database transactions, production availability, linearizability or general distributed scalability.

- [ ] **Step 1: Write claim-audit RED tests for the new evidence IDs**

Require each active claim to bind the formal run manifest, topology v6, aggregate hash, 15-cell count, 450-repetition count and service image digests. Require any diagnostic smoke file to remain non-claim evidence.

- [ ] **Step 2: Verify RED**

Run:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest \
  tests.test_txnmem_claim_audit tests.test_txnmem_paper_projection -v
```

Expected: FAIL until claim routes point to the promoted v3 results.

- [ ] **Step 3: Update evidence map, limitations and performance section**

Report graph sizes `100/1000/10000`, concurrency `1/2/4/8/16`, 30 repetitions per cell, p50/p95/p99, successful throughput and cluster-bootstrap 95% CI. Explain that proxy bytes are B-to-final deltas and that Docker-proxy ingress is root-trusted only for the exact attested destination.

- [ ] **Step 4: Rebuild figures and paper artifacts**

Use the existing figure and DOCX builders. Render the DOCX/PDF to page PNGs, inspect every page for clipping, table overflow, missing CJK glyphs, stale numbers and caption/reference mismatches, then rerender after each correction.

- [ ] **Step 5: Run final release verification**

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest discover -s tests -v
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B src/txnmem_claim_audit.py audit \
  --root . --ledger configs/paper_claims.json --out /private/tmp/txnmem-final-claim-audit.json
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B src/txnmem_artifact_audit.py --root .
git diff --check
git status --short
```

Require all tests/audits pass, artifact violations equal zero, every manuscript number traceable to one active evidence ID, and no sensitive value in tracked additions.

- [ ] **Step 6: Request final review, commit and push**

Resolve all Critical/Important review findings and rerun Step 5. Then:

```bash
git add configs/paper_claims.json docs/paper/evidence_map_zh.md \
  docs/formal_paper_task_status_zh.md docs/paper/txnmem_ccfa_draft_zh.md \
  scripts/build_txnmem_paper_figures.py
git commit -m "docs: publish attested backend performance evidence"
git push origin codex/evidence-scale-up
```

Report separately: implemented code, passed same-path smoke, promoted formal matrix, paper evidence activated, and any unrelated public-benchmark/GPU tasks that remain incomplete.
