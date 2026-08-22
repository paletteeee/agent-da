# External-memory baseline protocol

This protocol defines how the benchmark's operation trace is replayed against
Mem0 and LangGraph Store without representing unsupported behavior as a native
backend feature. The adapter records the benchmark `instance_id`, `txn_id`,
operation step, actor, scope, policy version, provenance, and status in each
backend record or Store value.

The required imports are `from mem0 import Memory`,
`from langgraph.store.memory import InMemoryStore`, and
`from langgraph.store.postgres import PostgresStore`. `InMemoryStore` is used
only by native adapter tests; `PostgresStore` is the persistent LangGraph Store
implementation for a configured formal run. The formal host also pins
`psycopg-binary` because the base PostgreSQL package otherwise has no usable
`libpq` wrapper there. It still requires a reachable PostgreSQL service;
package installation and import success do not create or configure that
service.

## Operation mapping

`controlled-baseline action` means deterministic single-operation adapter
bookkeeping, logged with the trace, rather than a claimed native transaction or
provenance API. It must not buffer writes, replay a commit, or traverse a
provenance closure. `capability_absent` means that no native call has the
required benchmark semantics. `runtime_failure` means an otherwise selected
native call could not execute (for example, because its service, network, or
configuration is unavailable); it is not evidence of a missing capability.

| TxnMemBench operation | Mem0 adapter mapping | LangGraph Store adapter mapping | Semantics retained / limitation |
| --- | --- | --- | --- |
| `begin_txn` | scheduling marker only; no backend call | scheduling marker only; no backend call | Neither system is credited with a transaction. |
| `read` | `Memory.get(sdk_uuid)` followed by a controlled agent/scope/status check | `store.get(namespace, key)` followed by a controlled scope and status check | Direct retrieval is native; authorization remains benchmark-controlled. |
| `search` | `Memory.search(query, filters={"user_id": instance_namespace})`, then controlled agent/scope/status filtering | `store.search(namespace_prefix, query=...)`, then controlled scope/status filtering | Retrieval is native; filtering is recorded so scores do not infer access control. |
| `get_by_id` | `Memory.get(sdk_uuid)` followed by the same controlled agent/scope/status check as `read` | `store.get(namespace, key)` followed by the same controlled scope/status check as `read` | Direct-ID access cannot bypass the benchmark policy check. |
| `write` | `Memory.add(text, user_id=..., agent_id=..., metadata=..., infer=False)` with benchmark IDs and non-identity lineage metadata | `store.put(namespace, key, value, index=...)` | Native individual-record write; no multi-record atomicity claim. |
| `supersede` | controlled-baseline action: ordered `Memory.update(sdk_uuid, text=..., metadata=...)` calls: old then new | controlled-baseline action: `store.put` updated values for old and new keys | `capability_absent` for atomic supersession; the adapter exposes the ordered writes. |
| `propagate` | `capability_absent`; no native provenance-propagation call | `capability_absent`; no native provenance-propagation call | The adapter does not create or traverse derived records to emulate a graph. |
| `invalidate` | controlled-baseline action: `Memory.update` the named source record metadata with `status: invalid` | controlled-baseline action: `store.put` the named source value with `status: invalid` | Single-record update only. Native recursive invalidation is `capability_absent`; the adapter does not traverse descendants. |
| `commit` | scheduling marker only; no backend call | scheduling marker only; no backend call | Neither system is credited with an atomic commit. |

### Verified Mem0 2.0.18 details

The Mem0 adapter uses the installed OSS SDK lazily, so importing the
dependency-free benchmark does not require `mem0ai`. Every formal add uses
`infer=False`; it therefore makes no LLM call. Pinned `Memory.add` returns
`{"results": [{"id": SDK_UUID, "memory": text, "event": "ADD", ...}]}`.
`Memory.get(SDK_UUID)` returns `None` or a dictionary containing `id`,
`memory`, promoted `agent_id`, and benchmark metadata under `metadata`.
`search` and `get_all` return `{"results": [...]}` when passed
`filters={"user_id": namespace}`.
`update` and `delete` return a success-message dictionary. Unknown response
envelopes, missing metadata, and conflicting metadata are `runtime_error`s
containing the instance and operation identifiers but not stored content.

Mem0 UUIDs are SDK-owned. The adapter stores each benchmark `memory_id` under
metadata key `benchmark_memory_id`, with `instance_id`, `scope`, `entity_id`,
`attribute`, `status`, `policy_version`, `supersedes_id`, and `derived_from`.
Mem0 2.0.18 promotes identity fields, so `agent_id` is passed as the native
top-level add argument, is verified from each returned record, and is not
placed in update metadata. The adapter keeps an in-process
benchmark-ID-to-SDK-UUID map (and its reverse) and never passes a benchmark ID
to native `get`, `update`, or `delete`. A repeated benchmark ID uses native
`update` of that mapped UUID, so the normalized output has one record rather
than duplicate benchmark IDs.

Each adapter run uses its own `user_id` namespace and supplies it as the
native `filters` value to every `search` and `get_all`. Native direct-ID gets
are followed by the same controlled agent, scope, and visible-status check as
search; denied and exposed benchmark IDs are recorded in the trace/metrics.
This is benchmark bookkeeping, not a claim that Mem0 implements those access
controls.

`begin_txn` and `commit` are trace markers only. Supersession is two ordered,
non-atomic updates. Invalidation updates only the named source record; no
adapter provenance traversal or dependency closure is added. A crash stops
the trace immediately. Recovery is reported as `capability_absent` unless the
injected factory explicitly declares persistent reopening support. Before that
factory is reopened, the adapter closes both the original Memory and embedded
Qdrant client, then snapshots the reopened backend exactly once; it does not
infer or fabricate durable recovery from an on-disk path.

For formal local runs, `deterministic_mem0_factory(root)` must receive a
caller-owned path under `/data/agent-da-results/mem0/`. It sets
`MEM0_TELEMETRY=false` before importing Mem0, creates a per-instance embedded
Qdrant collection plus a separate per-instance `history.db`, and replaces the
SDK-required inert client with a local, content-sensitive 64-dimensional
`sha256-counter-v1` embedding. Its unused LLM configuration is local vLLM at
`http://127.0.0.1:9`; the embedding bootstrap uses literal
`not-a-secret-bootstrap-only`, a non-secret placeholder needed only for SDK
construction. `infer=False` prevents an LLM call. Formal native tests must
start a new process with telemetry set before any Mem0 import. Factory users
and native tests close the embedded Qdrant client after each run. This local
embedding is deterministic for reproducibility, not a claim of semantic
equivalence to a production embedding model or of retrieval-quality parity.

### Verified LangGraph Store details

The pinned synchronous Store API exposes `put(namespace, key, value, index=None)`,
`get(namespace, key)`, `search(namespace_prefix, query=..., filter=...)`, and
`delete(namespace, key)`. The benchmark has no delete operation, so replay
invokes only `put`, `get`, and `search`; `delete` availability is verified by
the native Store API test but is not credited as a replay capability. Search
passes the verified
`{"status": {"$ne": "invalid"}}` native filter, then retains the benchmark's
controlled `{active, pending}` visibility check.
It normalizes native `Item` and `SearchItem` objects through their `.value`
dictionary before passing any result to the independent oracle. Each record is
stored under namespace `(experiment_run_id, instance_id, agent_id,
shared_scope)`, with `memory_id` as its key and the normalized memory object as
its value. Thus a Store search or direct get in a different shared scope cannot
return the record in the source scope.

An `invalidate` operation is a native `get` followed by one `put` of the named
record with `status: invalid`; it does not delete or inspect descendants. A
`supersede` operation is two ordered native `put` updates—old record first,
then new record—and is explicitly not credited as atomic. `begin_txn` and
`commit` remain trace markers, so no adapter write buffering or commit-time
policy revalidation is introduced.

`InMemoryStore` is ephemeral: a crash-recovery row from this unit-test backend
is recorded as `capability_absent`. A configured persistent Store whose factory
or native call fails is reported by the adapter contract as `runtime_error`
(called `runtime_failure` in this protocol's environment discussion), never as
an invariant violation. If a persistent configuration cannot be initialized,
the formal runner may retain an in-memory replay for non-recovery workloads;
recovery-only claims in that fallback are `unsupported_mapping`, rather than a
claim of durable recovery. The capability helper returns a deterministic tuple
of `CapabilitySupport` rows so the runner can report native put/get/search separately
from unavailable atomic commit, provenance, invalidation, and recovery
semantics.

## Reproducible environment

On the isolated remote worktree, install the pinned direct dependencies and
verify the dependency contract before replaying a trace:

```bash
cd /data/agent-da-baselines-worktree
python3 -m venv .venv-baselines
.venv-baselines/bin/python -m pip install --upgrade pip
.venv-baselines/bin/pip install -r requirements-baselines.txt
.venv-baselines/bin/python -m unittest tests.test_external_dependencies -v
.venv-baselines/bin/python -m unittest discover -s tests -v
.venv-baselines/bin/pip freeze
```

Save the resulting `pip freeze` output in the formal environment manifest for
each run. A successful import alone does not make the LangGraph baseline
operational: if the required PostgreSQL service is unavailable, record
`runtime_failure`, not `capability_absent`. The Store's missing transaction and
recursive-provenance semantics remain `capability_absent` even when the service
is healthy.
