# InferTxn Design

## Goal and scenario

InferTxn is a runnable Python prototype of a transactional metadata database for prefill-decode disaggregated LLM inference. It atomically migrates an active decode request across nodes while changing three independently sharded records: KV-cache location, request routing, and request ownership/epoch.

The prototype stores metadata only, not KV tensors. A successful migration guarantees that route, cache location, and request ownership identify the same node and epoch. A failed migration leaves the previously committed state visible.

## Architecture

```text
Inference scheduler -> 2PC coordinator -> route shard
                                      -> KV metadata shard
                                      -> request-state shard
```

Each shard owns an MVCC store. Tests use direct in-process transports for determinism. The demonstration can expose each participant as an independent standard-library HTTP service without coupling transaction logic to HTTP.

## MVCC

Each key retains immutable `(commit_ts, value)` versions. A shared thread-safe logical clock assigns transaction start and commit timestamps. Reads return the newest committed version at or before the transaction snapshot. During prepare, first-committer-wins validation rejects a key if a newer version exists, while prepared-key locks reject concurrent writers. This provides snapshot isolation, prevents dirty reads and lost updates, and does not claim full serializability.

## Two-phase commit

The coordinator sends prepare requests containing the transaction snapshot and shard writes. If every participant votes yes, it flushes a COMMIT decision with a global commit timestamp to a JSONL log before sending commit. Otherwise it records ABORT and releases prepared state. Prepare, commit, and abort are idempotent. Recovery reloads durable decisions and resends them to participants after lost responses.

## Inference migration

A migration from node A to B increments the request epoch and atomically writes:

- route: `{decode_node: B, epoch: e+1}`;
- KV metadata: `{location: B, state: ready, epoch: e+1}`;
- request state: `{owner: B, phase: decoding, epoch: e+1}`.

The target cache must first be persisted as a ready `kv-copy/<request>/<node>` staging record bound to the current source epoch, cache version, and generated-token position. Migration validates it and marks it consumed in the same transaction as the canonical metadata update. If decoding advances after the copy, migration rejects the stale staged cache. Decode progress updates carry an epoch; after migration, updates from A are rejected as stale. Concurrent migrations conflict on the same keys, so at most one commits. A coordinator visibility watermark prevents database readers from observing participant-by-participant commit application. Recovery advances the logical clock beyond every durable commit timestamp before accepting new transactions.

## Failure model

The prototype injects prepare rejection and lost commit acknowledgements. A rejection aborts every prepared participant. A lost acknowledgement leaves a durable commit decision; coordinator recovery safely resends it. Coordinator replication, real tensor transfer, model execution, and continuous availability are out of scope.

## Acceptance criteria

- Snapshot reads hide uncommitted data.
- Stale writers cannot overwrite newer versions.
- Normal migration updates all shards consistently.
- Prepare failure leaves all committed records unchanged.
- Exactly one of two conflicting migrations commits.
- Commit replay is idempotent after an acknowledgement is lost.
- A pre-migration decode epoch cannot update token progress.
- Existing TxnMem tests remain green.
