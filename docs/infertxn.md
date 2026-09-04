# InferTxn

InferTxn is a dependency-free research prototype of a distributed
transactional metadata database for large-model inference. It demonstrates a
specific failure-prone operation: moving a live decode request between nodes
in a prefill-decode disaggregated serving system.

## Why a database transaction is needed

The large KV-cache tensor remains in GPU/CPU cache storage. InferTxn stores
only the metadata required to find and safely use it:

- `route/<request>` identifies the decode node receiving the next token step;
- `kv/<request>` identifies the ready cache copy and its version;
- `request/<request>` identifies the request owner, progress, and execution
  epoch.

These keys belong to independent shards. Updating them one by one can expose
a route whose cache is not ready, free the source cache before ownership
moves, or leave two nodes decoding the same request.

Before migration, the completed tensor copy is represented by a separate
`kv-copy/<request>/<target>` staging record. Migration refuses to move the
canonical route and ownership unless that record is committed, `ready`, and
bound to the current source epoch, cache version, and generated-token
position. The same transaction
marks the record `consumed`, preventing a later migration from reusing an old
cache copy.

## Consistency mechanisms

### MVCC snapshot isolation

Every committed value is an immutable timestamped version. Transactions read
the newest version visible at their start timestamp. Prepared writes are not
visible. During prepare, a write is rejected when another transaction has
committed a newer version or currently owns the key's prepare lock. The rule
prevents dirty reads and lost updates under the prototype's snapshot-isolation
model; it does not claim full serializability.

### Two-phase commit

The coordinator asks the route, KV, and request shards to prepare. It records
and flushes one global decision before notifying participants. A failed vote
aborts every prepared write. A commit-acknowledgement failure is recovered by
replaying the durable decision; participant commit and abort operations are
idempotent.

Participant commits are necessarily delivered one at a time. A coordinator
visibility watermark remains at the previous commit until every participant
acknowledges, so database-level reads continue to see the complete old
snapshot rather than a mixture of old and new shard versions.

### Epoch fencing

Migration increments the request epoch. Every token-progress update supplies
the writer's node and epoch. After ownership moves from node A to B, an update
from A carries an old epoch and is rejected, even if A resumes after a pause.

## Code map

- `src/infertxn/mvcc.py`: immutable versions, snapshots, conflict validation,
  prepared writes, and key locks.
- `src/infertxn/coordinator.py`: JSONL decision log, 2PC, and recovery.
- `src/infertxn/participant.py`: shard participant and fault injection.
- `src/infertxn/migration.py`: inference metadata invariants and migration.
- `src/infertxn/http_service.py`: standard-library JSON/HTTP transport.
- `src/infertxn/demo.py`: three participant processes and one migration.

## Run

No third-party packages are required.

```bash
python3 -m unittest discover -s tests -p 'test_infertxn*.py' -v
python3 -m src.infertxn.demo
python3 -m unittest discover -s tests -v
```

## Demonstrated failures

The tests cover a rejected prepare vote, concurrent migrations, stale decode
epochs, and a lost commit acknowledgement. They establish that a prepare
failure exposes no partial write, conflicting migrations have at most one
winner, old owners cannot update progress, and recovery can safely replay an
already-applied commit.

Recovery also advances the logical clock beyond every durable commit
timestamp before accepting a new transaction, preventing timestamp reuse
after a coordinator restart.

## Limits

InferTxn is not a production database or inference engine. It does not move
real KV tensors, execute a model, replicate shards with Raft, tolerate a
permanently lost coordinator, or provide strict serializability. Its HTTP demo
uses separate local processes; durable shard storage and deployment across
physical machines are future work.
