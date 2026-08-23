# TxnMem real backend services

This stack is evidence infrastructure, not a production deployment. Qdrant
stores deterministic fixture embeddings and memory metadata, Neo4j stores
memory nodes and provenance edges, and Toxiproxy supplies deterministic fault
injection between the runner and the services.

Start it on the remote host with:

```bash
docker compose -f infra/real_backend/docker-compose.yml up -d
```

The backend runner must record the image tags/digests, health checks, proxy
configuration, and `production_latency_claim: false`. Raw database volumes and
service logs stay on the remote host and are never committed.

The Qdrant service fixes its container `nofile` soft and hard limits at 65,536.
This prevents RocksDB segment growth in the registered 10,000-node provenance
cell from inheriting Docker's 1,024-descriptor default. Deployment preflight
must attest the effective limits before a scale result is eligible.

`scripts/run_real_backend_smoke.sh` remains an ordinary diagnostic check. Only
`scripts/run_formal_provenance_smoke.sh`, through the installed protected
controller, proves the formal same-path ingress and proxy-attribution gate. The
formal smoke emits no candidate output or claim-bearing artifact. Neither smoke
is a production latency result.

The provenance performance v2 protocol keeps setup outside every measured
operation interval. It preloads the deterministic DAG through the same
canonical write/CAS path, with fixed layer barriers and at most eight workers
inside one layer. A setup record may be reconciled once only when cross-store
readback proves that every persisted fragment matches the exact requested
canonical state. The repetition evidence reports the preload method, worker
limit, observed peak, setup repair budget/count, setup elapsed time, and
`retry_scope: measured_operations_only`. Backend and Neo4j driver retries stay
at zero for all measured operations. Qdrant collection readiness is cached
only after an exact HTTP creation/conflict outcome followed by a GET check of
the registered vector size and distance. The thread-safe Qdrant/Neo4j clients
are shared across repetitions and closed once after the matrix; backend-local
memory/key, metric and event bookkeeping is synchronized across preload
workers. Formal v2 validation requires the graph-derived layered worker limit
and one-record repair ceiling exactly. Namespaces and persistent-state
validation remain distinct per repetition.
