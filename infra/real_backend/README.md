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

`scripts/run_real_backend_smoke.sh` remains an ordinary diagnostic check. Only
`scripts/run_formal_provenance_smoke.sh`, through the installed protected
controller, proves the formal same-path ingress and proxy-attribution gate. The
formal smoke emits no candidate output or claim-bearing artifact. Neither smoke
is a production latency result.
