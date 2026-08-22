# Fix 320f85e — formal proxy-route verification

## Scope

Repaired the formal proxy-route verification boundary in
`src/txnmem_service_faults.py` without changing public results, paper claims,
remote files, or deployment state.

The implementation replaces trailing-port extraction with a fail-closed
endpoint normalizer. It accepts only a proper URL authority or a bare
`host:port` authority, rejects credentials and URL path/parameter/query/
fragment components, requires a host and port in `1..65535`, and normalizes
hostnames before comparing identities. Proxy verification now requires an
explicit boolean `enabled: true`, matching expected/observed listen identities,
nonempty valid matching upstream identities, and a valid client endpoint on the
observed listen port. The client host may differ from the listen host.

## TDD record

Consumer-visible tests were added before the production change in
`tests/test_txnmem_service_faults.py`.

- RED: `PYTHONPATH=src python3 -m unittest tests/test_txnmem_service_faults.py -v`
  ran 9 tests and failed as intended with 15 unsafe acceptances plus one
  malformed-port `ValueError`.
- GREEN: `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest -v tests.test_txnmem_service_faults`
  ran 10 tests successfully.

The added cases cover every review-specified malformed/invalid endpoint value,
credentials/path/parameters/query/fragment, missing and disabled `enabled`,
missing expected/observed/both upstreams, wrong upstream, malformed listen and
upstream endpoints, normalized valid host comparison, listen-host mismatch,
wrong client port, and the Python-3.10 bare-listen parse characterization.

## Verification

- Full suite: `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest discover -s tests -q` — exit 0.
- Claim audit: `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B src/txnmem_claim_audit.py audit --root . --ledger configs/paper_claims.json --out /tmp/txnmem-evidence-scale-up-claim-audit.json` — 15 claims, 0 findings.
- Artifact audit: `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B src/txnmem_artifact_audit.py --root .` — 0 findings.
- `git diff --check` — clean.

## Changed files

- `src/txnmem_service_faults.py`
- `tests/test_txnmem_service_faults.py`
- `.superpowers/sdd/2026-08-18-evidence-scale-up/progress.md`
- `.superpowers/sdd/2026-08-18-evidence-scale-up/fix-320f85e-report.md`

## Concerns

None. The requested remote CPython 3.10 compatibility is retained by the
existing patched-`urlparse` consumer test; no remote deployment was attempted.
