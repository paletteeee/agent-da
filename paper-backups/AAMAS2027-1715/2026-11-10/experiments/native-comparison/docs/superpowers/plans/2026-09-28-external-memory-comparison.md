# External Memory Comparison Implementation Plan
> **For agentic workers:** Use superpowers:executing-plans to implement inline. The user has explicitly asked to complete both experiments.
**Goal:** Complete E1 problem discovery and E2 shared-memory comparison using real third-party libraries.
**Architecture:** Reuse the six workload generators. Separate stateless scheduling and observational judging from isolated native adapter workers; freeze inputs before formal execution.
**Tech Stack:** Python 3.12, unittest, subprocess JSONL, Mem0/Qdrant, G-Memory/Chroma, A-MEM/Chroma.
**Spec:** docs/superpowers/specs/2026-09-28-external-memory-comparison.md
## Global Constraints
Remote execution only. No fabricated support, results or native transactions. Preserve existing source behavior and snapshot all observations.
Controlled model/embedding providers are explicit experimental conditions, not claims about default configurations.
## Review Focus
- Missing observable data must be inconclusive, never empty-success.
- A deleted source must still trigger external provenance checking.
- Normal-control success must require expected effects.
- Random scheduling preserves intra-agent order and necessary source dependencies.
- Unsupported and runtime failure remain distinct from scientific violations.
## Task 1 Environment and source isolation
- [x] Establish remote identity and isolated directory.
- [ ] Record source hashes, upstream revisions, dependency lock and baseline-test outcomes.
- [ ] Install separate environments and verify native imports without external model calls.
## Task 2 Observations, workloads and checker
Files: src/txnmem_external_compare.py; tests/test_txnmem_external_compare.py.
Interfaces: build_case(family, seed, strategy, control=False)->dict; judge(case, observations)->dict.
- [ ] Write tests for the five Review Focus cases plus ordinary/random/directed reproducibility.
- [ ] Run them red; implement observations and independent set/ordering checks; run green.
## Task 3 Native adapters
Files: src/txnmem_external_native.py; tests/test_txnmem_external_native.py.
Interfaces: worker accepts JSONL case and returns raw observations, support reason and provider evidence.
- [ ] Test real Mem0 add/get/delete and isolated state.
- [ ] Test real A-MEM add/read/search/delete and isolated state.
- [ ] Test G-Memory add/retrieve path; explicitly reject unsupported update/revoke mappings.
- [ ] Test actual TxnMem coordinator and storage API without using simulator output as backend evidence.
- [ ] Verify fault hooks do not run cleanup or repair except as executed by the actual system.
## Task 4 Campaign and witness replay
Files: scripts/run_external_comparison.py; configs/external_comparison_v1.json.
- [ ] Test exclusions, first-hit right censoring, evidence-linked violation groups, normal denominators and repeated witnesses.
- [ ] Implement durable experiment commands and process timeouts, retaining raw JSONL.
- [ ] Pilot each supported family; freeze manifest before formal 50-seed execution.
- [ ] Run E1 and E2, then 5 replays per selected native-system witness.
## Task 5 Audit and delivery
- [ ] Recompute all summary counts from raw observations.
- [ ] Run appropriate suite and report any inherited failures by name.
- [ ] Perform independent whole-change review; fix important issues with regression tests.
- [ ] Produce Chinese report with coverage, findings, paired results and limits; copy deliverables locally.
