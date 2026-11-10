# TxnMem provenance performance v10 measurement results

This directory contains the sanitized aggregate measurements from the TxnMem v10 provenance-performance matrix.

## Matrix

- Graph node counts: 100, 1,000, and 10,000
- Concurrency levels: 1, 2, 4, 8, and 16
- Cells: 15
- Repetitions: 450 total, 30 per cell
- Operation samples: 14,400 total, 960 per cell, and 240 per operation within each cell
- Successful samples: 14,400
- Failed samples, retries, and setup repairs: 0

Latency values are in nanoseconds. Throughput values count successful operations per second. Throughput confidence intervals use 10,000 whole-repetition bootstrap resamples with seed 17. Latency populations and throughput numerators include successful operations only.

## Files

- `aggregate.json`: canonical machine-readable aggregate, including cell and operation metrics
- `cells.csv`: one row per graph-size/concurrency cell
- `operations.csv`: one row per cell and operation
- `manifest.json`: package counts, file sizes, and SHA-256 checksums

The canonical aggregate was independently recomputed from the measured samples and matched the source aggregate. The package contains aggregate measurements only and excludes infrastructure identity, process identity, private locations, raw logs, raw payloads, and database contents.
