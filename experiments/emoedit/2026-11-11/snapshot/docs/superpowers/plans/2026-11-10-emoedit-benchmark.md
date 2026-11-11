# EmoEdit Full Benchmark Implementation Plan

> For agentic workers: execute inline with superpowers:executing-plans; preserve prior demonstration artifacts.

**Goal:** Evaluate the released EmoEdit Q-Former_4000 checkpoint on all 405 official inputs and eight target emotions, producing all six Table 1 metrics.
**Architecture:** Freeze a hashed protocol and input manifest; generate deterministic outputs with resumable validation; calculate per-pair and grouped metrics; validate coverage and export results.
**Tech Stack:** Existing Python 3.12 / PyTorch CUDA environment, diffusers 0.30.3, transformers 4.44.2, lpips 0.1.4, scikit-image 0.24.0.
**Spec:** User request in this chat; upstream Readme.md, scripts/validation_args.py, metrics/ACC_args.py, metrics/Metric_args.py; CVPR 2025 paper Table 1 and Section 4.1.

## Global Constraints
- Source commit 88f622cdb5c7afdc8954eef899064296fe4a4273. Keep pristine upstream files in benchmark/upstream.
- Drop input IDs 21,18,111,137,218,228,237,266,277,333 as officially instructed; retain exactly 405 inputs and 3240 pairs.
- Seed 48000 reset using a CUDA Generator per pair; 30 steps; guidance 7.5; image guidance 1.5; batch size 1; official aspect-preserving resize; JPEG outputs.
- Use released Q-Former_4000.pth and record SHA256. No retraining or tuning based on test outcomes.
- Report PSNR, SSIM, LPIPS, CLIP-I, Emo-A, Emo-S with official code conventions. Additionally report standard-normalized LPIPS separately because upstream feeds [0,1] instead of [-1,1]. Do not silently substitute it.
- Use CLIP ViT-B/32 for metrics and released classifier, CLIP ViT-L/14 for generation. No duplicate softmax.
- No claim of matching paper scores; record runtime/model/environment deviations.

## Review Focus
- Missing, duplicated, corrupt, or stale outputs must block complete summaries.
- Class index order must be amusement,awe,contentment,excitement,anger,disgust,fear,sadness.
- Resume must preserve exact protocol and image identity.
- Pixel metrics use upstream source crop/resize and image-dependent PSNR range; standard alternatives stay distinct.
- Published checkpoint and metrics are not proof of identical original training checkpoint.

## Tasks
- [ ] 1. Write tests for official exclusions, class mapping, manifest uniqueness, output completeness and stale protocol detection; implement protocol.py and build hashed manifest. Verify 415 raw -> 405 selected -> 3240 pairs.
- [ ] 2. Implement generation runner from upstream validation logic; smoke test a fixed small subset; verify reload and resume; launch full generation with durable logs and JSON progress.
- [ ] 3. Implement metric runner preserving official formulas; identity-pair and scalar-vs-batch checks; calculate all six metrics and secondary standard LPIPS. Save per-pair JSONL, per-emotion summary and confusion counts.
- [ ] 4. Fresh review of scripts/protocol, full integrity audit, collect results and compare reported metrics to Table 1 without claiming equivalence.
