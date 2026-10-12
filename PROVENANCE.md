# Provenance

## What is original
- OfflineAI-Bench scenario generator (synthetic inspection / work-order records)
- Pure-Python network fault injector (no tc/netem)
- Sync clients: `cloud_only`, `naive_retry_queue`, `durable_idempotent_queue` (clean-room)
- Eval harness, metrics, Milestone-1 freeze

## Soft companions (read-only)
- Paper 25 EdgeCloud (`paper-25-edgecloud`) — inspection-domain flavor only; **not modified**
- Paper 24 (same domain if present) — **not modified**
- Portfolio writing brief (`portfolio/writing/LEADING_LAB_STYLE_BRIEF.md`) — style only

## What is not included
- Commercial field-app source, schemas, or traces
- Kernel netem / root networking experiments
- Paid LLM calls

## IP posture
Synthetic records only. Numbers in the draft are copied from frozen `artifacts/RESULTS_MILESTONE1.md` / `runs/summary.json`.
