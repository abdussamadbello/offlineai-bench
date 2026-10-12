# RESULTS — Milestone 1 (OfflineAI-Bench)

**Dataset:** OfflineAI-Bench v0.1 (SEED=42, n=450)
**Splits:** train=315 / dev=72 / test=63 (70/15/15 stratified by fault_type)
**Network model:** pure Python event simulator (packet loss, latency/jitter, partition, dup, reorder, crash, long offline, concurrent edits, large payload) — no root `tc/netem`
**Frozen at (UTC):** 2026-09-19T17:46:32.480774+00:00
**TEST checksum:** `7789dadacc939c76d49392b783e1380b60c9fced6edf0665e98db60af9353a31`

## TEST (frozen claims)

| condition | lost_mutation_rate | duplicate_rate | convergence_time_proxy | conflict_count | sync_throughput_proxy | user_visible_failure_rate |
|---|---:|---:|---:|---:|---:|---:|
| `cloud_only` | 0.211 | 0.239 | 11279.7 | 0.000 | 2.778 | 0.211 |
| `naive_retry_queue` | 0.008 | 0.278 | 2381.0 | 0.000 | 4.431 | 0.008 |
| `durable_idempotent_queue` | 0.000 | 0.000 | 1197.0 | 0.159 | 4.722 | 0.000 |

## DEV (diagnostic only; not for claims)

| condition | lost_mutation_rate | duplicate_rate | convergence_time_proxy | conflict_count | sync_throughput_proxy | user_visible_failure_rate |
|---|---:|---:|---:|---:|---:|---:|
| `cloud_only` | 0.225 | 0.306 | 10928.3 | 0.000 | 3.065 | 0.225 |
| `naive_retry_queue` | 0.016 | 0.366 | 2901.4 | 0.000 | 4.400 | 0.016 |
| `durable_idempotent_queue` | 0.000 | 0.000 | 1173.5 | 0.069 | 4.750 | 0.000 |


## TEST by fault type (lost_mutation_rate / duplicate_rate / user_visible_failure_rate)

| fault | cloud_only lost/dup/uvf | naive_retry_queue lost/dup/uvf | durable_idempotent_queue lost/dup/uvf |
|---|---|---|---|
| `concurrent_edits` | 0.000/0.262/0.000 | 0.000/0.262/0.000 | 0.000/0.000/0.000 |
| `crash` | 0.175/0.067/0.175 | 0.070/0.116/0.070 | 0.000/0.000/0.000 |
| `dup` | 0.000/0.786/0.000 | 0.000/0.786/0.000 | 0.000/0.000/0.000 |
| `large_payload_sync` | 0.089/0.104/0.089 | 0.000/0.122/0.000 | 0.000/0.000/0.000 |
| `latency_jitter` | 0.000/0.277/0.000 | 0.000/0.277/0.000 | 0.000/0.000/0.000 |
| `long_offline` | 1.000/0.000/1.000 | 0.000/0.146/0.000 | 0.000/0.000/0.000 |
| `packet_loss` | 0.321/0.242/0.321 | 0.000/0.310/0.000 | 0.000/0.000/0.000 |
| `partition` | 0.316/0.169/0.316 | 0.000/0.234/0.000 | 0.000/0.000/0.000 |
| `reorder` | 0.000/0.247/0.000 | 0.000/0.247/0.000 | 0.000/0.000/0.000 |

## Interpretation (M1)

- `cloud_only` drops mutations whenever the channel is unavailable → high lost-mutation and user-visible failure rates under partition / long_offline / crash / lossy links.
- `naive_retry_queue` reduces some losses via in-memory retries but **duplicates** under `dup` / lossy retries, **loses the queue on crash**, and does not detect concurrent-edit conflicts.
- `durable_idempotent_queue` (clean-room reference) persists the queue across crash, deduplicates via idempotency keys, and records conflicts under concurrent edits — lowest lost-mutation and duplicate rates on frozen TEST.

## Which faults break naive retry (honest)

- **crash:** in-memory queue wiped → lost mutations + user-visible failures.
- **dup** / lossy **packet_loss** with retries: at-least-once without idempotency → elevated duplicate_rate.
- **concurrent_edits:** no version check → silent overwrite path; conflict_count stays near zero while durable records conflicts.
- **long_offline** / **partition:** naive eventually drains if process stays up; cloud_only does not.
- **large_payload_sync:** retries inflate attempts; durable still converges via idempotent resume.

## Artifacts

- `runs/summary.json`
- `runs/test_<condition>.json` + `_preds.jsonl`
- `data/generated/offlineaibench_v0.1_*.jsonl`
- `data/manifests/composition.json`

## Blockers

- Synthetic inspection/work-order records only; no commercial field-app code.
- Pure-Python network model approximates intermittent connectivity; not a kernel netem trace replay.
- Soft companion to Paper 24 / Paper 25 EdgeCloud (sibling repos not modified).
