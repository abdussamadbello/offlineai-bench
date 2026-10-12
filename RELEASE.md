# OfflineAI-Bench release v0.1 (2026-09-19)

Offline-first sync under intermittent connectivity: pure-Python fault injector + OfflineAI-Bench over synthetic inspection/work-order records.

**IP:** clean-room research — synthetic only. No commercial field-app code. Soft companion to Paper 24 / Paper 25 EdgeCloud (siblings **not** modified).

**M1 frozen.** SEED=42; TEST n=63; TEST sha256=`7789dadacc939c76d49392b783e1380b60c9fced6edf0665e98db60af9353a31`.

## Headline M1 — frozen TEST

| condition | lost_mutation_rate | duplicate_rate | convergence_time_proxy | conflict_count | sync_throughput_proxy | user_visible_failure_rate |
|---|---:|---:|---:|---:|---:|---:|
| `cloud_only` | 0.211 | 0.239 | 11279.7 | 0.000 | 2.778 | 0.211 |
| `naive_retry_queue` | 0.008 | 0.278 | 2381.0 | 0.000 | 4.431 | 0.008 |
| **`durable_idempotent_queue`** | **0.000** | **0.000** | **1197.0** | 0.159 | **4.722** | **0.000** |

- **Lead:** durable zeros lost/duplicate/UVF vs cloud_only UVF **0.211** and naive dup **0.278**.
- Naive still loses on **crash** and duplicates under **dup**.
- Durable records conflicts under concurrent_edits / reorder (mean **0.159**).
- **Honest:** synthetic event model; no tc/netem; no commercial field-app code.
- Draft: `paper/OFFLINEAI_PAPER_DRAFT.md`; table: `artifacts/RESULTS_MILESTONE1.md`.
- Frozen at (UTC): 2026-09-19T17:46:32.480774+00:00

## Reproduce

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=src
python scripts/generate_data.py
python scripts/run_milestone1.py
sha256sum -c CHECKSUMS.sha256
```

SEED=42; splits 70/15/15 by fault_type. Do not tune on TEST.
