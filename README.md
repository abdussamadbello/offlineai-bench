# OfflineAI-Bench (Paper 23)

**Benchmarking Offline-First AI Applications Under Network Failure**

**RQ:** What failure modes appear in AI-enabled field apps when connectivity is intermittent rather than binary online/offline?

## Headline (frozen TEST, SEED=42, n_test=63)

| condition | lost_mutation_rate | duplicate_rate | convergence_time_proxy | conflict_count | sync_throughput_proxy | user_visible_failure_rate |
|---|---:|---:|---:|---:|---:|---:|
| `cloud_only` | 0.211 | 0.239 | 11279.7 | 0.000 | 2.778 | 0.211 |
| `naive_retry_queue` | 0.008 | 0.278 | 2381.0 | 0.000 | 4.431 | 0.008 |
| **`durable_idempotent_queue`** | **0.000** | **0.000** | **1197.0** | 0.159 | **4.722** | **0.000** |

Naive retry still loses mutations on **crash** and duplicates under **dup**; durable recovers both. Soft companion to Paper 24 / Paper 25 EdgeCloud (siblings not modified).

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=src
python scripts/generate_data.py
python scripts/run_milestone1.py
sha256sum -c CHECKSUMS.sha256
```

SEED=42; splits 70/15/15 by `fault_type`. Do not tune on TEST.

## Layout

- `src/offlineaibench/` — generator, network injector, baselines, simulator, metrics, eval
- `scripts/generate_data.py`, `scripts/run_milestone1.py`
- `data/generated/` — frozen jsonl splits
- `artifacts/RESULTS_MILESTONE1.md` — headline tables
- `paper/OFFLINEAI_PAPER_DRAFT.md` — draft v0.1
- `RESEARCH_CONTRACT.md`, `PROVENANCE.md`, `GAP_ANALYSIS.md`

## IP

Synthetic inspection/work-order records only. No commercial field-app code.

TEST jsonl sha256: `7789dadacc939c76d49392b783e1380b60c9fced6edf0665e98db60af9353a31`
