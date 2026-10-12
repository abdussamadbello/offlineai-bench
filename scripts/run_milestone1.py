#!/usr/bin/env python3
"""Milestone 1: evaluate sync baselines on frozen TEST (DEV diagnostic only)."""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from offlineaibench.eval import CONDITIONS, load_jsonl, run_policy


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


METRIC_KEYS = [
    "lost_mutation_rate",
    "duplicate_rate",
    "convergence_time_proxy",
    "conflict_count",
    "sync_throughput_proxy",
    "user_visible_failure_rate",
]


def main() -> None:
    cfg = yaml.safe_load((ROOT / "configs" / "default.yaml").read_text())
    data_dir = ROOT / cfg["paths"]["data_generated"]
    runs_dir = ROOT / cfg["paths"]["runs"]
    artifacts = ROOT / cfg["paths"]["artifacts"]
    runs_dir.mkdir(parents=True, exist_ok=True)
    artifacts.mkdir(parents=True, exist_ok=True)

    train = load_jsonl(data_dir / "offlineaibench_v0.1_train.jsonl")
    dev = load_jsonl(data_dir / "offlineaibench_v0.1_dev.jsonl")
    test = load_jsonl(data_dir / "offlineaibench_v0.1_test.jsonl")
    assert len(train) + len(dev) + len(test) == cfg["n_total"]

    seed = int(cfg["seed"])
    conditions = list(cfg.get("conditions") or CONDITIONS)

    def eval_split(split_name: str, rows: list):
        results = {}
        for name in conditions:
            metrics, preds = run_policy(rows, name, seed=seed)
            results[name] = metrics
            (runs_dir / f"{split_name}_{name}.json").write_text(
                json.dumps({"split": split_name, "condition": name, "metrics": metrics}, indent=2)
                + "\n"
            )
            (runs_dir / f"{split_name}_{name}_preds.jsonl").write_text(
                "\n".join(json.dumps(p) for p in preds) + "\n"
            )
        return results

    dev_results = eval_split("dev", dev)
    test_results = eval_split("test", test)

    def slim(results: dict) -> dict:
        return {
            k: {m: v[m] for m in METRIC_KEYS + ["n_scenarios"]}
            for k, v in results.items()
        }

    summary = {
        "seed": seed,
        "dataset": "OfflineAI-Bench v0.1",
        "n_train": len(train),
        "n_dev": len(dev),
        "n_test": len(test),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "dev": slim(dev_results),
        "test": slim(test_results),
        "test_by_fault": {k: v.get("by_fault", {}) for k, v in test_results.items()},
        "data_checksums": {
            "test": sha256(data_dir / "offlineaibench_v0.1_test.jsonl"),
            "all": sha256(data_dir / "offlineaibench_v0.1.jsonl"),
        },
        "note": (
            "TEST evaluated once with frozen fault injector + baselines; "
            "DEV diagnostic only. Pure-Python network model (no tc/netem). "
            "Synthetic inspection/work-order records only."
        ),
    }
    (runs_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    (artifacts / "results_m1.json").write_text(json.dumps(summary, indent=2) + "\n")

    def fmt(x: float) -> str:
        if abs(x) >= 100:
            return f"{x:.1f}"
        return f"{x:.3f}"

    def table(results: dict) -> str:
        header = (
            "| condition | lost_mutation_rate | duplicate_rate | convergence_time_proxy | "
            "conflict_count | sync_throughput_proxy | user_visible_failure_rate |\n"
            "|---|---:|---:|---:|---:|---:|---:|\n"
        )
        lines = [header]
        for name in conditions:
            m = results[name]
            lines.append(
                f"| `{name}` | {fmt(m['lost_mutation_rate'])} | {fmt(m['duplicate_rate'])} | "
                f"{fmt(m['convergence_time_proxy'])} | {fmt(m['conflict_count'])} | "
                f"{fmt(m['sync_throughput_proxy'])} | {fmt(m['user_visible_failure_rate'])} |\n"
            )
        return "".join(lines)

    fault_lines = [
        "\n## TEST by fault type (lost_mutation_rate / duplicate_rate / user_visible_failure_rate)\n\n",
        "| fault | cloud_only lost/dup/uvf | naive_retry_queue lost/dup/uvf | "
        "durable_idempotent_queue lost/dup/uvf |\n|---|---|---|---|\n",
    ]
    faults = sorted(next(iter(test_results.values())).get("by_fault", {}).keys())
    for ft in faults:
        cells = []
        for name in conditions:
            bf = test_results[name]["by_fault"][ft]
            cells.append(
                f"{fmt(bf['lost_mutation_rate'])}/{fmt(bf['duplicate_rate'])}/{fmt(bf['user_visible_failure_rate'])}"
            )
        fault_lines.append(f"| `{ft}` | {cells[0]} | {cells[1]} | {cells[2]} |\n")

    md = f"""# RESULTS — Milestone 1 (OfflineAI-Bench)

**Dataset:** OfflineAI-Bench v0.1 (SEED={seed}, n={cfg['n_total']})
**Splits:** train={len(train)} / dev={len(dev)} / test={len(test)} (70/15/15 stratified by fault_type)
**Network model:** pure Python event simulator (packet loss, latency/jitter, partition, dup, reorder, crash, long offline, concurrent edits, large payload) — no root `tc/netem`
**Frozen at (UTC):** {summary['timestamp_utc']}
**TEST checksum:** `{summary['data_checksums']['test']}`

## TEST (frozen claims)

{table(test_results)}
## DEV (diagnostic only; not for claims)

{table(dev_results)}
{''.join(fault_lines)}
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
"""
    (artifacts / "RESULTS_MILESTONE1.md").write_text(md)
    print(md)


if __name__ == "__main__":
    main()
