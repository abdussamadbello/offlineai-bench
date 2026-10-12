#!/usr/bin/env python3
"""Camera-ready evaluation: v0.1 harness over 10 seeds (+ duplicate decomposition) and
v0.2 harness (12 faults x 5 clients incl. pycrdt/Yjs) over 10 seeds, TEST split only.
Reports seed-level means with 95% t-intervals. Writes artifacts/camera_ready.json."""
from __future__ import annotations
import json, resource, subprocess, sys, time, platform
from pathlib import Path
import numpy as np
from scipy import stats

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from offlineaibench.generator import generate_scenarios  # noqa: E402
from offlineaibench.simulator import run_scenario  # noqa: E402
from offlineaibench.metrics import result_to_row  # noqa: E402
from offlineaibench import v02  # noqa: E402

SEEDS = list(range(42, 52))
V01 = ["cloud_only", "naive_retry_queue", "durable_idempotent_queue"]


def ci(xs):
    xs = np.asarray(xs, float); m = float(xs.mean())
    if len(xs) < 2 or xs.std(ddof=1) == 0:
        return {"mean": m, "lo": m, "hi": m}
    h = float(stats.t.ppf(0.975, len(xs) - 1) * xs.std(ddof=1) / np.sqrt(len(xs)))
    return {"mean": m, "lo": m - h, "hi": m + h}


def v01_multiseed():
    keys = ["lost_mutation_rate", "duplicate_rate", "redelivery_dup_rate", "create_collision_rate",
            "user_visible_failure_rate", "convergence_time_proxy", "conflict_count"]
    per = {c: {k: [] for k in keys} for c in V01}
    per_fault = {c: {} for c in V01}
    for s in SEEDS:
        test = [x.to_dict() for x in generate_scenarios(n=450, seed=s) if x.split == "test"]
        for c in V01:
            rows = [result_to_row(run_scenario(sc, c, seed=s)) for sc in test]
            for k in keys:
                per[c][k].append(float(np.mean([r[k] for r in rows])))
            for ft in sorted({r["fault_type"] for r in rows}):
                rr = [r for r in rows if r["fault_type"] == ft]
                d = per_fault[c].setdefault(ft, {k: [] for k in keys})
                for k in keys:
                    d[k].append(float(np.mean([r[k] for r in rr])))
    return ({c: {k: ci(v) for k, v in d.items()} for c, d in per.items()},
            {c: {ft: {k: ci(v) for k, v in d.items()} for ft, d in fd.items()} for c, fd in per_fault.items()})


def v02_condition(cond: str):
    """Runs in a child process so ru_maxrss is per condition."""
    base_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    cpu = 0.0; nscn = 0
    per_seed = []
    for s in SEEDS:
        test = [x for x in v02.generate_v02(s) if x["split"] == "test"]
        t0 = time.process_time()
        rows = [v02.run(sc, cond, s) for sc in test]
        cpu += time.process_time() - t0; nscn += len(rows)
        per_seed.append([r.__dict__ for r in rows])
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {"rows": per_seed, "cpu_s": cpu, "n_scenarios": nscn, "maxrss_kib": rss, "base_rss_kib": base_rss}


RATE_KEYS = ["lost", "redelivery_dups", "uvf", "phantom_failures", "conflicts_surfaced",
             "silent_overwrites", "silent_discards", "create_collisions"]


def summarize_v02(raw):
    out = {}
    for c, r in raw.items():
        overall = {k: [] for k in RATE_KEYS + ["silent_total", "convergence_ms", "bytes_amp", "bytes_per_mut"]}
        byf = {}
        for seed_rows in r["rows"]:
            def agg(rows):
                n = sum(x["n"] for x in rows)
                d = {k: sum(x[k] for x in rows) / n for k in RATE_KEYS}
                d["silent_total"] = d["silent_overwrites"] + d["silent_discards"]
                d["convergence_ms"] = float(np.mean([x["convergence_ms"] for x in rows]))
                d["bytes_amp"] = sum(x["bytes_sent"] for x in rows) / max(1, sum(x["intended_bytes"] for x in rows))
                d["bytes_per_mut"] = sum(x["bytes_sent"] for x in rows) / n
                return d
            a = agg(seed_rows)
            for k in overall: overall[k].append(a[k])
            for ft in v02.ALL_FAULTS:
                rr = [x for x in seed_rows if x["fault_type"] == ft]
                b = agg(rr); dd = byf.setdefault(ft, {k: [] for k in overall})
                for k in overall: dd[k].append(b[k])
        out[c] = {"overall": {k: ci(v) for k, v in overall.items()},
                  "by_fault": {ft: {k: ci(v) for k, v in d.items()} for ft, d in byf.items()},
                  "cpu_ms_per_scenario": 1000 * r["cpu_s"] / r["n_scenarios"],
                  "n_scenarios": r["n_scenarios"],
                  "maxrss_mib": r["maxrss_kib"] / 1024, "rss_after_import_mib": r["base_rss_kib"] / 1024}
    return out


def main():
    if len(sys.argv) > 2 and sys.argv[1] == "--child":
        print(json.dumps(v02_condition(sys.argv[2]))); return
    res = {"seeds": SEEDS, "python": platform.python_version(), "platform": platform.platform(),
           "started_local": time.strftime("%Y-%m-%d %H:%M:%S %Z")}
    res["v01_overall"], res["v01_by_fault"] = v01_multiseed()
    raw = {}
    for c in v02.CONDITIONS_V02:
        p = subprocess.run([sys.executable, __file__, "--child", c], capture_output=True, text=True, check=True)
        raw[c] = json.loads(p.stdout)
    res["v02"] = summarize_v02(raw)
    (ROOT / "artifacts" / "camera_ready.json").write_text(json.dumps(res, indent=1))
    print("ok")


if __name__ == "__main__":
    main()
