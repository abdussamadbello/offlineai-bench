#!/usr/bin/env python3
"""Repeat the per-client resource measurement (CPU ms/scenario, peak-RSS increase) REPS times,
each in a fresh child process, interleaving clients. Writes artifacts/resources_reps.json."""
import json, subprocess, sys, statistics as st
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from offlineaibench import v02  # noqa: E402
REPS = 5
script = str(ROOT / "scripts" / "run_camera_ready.py")
reps = {c: [] for c in v02.CONDITIONS_V02}
for _ in range(REPS):
    for c in v02.CONDITIONS_V02:
        p = subprocess.run([sys.executable, script, "--child", c], capture_output=True, text=True, check=True)
        d = json.loads(p.stdout)
        reps[c].append({"cpu_ms_per_scenario": 1000 * d["cpu_s"] / d["n_scenarios"],
                        "drss_mib": (d["maxrss_kib"] - d["base_rss_kib"]) / 1024})
out = {c: {"reps": r,
           "cpu_median": st.median(x["cpu_ms_per_scenario"] for x in r),
           "cpu_min": min(x["cpu_ms_per_scenario"] for x in r), "cpu_max": max(x["cpu_ms_per_scenario"] for x in r),
           "drss_median": st.median(x["drss_mib"] for x in r),
           "drss_min": min(x["drss_mib"] for x in r), "drss_max": max(x["drss_mib"] for x in r)}
       for c, r in reps.items()}
(ROOT / "artifacts" / "resources_reps.json").write_text(json.dumps(out, indent=1))
for c, d in out.items():
    print(f"{c:28s} cpu {d['cpu_median']:.2f} [{d['cpu_min']:.2f}-{d['cpu_max']:.2f}]  dRSS {d['drss_median']:.1f} [{d['drss_min']:.1f}-{d['drss_max']:.1f}]")
