#!/usr/bin/env python3
"""Emit LaTeX tables/figure for the camera-ready from artifacts/camera_ready.json (no hand-copied numbers)."""
import json, sys
from pathlib import Path
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
ROOT = Path(__file__).resolve().parents[1]
OUT = Path(sys.argv[1])
r = json.loads((ROOT / "artifacts/camera_ready.json").read_text())
def pm(d, p=3):
    return f"${d['mean']:.{p}f}{{\\pm}}{(d['hi']-d['lo'])/2:.{p}f}$"
def m(d, p=3): return f"{d['mean']:.{p}f}"
C3 = ["cloud_only", "naive_retry_queue", "durable_idempotent_queue"]
short = {"cloud_only": "cloud", "naive_retry_queue": "naive", "durable_idempotent_queue": "durable",
         "durable_batched_fsync": "dur.-batch", "crdt_yjs": "CRDT (Yjs)"}
# T1: v0.1 overall multi-seed
L = ["\\begin{tabular}{@{}lccccc@{}}\\toprule",
     "Client & Lost & UVF & Dup$_{v0.1}$ & Redeliv. & Coll. \\\\ \\midrule"]
for c in C3:
    d = r["v01_overall"][c]
    L.append(f"\\texttt{{{short[c]}}} & {pm(d['lost_mutation_rate'])} & {pm(d['user_visible_failure_rate'])} & {pm(d['duplicate_rate'])} & {pm(d['redelivery_dup_rate'])} & {pm(d['create_collision_rate'])} \\\\")
L.append("\\bottomrule\\end{tabular}")
(OUT / "tab_v01_overall.tex").write_text("\n".join(L))
# T2: v0.1 by fault, all 9
L = ["\\begin{tabular}{@{}lccc@{}}\\toprule", "Fault & cloud & naive & durable \\\\ \\midrule"]
for ft in ["packet_loss","latency_jitter","partition","dup","reorder","crash","long_offline","concurrent_edits","large_payload_sync"]:
    cells = []
    for c in C3:
        d = r["v01_by_fault"][c][ft]
        cells.append("/".join(m(d[k], 2) for k in ["lost_mutation_rate","redelivery_dup_rate","user_visible_failure_rate"]))
    L.append(f"\\texttt{{{ft.replace('_','\\_')}}} & " + " & ".join(cells) + " \\\\")
L.append("\\bottomrule\\end{tabular}")
(OUT / "tab_v01_fault.tex").write_text("\n".join(L))
# T3: v0.2 hard faults
L = ["\\begin{tabular}{@{}llcccc@{}}\\toprule", "Fault & Client & Lost & UVF & Dup & Silent / Surf. \\\\ \\midrule"]
for ft in ["partial_write","clock_skew","conflict_storm"]:
    for i, c in enumerate(r["v02"]):
        d = r["v02"][c]["by_fault"][ft]
        lab = f"\\texttt{{{ft.replace('_','\\_')}}}" if i == 0 else ""
        L.append(f"{lab} & {short[c]} & {m(d['lost'])} & {m(d['uvf'])} & {m(d['redelivery_dups'])} & {m(d['silent_total'])} / {m(d['conflicts_surfaced'])} \\\\")
    if ft != "conflict_storm": L.append("\\midrule")
L.append("\\bottomrule\\end{tabular}")
(OUT / "tab_v02_hard.tex").write_text("\n".join(L))
# T4: v0.2 overall + resources
L = ["\\begin{tabular}{@{}lcccccc@{}}\\toprule",
     "Client & Lost & Silent & Phant. & Bytes$\\times$ & CPU & $\\Delta$RSS \\\\",
     " & & ovw. & fail. & & ms/scn & MiB \\\\ \\midrule"]
for c, d in r["v02"].items():
    o = d["overall"]
    L.append(f"{short[c]} & {pm(o['lost'])} & {pm(o['silent_total'])} & {m(o['phantom_failures'])} & {o['bytes_amp']['mean']:.2f} & {d['cpu_ms_per_scenario']:.2f} & {d['maxrss_mib']-d['rss_after_import_mib']:.1f} \\\\")
L.append("\\bottomrule\\end{tabular}")
(OUT / "tab_v02_overall.tex").write_text("\n".join(L))
# Figure: hard faults, lost / dup / silent / surfaced with CI
fig, axes = plt.subplots(1, 3, figsize=(7.0, 2.1), sharey=True)
keys = [("lost","lost"),("redelivery_dups","dup"),("silent_total","silent"),("conflicts_surfaced","surfaced")]
conds = list(r["v02"])
for ax, ft in zip(axes, ["partial_write","clock_skew","conflict_storm"]):
    x = np.arange(len(keys)); w = 0.16
    for j, c in enumerate(conds):
        d = r["v02"][c]["by_fault"][ft]
        mu = [d[k]["mean"] for k, _ in keys]; err = [(d[k]["hi"]-d[k]["lo"])/2 for k, _ in keys]
        ax.bar(x + (j-2)*w, mu, w, yerr=err, capsize=1.5, label=short[c])
    ax.set_xticks(x); ax.set_xticklabels([l for _, l in keys], fontsize=7); ax.set_title(ft, fontsize=8)
    ax.tick_params(axis="y", labelsize=7)
axes[0].set_ylabel("rate per mutation", fontsize=7)
axes[0].legend(fontsize=5.5, ncol=1, loc="upper right")
fig.tight_layout(); fig.savefig(OUT / "figures/fig_hard_faults.pdf")
print("written")
