#!/usr/bin/env python3
"""Smoke: run one TEST scenario under all three conditions."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from offlineaibench.eval import load_jsonl
from offlineaibench.simulator import run_scenario

rows = load_jsonl(Path(__file__).resolve().parents[1] / "data/generated/offlineaibench_v0.1_test.jsonl")
scn = next(r for r in rows if r["fault_type"] == "crash")
for cond in ("cloud_only", "naive_retry_queue", "durable_idempotent_queue"):
    r = run_scenario(scn, cond, seed=42)
    print(cond, "lost", r.lost_mutations, "dup", r.duplicate_applies, "uvf", r.user_visible_failures, "crashed", r.crashed)
