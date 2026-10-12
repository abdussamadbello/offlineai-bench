"""Aggregate metrics for OfflineAI-Bench."""
from __future__ import annotations

from typing import Any

from .simulator import SimResult


def result_to_row(r: SimResult) -> dict[str, Any]:
    n = max(1, r.n_local_mutations)
    return {
        "scenario_id": r.scenario_id,
        "condition": r.condition,
        "fault_type": r.fault_type,
        "n_local_mutations": r.n_local_mutations,
        "lost_mutation_rate": r.lost_mutations / n,
        "duplicate_rate": r.duplicate_applies / n,
        "convergence_time_proxy": float(r.convergence_time_ms),
        "conflict_count": float(r.conflicts),
        "sync_throughput_proxy": r.applied_unique / max(1.0, r.convergence_time_ms / 1000.0),
        "user_visible_failure_rate": r.user_visible_failures / n,
        "lost_mutations": r.lost_mutations,
        "duplicate_applies": r.duplicate_applies,
        "bytes_sent": r.bytes_sent,
        "sync_attempts": r.sync_attempts,
        "crashed": r.crashed,
        "redelivery_dup_rate": r.details.get("redelivery_dups", 0) / n,
        "create_collision_rate": r.details.get("create_collisions", 0) / n,
        "bytes_received": r.bytes_received,
    }


def aggregate(rows: list[dict[str, Any]]) -> dict[str, float]:
    keys = [
        "lost_mutation_rate",
        "duplicate_rate",
        "convergence_time_proxy",
        "conflict_count",
        "sync_throughput_proxy",
        "user_visible_failure_rate",
    ]
    if not rows:
        out = {k: 0.0 for k in keys}
        out["n_scenarios"] = 0.0
        return out
    n = len(rows)
    out = {k: sum(float(r[k]) for r in rows) / n for k in keys}
    out["n_scenarios"] = float(n)
    return out


def aggregate_by_fault(rows: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    by: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        by.setdefault(r["fault_type"], []).append(r)
    return {ft: aggregate(rs) for ft, rs in sorted(by.items())}
