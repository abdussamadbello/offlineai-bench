"""Evaluation harness for OfflineAI-Bench."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .metrics import aggregate, aggregate_by_fault, result_to_row
from .simulator import run_scenario

CONDITIONS = ("cloud_only", "naive_retry_queue", "durable_idempotent_queue")


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def run_policy(
    scenarios: list[dict[str, Any]],
    condition: str,
    seed: int = 42,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    rows = []
    for scn in scenarios:
        result = run_scenario(scn, condition, seed=seed)
        rows.append(result_to_row(result))
    metrics = aggregate(rows)
    metrics["by_fault"] = aggregate_by_fault(rows)
    return metrics, rows
