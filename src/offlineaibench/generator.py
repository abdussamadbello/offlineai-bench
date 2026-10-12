"""Synthetic inspection / work-order scenario generator (OfflineAI-Bench v0.1)."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np

from . import GENERATOR_VERSION
from .schema import FaultParams, FaultType, Scenario

FAULT_TYPES: list[FaultType] = [
    "packet_loss",
    "latency_jitter",
    "partition",
    "dup",
    "reorder",
    "crash",
    "long_offline",
    "concurrent_edits",
    "large_payload_sync",
]

STATUSES = ("open", "in_progress", "blocked", "done")
SEVERITIES = ("low", "medium", "high", "critical")
FINDINGS = (
    "Visual corrosion on flange {n}",
    "Pressure reading drift {n} kPa",
    "Seal wear observed at joint {n}",
    "Vibration anomaly band {n}",
    "Thermal hotspot sector {n}",
    "Lubricant level below mark {n}",
)


def _rid(prefix: str, i: int) -> str:
    return f"{prefix}-{i:05d}"


def _make_record(rng: np.random.Generator, idx: int, ts0: int) -> dict[str, Any]:
    return {
        "record_id": _rid("rec", idx),
        "work_order_id": _rid("wo", int(rng.integers(1, 5000))),
        "site_id": f"site-{int(rng.integers(1, 40)):02d}",
        "asset_tag": f"AST-{int(rng.integers(100, 999))}",
        "inspector_id": f"insp-{int(rng.integers(1, 25)):02d}",
        "status": STATUSES[int(rng.integers(0, len(STATUSES)))],
        "severity": SEVERITIES[int(rng.integers(0, len(SEVERITIES)))],
        "findings": FINDINGS[int(rng.integers(0, len(FINDINGS)))].format(n=int(rng.integers(1, 50))),
        "version": 1,
        "payload_bytes": int(rng.integers(200, 4000)),
        "updated_at_ms": ts0,
    }


def _fault_params(fault: FaultType, rng: np.random.Generator) -> FaultParams:
    if fault == "packet_loss":
        return FaultParams(fault_type=fault, packet_loss_p=float(rng.uniform(0.25, 0.55)))
    if fault == "latency_jitter":
        return FaultParams(
            fault_type=fault,
            latency_ms_mean=float(rng.uniform(200, 800)),
            jitter_ms=float(rng.uniform(100, 500)),
        )
    if fault == "partition":
        start = int(rng.integers(40, 120))
        dur = int(rng.integers(600, 2000))
        return FaultParams(fault_type=fault, partition_start_ms=start, partition_end_ms=start + dur)
    if fault == "dup":
        return FaultParams(fault_type=fault, dup_p=float(rng.uniform(0.35, 0.70)))
    if fault == "reorder":
        return FaultParams(
            fault_type=fault,
            reorder_window=int(rng.integers(2, 5)),
            latency_ms_mean=float(rng.uniform(50, 200)),
            jitter_ms=float(rng.uniform(80, 300)),
        )
    if fault == "crash":
        return FaultParams(
            fault_type=fault,
            crash_at_ms=int(rng.integers(100, 600)),
            packet_loss_p=float(rng.uniform(0.05, 0.20)),
        )
    if fault == "long_offline":
        dur = int(rng.integers(3000, 12000))
        return FaultParams(
            fault_type=fault,
            offline_duration_ms=dur,
            partition_start_ms=0,
            partition_end_ms=dur,
        )
    if fault == "concurrent_edits":
        return FaultParams(
            fault_type=fault,
            concurrent_server_ops=int(rng.integers(1, 4)),
            latency_ms_mean=float(rng.uniform(40, 120)),
        )
    return FaultParams(
        fault_type=fault,
        large_payload_bytes=int(rng.integers(80_000, 250_000)),
        packet_loss_p=float(rng.uniform(0.10, 0.30)),
        latency_ms_mean=float(rng.uniform(100, 400)),
        jitter_ms=float(rng.uniform(50, 200)),
    )


def _mutations(
    rng: np.random.Generator,
    records: list[dict[str, Any]],
    n_mut: int,
    ts0: int,
    large_bytes: int = 0,
) -> list[dict[str, Any]]:
    ops = ("update", "status_change", "attach_note", "create")
    out: list[dict[str, Any]] = []
    for i in range(n_mut):
        if ops[i % len(ops)] == "create" or (i == 0 and rng.random() < 0.15):
            rid = _rid("rec", 10_000 + int(rng.integers(0, 50_000)))
            fields = {
                "work_order_id": _rid("wo", int(rng.integers(1, 9000))),
                "site_id": f"site-{int(rng.integers(1, 40)):02d}",
                "asset_tag": f"AST-{int(rng.integers(100, 999))}",
                "inspector_id": f"insp-{int(rng.integers(1, 25)):02d}",
                "status": "open",
                "severity": SEVERITIES[int(rng.integers(0, len(SEVERITIES)))],
                "findings": FINDINGS[int(rng.integers(0, len(FINDINGS)))].format(
                    n=int(rng.integers(1, 50))
                ),
            }
            op, base_v = "create", 0
            pb = large_bytes if large_bytes else int(rng.integers(200, 3000))
        else:
            rec = records[int(rng.integers(0, len(records)))]
            rid = rec["record_id"]
            base_v = int(rec["version"])
            op = ops[int(rng.integers(1, len(ops)))]
            if op == "status_change":
                fields = {"status": STATUSES[int(rng.integers(0, len(STATUSES)))]}
            elif op == "attach_note":
                fields = {"findings": rec["findings"] + f" | note-{int(rng.integers(1, 999))}"}
            else:
                fields = {
                    "severity": SEVERITIES[int(rng.integers(0, len(SEVERITIES)))],
                    "findings": FINDINGS[int(rng.integers(0, len(FINDINGS)))].format(
                        n=int(rng.integers(1, 50))
                    ),
                }
            pb = large_bytes if large_bytes else int(rng.integers(200, 4000))
        mid = _rid("mut", int(rng.integers(1, 1_000_000)))
        ikey = hashlib.sha256(f"{mid}:{rid}:{op}:{sorted(fields.items())}".encode()).hexdigest()[:24]
        out.append(
            {
                "mutation_id": mid,
                "record_id": rid,
                "op": op,
                "fields": fields,
                "client_ts_ms": ts0 + 10 * (i + 1),
                "base_version": base_v,
                "idempotency_key": ikey,
                "payload_bytes": pb,
            }
        )
    return out


def _server_muts(
    rng: np.random.Generator, records: list[dict[str, Any]], n_ops: int, ts0: int
) -> list[dict[str, Any]]:
    out = []
    for i in range(n_ops):
        rec = records[int(rng.integers(0, len(records)))]
        mid = _rid("smut", int(rng.integers(1, 1_000_000)))
        fields = {
            "status": STATUSES[int(rng.integers(0, len(STATUSES)))],
            "findings": rec["findings"] + f" | server-edit-{i}",
        }
        ikey = hashlib.sha256(f"server:{mid}".encode()).hexdigest()[:24]
        out.append(
            {
                "mutation_id": mid,
                "record_id": rec["record_id"],
                "op": "update",
                "fields": fields,
                "client_ts_ms": ts0 + 5 + 7 * i,
                "base_version": int(rec["version"]),
                "idempotency_key": ikey,
                "payload_bytes": int(rng.integers(200, 800)),
            }
        )
    return out


def generate_scenarios(
    n: int = 450,
    seed: int = 42,
    train_frac: float = 0.70,
    dev_frac: float = 0.15,
) -> list[Scenario]:
    rng = np.random.default_rng(seed)
    per = n // len(FAULT_TYPES)
    rem = n - per * len(FAULT_TYPES)
    plan: list[FaultType] = []
    for i, ft in enumerate(FAULT_TYPES):
        plan.extend([ft] * (per + (1 if i < rem else 0)))
    rng.shuffle(plan)

    scenarios: list[Scenario] = []
    for i, fault in enumerate(plan):
        local_rng = np.random.default_rng(int(rng.integers(0, 2**31 - 1)))
        ts0 = 1_700_000_000_000 + i * 10_000
        n_rec = int(local_rng.integers(3, 8))
        records = [_make_record(local_rng, i * 20 + j, ts0) for j in range(n_rec)]
        fp = _fault_params(fault, local_rng)
        n_mut = int(local_rng.integers(3, 9))
        large = fp.large_payload_bytes if fault == "large_payload_sync" else 0
        local_muts = _mutations(local_rng, records, n_mut, ts0, large_bytes=large)
        server_muts: list[dict[str, Any]] = []
        if fault == "concurrent_edits":
            server_muts = _server_muts(local_rng, records, fp.concurrent_server_ops, ts0)
        exp: dict[str, int] = {r["record_id"]: int(r["version"]) for r in records}
        for m in local_muts:
            rid = m["record_id"]
            exp[rid] = 1 if m["op"] == "create" else exp.get(rid, 1) + 1
        for m in server_muts:
            exp[m["record_id"]] = exp.get(m["record_id"], 1) + 1
        scenarios.append(
            Scenario(
                scenario_id=_rid("scn", i + 1),
                split="",
                fault_type=fault,
                fault_params=fp,
                seed_local=int(local_rng.integers(0, 2**31 - 1)),
                initial_records=records,
                local_mutations=local_muts,
                server_mutations=server_muts,
                expected_final_versions=exp,
                meta={
                    "generator": GENERATOR_VERSION,
                    "n_records": n_rec,
                    "n_local_mutations": n_mut,
                    "n_server_mutations": len(server_muts),
                },
            )
        )

    by_fault: dict[str, list[Scenario]] = {ft: [] for ft in FAULT_TYPES}
    for s in scenarios:
        by_fault[s.fault_type].append(s)

    train: list[Scenario] = []
    dev: list[Scenario] = []
    test: list[Scenario] = []
    for ft, group in by_fault.items():
        group = sorted(group, key=lambda x: x.scenario_id)
        g_rng = np.random.default_rng(seed + int(hashlib.sha256(str(ft).encode()).hexdigest()[:8], 16) % 10_000)
        idx = np.arange(len(group))
        g_rng.shuffle(idx)
        ordered = [group[int(j)] for j in idx]
        n_g = len(ordered)
        n_train = int(round(n_g * train_frac))
        n_dev = int(round(n_g * dev_frac))
        if n_g >= 3 and n_train + n_dev >= n_g:
            n_dev = max(1, n_g - n_train - 1)
        n_test = n_g - n_train - n_dev
        if n_test < 0:
            n_test = 0
            n_dev = n_g - n_train
        for s in ordered[:n_train]:
            s.split = "train"
            train.append(s)
        for s in ordered[n_train : n_train + n_dev]:
            s.split = "dev"
            dev.append(s)
        for s in ordered[n_train + n_dev :]:
            s.split = "test"
            test.append(s)

    out = train + dev + test
    out.sort(key=lambda s: s.scenario_id)
    return out


def write_splits(scenarios: list[Scenario], out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    by: dict[str, list] = {"train": [], "dev": [], "test": [], "all": []}
    for s in scenarios:
        d = s.to_dict()
        by[s.split].append(d)
        by["all"].append(d)
    paths = {}
    for name, rows in by.items():
        p = out_dir / ("offlineaibench_v0.1.jsonl" if name == "all" else f"offlineaibench_v0.1_{name}.jsonl")
        with p.open("w") as f:
            for r in rows:
                f.write(json.dumps(r, sort_keys=True) + "\n")
        paths[name] = str(p)
    return {
        "generator": GENERATOR_VERSION,
        "n_total": len(scenarios),
        "n_train": len(by["train"]),
        "n_dev": len(by["dev"]),
        "n_test": len(by["test"]),
        "fault_counts_all": {ft: sum(1 for s in scenarios if s.fault_type == ft) for ft in FAULT_TYPES},
        "fault_counts_test": {ft: sum(1 for s in by["test"] if s["fault_type"] == ft) for ft in FAULT_TYPES},
        "paths": paths,
    }
