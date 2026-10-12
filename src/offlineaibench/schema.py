"""Typed schemas for synthetic inspection records and sync scenarios."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

FaultType = Literal[
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

OpType = Literal["create", "update", "status_change", "attach_note"]


@dataclass
class FaultParams:
    fault_type: FaultType
    packet_loss_p: float = 0.0
    latency_ms_mean: float = 0.0
    jitter_ms: float = 0.0
    partition_start_ms: int = 0
    partition_end_ms: int = 0
    dup_p: float = 0.0
    reorder_window: int = 0
    crash_at_ms: int | None = None
    offline_duration_ms: int = 0
    concurrent_server_ops: int = 0
    large_payload_bytes: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Scenario:
    scenario_id: str
    split: str
    fault_type: FaultType
    fault_params: FaultParams
    seed_local: int
    initial_records: list[dict[str, Any]]
    local_mutations: list[dict[str, Any]]
    server_mutations: list[dict[str, Any]] = field(default_factory=list)
    expected_final_versions: dict[str, int] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "split": self.split,
            "fault_type": self.fault_type,
            "fault_params": self.fault_params.to_dict(),
            "seed_local": self.seed_local,
            "initial_records": self.initial_records,
            "local_mutations": self.local_mutations,
            "server_mutations": self.server_mutations,
            "expected_final_versions": self.expected_final_versions,
            "meta": self.meta,
        }
