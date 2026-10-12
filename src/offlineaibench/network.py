"""Pure-Python network fault injector (no tc/netem / root required)."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

from .schema import FaultParams


@dataclass
class WireMessage:
    msg_id: str
    mutation: dict[str, Any]
    sent_at_ms: int
    deliver_at_ms: int
    attempt: int = 1
    is_duplicate: bool = False


@dataclass
class NetworkModel:
    """Event-driven channel: loss, latency/jitter, partition, dup, reorder."""

    params: FaultParams
    rng: np.random.Generator
    _reorder_buf: list[WireMessage] = field(default_factory=list)
    _seq: int = 0

    def in_partition(self, now_ms: int) -> bool:
        p = self.params
        if p.fault_type in ("partition", "long_offline"):
            return p.partition_start_ms <= now_ms < p.partition_end_ms
        if p.offline_duration_ms > 0 and now_ms < p.offline_duration_ms:
            return True
        return False

    def _latency(self) -> int:
        mean = max(0.0, self.params.latency_ms_mean)
        jitter = max(0.0, self.params.jitter_ms)
        if mean == 0 and jitter == 0:
            return 0
        delay = mean + float(self.rng.normal(0.0, jitter / 2.0 if jitter else 1.0))
        return max(0, int(delay))

    def try_send(
        self, mutation: dict[str, Any], now_ms: int, attempt: int = 1
    ) -> tuple[list[WireMessage], str]:
        """Return (messages, reason) with reason in ok|partition|loss|reorder_hold."""
        if self.in_partition(now_ms):
            return [], "partition"

        loss_p = float(self.params.packet_loss_p)
        if self.params.fault_type in ("partition", "long_offline"):
            loss_p = 0.0
        if loss_p > 0 and float(self.rng.random()) < loss_p:
            return [], "loss"

        self._seq += 1
        deliver = now_ms + self._latency()
        msg = WireMessage(
            msg_id=f"wire-{self._seq}",
            mutation=mutation,
            sent_at_ms=now_ms,
            deliver_at_ms=deliver,
            attempt=attempt,
        )
        out = [msg]

        dup_p = float(self.params.dup_p)
        if dup_p > 0 and float(self.rng.random()) < dup_p:
            self._seq += 1
            out.append(
                WireMessage(
                    msg_id=f"wire-{self._seq}",
                    mutation=mutation,
                    sent_at_ms=now_ms,
                    deliver_at_ms=deliver + int(self.rng.integers(5, 40)),
                    attempt=attempt,
                    is_duplicate=True,
                )
            )

        window = int(self.params.reorder_window)
        if window > 1:
            self._reorder_buf.extend(out)
            if len(self._reorder_buf) >= window:
                chunk = self._reorder_buf[:window]
                self._reorder_buf = self._reorder_buf[window:]
                order = np.arange(len(chunk))
                self.rng.shuffle(order)
                reordered = [chunk[int(i)] for i in order]
                base = min(m.deliver_at_ms for m in reordered)
                for j, m in enumerate(reordered):
                    m.deliver_at_ms = base + j * 3
                return reordered, "ok"
            return [], "reorder_hold"

        return out, "ok"

    def flush_reorder(self) -> list[WireMessage]:
        buf = self._reorder_buf
        self._reorder_buf = []
        return buf
