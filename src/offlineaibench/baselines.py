"""Sync client baselines: cloud_only, naive_retry_queue, durable_idempotent_queue."""
from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any


@dataclass
class QueueItem:
    mutation: dict[str, Any]
    attempts: int = 0
    acked: bool = False
    durable: bool = False


@dataclass
class ClientState:
    name: str
    crashed: bool = False
    durable_log: list[QueueItem] = field(default_factory=list)
    in_flight: dict[str, QueueItem] = field(default_factory=dict)
    acked_keys: set[str] = field(default_factory=set)
    user_visible_failures: int = 0
    bytes_sent: int = 0
    sync_attempts: int = 0
    last_progress_ms: int = 0


class SyncClient(ABC):
    def __init__(self, name: str):
        self.state = ClientState(name=name)

    @abstractmethod
    def enqueue(self, mutation: dict[str, Any], now_ms: int) -> None: ...

    @abstractmethod
    def on_crash(self, now_ms: int) -> None: ...

    @abstractmethod
    def pending(self) -> list[QueueItem]: ...

    @abstractmethod
    def on_ack(self, mutation_id: str, idempotency_key: str, now_ms: int) -> None: ...

    @abstractmethod
    def on_nack(self, mutation_id: str, now_ms: int, reason: str) -> None: ...

    def mark_sent(self, item: QueueItem, nbytes: int) -> None:
        item.attempts += 1
        self.state.sync_attempts += 1
        self.state.bytes_sent += nbytes
        self.state.in_flight[item.mutation["mutation_id"]] = item

    def clear_inflight(self, mutation_id: str) -> None:
        self.state.in_flight.pop(mutation_id, None)


class CloudOnlyClient(SyncClient):
    """No offline queue: failed send => mutation lost + user-visible failure."""

    def __init__(self) -> None:
        super().__init__("cloud_only")
        self._holding: list[QueueItem] = []

    def enqueue(self, mutation: dict[str, Any], now_ms: int) -> None:
        self._holding.append(QueueItem(mutation=mutation, durable=False))

    def pending(self) -> list[QueueItem]:
        return [q for q in self._holding if not q.acked]

    def on_crash(self, now_ms: int) -> None:
        lost = len([q for q in self._holding if not q.acked])
        self.state.user_visible_failures += lost
        self._holding.clear()
        self.state.in_flight.clear()
        self.state.crashed = True

    def on_ack(self, mutation_id: str, idempotency_key: str, now_ms: int) -> None:
        for q in self._holding:
            if q.mutation["mutation_id"] == mutation_id:
                q.acked = True
        self.state.last_progress_ms = now_ms
        self.clear_inflight(mutation_id)

    def on_nack(self, mutation_id: str, now_ms: int, reason: str) -> None:
        self._holding = [q for q in self._holding if q.mutation["mutation_id"] != mutation_id]
        self.state.user_visible_failures += 1
        self.clear_inflight(mutation_id)


class NaiveRetryQueueClient(SyncClient):
    """In-memory retry queue without idempotency; crash loses the queue."""

    def __init__(self, max_attempts: int = 10) -> None:
        super().__init__("naive_retry_queue")
        self.max_attempts = max_attempts
        self._queue: list[QueueItem] = []

    def enqueue(self, mutation: dict[str, Any], now_ms: int) -> None:
        self._queue.append(QueueItem(mutation=mutation, durable=False))

    def pending(self) -> list[QueueItem]:
        return [q for q in self._queue if not q.acked and q.attempts < self.max_attempts]

    def on_crash(self, now_ms: int) -> None:
        lost = len([q for q in self._queue if not q.acked])
        self.state.user_visible_failures += lost
        self._queue.clear()
        self.state.in_flight.clear()
        self.state.crashed = True

    def on_ack(self, mutation_id: str, idempotency_key: str, now_ms: int) -> None:
        for q in self._queue:
            if q.mutation["mutation_id"] == mutation_id:
                q.acked = True
        self.state.last_progress_ms = now_ms
        self.clear_inflight(mutation_id)

    def on_nack(self, mutation_id: str, now_ms: int, reason: str) -> None:
        for q in self._queue:
            if q.mutation["mutation_id"] == mutation_id:
                self.clear_inflight(mutation_id)
                if q.attempts >= self.max_attempts:
                    self.state.user_visible_failures += 1
                    q.acked = True


class DurableIdempotentQueueClient(SyncClient):
    """Clean-room reference: durable log + idempotency keys + resume after crash."""

    def __init__(self, max_attempts: int = 20) -> None:
        super().__init__("durable_idempotent_queue")
        self.max_attempts = max_attempts
        self._queue: list[QueueItem] = []

    def enqueue(self, mutation: dict[str, Any], now_ms: int) -> None:
        item = QueueItem(mutation=mutation, durable=True)
        self._queue.append(item)
        self.state.durable_log.append(deepcopy(item))

    def pending(self) -> list[QueueItem]:
        return [
            q
            for q in self._queue
            if not q.acked
            and q.attempts < self.max_attempts
            and q.mutation["idempotency_key"] not in self.state.acked_keys
        ]

    def on_crash(self, now_ms: int) -> None:
        self.state.crashed = True
        self.state.in_flight.clear()
        recovered: list[QueueItem] = []
        seen: set[str] = set()
        for q in self.state.durable_log:
            key = q.mutation["idempotency_key"]
            mid = q.mutation["mutation_id"]
            if key in self.state.acked_keys or mid in seen or q.acked:
                continue
            seen.add(mid)
            recovered.append(
                QueueItem(mutation=deepcopy(q.mutation), attempts=0, acked=False, durable=True)
            )
        self._queue = recovered

    def on_ack(self, mutation_id: str, idempotency_key: str, now_ms: int) -> None:
        self.state.acked_keys.add(idempotency_key)
        for q in self._queue:
            if q.mutation["mutation_id"] == mutation_id:
                q.acked = True
        for q in self.state.durable_log:
            if q.mutation["mutation_id"] == mutation_id:
                q.acked = True
        self.state.last_progress_ms = now_ms
        self.clear_inflight(mutation_id)

    def on_nack(self, mutation_id: str, now_ms: int, reason: str) -> None:
        for q in self._queue:
            if q.mutation["mutation_id"] == mutation_id:
                self.clear_inflight(mutation_id)
                if q.attempts >= self.max_attempts:
                    self.state.user_visible_failures += 1
                    q.acked = True


def make_client(name: str) -> SyncClient:
    if name == "cloud_only":
        return CloudOnlyClient()
    if name == "naive_retry_queue":
        return NaiveRetryQueueClient()
    if name == "durable_idempotent_queue":
        return DurableIdempotentQueueClient()
    raise ValueError(f"unknown client: {name}")
