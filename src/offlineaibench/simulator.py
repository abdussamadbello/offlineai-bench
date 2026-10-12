"""Event-driven offline-sync simulator for OfflineAI-Bench."""
from __future__ import annotations

import hashlib

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

import numpy as np

from .baselines import SyncClient, make_client
from .network import NetworkModel, WireMessage
from .schema import FaultParams


@dataclass
class ServerState:
    records: dict[str, dict[str, Any]]
    applied_ids: set[str] = field(default_factory=set)
    applied_idempotency: set[str] = field(default_factory=set)
    apply_log: list[dict[str, Any]] = field(default_factory=list)
    duplicate_applies: int = 0
    conflicts: int = 0
    bytes_received: int = 0
    # v0.2 diagnostics (do not change v0.1 duplicate_applies semantics):
    redelivery_dups: int = 0      # same mutation applied again (true duplicate)
    create_collisions: int = 0    # 'create' op on an existing record_id (upsert collision)

    def apply_mutation(
        self,
        mut: dict[str, Any],
        *,
        enforce_idempotency: bool,
        detect_conflicts: bool,
        now_ms: int,
    ) -> str:
        mid = mut["mutation_id"]
        ikey = mut["idempotency_key"]
        self.bytes_received += int(mut.get("payload_bytes", 256))

        if enforce_idempotency and ikey in self.applied_idempotency:
            return "duplicate"

        if not enforce_idempotency:
            if ikey in self.applied_idempotency or mid in self.applied_ids:
                self.duplicate_applies += 1
                self.redelivery_dups += 1
        if mut["op"] == "create" and mut["record_id"] in self.records and not (
            ikey in self.applied_idempotency or mid in self.applied_ids
        ):
            self.create_collisions += 1

        rid = mut["record_id"]
        op = mut["op"]
        fields = mut["fields"]

        if op == "create":
            if rid in self.records and not enforce_idempotency:
                self.duplicate_applies += 1
            self.records[rid] = {
                "record_id": rid,
                "work_order_id": fields.get("work_order_id", ""),
                "site_id": fields.get("site_id", ""),
                "asset_tag": fields.get("asset_tag", ""),
                "inspector_id": fields.get("inspector_id", ""),
                "status": fields.get("status", "open"),
                "severity": fields.get("severity", "low"),
                "findings": fields.get("findings", ""),
                "version": 1,
                "payload_bytes": int(mut.get("payload_bytes", 256)),
                "updated_at_ms": now_ms,
            }
            self.applied_ids.add(mid)
            self.applied_idempotency.add(ikey)
            self.apply_log.append({"mutation_id": mid, "record_id": rid, "op": op, "ts": now_ms})
            return "ok_create"

        if rid not in self.records:
            if detect_conflicts:
                self.conflicts += 1
            self.records[rid] = {
                "record_id": rid,
                "work_order_id": "",
                "site_id": "",
                "asset_tag": "",
                "inspector_id": "",
                "status": fields.get("status", "open"),
                "severity": fields.get("severity", "low"),
                "findings": fields.get("findings", ""),
                "version": 1,
                "payload_bytes": int(mut.get("payload_bytes", 256)),
                "updated_at_ms": now_ms,
            }
            self.applied_ids.add(mid)
            self.applied_idempotency.add(ikey)
            self.apply_log.append({"mutation_id": mid, "record_id": rid, "op": op, "ts": now_ms})
            return "conflict" if detect_conflicts else "applied"

        rec = self.records[rid]
        conflict = False
        if detect_conflicts and int(mut.get("base_version", 0)) != int(rec["version"]):
            self.conflicts += 1
            conflict = True

        for k, v in fields.items():
            rec[k] = v
        rec["version"] = int(rec["version"]) + 1
        rec["updated_at_ms"] = now_ms
        self.applied_ids.add(mid)
        self.applied_idempotency.add(ikey)
        self.apply_log.append({"mutation_id": mid, "record_id": rid, "op": op, "ts": now_ms})
        return "conflict" if conflict else "applied"


@dataclass
class SimResult:
    condition: str
    scenario_id: str
    fault_type: str
    n_local_mutations: int
    applied_unique: int
    lost_mutations: int
    duplicate_applies: int
    conflicts: int
    user_visible_failures: int
    convergence_time_ms: int
    bytes_sent: int
    bytes_received: int
    sync_attempts: int
    crashed: bool
    details: dict[str, Any] = field(default_factory=dict)


def run_scenario(scenario: dict[str, Any], condition: str, seed: int = 42) -> SimResult:
    rng = np.random.default_rng(seed + (int(hashlib.sha256(scenario["scenario_id"].encode()).hexdigest()[:8], 16) % 10_000))
    fp = FaultParams(**scenario["fault_params"])
    net = NetworkModel(params=fp, rng=rng)
    client: SyncClient = make_client(condition)

    enforce_idem = condition == "durable_idempotent_queue"
    # Conflict detection only when server concurrent edits exist or reorder can skew versions.
    detect_conflicts = (
        condition == "durable_idempotent_queue"
        and (
            bool(scenario.get("server_mutations"))
            or scenario.get("fault_type") in ("concurrent_edits", "reorder")
        )
    )

    server = ServerState(
        records={r["record_id"]: deepcopy(r) for r in scenario["initial_records"]}
    )

    for sm in scenario.get("server_mutations", []):
        server.apply_mutation(
            sm, enforce_idempotency=True, detect_conflicts=False, now_ms=int(sm["client_ts_ms"])
        )

    local_muts = [deepcopy(m) for m in scenario["local_mutations"]]
    local_ver = {r["record_id"]: int(r["version"]) for r in scenario["initial_records"]}
    server_touched = {sm["record_id"] for sm in scenario.get("server_mutations", [])}
    for m in local_muts:
        rid = m["record_id"]
        if m["op"] == "create":
            m["base_version"] = 0
            local_ver[rid] = 1
        elif rid in server_touched:
            m["base_version"] = int(
                next(r["version"] for r in scenario["initial_records"] if r["record_id"] == rid)
            )
            local_ver[rid] = local_ver.get(rid, 1) + 1
        else:
            m["base_version"] = int(local_ver.get(rid, 1))
            local_ver[rid] = int(local_ver.get(rid, 1)) + 1

    tick = 10
    enqueue_at = {m["mutation_id"]: (i + 1) * 20 for i, m in enumerate(local_muts)}

    crash_at = None
    if fp.crash_at_ms is not None and local_muts:
        crash_at = max(30, int(0.4 * len(local_muts) * 20))

    now = 0
    horizon = 25_000
    if fp.partition_end_ms:
        horizon = max(horizon, int(fp.partition_end_ms) + 8_000)
    if fp.offline_duration_ms:
        horizon = max(horizon, int(fp.offline_duration_ms) + 8_000)

    inflight: list[WireMessage] = []
    crash_done = False
    intended = {m["mutation_id"] for m in local_muts}
    intended_keys = {m["idempotency_key"] for m in local_muts}
    enqueued: set[str] = set()
    acked_ids: set[str] = set()
    last_ack_ms = 0
    converged_at: int | None = None

    while now <= horizon:
        if crash_at is not None and not crash_done and now >= crash_at:
            client.on_crash(now)
            inflight = []
            crash_done = True
            now += 40
            continue

        for m in local_muts:
            mid = m["mutation_id"]
            if mid not in enqueued and now >= enqueue_at[mid]:
                client.enqueue(m, now)
                enqueued.add(mid)

        still: list[WireMessage] = []
        for msg in inflight:
            if msg.deliver_at_ms <= now:
                status = server.apply_mutation(
                    msg.mutation,
                    enforce_idempotency=enforce_idem,
                    detect_conflicts=detect_conflicts,
                    now_ms=now,
                )
                mid = msg.mutation["mutation_id"]
                ikey = msg.mutation["idempotency_key"]
                if status in ("applied", "ok_create", "duplicate", "conflict"):
                    client.on_ack(mid, ikey, now)
                    acked_ids.add(mid)
                    last_ack_ms = now
                else:
                    client.on_nack(mid, now, status)
            else:
                still.append(msg)
        inflight = still

        sent_this_tick = 0
        for item in client.pending():
            if sent_this_tick >= 3:
                break
            mid = item.mutation["mutation_id"]
            if mid in client.state.in_flight:
                continue

            msgs, reason = net.try_send(item.mutation, now, attempt=item.attempts + 1)

            if reason == "partition":
                if condition == "cloud_only":
                    client.on_nack(mid, now, "partition")
                continue

            if reason == "reorder_hold":
                client.mark_sent(item, int(item.mutation.get("payload_bytes", 256)))
                sent_this_tick += 1
                continue

            if reason == "loss" or not msgs:
                if condition == "cloud_only":
                    client.on_nack(mid, now, "loss")
                else:
                    item.attempts += 1
                    client.state.sync_attempts += 1
                    client.clear_inflight(mid)
                    if item.attempts >= getattr(client, "max_attempts", 10):
                        client.on_nack(mid, now, "exhausted")
                continue

            client.mark_sent(item, int(item.mutation.get("payload_bytes", 256)))
            inflight.extend(msgs)
            sent_this_tick += 1

        if now % 100 == 0 or (fp.partition_end_ms and now == int(fp.partition_end_ms)):
            inflight.extend(net.flush_reorder())

        if enforce_idem:
            applied_unique = len(intended_keys & server.applied_idempotency)
        else:
            applied_unique = len(intended & server.applied_ids)

        if (
            applied_unique >= len(intended)
            and not client.pending()
            and not inflight
            and len(enqueued) >= len(intended)
        ):
            converged_at = now
            break

        now += tick

    inflight.extend(net.flush_reorder())
    drain_t = now
    for msg in sorted(inflight, key=lambda m: m.deliver_at_ms):
        drain_t = max(drain_t, msg.deliver_at_ms)
        status = server.apply_mutation(
            msg.mutation,
            enforce_idempotency=enforce_idem,
            detect_conflicts=detect_conflicts,
            now_ms=drain_t,
        )
        mid = msg.mutation["mutation_id"]
        ikey = msg.mutation["idempotency_key"]
        if status in ("applied", "ok_create", "duplicate", "conflict"):
            client.on_ack(mid, ikey, drain_t)
            acked_ids.add(mid)
            last_ack_ms = drain_t

    if condition != "cloud_only":
        t = drain_t
        extra_deadline = drain_t + 5_000
        while t <= extra_deadline and client.pending():
            for item in list(client.pending())[:3]:
                mid = item.mutation["mutation_id"]
                if mid in client.state.in_flight:
                    continue
                msgs, reason = net.try_send(item.mutation, t, attempt=item.attempts + 1)
                if reason == "partition":
                    continue
                if reason == "loss" or not msgs:
                    item.attempts += 1
                    client.state.sync_attempts += 1
                    if item.attempts >= getattr(client, "max_attempts", 10):
                        client.on_nack(mid, t, "exhausted")
                    continue
                if reason == "reorder_hold":
                    client.mark_sent(item, int(item.mutation.get("payload_bytes", 256)))
                    continue
                client.mark_sent(item, int(item.mutation.get("payload_bytes", 256)))
                for msg in msgs:
                    status = server.apply_mutation(
                        msg.mutation,
                        enforce_idempotency=enforce_idem,
                        detect_conflicts=detect_conflicts,
                        now_ms=t + max(0, msg.deliver_at_ms - msg.sent_at_ms),
                    )
                    if status in ("applied", "ok_create", "duplicate", "conflict"):
                        client.on_ack(
                            msg.mutation["mutation_id"],
                            msg.mutation["idempotency_key"],
                            t,
                        )
                        acked_ids.add(msg.mutation["mutation_id"])
                        last_ack_ms = t
            for msg in net.flush_reorder():
                status = server.apply_mutation(
                    msg.mutation,
                    enforce_idempotency=enforce_idem,
                    detect_conflicts=detect_conflicts,
                    now_ms=t,
                )
                if status in ("applied", "ok_create", "duplicate", "conflict"):
                    client.on_ack(
                        msg.mutation["mutation_id"],
                        msg.mutation["idempotency_key"],
                        t,
                    )
                    acked_ids.add(msg.mutation["mutation_id"])
                    last_ack_ms = t
            t += tick
        drain_t = t

    if enforce_idem:
        applied_unique = len(intended_keys & server.applied_idempotency)
    else:
        applied_unique = len(intended & server.applied_ids)

    lost = max(0, len(intended) - applied_unique)
    if converged_at is None:
        if lost == 0 and applied_unique >= len(intended):
            converged_at = last_ack_ms if last_ack_ms else drain_t
        else:
            converged_at = horizon

    return SimResult(
        condition=condition,
        scenario_id=scenario["scenario_id"],
        fault_type=scenario["fault_type"],
        n_local_mutations=len(intended),
        applied_unique=applied_unique,
        lost_mutations=lost,
        duplicate_applies=server.duplicate_applies,
        conflicts=server.conflicts,
        user_visible_failures=client.state.user_visible_failures,
        convergence_time_ms=int(converged_at),
        bytes_sent=client.state.bytes_sent,
        bytes_received=server.bytes_received,
        sync_attempts=client.state.sync_attempts,
        crashed=client.state.crashed,
        details={
            "redelivery_dups": server.redelivery_dups,
            "create_collisions": server.create_collisions,
            "acked_ids": sorted(acked_ids),
            "server_apply_count": len(server.apply_log),
            "crash_at": crash_at,
        },
    )
