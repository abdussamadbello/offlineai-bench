"""OfflineAI-Bench v0.2 harness (camera-ready).

Adds to v0.1: (a) three harder faults -- partial_write (ack loss + crash with
batched local fsync), clock_skew (timestamp-LWW backend, skewed client clock),
conflict_storm (many server edits on the same fields during sync);
(b) two more clients -- durable_batched_fsync (durable log fsynced every
F=50 ms, i.e. a torn-tail log) and crdt_yjs (pycrdt / Yjs CRDT doc
sync with state-vector diffs); (c) metrics: true redelivery duplicates,
create collisions, silent concurrent overwrites, phantom failures,
bytes on the wire, CPU time and peak memory.

Same channel model as v0.1 (network.NetworkModel). Pure Python, CPU only.
"""
from __future__ import annotations

import hashlib
import time
import tracemalloc
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pycrdt

from .generator import FAULT_TYPES, _fault_params, _make_record, _mutations, _rid
from .network import NetworkModel
from .schema import FaultParams

HARD_FAULTS = ["partial_write", "clock_skew", "conflict_storm"]
ALL_FAULTS = list(FAULT_TYPES) + HARD_FAULTS
CONDITIONS_V02 = [
    "cloud_only",
    "naive_retry_queue",
    "durable_idempotent_queue",
    "durable_batched_fsync",
    "crdt_yjs",
]
EDITABLE = ("status", "severity", "findings")
TICK = 10
SEND_PER_TICK = 3
ACK_TIMEOUT_MS = 400


# ----------------------------------------------------------------- scenarios
def _hard_params(fault: str, rng: np.random.Generator) -> dict[str, Any]:
    if fault == "partial_write":
        return dict(fault_type=fault, packet_loss_p=float(rng.uniform(0.02, 0.10)),
                    crash_at_ms=int(rng.integers(100, 600)),
                    ack_loss_p=float(rng.uniform(0.20, 0.45)))
    if fault == "clock_skew":
        return dict(fault_type=fault, latency_ms_mean=float(rng.uniform(40, 120)),
                    clock_skew_ms=int(rng.integers(-60_000, 60_001)),
                    server_edit_age_ms=int(rng.integers(1_000, 30_000)),
                    concurrent_server_ops=int(rng.integers(2, 5)))
    return dict(fault_type=fault, latency_ms_mean=float(rng.uniform(40, 150)),
                jitter_ms=float(rng.uniform(20, 100)),
                concurrent_server_ops=int(rng.integers(10, 31)),
                storm_window_ms=int(rng.integers(500, 2000)))


def _server_edits(rng, records, n_ops, focus: list[str] | None = None):
    out = []
    for _ in range(n_ops):
        rid = focus[int(rng.integers(0, len(focus)))] if focus else records[int(rng.integers(0, len(records)))]["record_id"]
        k = EDITABLE[int(rng.integers(0, len(EDITABLE)))]
        out.append({"record_id": rid, "field": k, "value": f"srv-{k}-{int(rng.integers(0, 10**6))}"})
    return out


def generate_v02(seed: int, n_per_fault: int = 50) -> list[dict[str, Any]]:
    """Scenarios for all 12 faults; v0.1 parameter ranges reused for the 9 original faults."""
    rng = np.random.default_rng(seed)
    out = []
    for fault in ALL_FAULTS:
        for j in range(n_per_fault):
            lr = np.random.default_rng(int(rng.integers(0, 2**31 - 1)))
            ts0 = 1_700_000_000_000 + j * 10_000
            recs = [_make_record(lr, j * 20 + k, ts0) for k in range(int(lr.integers(3, 8)))]
            if fault in HARD_FAULTS:
                fp = _hard_params(fault, lr)
            else:
                fp = _fault_params(fault, lr).to_dict()
            n_mut = int(lr.integers(3, 9))
            large = int(fp.get("large_payload_bytes", 0) or 0)
            muts = _mutations(lr, recs, n_mut, ts0, large_bytes=large)
            touched = sorted({m["record_id"] for m in muts if m["record_id"] in {r["record_id"] for r in recs}})
            sedits: list[dict[str, Any]] = []
            if fault in ("concurrent_edits", "clock_skew"):
                sedits = _server_edits(lr, recs, int(fp["concurrent_server_ops"]))
            elif fault == "conflict_storm":
                sedits = _server_edits(lr, recs, int(fp["concurrent_server_ops"]), focus=touched or None)
                w = int(fp["storm_window_ms"])
                for e in sedits:
                    e["at_ms"] = int(lr.integers(0, w))
            out.append({"scenario_id": f"{fault}-{seed}-{j:03d}", "fault_type": fault, "fault_params": fp,
                        "initial_records": recs, "local_mutations": muts, "server_edits": sedits})
    # stratified split as in v0.1 (35/8/7 of 50)
    by: dict[str, list] = {}
    for s in out:
        by.setdefault(s["fault_type"], []).append(s)
    for ft, g in by.items():
        g_rng = np.random.default_rng(seed + int(hashlib.sha256(ft.encode()).hexdigest()[:8], 16) % 10_000)
        idx = np.arange(len(g)); g_rng.shuffle(idx)
        n_tr, n_dv = int(round(len(g) * 0.7)), int(round(len(g) * 0.15))
        for r, i in enumerate(idx):
            g[int(i)]["split"] = "train" if r < n_tr else ("dev" if r < n_tr + n_dv else "test")
    return out


# --------------------------------------------------------------- server model
@dataclass
class Server:
    """Record store. policy: 'arrival' (last arrival wins) or 'lww_ts' (client timestamp LWW)."""
    records: dict[str, dict[str, Any]]
    policy: str = "arrival"
    field_ts: dict[tuple[str, str], int] = field(default_factory=dict)
    field_writer: dict[tuple[str, str], str] = field(default_factory=dict)
    server_seq: dict[str, int] = field(default_factory=dict)          # server edits per record
    field_srv_seq: dict[tuple[str, str], int] = field(default_factory=dict)
    applied: dict[str, int] = field(default_factory=dict)              # mutation_id -> apply count
    idem: set[str] = field(default_factory=set)
    create_collisions: int = 0
    bytes_in: int = 0

    def server_edit(self, e: dict[str, Any], ts: int) -> None:
        rid, k = e["record_id"], e["field"]
        rec = self.records.setdefault(rid, {"record_id": rid, "version": 1})
        rec[k] = e["value"]; rec["version"] = int(rec.get("version", 1)) + 1
        self.server_seq[rid] = self.server_seq.get(rid, 0) + 1
        self.field_srv_seq[(rid, k)] = self.server_seq[rid]
        self.field_ts[(rid, k)] = ts; self.field_writer[(rid, k)] = "server"

    def apply(self, m: dict[str, Any], *, idempotent: bool, ts: int, nbytes: int) -> str:
        self.bytes_in += nbytes
        mid, key, rid = m["mutation_id"], m["idempotency_key"], m["record_id"]
        if idempotent and key in self.idem:
            return "dup_suppressed"
        if m["op"] == "create" and rid in self.records and mid not in self.applied:
            self.create_collisions += 1
        rec = self.records.setdefault(rid, {"record_id": rid, "version": 0})
        for k, v in m["fields"].items():
            if self.policy == "lww_ts" and self.field_ts.get((rid, k), -10**18) > ts:
                continue  # silently discarded by timestamp LWW
            rec[k] = v; self.field_ts[(rid, k)] = ts; self.field_writer[(rid, k)] = mid
        rec["version"] = int(rec.get("version", 0)) + 1
        self.applied[mid] = self.applied.get(mid, 0) + 1
        self.idem.add(key)
        return "applied"


# ---------------------------------------------------------------- simulation
@dataclass
class Out:
    condition: str
    scenario_id: str
    fault_type: str
    n: int
    lost: int = 0
    redelivery_dups: int = 0
    create_collisions: int = 0
    uvf: int = 0
    phantom_failures: int = 0
    conflicts_surfaced: int = 0
    concurrent_pairs: int = 0
    silent_overwrites: int = 0      # local write clobbered a concurrent server edit, not surfaced
    silent_discards: int = 0        # local write lost to server edit / LWW, not surfaced
    convergence_ms: int = 0
    bytes_sent: int = 0
    intended_bytes: int = 0
    sends: int = 0


def _prep(scn):
    fp_d = dict(scn["fault_params"])
    extra = {k: fp_d.pop(k) for k in list(fp_d) if k not in FaultParams.__dataclass_fields__}
    fp = FaultParams(**fp_d)
    return fp, extra


def _horizon(fp: FaultParams) -> int:
    h = 25_000
    if fp.partition_end_ms:
        h = max(h, int(fp.partition_end_ms) + 8_000)
    if fp.offline_duration_ms:
        h = max(h, int(fp.offline_duration_ms) + 8_000)
    return h


def _rng(scn, seed):
    return np.random.default_rng(seed + int(hashlib.sha256(scn["scenario_id"].encode()).hexdigest()[:8], 16) % 10_000)


def _crash_at(fp, muts):
    if fp.crash_at_ms is None or not muts:
        return None
    return max(30, int(0.4 * len(muts) * 20))


def _concurrency_eval(o: Out, scn, snap, local_intent, surfaced_ids: set[str], known_seq, cutoff):
    """Pairs = (local mutation, field) with a *concurrent* server edit to that field: applied after the
    client's base snapshot (seq > known) and before the local write reached the server (seq <= cutoff)."""
    for m, k, v in local_intent:
        rid = m["record_id"]
        cut = cutoff.get(m["mutation_id"], 10**9)
        srv_edits_on_field = [e for e in scn["_srv_applied"] if e["record_id"] == rid and e["field"] == k
                              and known_seq.get(rid, 0) < e["_seq"] <= cut]
        got = snap.get((m["mutation_id"], k))
        lww_loss = scn["fault_type"] == "clock_skew" and got != v and m["mutation_id"] in scn["_applied_ids"]
        if not srv_edits_on_field and not lww_loss:
            continue
        o.concurrent_pairs += 1
        if m["mutation_id"] in surfaced_ids:
            continue
        if got == v:
            o.silent_overwrites += 1
        else:
            o.silent_discards += 1


def run_legacy(scn, condition: str, seed: int) -> Out:
    rng = _rng(scn, seed)
    fp, extra = _prep(scn)
    net = NetworkModel(params=fp, rng=rng)
    muts = [deepcopy(m) for m in scn["local_mutations"]]
    o = Out(condition, scn["scenario_id"], scn["fault_type"], len(muts))
    o.intended_bytes = sum(int(m["payload_bytes"]) for m in muts)
    policy = "lww_ts" if scn["fault_type"] == "clock_skew" else "arrival"
    srv = Server(records={r["record_id"]: deepcopy(r) for r in scn["initial_records"]}, policy=policy)
    idem = condition in ("durable_idempotent_queue", "durable_batched_fsync")
    durable = idem
    fsync_every = 50 if condition == "durable_batched_fsync" else 0
    ack_loss = float(extra.get("ack_loss_p", 0.0))
    skew = int(extra.get("clock_skew_ms", 0))
    scn["_srv_applied"] = []
    known_seq: dict[str, int] = {}
    # pre-sync server edits
    pre = [e for e in scn["server_edits"] if "at_ms" not in e]
    timed = sorted([e for e in scn["server_edits"] if "at_ms" in e], key=lambda e: e["at_ms"])
    age = int(extra.get("server_edit_age_ms", 0))
    for e in pre:
        srv.server_edit(e, ts=-age)
        scn["_srv_applied"].append({**e, "_seq": srv.server_seq[e["record_id"]]})
    if scn["fault_type"] == "clock_skew":
        known_seq = dict(srv.server_seq)  # client pulled these edits before going to work
    enqueue_at = {m["mutation_id"]: (i + 1) * 20 for i, m in enumerate(muts)}
    crash_at = _crash_at(fp, muts)
    horizon = _horizon(fp)
    queue: list[dict[str, Any]] = []       # items: {m, attempts, acked, inflight_until, enq}
    log_synced_upto = 0                    # durable_batched: entries enqueued before this time are on disk
    last_fsync = 0
    inflight: list = []
    acked: set[str] = set()
    surfaced: set[str] = set()
    cutoff: dict[str, int] = {}
    snap: dict[tuple[str, str], Any] = {}
    enq: set[str] = set()
    now, crashed, conv = 0, False, None
    max_att = 20 if durable else 10
    ti = 0
    while now <= horizon:
        while ti < len(timed) and timed[ti]["at_ms"] <= now:
            e = timed[ti]; srv.server_edit(e, ts=now)
            scn["_srv_applied"].append({**e, "_seq": srv.server_seq[e["record_id"]]}); ti += 1
        if fsync_every and now - last_fsync >= fsync_every:
            log_synced_upto, last_fsync = now, now
        if crash_at is not None and not crashed and now >= crash_at:
            crashed = True
            unacked = [q for q in queue if not q["acked"]]
            if condition in ("cloud_only", "naive_retry_queue"):
                o.uvf += len(unacked); queue = [q for q in queue if q["acked"]]
            elif condition == "durable_batched_fsync":
                torn = [q for q in unacked if q["enq"] >= log_synced_upto]
                o.uvf += 0  # torn-tail loss is silent: the app believed the write was saved
                queue = [q for q in queue if q not in torn]
            for q in queue:
                q["inflight_until"] = -1
            inflight = []
            now += 40
            continue
        for m in muts:
            if m["mutation_id"] not in enq and now >= enqueue_at[m["mutation_id"]]:
                enq.add(m["mutation_id"])
                queue.append({"m": m, "attempts": 0, "acked": False, "inflight_until": -1, "enq": now,
                              "base_seq": known_seq.get(m["record_id"], 0)})
        # deliveries
        still = []
        for msg in inflight:
            if msg.deliver_at_ms > now:
                still.append(msg); continue
            m = msg.mutation
            st = srv.apply(m, idempotent=idem, ts=int(enqueue_at[m["mutation_id"]]) + skew,
                           nbytes=int(m["payload_bytes"]))
            if st == "applied" and srv.applied[m["mutation_id"]] > 1:
                o.redelivery_dups += 1
            if st == "applied" and m["mutation_id"] not in cutoff:
                cutoff[m["mutation_id"]] = srv.server_seq.get(m["record_id"], 0)
                for k in m["fields"]:
                    snap[(m["mutation_id"], k)] = srv.records.get(m["record_id"], {}).get(k)
            if durable and st == "applied":
                rid = m["record_id"]
                if srv.server_seq.get(rid, 0) > known_seq.get(rid, 0) and any(
                        e["record_id"] == rid and e["_seq"] > known_seq.get(rid, 0) for e in scn["_srv_applied"]):
                    if m["mutation_id"] not in surfaced:
                        surfaced.add(m["mutation_id"]); o.conflicts_surfaced += 1
            if ack_loss and float(rng.random()) < ack_loss:
                continue  # partial write: server applied, client never hears back
            for q in queue:
                if q["m"]["mutation_id"] == m["mutation_id"] and not q["acked"]:
                    q["acked"] = True; acked.add(m["mutation_id"])
        inflight = still
        # sends
        sent = 0
        for q in queue:
            if sent >= SEND_PER_TICK:
                break
            if q["acked"] or q["inflight_until"] > now or q["attempts"] >= max_att or q.get("dead"):
                continue
            if condition == "cloud_only" and q["attempts"] >= 1:
                # no retry: an unanswered request is a user-visible failure
                q["dead"] = True; o.uvf += 1
                if q["m"]["mutation_id"] in srv.applied:
                    o.phantom_failures += 1
                continue
            msgs, reason = net.try_send(q["m"], now, attempt=q["attempts"] + 1)
            if reason == "partition":
                if condition == "cloud_only":
                    q["dead"] = True; o.uvf += 1
                continue
            q["attempts"] += 1; o.sends += 1
            o.bytes_sent += int(q["m"]["payload_bytes"])
            q["inflight_until"] = now + ACK_TIMEOUT_MS + int(fp.latency_ms_mean) * 2
            sent += 1
            if reason in ("loss",):
                continue
            inflight.extend(msgs)
            if q["attempts"] >= max_att and condition != "cloud_only":
                pass
        if now % 100 == 0:
            inflight.extend(net.flush_reorder())
        for q in queue:
            if not q["acked"] and q["attempts"] >= max_att and q["inflight_until"] <= now and not q.get("dead"):
                q["dead"] = True; o.uvf += 1
                if q["m"]["mutation_id"] in srv.applied:
                    o.phantom_failures += 1
        if len(enq) == len(muts) and all(q["acked"] or q.get("dead") for q in queue) and not inflight \
                and ti >= len(timed) and (crash_at is None or crashed):
            conv = now; break
        now += TICK
    for msg in inflight + net.flush_reorder():
        srv.apply(msg.mutation, idempotent=idem, ts=int(enqueue_at[msg.mutation["mutation_id"]]) + skew,
                  nbytes=int(msg.mutation["payload_bytes"]))
    scn["_applied_ids"] = set(srv.applied)
    o.lost = sum(1 for m in muts if m["mutation_id"] not in srv.applied)
    o.create_collisions = srv.create_collisions
    o.convergence_ms = conv if conv is not None else horizon
    intent = [(m, k, v) for m in muts if m["mutation_id"] in srv.applied for k, v in m["fields"].items() if k in EDITABLE]
    base = {m["mutation_id"]: q["base_seq"] for q in queue for m in [q["m"]]}
    ks = {m["record_id"]: base.get(m["mutation_id"], 0) for m in muts}
    _concurrency_eval(o, scn, snap, intent, surfaced, ks, cutoff)
    return o


def run_crdt(scn, seed: int) -> Out:
    """pycrdt (Yjs) local-first client: every local edit is a CRDT op persisted locally; sync sends
    state-vector diffs; server merges (idempotent, order-insensitive). One map key per record field."""
    rng = _rng(scn, seed)
    fp, extra = _prep(scn)
    net = NetworkModel(params=fp, rng=rng)
    muts = [deepcopy(m) for m in scn["local_mutations"]]
    o = Out("crdt_yjs", scn["scenario_id"], scn["fault_type"], len(muts))
    o.intended_bytes = sum(int(m["payload_bytes"]) for m in muts)
    ack_loss = float(extra.get("ack_loss_p", 0.0))
    sdoc = pycrdt.Doc(client_id=1); smap = sdoc.get("r", type=pycrdt.Map)
    with sdoc.transaction():
        for r in scn["initial_records"]:
            for k in EDITABLE:
                smap[f"{r['record_id']}/{k}"] = r[k]
    scn["_srv_applied"] = []
    seq: dict[str, int] = {}
    def srv_edit(e):
        with sdoc.transaction():
            smap[f"{e['record_id']}/{e['field']}"] = e["value"]
        seq[e["record_id"]] = seq.get(e["record_id"], 0) + 1
        scn["_srv_applied"].append({**e, "_seq": seq[e["record_id"]]})
    cdoc = pycrdt.Doc(client_id=2); cmap = cdoc.get("r", type=pycrdt.Map)
    pre = [e for e in scn["server_edits"] if "at_ms" not in e]
    timed = sorted([e for e in scn["server_edits"] if "at_ms" in e], key=lambda e: e["at_ms"])
    known_seq: dict[str, int] = {}
    if scn["fault_type"] == "clock_skew":
        for e in pre: srv_edit(e)
        cdoc.apply_update(sdoc.get_update()); known_seq = dict(seq)   # client pulled edits
    else:
        cdoc.apply_update(sdoc.get_update())
        for e in pre: srv_edit(e)
    known_sv = sdoc.get_state() if scn["fault_type"] == "clock_skew" else cdoc.get_state()
    enqueue_at = {m["mutation_id"]: (i + 1) * 20 for i, m in enumerate(muts)}
    horizon = _horizon(fp)
    inflight: list = []
    enq: set[str] = set()
    now, conv, ti, wait_until = 0, None, 0, -1
    crash_at = _crash_at(fp, muts); crashed = False
    dirty = False
    cutoff: dict[str, int] = {}
    snap: dict[tuple[str, str], Any] = {}
    while now <= horizon:
        while ti < len(timed) and timed[ti]["at_ms"] <= now:
            srv_edit(timed[ti]); ti += 1
        if crash_at is not None and not crashed and now >= crash_at:
            crashed = True; inflight = []; wait_until = -1; now += 40; continue  # doc persisted per edit
        for m in muts:
            if m["mutation_id"] not in enq and now >= enqueue_at[m["mutation_id"]]:
                enq.add(m["mutation_id"]); dirty = True
                with cdoc.transaction():
                    for k, v in m["fields"].items():
                        cmap[f"{m['record_id']}/{k}"] = v
                    cmap[f"__m/{m['mutation_id']}"] = "x" * int(m["payload_bytes"])  # carries payload bytes
        still = []
        for msg in inflight:
            if msg.deliver_at_ms > now:
                still.append(msg); continue
            upd = msg.mutation["update"]
            sdoc.apply_update(upd)
            for m in muts:
                if m["mutation_id"] not in cutoff and f"__m/{m['mutation_id']}" in smap:
                    cutoff[m["mutation_id"]] = seq.get(m["record_id"], 0)
                    for k in m["fields"]:
                        snap[(m["mutation_id"], k)] = smap.get(f"{m['record_id']}/{k}")
            if ack_loss and float(rng.random()) < ack_loss:
                continue
            known_sv = sdoc.get_state(); wait_until = -1
        inflight = still
        if now % 100 == 0:
            inflight.extend(net.flush_reorder())
        pending = any(f"__m/{m['mutation_id']}" not in smap for m in muts if m["mutation_id"] in enq)
        if dirty and pending and wait_until <= now:
            upd = cdoc.get_update(known_sv)
            msgs, reason = net.try_send({"update": upd, "mutation_id": "diff"}, now)
            if reason != "partition":
                o.sends += 1; o.bytes_sent += len(upd)
                wait_until = now + ACK_TIMEOUT_MS + int(fp.latency_ms_mean) * 2
                if reason != "loss":
                    inflight.extend(msgs)
        if len(enq) == len(muts) and not pending and ti >= len(timed) and (crash_at is None or crashed):
            conv = now; break
        now += TICK
    for msg in inflight + net.flush_reorder():
        sdoc.apply_update(msg.mutation["update"])
    applied = {m["mutation_id"] for m in muts if f"__m/{m['mutation_id']}" in smap}
    scn["_applied_ids"] = applied
    o.lost = len(muts) - len(applied)
    o.convergence_ms = conv if conv is not None else horizon
    o.bytes_sent = int(o.bytes_sent)
    intent = [(m, k, v) for m in muts if m["mutation_id"] in applied for k, v in m["fields"].items() if k in EDITABLE]
    # within-client later writes to same key supersede earlier ones: keep last intent per key
    last: dict[tuple[str, str], tuple] = {}
    for m, k, v in intent:
        last[(m["record_id"], k)] = (m, k, v)
    _concurrency_eval(o, scn, snap, list(last.values()), set(), known_seq, cutoff)
    return o


def run(scn, condition: str, seed: int) -> Out:
    return run_crdt(scn, seed) if condition == "crdt_yjs" else run_legacy(scn, condition, seed)


def measure(scns, condition, seed):
    """Run a set of scenarios; return rows plus CPU seconds and peak traced memory (KiB)."""
    tracemalloc.start()
    t0 = time.process_time()
    rows = [run(s, condition, seed) for s in scns]
    cpu = time.process_time() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return rows, cpu, peak / 1024
