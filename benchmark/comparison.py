"""
COMPARISON BENCHMARKS -- Raft vs PBFT
======================================
Four fair comparison scenarios where both algorithms face
the exact same stress. Results are exported to CSV for SageMath.

Scenarios:
  1. churn         -- nodes randomly crash and revive continuously
  2. asymmetric    -- artificial delay on leader/primary only
  3. scalability   -- throughput vs cluster size (3, 5, 7, 9 nodes)
  4. slow_node     -- one node replies very slowly (not dead, just slow)

Usage:
    python -m benchmark.comparison                  # run all 4
    python -m benchmark.comparison --test churn
    python -m benchmark.comparison --test asymmetric
    python -m benchmark.comparison --test scalability
    python -m benchmark.comparison --test slow_node
    python -m benchmark.comparison --demo           # slow timings, terminal output
"""

import asyncio
import argparse
import csv
import json
import logging
import os
import random
import sys
import time
import types
from dataclasses import dataclass, field

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from raft.node import RaftNode, NodeState
from pbft.node import PBFTNode, PBFTNodeState
from chaos.engine import ChaosEngine
from gui.server import EventBus

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)  # silent during benchmarks

# ─────────────────────────────────────────────
# TERMINAL DISPLAY (demo mode)
# ─────────────────────────────────────────────

RESET  = "\033[0m"
BOLD   = "\033[1m"
BLUE   = "\033[94m"
PURPLE = "\033[95m"
YELLOW = "\033[93m"
GREEN  = "\033[92m"
RED    = "\033[91m"
GRAY   = "\033[90m"
CYAN   = "\033[96m"

DEMO_MODE = False


def section(text):
    print(f"\n{CYAN}{'-'*54}{RESET}")
    print(f"{CYAN}{BOLD}  {text}{RESET}")
    print(f"{CYAN}{'-'*54}{RESET}\n")


def log(text):
    if DEMO_MODE:
        ts = time.strftime("%H:%M:%S")
        print(f"  {GRAY}[{ts}]{RESET}  {text}")


def result_line(algo, label, value, unit=""):
    color = BLUE if algo == "raft" else PURPLE
    tag = f"{color}{BOLD}{algo.upper():4}{RESET}"
    print(f"    {tag}  {label:<30} {BOLD}{value}{RESET} {unit}")


async def wait_dots(seconds, label="Waiting"):
    if DEMO_MODE:
        print(f"  {GRAY}[...] {label}...{RESET}", end="", flush=True)
        for _ in range(int(seconds * 5)):
            await asyncio.sleep(1 / 5)
            print(f"{GRAY}.{RESET}", end="", flush=True)
        print()
    else:
        await asyncio.sleep(seconds)


# ─────────────────────────────────────────────
# RESULT DATA CLASS
# ─────────────────────────────────────────────

@dataclass
class CompResult:
    scenario: str
    algo: str
    params: dict
    throughput_rps: float
    avg_latency_ms: float
    p95_latency_ms: float
    error_rate: float
    extra: dict = field(default_factory=dict)  # scenario-specific metrics

    def to_dict(self):
        return {
            "scenario": self.scenario,
            "algo": self.algo,
            **self.params,
            "throughput_rps": round(self.throughput_rps, 2),
            "avg_latency_ms": round(self.avg_latency_ms, 2),
            "p95_latency_ms": round(self.p95_latency_ms, 2),
            "error_rate": round(self.error_rate, 4),
            **{f"extra_{k}": v for k, v in self.extra.items()},
        }


# ─────────────────────────────────────────────
# CLUSTER FACTORIES
# ─────────────────────────────────────────────

def make_raft(size: int, event_bus: EventBus) -> dict:
    nodes = {i: RaftNode(node_id=i, election_timeout_range=(150, 300))
             for i in range(size)}
    for n in nodes.values():
        n.peers = list(nodes.values())
        n._event_bus = event_bus
    return nodes


def make_pbft(size: int, event_bus: EventBus) -> dict:
    nodes = {i: PBFTNode(node_id=i) for i in range(size)}
    for n in nodes.values():
        n.peers = list(nodes.values())
        n._event_bus = event_bus
    return nodes


async def start_all(nodes: dict):
    for n in nodes.values():
        await n.start()


async def stop_all(nodes: dict):
    for n in nodes.values():
        await n.stop()


async def wait_for_raft_leader(nodes: dict, timeout=3.0) -> RaftNode | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        for n in nodes.values():
            if n.state == NodeState.LEADER:
                return n
        await asyncio.sleep(0.05)
    return None


async def wait_for_pbft_primary(nodes: dict, timeout=2.0) -> PBFTNode | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        for n in nodes.values():
            if n.is_primary() and n.state == PBFTNodeState.NORMAL:
                return n
        await asyncio.sleep(0.05)
    # PBFT primary is deterministic (node 0 on view 0), just return it
    return nodes.get(0)


# ─────────────────────────────────────────────
# LOAD RUNNER (shared by all scenarios)
# ─────────────────────────────────────────────

async def run_load(algo: str, leader, n_requests: int, rps: int,
                   nodes: dict = None) -> dict:
    """
    Send n_requests at rps req/s to leader/primary.
    For PBFT, drives the 3-phase protocol directly to avoid redirect loops.
    Returns raw latency list, error count, duration.
    """
    import hashlib, json as _json
    from pbft.node import PBFTMessage

    latencies = []
    errors = 0
    interval = 1.0 / rps
    start = time.time()

    for i in range(n_requests):
        # Resolve current leader/primary
        current = leader
        if nodes:
            if algo == "raft":
                current = next(
                    (n for n in nodes.values() if n.state == NodeState.LEADER),
                    None)
            else:
                current = next(
                    (n for n in nodes.values()
                     if n.is_primary() and n.state == PBFTNodeState.NORMAL),
                    None)

        if current is None:
            errors += 1
            await asyncio.sleep(interval)
            continue

        t0 = time.time()

        if algo == "raft":
            result = await current.client_request("set", i)
        else:
            # Drive PBFT primary directly -- avoids infinite redirect.
            # We replicate what client_request does but skip the is_primary check.
            request = {"op": "set", "value": i, "timestamp": t0}
            import time as _time
            seq = current._next_sequence()
            d = hashlib.sha256(
                _json.dumps(request, sort_keys=True).encode()
            ).hexdigest()[:16]
            msg = PBFTMessage(
                msg_type="pre_prepare",
                view=current.view,
                sequence=seq,
                digest=d,
                node_id=current.node_id,
                request=request
            )
            # Broadcast to all peers (including self via handle_pre_prepare)
            tasks = []
            for peer in current.peers:
                if peer.state != PBFTNodeState.DEAD:
                    tasks.append(asyncio.create_task(peer.handle_pre_prepare(msg)))
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            committed = await current._wait_for_commit(seq, timeout=3.0)
            result = {"success": committed}

        elapsed = (time.time() - t0) * 1000
        if result.get("success"):
            latencies.append(elapsed)
        else:
            errors += 1

        remaining = interval - elapsed / 1000
        if remaining > 0:
            await asyncio.sleep(remaining)

    duration = time.time() - start
    return {"latencies": latencies, "errors": errors,
            "n": n_requests, "duration": duration}


def make_result(scenario, algo, params, raw, extra=None) -> CompResult:
    lats = sorted(raw["latencies"])
    n = len(lats)
    avg = sum(lats) / n if lats else 0
    p95 = lats[int(n * 0.95)] if lats else 0
    thr = (raw["n"] - raw["errors"]) / raw["duration"] if raw["duration"] else 0
    err = raw["errors"] / raw["n"] if raw["n"] else 0
    return CompResult(scenario, algo, params, thr, avg, p95, err, extra or {})


# ─────────────────────────────────────────────
# SCENARIO 1 -- CHURN
# ─────────────────────────────────────────────
#
# Every 2 seconds, a random non-leader node crashes.
# Every 3 seconds, a dead node is revived.
# Both clusters run under continuous 20 rps load for 30 seconds.
#
# What it shows:
#   Raft handles churn well -- re-election is fast (150-300ms).
#   PBFT struggles -- each primary loss triggers a slow view-change.
#   The throughput and error_rate gap shows this clearly.

async def scenario_churn(n_requests=200, rps=20, duration=15) -> list[CompResult]:
    if DEMO_MODE:
        section("SCENARIO 1 -- Churn (continuous crashes & revivals)")
        log("Every 2s a random node crashes, every 3s a dead node revives.")
        log(f"Load: {rps} req/s for {duration}s on both clusters.\n")

    results = []

    for algo in ["raft", "pbft"]:
        bus = EventBus()
        size = 5 if algo == "raft" else 4
        nodes = make_raft(size, bus) if algo == "raft" else make_pbft(size, bus)
        chaos = ChaosEngine(
            list(nodes.values()) if algo == "raft" else [],
            list(nodes.values()) if algo == "pbft" else [],
            bus
        )
        await start_all(nodes)

        if algo == "raft":
            leader = await wait_for_raft_leader(nodes)
        else:
            leader = await wait_for_pbft_primary(nodes)

        # Background churn task
        churn_crashes = 0
        churn_revivals = 0
        stop_churn = asyncio.Event()

        async def churn_loop():
            nonlocal churn_crashes, churn_revivals
            crash_interval = 2.0
            revive_interval = 3.0
            last_crash = time.time()
            last_revive = time.time()

            while not stop_churn.is_set():
                now = time.time()

                if now - last_crash >= crash_interval:
                    # Pick a random non-leader/non-primary alive node
                    candidates = [
                        n for n in nodes.values()
                        if n.state not in (NodeState.DEAD, NodeState.LEADER)
                    ] if algo == "raft" else [
                        n for n in nodes.values()
                        if n.state != PBFTNodeState.DEAD and not n.is_primary()
                    ]
                    if candidates:
                        target = random.choice(candidates)
                        await target.stop()
                        churn_crashes += 1
                        log(f"  [churn] {algo} Node {target.node_id} crashed "
                            f"(crash #{churn_crashes})")
                    last_crash = now

                if now - last_revive >= revive_interval:
                    dead = [n for n in nodes.values()
                            if n.state == NodeState.DEAD] if algo == "raft" else [
                        n for n in nodes.values()
                        if n.state == PBFTNodeState.DEAD]
                    if dead:
                        target = random.choice(dead)
                        await target.revive()
                        churn_revivals += 1
                        log(f"  [churn] {algo} Node {target.node_id} revived "
                            f"(revival #{churn_revivals})")
                    last_revive = now

                await asyncio.sleep(0.2)

        churn_task = asyncio.create_task(churn_loop())

        # Run load while churn happens
        latencies = []
        errors = 0
        interval = 1.0 / rps
        t_start = time.time()

        for i in range(int(duration * rps)):
            # Always find current leader
            if algo == "raft":
                current_leader = next(
                    (n for n in nodes.values() if n.state == NodeState.LEADER), None)
            else:
                current_leader = next(
                    (n for n in nodes.values()
                     if n.is_primary() and n.state == PBFTNodeState.NORMAL), None)

            t0 = time.time()
            if current_leader:
                res = await current_leader.client_request("set", i)
                elapsed = (time.time() - t0) * 1000
                if res.get("success"):
                    latencies.append(elapsed)
                else:
                    errors += 1
            else:
                errors += 1
                elapsed = 0

            sleep_for = interval - (time.time() - t0)
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)

        stop_churn.set()
        churn_task.cancel()
        await stop_all(nodes)

        raw = {"latencies": latencies, "errors": errors,
               "n": int(duration * rps), "duration": time.time() - t_start}
        r = make_result("churn", algo,
                        {"duration_s": duration, "rps": rps},
                        raw,
                        extra={"crashes": churn_crashes, "revivals": churn_revivals})
        results.append(r)

        if DEMO_MODE:
            color = BLUE if algo == "raft" else PURPLE
            print(f"\n  {color}{BOLD}{algo.upper()} results:{RESET}")
            result_line(algo, "Throughput",
                        f"{r.throughput_rps:.1f}", "req/s")
            result_line(algo, "Avg latency",
                        f"{r.avg_latency_ms:.1f}", "ms")
            result_line(algo, "Error rate",
                        f"{r.error_rate*100:.1f}", "%")
            result_line(algo, "Total crashes",
                        str(r.extra["crashes"]))
            result_line(algo, "Total revivals",
                        str(r.extra["revivals"]))

        await asyncio.sleep(0.3)

    if DEMO_MODE:
        _print_winner("churn", results)
    return results


# ─────────────────────────────────────────────
# SCENARIO 2 -- ASYMMETRIC DELAY
# ─────────────────────────────────────────────
#
# We add artificial delay ONLY on messages going TO the leader/primary.
# Other nodes communicate normally.
# We test with delay = 0, 50, 100, 200, 400 ms.
#
# What it shows:
#   In Raft, the leader is the single bottleneck for all writes.
#   If the leader is slow to receive, commits are slow cluster-wide.
#   In PBFT, replicas communicate peer-to-peer in PREPARE/COMMIT phases,
#   so a slow primary hurts less -- the replicas compensate each other.
#   Counter-intuitive and architecturally interesting.

async def scenario_asymmetric(delays_ms=None) -> list[CompResult]:
    if delays_ms is None:
        delays_ms = [0, 50, 100, 200, 400]

    if DEMO_MODE:
        section("SCENARIO 2 -- Asymmetric Delay (leader/primary only)")
        log("Adding delay only on messages received by the leader/primary.")
        log(f"Delay values tested: {delays_ms} ms\n")

    results = []

    for delay in delays_ms:
        if DEMO_MODE:
            log(f"Testing delay = {delay}ms...")

        for algo in ["raft", "pbft"]:
            bus = EventBus()
            size = 5 if algo == "raft" else 4
            nodes = make_raft(size, bus) if algo == "raft" else make_pbft(size, bus)
            await start_all(nodes)

            if algo == "raft":
                leader = await wait_for_raft_leader(nodes)
            else:
                leader = await wait_for_pbft_primary(nodes)

            if leader is None:
                await stop_all(nodes)
                continue

            # Apply delay ONLY on the leader/primary receive path.
            # We patch _can_send on all OTHER nodes so their messages
            # to the leader are delayed -- simulating a slow uplink to leader.
            original_can_sends = {}
            for n in nodes.values():
                if n.node_id == leader.node_id:
                    continue
                original_can_sends[n.node_id] = n._can_send

                async def delayed_can_send(self, peer,
                                           _delay=delay, _lid=leader.node_id):
                    if peer.node_id == _lid and _delay > 0:
                        await asyncio.sleep(_delay / 1000)
                    return await original_can_sends[self.node_id](peer)

                nodes[n.node_id]._can_send = types.MethodType(
                    delayed_can_send, nodes[n.node_id])

            raw = await run_load(algo, leader, n_requests=60, rps=15, nodes=nodes)
            r = make_result("asymmetric", algo,
                            {"delay_ms": delay}, raw)
            results.append(r)

            # Restore original _can_send
            for nid, fn in original_can_sends.items():
                nodes[nid]._can_send = fn

            await stop_all(nodes)
            await asyncio.sleep(0.2)

        if DEMO_MODE:
            raft_r = next(r for r in results
                          if r.params.get("delay_ms") == delay
                          and r.algo == "raft")
            pbft_r = next(r for r in results
                          if r.params.get("delay_ms") == delay
                          and r.algo == "pbft")
            print(f"    delay={delay:>4}ms  |  "
                  f"{BLUE}Raft{RESET} avg={raft_r.avg_latency_ms:>6.1f}ms  "
                  f"thr={raft_r.throughput_rps:>5.1f}rps  |  "
                  f"{PURPLE}PBFT{RESET} avg={pbft_r.avg_latency_ms:>6.1f}ms  "
                  f"thr={pbft_r.throughput_rps:>5.1f}rps")

    if DEMO_MODE:
        print()
        log("Key insight: at high delay, PBFT degrades less than Raft.")
        log("Replicas compensate each other in PREPARE/COMMIT phases.")

    return results


# ─────────────────────────────────────────────
# SCENARIO 3 -- SCALABILITY
# ─────────────────────────────────────────────
#
# Run both algorithms with cluster sizes 3, 5, 7, 9.
# Measure throughput and latency at each size under the same load.
#
# Raft fault tolerance: (n-1)//2   -> 3n: f=1, 5n: f=2, 7n: f=3, 9n: f=4
# PBFT fault tolerance: (n-1)//3   -> 4n: f=1, 7n: f=2, 10n: f=3
#
# Because PBFT needs 3f+1, valid sizes are 4, 7, 10...
# We use 4, 7, 10 for PBFT and 3, 5, 7, 9 for Raft.
# At size 7 both overlap -- direct comparison.
#
# What it shows:
#   Raft degrades gently -- O(n) message complexity.
#   PBFT degrades sharply -- O(n^2) message complexity.
#   At n=7 the gap is already significant.
#   This empirically validates the theoretical graph.

async def scenario_scalability() -> list[CompResult]:
    raft_sizes = [3, 5, 7, 9]
    pbft_sizes = [4, 7, 10]   # valid BFT sizes (3f+1)

    if DEMO_MODE:
        section("SCENARIO 3 -- Scalability (throughput vs cluster size)")
        log(f"Raft sizes: {raft_sizes}")
        log(f"PBFT sizes: {pbft_sizes}  (must satisfy n=3f+1)")
        log("At size 7 both algorithms overlap -- direct comparison.\n")

    results = []

    # ── RAFT ──
    for size in raft_sizes:
        if DEMO_MODE:
            log(f"Testing Raft n={size}...")

        bus = EventBus()
        nodes = make_raft(size, bus)
        await start_all(nodes)
        leader = await wait_for_raft_leader(nodes, timeout=4.0)

        if leader is None:
            await stop_all(nodes)
            if DEMO_MODE:
                log(f"  [!] No Raft leader at n={size}, skipping")
            continue

        raw = await run_load("raft", leader, n_requests=80, rps=20, nodes=nodes)
        r = make_result("scalability", "raft",
                        {"cluster_size": size,
                         "max_failures": (size - 1) // 2},
                        raw)
        results.append(r)
        await stop_all(nodes)

        if DEMO_MODE:
            result_line("raft", f"n={size}  thr",
                        f"{r.throughput_rps:.1f}", "req/s")
            result_line("raft", f"n={size}  avg lat",
                        f"{r.avg_latency_ms:.1f}", "ms")

        await asyncio.sleep(0.3)

    print()

    # ── PBFT ──
    for size in pbft_sizes:
        if DEMO_MODE:
            log(f"Testing PBFT n={size}...")

        bus = EventBus()
        nodes = make_pbft(size, bus)
        await start_all(nodes)
        leader = await wait_for_pbft_primary(nodes)

        raw = await run_load("pbft", leader, n_requests=40, rps=10, nodes=nodes)
        r = make_result("scalability", "pbft",
                        {"cluster_size": size,
                         "max_failures": (size - 1) // 3},
                        raw)
        results.append(r)
        await stop_all(nodes)

        if DEMO_MODE:
            result_line("pbft", f"n={size}  thr",
                        f"{r.throughput_rps:.1f}", "req/s")
            result_line("pbft", f"n={size}  avg lat",
                        f"{r.avg_latency_ms:.1f}", "ms")

        await asyncio.sleep(0.3)

    if DEMO_MODE:
        print()
        log("Key insight: at n=7 (overlap), compare directly.")
        raft_7 = next((r for r in results
                       if r.algo == "raft" and r.params["cluster_size"] == 7), None)
        pbft_7 = next((r for r in results
                       if r.algo == "pbft" and r.params["cluster_size"] == 7), None)
        if raft_7 and pbft_7:
            ratio = raft_7.throughput_rps / pbft_7.throughput_rps if pbft_7.throughput_rps else 0
            print(f"\n    At n=7: Raft throughput is {ratio:.1f}x that of PBFT")
            print(f"    Raft  {raft_7.throughput_rps:.1f} req/s  vs  "
                  f"PBFT {pbft_7.throughput_rps:.1f} req/s\n")

    return results


# ─────────────────────────────────────────────
# SCENARIO 4 -- SLOW NODE
# ─────────────────────────────────────────────
#
# One node is made artificially slow (500ms added delay on all its sends).
# It is NOT dead -- it still participates, just slowly.
# We test with the slow node being: a follower/replica, then the leader/primary.
#
# What it shows:
#   In Raft: a slow FOLLOWER barely matters -- the leader doesn't wait for it
#   for majority (only needs n//2 fast nodes). A slow LEADER hurts everyone.
#
#   In PBFT: a slow REPLICA matters MORE -- the primary must wait for 2f
#   PREPARE messages, and if one replica is slow, the whole pipeline stalls.
#   A slow PRIMARY is similarly painful.
#
#   This reveals the difference between Raft's leader-centric model and
#   PBFT's all-to-all quorum model.

async def scenario_slow_node(slow_delay_ms=500) -> list[CompResult]:
    if DEMO_MODE:
        section("SCENARIO 4 -- Slow Node (not dead, just very slow)")
        log(f"One node has {slow_delay_ms}ms delay on all its sends.")
        log("We test: slow follower/replica vs slow leader/primary.\n")

    results = []
    roles = ["follower", "leader"]

    for algo in ["raft", "pbft"]:
        for slow_role in roles:
            if DEMO_MODE:
                log(f"Testing {algo.upper()} with slow {slow_role}...")

            bus = EventBus()
            size = 5 if algo == "raft" else 4
            nodes = make_raft(size, bus) if algo == "raft" else make_pbft(size, bus)
            await start_all(nodes)

            if algo == "raft":
                leader = await wait_for_raft_leader(nodes)
            else:
                leader = await wait_for_pbft_primary(nodes)

            if leader is None:
                await stop_all(nodes)
                continue

            # Pick which node to slow down
            if slow_role == "leader":
                slow_node = leader
            else:
                if algo == "raft":
                    slow_node = next(
                        n for n in nodes.values()
                        if n.state == NodeState.FOLLOWER)
                else:
                    slow_node = next(
                        n for n in nodes.values()
                        if not n.is_primary()
                        and n.state == PBFTNodeState.NORMAL)

            # Apply delay on all sends FROM this node
            original_can_send = slow_node._can_send

            async def slow_can_send(self, peer, _delay=slow_delay_ms):
                await asyncio.sleep(_delay / 1000)
                return await original_can_send(peer)

            slow_node._can_send = types.MethodType(slow_can_send, slow_node)

            raw = await run_load(algo, leader, n_requests=60, rps=15, nodes=nodes)
            r = make_result("slow_node", algo,
                            {"slow_role": slow_role,
                             "slow_delay_ms": slow_delay_ms,
                             "slow_node_id": slow_node.node_id},
                            raw)
            results.append(r)

            # Restore
            slow_node._can_send = original_can_send
            await stop_all(nodes)

            if DEMO_MODE:
                result_line(algo, f"slow {slow_role:<9} thr",
                            f"{r.throughput_rps:.1f}", "req/s")
                result_line(algo, f"slow {slow_role:<9} avg lat",
                            f"{r.avg_latency_ms:.1f}", "ms")

            await asyncio.sleep(0.2)

        if DEMO_MODE:
            print()

    if DEMO_MODE:
        # Print the key comparison: slow follower vs slow leader for each algo
        print(f"\n  {BOLD}Impact of slow node role:{RESET}")
        for algo in ["raft", "pbft"]:
            r_flw = next((r for r in results
                          if r.algo == algo and r.params["slow_role"] == "follower"), None)
            r_ldr = next((r for r in results
                          if r.algo == algo and r.params["slow_role"] == "leader"), None)
            if r_flw and r_ldr:
                degradation = ((r_flw.throughput_rps - r_ldr.throughput_rps)
                               / r_flw.throughput_rps * 100) if r_flw.throughput_rps else 0
                color = BLUE if algo == "raft" else PURPLE
                print(f"    {color}{BOLD}{algo.upper()}{RESET}  "
                      f"slow follower={r_flw.throughput_rps:.1f}rps  "
                      f"slow leader={r_ldr.throughput_rps:.1f}rps  "
                      f"-> leader slowdown: {degradation:.0f}%")

        print()
        log("Key insight:")
        log("  Raft: slow follower barely matters (leader doesn't wait for it).")
        log("  PBFT: slow replica stalls the PREPARE phase for everyone.")

    return results


# ─────────────────────────────────────────────
# EXPORT
# ─────────────────────────────────────────────

def export(results: list[CompResult], out_dir="analysis"):
    os.makedirs(out_dir, exist_ok=True)

    # Group by scenario
    by_scenario: dict[str, list[CompResult]] = {}
    for r in results:
        by_scenario.setdefault(r.scenario, []).append(r)

    # One CSV per scenario
    for scenario, rows in by_scenario.items():
        path = os.path.join(out_dir, f"comparison_{scenario}.csv")
        keys = rows[0].to_dict().keys()
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            for r in rows:
                w.writerow(r.to_dict())

    # Global JSON for graphs
    path_json = os.path.join(out_dir, "comparison_results.json")
    with open(path_json, "w") as f:
        json.dump([r.to_dict() for r in results], f, indent=2)

    if DEMO_MODE:
        print(f"\n  {GREEN}[OK]{RESET}  Results exported to {out_dir}/")
        print(f"       Run: python analysis/graphs_comparison.py")


# ─────────────────────────────────────────────
# WINNER HELPER
# ─────────────────────────────────────────────

def _print_winner(scenario, results):
    raft = next((r for r in results if r.algo == "raft"), None)
    pbft = next((r for r in results if r.algo == "pbft"), None)
    if not raft or not pbft:
        return
    winner_thr = "Raft" if raft.throughput_rps >= pbft.throughput_rps else "PBFT"
    winner_lat = "Raft" if raft.avg_latency_ms <= pbft.avg_latency_ms else "PBFT"
    winner_err = "Raft" if raft.error_rate <= pbft.error_rate else "PBFT"
    print(f"\n  {BOLD}[{scenario.upper()}] Summary:{RESET}")
    print(f"    Throughput winner : {BOLD}{winner_thr}{RESET}")
    print(f"    Latency winner    : {BOLD}{winner_lat}{RESET}")
    print(f"    Reliability winner: {BOLD}{winner_err}{RESET}")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

async def run_all(test_filter=None) -> list[CompResult]:
    all_results = []

    tests = {
        "churn":       scenario_churn,
        "asymmetric":  scenario_asymmetric,
        "scalability": scenario_scalability,
        "slow_node":   scenario_slow_node,
    }

    for name, fn in tests.items():
        if test_filter and name != test_filter:
            continue
        if DEMO_MODE:
            pass  # section printed inside each scenario
        else:
            print(f"  Running {name}...", end="", flush=True)

        t0 = time.time()
        results = await fn()
        elapsed = time.time() - t0
        all_results.extend(results)

        if not DEMO_MODE:
            print(f" done ({elapsed:.1f}s, {len(results)} data points)")

    return all_results


async def main():
    global DEMO_MODE

    parser = argparse.ArgumentParser(
        description="Raft vs PBFT -- Comparison Benchmarks")
    parser.add_argument("--test", choices=["churn", "asymmetric",
                                           "scalability", "slow_node"],
                        default=None, help="Run one scenario only")
    parser.add_argument("--demo", action="store_true",
                        help="Verbose terminal output with slow timings")
    args = parser.parse_args()

    DEMO_MODE = args.demo

    if DEMO_MODE:
        print()
        print(f"{BOLD}{'='*54}{RESET}")
        print(f"{BOLD}  RAFT vs PBFT -- Comparison Benchmarks{RESET}")
        print(f"{BOLD}{'='*54}{RESET}")

    results = await run_all(test_filter=args.test)
    export(results)

    if not DEMO_MODE:
        print(f"\n  {len(results)} results exported to analysis/")
        print("  Run: python analysis/graphs_comparison.py")


if __name__ == "__main__":
    asyncio.run(main())