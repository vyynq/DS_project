"""
DEMO MODE - Slow timings for oral presentation
==============================================
Same algorithms, visible delays, and terminal narration.

Usage:
    python demo.py                    # Complete guided demo
    python demo.py --step election    # Raft election only
    python demo.py --step pbft        # PBFT protocol only
    python demo.py --step chaos       # Failure handling only
    python demo.py --step partition   # Network partition only
"""

import asyncio
import argparse
import sys
import os
import time
import types

sys.path.insert(0, os.path.dirname(__file__))

from raft.node import RaftNode, NodeState
from pbft.node import PBFTNode, PBFTNodeState
from chaos.engine import ChaosEngine
from gui.server import EventBus

# Terminal colors.
RESET  = "\033[0m";  BOLD   = "\033[1m";  BLUE   = "\033[94m"
PURPLE = "\033[95m"; YELLOW = "\033[93m"; GREEN  = "\033[92m"
RED    = "\033[91m"; GRAY   = "\033[90m"; CYAN   = "\033[96m"

# Helper functions.

def header(text):
    print(f"\n{BOLD}{'='*56}\n  {text}\n{'='*56}{RESET}")

def section(text):
    print(f"\n{CYAN}{'-'*48}\n  {BOLD}{text}{RESET}{CYAN}\n{'-'*48}{RESET}\n")

def info(text):         print(f"  {GRAY}[i]{RESET}  {text}")
def event(text):        print(f"  {GRAY}[{time.strftime('%H:%M:%S')}]{RESET}  {text}")
def success(text):      print(f"  {GREEN}[OK]{RESET}  {BOLD}{text}{RESET}")
def warning(text):      print(f"  {YELLOW}[!]{RESET}  {text}")
def chaos_action(text): print(f"  {RED}[CHAOS]{RESET}  {BOLD}{RED}{text}{RESET}")


async def pause(seconds, label=""):
    if label:
        print(f"\n  {GRAY}[...] {label}...{RESET}", end="", flush=True)
    for _ in range(int(seconds * 10)):
        await asyncio.sleep(0.1)
        print(f"{GRAY}.{RESET}", end="", flush=True)
    print()


def print_cluster_state(raft_nodes, pbft_nodes=None):
    print()
    if raft_nodes:
        print(f"  {BLUE}{BOLD}RAFT{RESET}")
        for n in raft_nodes.values():
            s    = n.get_status()
            icon = (
                f"{YELLOW}[LEADER]   " if s["state"] == "leader" else
                f"{GRAY}[DEAD]     " if s["state"] == "dead"   else
                f"{BLUE}[CANDIDATE]" if s["state"] == "candidate" else
                f"{BLUE}[FOLLOWER] "
            ) + RESET
            print(
                f"    Node {s['node_id']}  {icon}  "
                f"term={BOLD}{s['term']}{RESET}  "
                f"log={s['log_length']}  "
                f"commits={s['metrics']['commits']}"
            )
    if pbft_nodes:
        print(f"\n  {PURPLE}{BOLD}PBFT{RESET}")
        for n in pbft_nodes.values():
            s    = n.get_status()
            icon = (
                f"{RED}[BYZANTINE]" if s["is_byzantine"] else
                f"{GRAY}[DEAD]     " if s["state"] == "dead"  else
                f"{YELLOW}[PRIMARY]  " if s["is_primary"]     else
                f"{PURPLE}[REPLICA]  "
            ) + RESET
            print(
                f"    Node {s['node_id']}  {icon}  "
                f"view={BOLD}{s['view']}{RESET}  "
                f"commits={s['commits']}"
            )
    print()


# Cluster factories.

def create_slow_raft_cluster(event_bus):
    """Create a Raft cluster with slow timings for live presentation."""
    nodes = {i: RaftNode(node_id=i, election_timeout_range=(3000, 5000))
             for i in range(5)}
    for node in nodes.values():
        node.peers      = list(nodes.values())
        node._event_bus = event_bus

        async def slow_loop(self):
            while self._running and self.state != NodeState.DEAD:
                if self.state == NodeState.LEADER:
                    await self._send_heartbeats()
                    await asyncio.sleep(2.0)
                else:
                    if (time.time() - self.last_heartbeat) * 1000 > self.election_timeout:
                        await self._start_election()
                    await asyncio.sleep(0.1)

        node._main_loop = types.MethodType(slow_loop, node)
    return nodes


def create_slow_pbft_cluster(event_bus):
    """Create a PBFT cluster with slow timings."""
    nodes = {i: PBFTNode(node_id=i) for i in range(4)}
    for node in nodes.values():
        node.peers               = list(nodes.values())
        node._event_bus          = event_bus
        node.view_change_timeout = 8.0
    return nodes


# Demo steps.

async def demo_election(raft_nodes, chaos):
    section("STEP 1 - Startup and leader election")
    info("5 Raft nodes started. All are FOLLOWER.")
    info("Each node has a random timeout (3-5s). The first expired timeout starts a candidacy.")

    await pause(3.5, "Waiting for the first timeout")

    leader = chaos._find_raft_leader()
    if not leader:
        await pause(2, "Still waiting")
        leader = chaos._find_raft_leader()

    if leader:
        success(f"Node {leader.node_id} elected LEADER - term {leader.current_term}")
    else:
        warning("No leader yet")

    print_cluster_state(raft_nodes)


async def demo_requests(raft_nodes, chaos):
    section("STEP 2 - Log replication")
    leader = chaos._find_raft_leader()
    if not leader:
        warning("No leader available, skipping this step")
        return

    info(f"Sending 3 requests to the leader (Node {leader.node_id}).")
    for cmd, val in [("set", "x=10"), ("set", "y=20"), ("set", "z=30")]:
        await asyncio.sleep(1.5)
        event(f"Client -> Node {leader.node_id}: {cmd}({val})")
        result = await leader.client_request(cmd, val)
        if result.get("success"):
            success(
                f"Commit completed in {result['latency_ms']:.0f}ms  "
                f"(index={result.get('index', '?')})"
            )
        else:
            warning(f"Failure: {result.get('error')}")

    print_cluster_state(raft_nodes)


async def demo_leader_failure(raft_nodes, chaos):
    section("STEP 3 - Leader failure and re-election")
    leader = chaos._find_raft_leader()
    if not leader:
        warning("No leader available, skipping this step")
        return

    info(f"Stopping Node {leader.node_id}, the current leader.")
    info("Followers detect the missing heartbeats and start a new election.")

    chaos_action(f"CRASH - Node {leader.node_id} stopped")
    await chaos.crash_node("raft", leader.node_id)
    print_cluster_state(raft_nodes)

    t = time.time()
    await pause(5, "Waiting for re-election")
    new_leader = chaos._find_raft_leader()

    if new_leader:
        success(
            f"New leader: Node {new_leader.node_id}  "
            f"(term {new_leader.current_term})  "
            f"recovery in {(time.time()-t)*1000:.0f}ms"
        )
        result = await new_leader.client_request("set", "recovery=ok")
        if result.get("success"):
            success(f"Post-failure request completed in {result['latency_ms']:.0f}ms")
    else:
        warning("No new leader found")

    print_cluster_state(raft_nodes)


async def demo_partition(raft_nodes, chaos):
    section("STEP 4 - Network partition")
    leader = chaos._find_raft_leader()
    if not leader:
        for node in raft_nodes.values():
            if node.state == NodeState.DEAD:
                await chaos.revive_node("raft", node.node_id)
        await pause(4, "Waiting for leader")

    info("Splitting the cluster into two groups:")
    info("  Group A: Nodes 0, 1    - minority (2 nodes)")
    info("  Group B: Nodes 2,3,4   - majority (3 nodes)")

    await asyncio.sleep(1.5)
    chaos_action("PARTITION - {0,1} isolated from {2,3,4}")
    await chaos.network_partition("raft", [0, 1], [2, 3, 4])

    await pause(5, "Observing the partition")
    info("The majority group can still elect a leader. The minority group is blocked, preserving safety.")
    print_cluster_state(raft_nodes)

    chaos_action("HEAL - Partition restored")
    await chaos.heal_partition("raft")
    await pause(3, "Cluster resynchronization")
    success("Cluster resynchronized automatically.")
    print_cluster_state(raft_nodes)


async def demo_pbft(pbft_nodes, chaos):
    section("STEP 5 - PBFT Byzantine fault tolerance")
    primary = chaos._find_pbft_primary()
    info(f"PBFT cluster with 4 nodes, primary = Node {primary.node_id if primary else '?'}")
    info("Tolerance: 1 Byzantine fault (f=1, because n=3f+1=4)")

    if primary:
        result = await primary.client_request("transfer", 500)
        if result.get("success"):
            event(f"transfer(500) committed in {result['latency_ms']:.0f}ms (3 phases)")

    await asyncio.sleep(2)

    replica = next(
        (n for n in pbft_nodes.values()
         if not n.is_primary() and n.state != PBFTNodeState.DEAD),
        None
    )
    if not replica:
        warning("No replica available")
        return

    info(f"Marking Node {replica.node_id} as BYZANTINE so it sends invalid digests.")
    chaos_action(f"BYZANTINE - Node {replica.node_id} is now malicious")
    await chaos.set_byzantine(replica.node_id)
    print_cluster_state(None, pbft_nodes)

    primary = chaos._find_pbft_primary()
    if primary:
        result = await primary.client_request("transfer", 1000)
        if result.get("success"):
            success(f"Commit still completed in {result['latency_ms']:.0f}ms")
            info("PBFT ignored the invalid digest. It required 2f+1=3 valid COMMIT messages.")
        else:
            info("Timeout, expected when the Byzantine node blocks too many messages.")

    print_cluster_state(None, pbft_nodes)


# Main entry point.

async def run_full_demo(step=None):
    header("CONSENSUS VISUALIZER - Demo")
    print(f"  {GRAY}Raft vs PBFT - Distributed Systems 2026 - Slow timings{RESET}\n")

    event_bus  = EventBus()
    raft_nodes = create_slow_raft_cluster(event_bus)
    pbft_nodes = create_slow_pbft_cluster(event_bus)
    chaos      = ChaosEngine(
        list(raft_nodes.values()),
        list(pbft_nodes.values()),
        event_bus
    )

    for node in list(raft_nodes.values()) + list(pbft_nodes.values()):
        await node.start()
    info("Clusters started. All nodes are FOLLOWER.\n")

    steps = {
        "election":  lambda: demo_election(raft_nodes, chaos),
        "requests":  lambda: demo_requests(raft_nodes, chaos),
        "chaos":     lambda: demo_leader_failure(raft_nodes, chaos),
        "partition": lambda: demo_partition(raft_nodes, chaos),
        "pbft":      lambda: demo_pbft(pbft_nodes, chaos),
    }

    try:
        if step:
            await steps[step]()
        else:
            for name, fn in steps.items():
                await fn()
                if name != "pbft":
                    input(f"\n  {GRAY}[ Press Enter to continue ]{RESET}")
    except KeyboardInterrupt:
        pass

    header("End of demo")
    print(f"  {GREEN}Benchmark:{RESET}  python main.py --benchmark")
    print(f"  {GREEN}Quick demo:{RESET} python demo.py --step election\n")


async def main():
    parser = argparse.ArgumentParser(
        description="Demo mode - slow timings for oral presentation"
    )
    parser.add_argument(
        "--step",
        choices=["election", "requests", "chaos", "partition", "pbft"]
    )
    args = parser.parse_args()
    await run_full_demo(step=args.step)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nGoodbye.")
