"""
DEMO MODE — Timings lents pour presentation orale
==================================================
Memes algorithmes, delais visibles, commentaires terminal.

Usage:
    python demo.py                    # Demo guidee complete
    python demo.py --step election    # Election Raft uniquement
    python demo.py --step pbft        # Protocole PBFT uniquement
    python demo.py --step chaos       # Pannes uniquement
    python demo.py --step partition   # Partition reseau uniquement
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

# ── Terminal colors ──────────────────────────────────────
RESET  = "\033[0m";  BOLD   = "\033[1m";  BLUE   = "\033[94m"
PURPLE = "\033[95m"; YELLOW = "\033[93m"; GREEN  = "\033[92m"
RED    = "\033[91m"; GRAY   = "\033[90m"; CYAN   = "\033[96m"

# ── Helpers ──────────────────────────────────────────────

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


# ── Cluster factories ─────────────────────────────────────

def create_slow_raft_cluster(event_bus):
    """Cluster Raft avec timings lents (visible a l'oral)."""
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
    """Cluster PBFT avec timings lents."""
    nodes = {i: PBFTNode(node_id=i) for i in range(4)}
    for node in nodes.values():
        node.peers               = list(nodes.values())
        node._event_bus          = event_bus
        node.view_change_timeout = 8.0
    return nodes


# ── Demo steps ────────────────────────────────────────────

async def demo_election(raft_nodes, chaos):
    section("ETAPE 1 — Demarrage et election du leader")
    info("5 noeuds Raft demarres. Tous sont FOLLOWER.")
    info("Chacun a un timeout aleatoire (3-5s). Le premier expire devient candidat.")

    await pause(3.5, "Attente du premier timeout")

    leader = chaos._find_raft_leader()
    if not leader:
        await pause(2, "Toujours en attente")
        leader = chaos._find_raft_leader()

    if leader:
        success(f"Node {leader.node_id} elu LEADER — term {leader.current_term}")
    else:
        warning("Pas de leader encore")

    print_cluster_state(raft_nodes)


async def demo_requests(raft_nodes, chaos):
    section("ETAPE 2 — Replication du log")
    leader = chaos._find_raft_leader()
    if not leader:
        warning("Pas de leader, etape ignoree")
        return

    info(f"Envoi de 3 requetes au leader (Node {leader.node_id}).")
    for cmd, val in [("set", "x=10"), ("set", "y=20"), ("set", "z=30")]:
        await asyncio.sleep(1.5)
        event(f"Client -> Node {leader.node_id}: {cmd}({val})")
        result = await leader.client_request(cmd, val)
        if result.get("success"):
            success(
                f"Commit en {result['latency_ms']:.0f}ms  "
                f"(index={result.get('index', '?')})"
            )
        else:
            warning(f"Echec : {result.get('error')}")

    print_cluster_state(raft_nodes)


async def demo_leader_failure(raft_nodes, chaos):
    section("ETAPE 3 — Panne du leader et re-election")
    leader = chaos._find_raft_leader()
    if not leader:
        warning("Pas de leader, etape ignoree")
        return

    info(f"Arret du Node {leader.node_id} (leader actuel).")
    info("Les followers detectent l'absence de heartbeats et declenchent une election.")

    chaos_action(f"CRASH — Node {leader.node_id} tue !")
    await chaos.crash_node("raft", leader.node_id)
    print_cluster_state(raft_nodes)

    t = time.time()
    await pause(5, "Attente de la re-election")
    new_leader = chaos._find_raft_leader()

    if new_leader:
        success(
            f"Nouveau leader : Node {new_leader.node_id}  "
            f"(term {new_leader.current_term})  "
            f"recuperation en {(time.time()-t)*1000:.0f}ms"
        )
        result = await new_leader.client_request("set", "recovery=ok")
        if result.get("success"):
            success(f"Requete post-panne reussie en {result['latency_ms']:.0f}ms")
    else:
        warning("Aucun nouveau leader trouve")

    print_cluster_state(raft_nodes)


async def demo_partition(raft_nodes, chaos):
    section("ETAPE 4 — Partition reseau")
    leader = chaos._find_raft_leader()
    if not leader:
        for node in raft_nodes.values():
            if node.state == NodeState.DEAD:
                await chaos.revive_node("raft", node.node_id)
        await pause(4, "Attente du leader")

    info("Separation du cluster en deux groupes :")
    info("  Groupe A : Nodes 0, 1   — minorite (2 noeuds)")
    info("  Groupe B : Nodes 2,3,4  — majorite (3 noeuds)")

    await asyncio.sleep(1.5)
    chaos_action("PARTITION — {0,1} isoles de {2,3,4}")
    await chaos.network_partition("raft", [0, 1], [2, 3, 4])

    await pause(5, "Observation de la partition")
    info("La majorite (B) peut encore elire un leader. La minorite (A) est bloquee — SAFETY respectee.")
    print_cluster_state(raft_nodes)

    chaos_action("HEAL — Partition reparee !")
    await chaos.heal_partition("raft")
    await pause(3, "Re-synchronisation du cluster")
    success("Cluster re-synchronise automatiquement.")
    print_cluster_state(raft_nodes)


async def demo_pbft(pbft_nodes, chaos):
    section("ETAPE 5 — PBFT : tolerance aux fautes byzantines")
    primary = chaos._find_pbft_primary()
    info(f"Cluster PBFT de 4 noeuds, primary = Node {primary.node_id if primary else '?'}")
    info("Tolerance : 1 faute byzantine (f=1, car n=3f+1=4)")

    if primary:
        result = await primary.client_request("transfer", 500)
        if result.get("success"):
            event(f"transfer(500) commite en {result['latency_ms']:.0f}ms (3 phases)")

    await asyncio.sleep(2)

    replica = next(
        (n for n in pbft_nodes.values()
         if not n.is_primary() and n.state != PBFTNodeState.DEAD),
        None
    )
    if not replica:
        warning("Aucun replica disponible")
        return

    info(f"Rendre le Node {replica.node_id} BYZANTIN (envoi de faux digests).")
    chaos_action(f"BYZANTINE — Node {replica.node_id} est maintenant malveillant !")
    await chaos.set_byzantine(replica.node_id)
    print_cluster_state(None, pbft_nodes)

    primary = chaos._find_pbft_primary()
    if primary:
        result = await primary.client_request("transfer", 1000)
        if result.get("success"):
            success(f"Commit quand meme en {result['latency_ms']:.0f}ms !")
            info("PBFT a ignore le faux digest. Il fallait 2f+1=3 COMMITs valides.")
        else:
            info("Timeout (attendu si le byzantin bloque trop de messages)")

    print_cluster_state(None, pbft_nodes)


# ── Main ──────────────────────────────────────────────────

async def run_full_demo(step=None):
    header("CONSENSUS VISUALIZER — Demo")
    print(f"  {GRAY}Raft vs PBFT — Systemes distribues 2026 — Timings lents{RESET}\n")

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
    info("Clusters demarres. Tous les noeuds sont FOLLOWER.\n")

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
                    input(f"\n  {GRAY}[ Appuyez sur Entree pour continuer ]{RESET}")
    except KeyboardInterrupt:
        pass

    header("Fin de la demo")
    print(f"  {GREEN}Benchmark :{RESET}  python main.py --benchmark")
    print(f"  {GREEN}Demo rapide:{RESET} python demo.py --step election\n")


async def main():
    parser = argparse.ArgumentParser(
        description="Demo mode — timings lents pour presentation orale"
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
        print("\nAu revoir !")