"""
CHAOS ENGINE
============
Injecteur de pannes controle et reproductible.

Scenarios supportes :
  - crash_node        : tue un noeud proprement
  - revive_node       : ressuscite un noeud
  - network_partition : isole des groupes de noeuds
  - heal_partition    : repare la partition
  - add_delay         : ajoute de la latence reseau
  - set_byzantine     : rend un noeud PBFT malveillant
  - leader_attack     : tue le leader Raft
  - stress_test       : charge maximale pendant N secondes
  - run_scenario      : execute un scenario scriptable
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Union

from raft.node import RaftNode, NodeState
from pbft.node import PBFTNode, PBFTNodeState

logger = logging.getLogger(__name__)


@dataclass
class ChaosEvent:
    timestamp:   float
    action:      str
    target:      str        # "raft" | "pbft" | "all"
    node_ids:    list
    params:      dict = field(default_factory=dict)
    description: str = ""


class ChaosEngine:
    """
    Moteur de chaos — injecte des pannes de facon scriptee et reproductible.

    Usage:
        chaos = ChaosEngine(raft_nodes, pbft_nodes, event_bus)
        await chaos.crash_node("raft", 1)
        await chaos.network_partition("raft", [0, 1], [2, 3, 4])
        await chaos.run_scenario("leader_failure")
    """

    BUILT_IN_SCENARIOS = {
        "leader_failure": [
            {"t": 0.0, "action": "log",            "msg": "=== Scenario : Leader Failure ==="},
            {"t": 1.0, "action": "leader_attack",   "target": "raft"},
            {"t": 4.0, "action": "log",             "msg": "Nouveau leader elu, verification..."},
            {"t": 5.0, "action": "client_request",  "target": "raft", "op": "set", "val": 42},
        ],
        "network_partition": [
            {"t": 0.0, "action": "log",             "msg": "=== Scenario : Network Partition ==="},
            {"t": 1.0, "action": "partition",        "target": "raft", "g1": [0, 1], "g2": [2, 3, 4]},
            {"t": 5.0, "action": "heal",             "target": "raft"},
            {"t": 6.0, "action": "client_request",   "target": "raft", "op": "set", "val": 99},
        ],
        "byzantine_attack": [
            {"t": 0.0, "action": "log",             "msg": "=== Scenario : Byzantine Attack ==="},
            {"t": 0.5, "action": "set_byzantine",    "target": "pbft", "node_id": 1},
            {"t": 1.0, "action": "client_request",   "target": "pbft", "op": "transfer", "val": 1000},
            {"t": 3.0, "action": "client_request",   "target": "pbft", "op": "transfer", "val": 500},
        ],
        "high_load": [
            {"t": 0.0, "action": "log",             "msg": "=== Scenario : High Load ==="},
            {"t": 0.0, "action": "stress",           "target": "raft", "duration": 5, "rps": 50},
            {"t": 0.0, "action": "stress",           "target": "pbft", "duration": 5, "rps": 20},
        ],
        "cascading_failures": [
            {"t": 0.0, "action": "log",             "msg": "=== Scenario : Cascading Failures ==="},
            {"t": 1.0, "action": "crash",            "target": "raft", "node_id": 2},
            {"t": 2.0, "action": "crash",            "target": "raft", "node_id": 3},
            {"t": 4.0, "action": "revive",           "target": "raft", "node_id": 2},
            {"t": 5.0, "action": "revive",           "target": "raft", "node_id": 3},
        ],
    }

    def __init__(self, raft_nodes: list, pbft_nodes: list, event_bus=None):
        self.raft_nodes  = {n.node_id: n for n in raft_nodes}
        self.pbft_nodes  = {n.node_id: n for n in pbft_nodes}
        self.event_bus   = event_bus
        self.history: list[ChaosEvent] = []
        self._partitions: dict = {"raft": [], "pbft": []}

    # ─────────────────────────────────────────────
    # INDIVIDUAL ACTIONS
    # ─────────────────────────────────────────────

    async def crash_node(self, target: str, node_id: int):
        node = self._get_node(target, node_id)
        if not node:
            return
        await node.stop()
        self._record(ChaosEvent(
            timestamp=time.time(), action="crash", target=target,
            node_ids=[node_id],
            description=f"[CRASH] Node {target}[{node_id}] stopped"
        ))
        logger.warning(f"[Chaos] {target}[{node_id}] CRASHED")
        await self._emit("node_crashed", {"target": target, "node_id": node_id})

    async def revive_node(self, target: str, node_id: int):
        node = self._get_node(target, node_id)
        if not node:
            return
        await node.revive()
        self._record(ChaosEvent(
            timestamp=time.time(), action="revive", target=target,
            node_ids=[node_id],
            description=f"[REVIVE] Node {target}[{node_id}] restarted"
        ))
        logger.info(f"[Chaos] {target}[{node_id}] REVIVED")
        await self._emit("node_revived", {"target": target, "node_id": node_id})

    async def leader_attack(self, target: str = "raft"):
        if target == "raft":
            leader = self._find_raft_leader()
            if leader:
                await self.crash_node("raft", leader.node_id)
                logger.warning(f"[Chaos] Leader {leader.node_id} killed")
            else:
                logger.warning("[Chaos] No leader to attack")
        elif target == "pbft":
            primary = self._find_pbft_primary()
            if primary:
                await self.crash_node("pbft", primary.node_id)

    async def network_partition(self, target: str,
                                group1: list, group2: list):
        nodes = self.raft_nodes if target == "raft" else self.pbft_nodes
        self._partitions[target] = [(group1, group2)]

        for nid in group1:
            node = nodes.get(nid)
            if node:
                node._partitioned_from = set(group2)

        for nid in group2:
            node = nodes.get(nid)
            if node:
                node._partitioned_from = set(group1)

        await self._apply_partition_filter(target, group1, group2)

        self._record(ChaosEvent(
            timestamp=time.time(), action="partition", target=target,
            node_ids=group1 + group2,
            description=f"[PARTITION] {target}: {group1} isolated from {group2}"
        ))
        logger.warning(f"[Chaos] Network partition: {group1} vs {group2}")
        await self._emit("network_partitioned", {
            "target": target, "group1": group1, "group2": group2
        })

    async def heal_partition(self, target: str):
        nodes = self.raft_nodes if target == "raft" else self.pbft_nodes
        for node in nodes.values():
            node._partitioned_from = set()
        self._partitions[target] = []

        self._record(ChaosEvent(
            timestamp=time.time(), action="heal", target=target,
            node_ids=[],
            description=f"[HEAL] Partition repaired on {target}"
        ))
        logger.info(f"[Chaos] Partition healed on {target}")
        await self._emit("partition_healed", {"target": target})

    async def add_delay(self, target: str, node_id: int, delay_ms: float):
        node = self._get_node(target, node_id)
        if node:
            node.chaos_delay_ms = delay_ms
            self._record(ChaosEvent(
                timestamp=time.time(), action="delay", target=target,
                node_ids=[node_id], params={"delay_ms": delay_ms},
                description=f"[DELAY] {target}[{node_id}] = {delay_ms}ms"
            ))
            logger.info(f"[Chaos] {target}[{node_id}] delay = {delay_ms}ms")
            await self._emit("delay_added", {
                "target": target, "node_id": node_id, "delay_ms": delay_ms
            })

    async def set_drop_rate(self, target: str, node_id: int, rate: float):
        node = self._get_node(target, node_id)
        if node:
            node.chaos_drop_rate = rate
            await self._emit("drop_rate_set", {
                "target": target, "node_id": node_id, "rate": rate
            })

    async def set_byzantine(self, node_id: int):
        node = self.pbft_nodes.get(node_id)
        if node:
            node.is_byzantine = True
            node.state        = PBFTNodeState.BYZANTINE
            self._record(ChaosEvent(
                timestamp=time.time(), action="set_byzantine", target="pbft",
                node_ids=[node_id],
                description=f"[BYZANTINE] PBFT[{node_id}] turned malicious"
            ))
            logger.warning(f"[Chaos] PBFT[{node_id}] is now BYZANTINE")
            await self._emit("node_byzantine", {"node_id": node_id})

    async def cure_byzantine(self, node_id: int):
        node = self.pbft_nodes.get(node_id)
        if node:
            node.is_byzantine = False
            node.state        = PBFTNodeState.NORMAL

    async def stress_test(self, target: str, duration_s: float,
                          requests_per_second: int = 20):
        logger.info(
            f"[Chaos] Stress test on {target} — "
            f"{requests_per_second} rps for {duration_s}s"
        )
        await self._emit("stress_start", {"target": target, "rps": requests_per_second})

        start    = time.time()
        sent     = 0
        errors   = 0
        latencies = []
        interval = 1.0 / requests_per_second
        deadline = start + duration_s

        while time.time() < deadline:
            node = self._get_leader_node(target)
            if node:
                t0     = time.time()
                result = await node.client_request("set", sent)
                latency = (time.time() - t0) * 1000
                if result.get("success"):
                    latencies.append(latency)
                else:
                    errors += 1
                sent += 1
            await asyncio.sleep(interval)

        avg_lat   = sum(latencies) / len(latencies) if latencies else 0
        throughput = sent / duration_s

        stats = {
            "target":         target,
            "sent":           sent,
            "errors":         errors,
            "throughput_rps": round(throughput, 2),
            "avg_latency_ms": round(avg_lat, 2),
            "p95_latency_ms": round(
                sorted(latencies)[int(len(latencies) * 0.95)]
                if latencies else 0, 2
            ),
        }
        logger.info(f"[Chaos] Stress results: {stats}")
        await self._emit("stress_done", stats)
        return stats

    # ─────────────────────────────────────────────
    # SCRIPTABLE SCENARIOS
    # ─────────────────────────────────────────────

    async def run_scenario(self, name: str):
        script = self.BUILT_IN_SCENARIOS.get(name)
        if not script:
            logger.error(f"[Chaos] Unknown scenario: {name}")
            return

        logger.info(f"[Chaos] Running scenario: '{name}'")
        await self._emit("scenario_start", {"name": name})

        start = time.time()
        for step in script:
            target_t = step["t"]
            now      = time.time() - start
            if target_t > now:
                await asyncio.sleep(target_t - now)

            action = step["action"]

            if action == "log":
                logger.info(f"[Scenario] {step['msg']}")
            elif action == "crash":
                await self.crash_node(step["target"], step["node_id"])
            elif action == "revive":
                await self.revive_node(step["target"], step["node_id"])
            elif action == "leader_attack":
                await self.leader_attack(step.get("target", "raft"))
            elif action == "partition":
                await self.network_partition(step["target"], step["g1"], step["g2"])
            elif action == "heal":
                await self.heal_partition(step["target"])
            elif action == "set_byzantine":
                await self.set_byzantine(step["node_id"])
            elif action == "stress":
                asyncio.create_task(
                    self.stress_test(
                        step["target"], step["duration"], step.get("rps", 20)
                    )
                )
            elif action == "client_request":
                node = self._get_leader_node(step.get("target", "raft"))
                if node:
                    result = await node.client_request(
                        step.get("op", "set"), step.get("val")
                    )
                    logger.info(f"[Scenario] Request result: {result}")

        logger.info(f"[Chaos] Scenario '{name}' completed")
        await self._emit("scenario_done", {"name": name})

    # ─────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────

    def _get_node(self, target: str, node_id: int):
        if target == "raft":
            return self.raft_nodes.get(node_id)
        elif target == "pbft":
            return self.pbft_nodes.get(node_id)
        return None

    def _get_leader_node(self, target: str):
        if target == "raft":
            return self._find_raft_leader()
        elif target == "pbft":
            return self._find_pbft_primary()
        return None

    def _find_raft_leader(self) -> RaftNode | None:
        for node in self.raft_nodes.values():
            if node.state == NodeState.LEADER:
                return node
        return None

    def _find_pbft_primary(self) -> PBFTNode | None:
        for node in self.pbft_nodes.values():
            if node.is_primary() and node.state != PBFTNodeState.DEAD:
                return node
        return None

    async def _apply_partition_filter(self, target: str,
                                      group1: list, group2: list):
        import types

        nodes  = self.raft_nodes if target == "raft" else self.pbft_nodes
        g1_set = set(group1)
        g2_set = set(group2)

        async def partitioned_can_send(self_node, peer):
            my_group   = g1_set if self_node.node_id in g1_set else g2_set
            peer_group = g1_set if peer.node_id in g1_set else g2_set
            if my_group != peer_group:
                return False
            return True

        for node in nodes.values():
            node._can_send = types.MethodType(partitioned_can_send, node)

    def _record(self, event: ChaosEvent):
        self.history.append(event)

    async def _emit(self, event_type: str, data: dict):
        if self.event_bus:
            await self.event_bus.emit(event_type, {**data, "source": "chaos"})

    def get_history(self) -> list:
        return [
            {
                "timestamp":   e.timestamp,
                "action":      e.action,
                "target":      e.target,
                "description": e.description,
            }
            for e in self.history
        ]