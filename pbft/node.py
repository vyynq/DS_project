"""
PBFT NODE — Practical Byzantine Fault Tolerant Consensus (Version Optimisée & Résiliente)
=========================================================================================
"""

import asyncio
import hashlib
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)

class PBFTNodeState(Enum):
    NORMAL = "normal"
    VIEW_CHANGE = "view_change"
    DEAD = "dead"
    BYZANTINE = "byzantine"

@dataclass
class PBFTMessage:
    msg_type: str          # pre_prepare | prepare | commit | view_change
    view: int
    sequence: int
    digest: str
    node_id: int
    request: Optional[dict] = None
    fake: bool = False

@dataclass
class PBFTMetrics:
    requests_handled: int = 0
    view_changes: int = 0
    byzantine_caught: int = 0
    messages_sent: int = 0
    messages_received: int = 0
    latencies: list = field(default_factory=list)

    def avg_latency(self):
        if not self.latencies: return 0
        return sum(self.latencies) / len(self.latencies)

def digest(data: dict) -> str:
    return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()[:16]

class PBFTNode:
    def __init__(self, node_id: int, peers: list = None, is_byzantine: bool = False):
        self.node_id = node_id
        self.peers: list["PBFTNode"] = peers or []
        self.is_byzantine = is_byzantine

        self.view = 0
        self.sequence = 0
        self.state = PBFTNodeState.BYZANTINE if is_byzantine else PBFTNodeState.NORMAL
        self._running = False

        # Mémoire locale du nœud
        self.pre_prepare_log: dict[tuple, str] = {}    # (view, seq) -> digest
        self.requests_log: dict[tuple, dict] = {}      # (view, seq) -> requête originale
        self.prepare_log: dict[tuple, list[PBFTMessage]] = defaultdict(list)
        self.commit_log: dict[tuple, list[PBFTMessage]] = defaultdict(list)
        self.committed_requests: dict[int, dict] = {}  # seq -> request

        # Garde-fous anti-spam pour éviter les doubles envois
        self.prepare_sent: dict[tuple, bool] = defaultdict(bool)
        self.commit_sent: dict[tuple, bool] = defaultdict(bool)

        self.view_change_votes: dict[int, set] = defaultdict(set)
        self.last_activity = time.time()
        self.view_change_timeout = 15.0  

        self.metrics = PBFTMetrics()
        self._event_bus = None
        self.chaos_delay_ms: float = 0.0
        self.chaos_drop_rate: float = 0.0

    def primary_id(self) -> int:
        return self.view % len(self.peers)

    def is_primary(self) -> bool:
        return self.node_id == self.primary_id()

    async def start(self):
        self._running = True
        asyncio.create_task(self._watchdog_loop())
        logger.info(f"[PBFT {self.node_id}] Started. Primary: {self.primary_id()}")

    async def stop(self):
        self._running = False
        self.state = PBFTNodeState.DEAD

    async def revive(self):
        self.state = PBFTNodeState.NORMAL
        self._running = True
        self.last_activity = time.time()
        asyncio.create_task(self._watchdog_loop())

    async def client_request(self, operation: str, value=None) -> dict:
        request = {"op": operation, "value": value, "timestamp": time.time()}
        if not self.is_primary():
            primary = self._get_primary_node()
            return await primary.client_request(operation, value) if primary else {"success": False}

        start = time.time()
        seq = self._next_sequence()
        d = digest(request)

        key = (self.view, seq)
        self.pre_prepare_log[key] = d
        self.requests_log[key] = request

        # CORRECTION : Le primaire s'auto-enregistre un message Prepare pour valider son propre quorum plus tard
        self.prepare_log[key].append(PBFTMessage("prepare", self.view, seq, d, self.node_id, fake=self.is_byzantine))

        pre_prepare_msg = PBFTMessage("pre_prepare", self.view, seq, d, self.node_id, request)
        await self._broadcast(pre_prepare_msg)

        committed = await self._wait_for_commit(seq, timeout=10.0)
        latency = (time.time() - start) * 1000
        self.metrics.latencies.append(latency)

        if committed:
            self.metrics.requests_handled += 1
            return {"success": True, "latency_ms": round(latency, 2), "sequence": seq}
        return {"success": False, "error": "timeout"}

    async def handle_pre_prepare(self, msg: PBFTMessage):
        if self.state == PBFTNodeState.DEAD or not await self._can_receive(): return
        self.metrics.messages_received += 1
        self.last_activity = time.time()

        if msg.view != self.view: return
        
        key = (msg.view, msg.sequence)
        self.pre_prepare_log[key] = msg.digest
        if msg.request:
            self.requests_log[key] = msg.request

        # CORRECTION : Envoi unique à l'aide du flag prepare_sent
        if not self.prepare_sent[key]:
            self.prepare_sent[key] = True
            my_digest = "FAKE_" + msg.digest if self.is_byzantine else msg.digest
            prepare_msg = PBFTMessage("prepare", self.view, msg.sequence, my_digest, self.node_id, fake=self.is_byzantine)
            
            # CORRECTION CRUCIALE : Le nœud stocke son propre vote Prepare localement
            self.prepare_log[key].append(prepare_msg)
            
            await self._broadcast(prepare_msg)
            await self._check_prepare_quorum(msg.view, msg.sequence)

    async def handle_prepare(self, msg: PBFTMessage):
        if self.state == PBFTNodeState.DEAD or not await self._can_receive(): return
        self.metrics.messages_received += 1
        key = (msg.view, msg.sequence)
        
        # Éviter d'ajouter des doublons d'un même nœud
        if any(m.node_id == msg.node_id for m in self.prepare_log[key]): return
        
        self.prepare_log[key].append(msg)
        await self._check_prepare_quorum(msg.view, msg.sequence)

    async def _check_prepare_quorum(self, view: int, sequence: int):
        key = (view, sequence)
        f = self._max_byzantine()
        target_digest = self._expected_digest(key)
        if not target_digest: return

        valid_prepares = [m for m in self.prepare_log[key] if m.digest == target_digest and not m.fake]

        # Quorum de Prepare : 2f messages valides (le nôtre inclus)
        if len(valid_prepares) >= 2 * f and not self.commit_sent[key]:
            await self._send_commit(view, sequence, target_digest)

    async def handle_commit(self, msg: PBFTMessage):
        if self.state == PBFTNodeState.DEAD or not await self._can_receive(): return
        self.metrics.messages_received += 1
        key = (msg.view, msg.sequence)
        
        # Éviter d'ajouter des doublons d'un même nœud
        if any(m.node_id == msg.node_id for m in self.commit_log[key]): return
        
        self.commit_log[key].append(msg)
        await self._check_commit_quorum(msg.view, msg.sequence)

    async def _check_commit_quorum(self, view: int, sequence: int):
        key = (view, sequence)
        f = self._max_byzantine()
        target_digest = self._expected_digest(key)
        if not target_digest: return

        valid_commits = [m for m in self.commit_log[key] if m.digest == target_digest and not m.fake]

        # Quorum de Commit : 2f + 1 messages valides (le nôtre inclus)
        if len(valid_commits) >= 2 * f + 1:
            if sequence not in self.committed_requests:
                req = self._find_request(key)
                self.committed_requests[sequence] = req or {"status": "ok"}
                logger.info(f"[PBFT {self.node_id}] COMMITTED seq={sequence}")

    def _expected_digest(self, key: tuple) -> Optional[str]:
        return self.pre_prepare_log.get(key)

    def _find_request(self, key: tuple) -> Optional[dict]:
        return self.requests_log.get(key)

    async def _watchdog_loop(self):
        while self._running:
            await asyncio.sleep(1.0)
            if self.state == PBFTNodeState.NORMAL and not self.is_primary():
                if time.time() - self.last_activity > self.view_change_timeout:
                    await self._trigger_view_change()

    async def _trigger_view_change(self):
        self.state = PBFTNodeState.VIEW_CHANGE
        new_view = self.view + 1
        
        # CORRECTION : Le nœud s'ajoute son propre vote pour le changement de vue
        self.view_change_votes[new_view].add(self.node_id)
        
        for peer in self.peers:
            if peer.node_id != self.node_id:
                asyncio.create_task(peer.handle_view_change(self.node_id, new_view))

    async def handle_view_change(self, from_node: int, new_view: int):
        if self.state == PBFTNodeState.DEAD: return
        self.view_change_votes[new_view].add(from_node)
        
        if len(self.view_change_votes[new_view]) >= 2 * self._max_byzantine() + 1:
            if new_view > self.view:
                self.view = new_view
                self.state = PBFTNodeState.NORMAL
                self.last_activity = time.time()

    async def _send_commit(self, view: int, sequence: int, msg_digest: str):
        key = (view, sequence)
        self.commit_sent[key] = True
        
        my_digest = "FAKE_" + msg_digest if self.is_byzantine else msg_digest
        commit_msg = PBFTMessage("commit", view, sequence, my_digest, self.node_id, fake=self.is_byzantine)
        
        # CORRECTION CRUCIALE : Le nœud stocke son propre vote Commit localement
        self.commit_log[key].append(commit_msg)
        
        await self._broadcast(commit_msg)
        await self._check_commit_quorum(view, sequence)

    async def _broadcast(self, msg: PBFTMessage):
        for peer in self.peers:
            if peer.node_id == self.node_id or peer.state == PBFTNodeState.DEAD: continue
            if not await self._can_send(peer): continue
            self.metrics.messages_sent += 1
            if msg.msg_type == "pre_prepare": asyncio.create_task(peer.handle_pre_prepare(msg))
            elif msg.msg_type == "prepare": asyncio.create_task(peer.handle_prepare(msg))
            elif msg.msg_type == "commit": asyncio.create_task(peer.handle_commit(msg))

    async def _wait_for_commit(self, seq: int, timeout: float) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if seq in self.committed_requests:
                return True
            await asyncio.sleep(0.01) 
        return False

    def _next_sequence(self) -> int:
        self.sequence += 1
        return self.sequence

    def _max_byzantine(self) -> int:
        return (len(self.peers) - 1) // 3

    def _get_primary_node(self):
        for p in self.peers:
            if p.node_id == self.primary_id(): return p
        return None

    async def _can_send(self, peer) -> bool:
        import random
        if self.chaos_delay_ms > 0:
            jitter = random.uniform(0.5, 1.5)
            await asyncio.sleep((self.chaos_delay_ms * jitter) / 1000)
        if self.chaos_drop_rate > 0:
            if random.random() < self.chaos_drop_rate:
                return False
        return True

    async def _can_receive(self) -> bool:
        return True

    def get_status(self) -> dict:
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "view": self.view,
            "primary": self.primary_id(),
            "sequence": self.sequence,
            "commits": len(self.committed_requests),
            "metrics": {"requests_handled": self.metrics.requests_handled}
        }