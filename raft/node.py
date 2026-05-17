"""
RAFT NODE — Crash Fault Tolerant Consensus
==========================================
Implemente le protocole Raft complet :
  - Leader election via randomized timeouts
  - Log replication
  - Safety (au plus 1 leader par term)
"""

import asyncio
import random
import time
import logging
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Optional

logger = logging.getLogger(__name__)


class NodeState(Enum):
    FOLLOWER  = "follower"
    CANDIDATE = "candidate"
    LEADER    = "leader"
    DEAD      = "dead"


@dataclass
class LogEntry:
    term: int
    index: int
    command: str
    value: object = None


@dataclass
class RaftMetrics:
    elections: int = 0
    messages_sent: int = 0
    messages_received: int = 0
    commits: int = 0
    leader_changes: int = 0
    latencies: list = field(default_factory=list)

    def avg_latency(self):
        if not self.latencies:
            return 0
        return sum(self.latencies) / len(self.latencies)


class RaftNode:
    """
    Noeud Raft complet avec gestion des pannes et metriques.

    Parametres:
        node_id                : identifiant unique (0, 1, 2, ...)
        peers                  : liste de tous les noeuds du cluster
        election_timeout_range : (min_ms, max_ms) pour le timeout aleatoire
    """

    def __init__(self, node_id: int, peers: list = None,
                 election_timeout_range=(150, 300)):
        self.node_id = node_id
        self.peers: list["RaftNode"] = peers or []

        # Persistent state
        self.current_term = 0
        self.voted_for: Optional[int] = None
        self.log: list[LogEntry] = []

        # Volatile state
        self.commit_index  = -1
        self.last_applied  = -1
        self.state         = NodeState.FOLLOWER
        self.current_leader: Optional[int] = None

        # Leader state (reinitialise a chaque election)
        self.next_index:  dict[int, int] = {}
        self.match_index: dict[int, int] = {}

        # Timing
        self.election_timeout_range = election_timeout_range
        self.last_heartbeat  = time.time()
        self.election_timeout = self._random_election_timeout()

        # Metrics & event bus
        self.metrics    = RaftMetrics()
        self._running   = False
        self._event_bus = None

        # Chaos engine hooks
        self.chaos_delay_ms:  float = 0.0
        self.chaos_drop_rate: float = 0.0

    # ─────────────────────────────────────────────
    # LIFECYCLE
    # ─────────────────────────────────────────────

    async def start(self):
        self._running = True
        logger.info(f"[Raft {self.node_id}] Starting as FOLLOWER")
        asyncio.create_task(self._main_loop())

    async def stop(self):
        self._running = False
        self.state = NodeState.DEAD
        logger.info(f"[Raft {self.node_id}] Stopped")

    async def revive(self):
        self.state        = NodeState.FOLLOWER
        self.last_heartbeat = time.time()
        self._running     = True
        asyncio.create_task(self._main_loop())
        logger.info(f"[Raft {self.node_id}] Revived")

    # ─────────────────────────────────────────────
    # MAIN LOOP
    # ─────────────────────────────────────────────

    async def _main_loop(self):
        while self._running and self.state != NodeState.DEAD:
            if self.state == NodeState.LEADER:
                await self._send_heartbeats()
                await asyncio.sleep(0.05)
            else:
                elapsed = time.time() - self.last_heartbeat
                if elapsed * 1000 > self.election_timeout:
                    await self._start_election()
                await asyncio.sleep(0.01)

    # ─────────────────────────────────────────────
    # LEADER ELECTION
    # ─────────────────────────────────────────────

    async def _start_election(self):
        self.state            = NodeState.CANDIDATE
        self.current_term    += 1
        self.voted_for        = self.node_id
        self.election_timeout = self._random_election_timeout()
        self.last_heartbeat   = time.time()
        self.metrics.elections += 1

        logger.info(f"[Raft {self.node_id}] Starting election for term {self.current_term}")
        await self._emit_event("election_started", {
            "node_id": self.node_id,
            "term":    self.current_term
        })

        votes          = 1
        last_log_index = len(self.log) - 1
        last_log_term  = self.log[-1].term if self.log else 0

        tasks = [
            self._request_vote(peer, last_log_index, last_log_term)
            for peer in self.peers
            if peer.node_id != self.node_id
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, dict) and result.get("vote_granted"):
                votes += 1

        majority = (len(self.peers) // 2) + 1

        if votes >= majority and self.state == NodeState.CANDIDATE:
            await self._become_leader()
        else:
            self.state = NodeState.FOLLOWER
            logger.info(
                f"[Raft {self.node_id}] Election lost ({votes}/{len(self.peers)} votes)"
            )

    async def _request_vote(self, peer: "RaftNode",
                            last_log_index: int, last_log_term: int) -> dict:
        if not await self._can_send(peer):
            return {"vote_granted": False}

        self.metrics.messages_sent += 1

        if peer.state == NodeState.DEAD:
            return {"vote_granted": False}

        return await peer.handle_vote_request({
            "term":           self.current_term,
            "candidate_id":   self.node_id,
            "last_log_index": last_log_index,
            "last_log_term":  last_log_term,
        })

    async def handle_vote_request(self, request: dict) -> dict:
        if self.state == NodeState.DEAD:
            return {"term": self.current_term, "vote_granted": False}

        self.metrics.messages_received += 1
        term         = request["term"]
        candidate_id = request["candidate_id"]

        if term > self.current_term:
            self.current_term = term
            self.state        = NodeState.FOLLOWER
            self.voted_for    = None

        if term < self.current_term:
            return {"term": self.current_term, "vote_granted": False}

        last_log_index = request["last_log_index"]
        last_log_term  = request["last_log_term"]
        my_last_index  = len(self.log) - 1
        my_last_term   = self.log[-1].term if self.log else 0

        log_ok = (last_log_term > my_last_term) or \
                 (last_log_term == my_last_term and last_log_index >= my_last_index)

        if (self.voted_for is None or self.voted_for == candidate_id) and log_ok:
            self.voted_for    = candidate_id
            self.last_heartbeat = time.time()
            await self._emit_event("vote_sent", {
                "from": self.node_id,
                "to":   candidate_id,
                "term": term,
            })
            return {"term": self.current_term, "vote_granted": True}

        return {"term": self.current_term, "vote_granted": False}

    async def _become_leader(self):
        self.state          = NodeState.LEADER
        self.current_leader = self.node_id
        self.metrics.leader_changes += 1

        for peer in self.peers:
            self.next_index[peer.node_id]  = len(self.log)
            self.match_index[peer.node_id] = -1

        logger.info(
            f"[Raft {self.node_id}] Became LEADER for term {self.current_term}"
        )
        await self._emit_event("leader_elected", {
            "node_id": self.node_id,
            "term":    self.current_term,
        })

    # ─────────────────────────────────────────────
    # LOG REPLICATION & HEARTBEATS
    # ─────────────────────────────────────────────

    async def _send_heartbeats(self):
        tasks = [
            self._send_append_entries(peer)
            for peer in self.peers
            if peer.node_id != self.node_id
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _send_append_entries(self, peer: "RaftNode"):
        if not await self._can_send(peer):
            return
        if peer.state == NodeState.DEAD:
            return

        next_idx       = self.next_index.get(peer.node_id, len(self.log))
        prev_log_index = next_idx - 1
        prev_log_term  = (
            self.log[prev_log_index].term
            if prev_log_index >= 0 and self.log else 0
        )
        entries = self.log[next_idx:]

        self.metrics.messages_sent += 1
        result = await peer.handle_append_entries({
            "term":           self.current_term,
            "leader_id":      self.node_id,
            "prev_log_index": prev_log_index,
            "prev_log_term":  prev_log_term,
            "entries":        [asdict(e) for e in entries],
            "leader_commit":  self.commit_index,
        })

        if result.get("success"):
            if entries:
                self.next_index[peer.node_id]  = next_idx + len(entries)
                self.match_index[peer.node_id] = self.next_index[peer.node_id] - 1
                await self._update_commit_index()
        else:
            if result.get("term", 0) > self.current_term:
                self.current_term = result["term"]
                self.state        = NodeState.FOLLOWER
                self.voted_for    = None
            else:
                self.next_index[peer.node_id] = max(0, next_idx - 1)

    async def handle_append_entries(self, request: dict) -> dict:
        if self.state == NodeState.DEAD:
            return {"term": self.current_term, "success": False}

        self.metrics.messages_received += 1
        term = request["term"]

        if term < self.current_term:
            return {"term": self.current_term, "success": False}

        self.current_term   = term
        self.state          = NodeState.FOLLOWER
        self.current_leader = request["leader_id"]
        self.last_heartbeat = time.time()

        prev_log_index = request["prev_log_index"]
        prev_log_term  = request["prev_log_term"]

        if prev_log_index >= 0:
            if prev_log_index >= len(self.log):
                return {"term": self.current_term, "success": False}
            if self.log[prev_log_index].term != prev_log_term:
                self.log = self.log[:prev_log_index]
                return {"term": self.current_term, "success": False}

        entries = request.get("entries", [])
        for entry_dict in entries:
            entry = LogEntry(**entry_dict)
            idx   = entry.index
            if idx < len(self.log):
                if self.log[idx].term != entry.term:
                    self.log = self.log[:idx]
                    self.log.append(entry)
            else:
                self.log.append(entry)

        leader_commit = request.get("leader_commit", -1)
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            await self._apply_committed()

        await self._emit_event("heartbeat_received", {
            "node_id": self.node_id,
            "from":    request["leader_id"],
            "entries": len(entries),
        })

        return {"term": self.current_term, "success": True}

    # ─────────────────────────────────────────────
    # CLIENT INTERFACE
    # ─────────────────────────────────────────────

    async def client_request(self, command: str, value=None) -> dict:
        if self.state != NodeState.LEADER:
            return {
                "success": False,
                "error":   "not_leader",
                "leader":  self.current_leader,
            }

        start = time.time()
        entry = LogEntry(
            term=self.current_term,
            index=len(self.log),
            command=command,
            value=value,
        )
        self.log.append(entry)
        await self._replicate_and_commit(entry)

        latency = (time.time() - start) * 1000
        self.metrics.latencies.append(latency)
        self.metrics.commits += 1

        return {"success": True, "latency_ms": round(latency, 2), "index": entry.index}

    async def _replicate_and_commit(self, entry: LogEntry):
        for _ in range(20):
            await self._send_heartbeats()
            majority  = (len(self.peers) // 2) + 1
            confirmed = 1
            for peer in self.peers:
                if self.match_index.get(peer.node_id, -1) >= entry.index:
                    confirmed += 1
            if confirmed >= majority:
                self.commit_index = entry.index
                return
            await asyncio.sleep(0.05)

    async def _update_commit_index(self):
        majority = (len(self.peers) // 2) + 1
        for n in range(len(self.log) - 1, self.commit_index, -1):
            if self.log[n].term == self.current_term:
                count = 1
                for peer in self.peers:
                    if self.match_index.get(peer.node_id, -1) >= n:
                        count += 1
                if count >= majority:
                    self.commit_index = n
                    await self._apply_committed()
                    break

    async def _apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            logger.debug(
                f"[Raft {self.node_id}] Applied: {entry.command}={entry.value}"
            )

    # ─────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────

    def _random_election_timeout(self) -> float:
        lo, hi = self.election_timeout_range
        return random.uniform(lo, hi)

    async def _can_send(self, peer: "RaftNode") -> bool:
        if self.chaos_drop_rate > 0 and random.random() < self.chaos_drop_rate:
            return False
        if self.chaos_delay_ms > 0:
            await asyncio.sleep(self.chaos_delay_ms / 1000)
        return True

    async def _emit_event(self, event_type: str, data: dict):
        if self._event_bus:
            await self._event_bus.emit(event_type, {**data, "algo": "raft"})

    def get_status(self) -> dict:
        return {
            "node_id":      self.node_id,
            "state":        self.state.value,
            "term":         self.current_term,
            "leader":       self.current_leader,
            "log_length":   len(self.log),
            "commit_index": self.commit_index,
            "metrics": {
                "elections":        self.metrics.elections,
                "commits":          self.metrics.commits,
                "avg_latency_ms":   round(self.metrics.avg_latency(), 2),
                "messages_sent":    self.metrics.messages_sent,
                "messages_received": self.metrics.messages_received,
            },
        }