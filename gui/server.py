"""
EVENT BUS & WEBSOCKET SERVER
=============================
Pont entre le backend Python et la GUI JavaScript.
Diffuse les événements en temps réel via WebSocket.
"""

import asyncio
import json
import logging
import time
from collections import deque

logger = logging.getLogger(__name__)


class EventBus:
    """
    Bus d'événements central.
    Collecte les événements de Raft, PBFT et Chaos Engine
    et les diffuse aux clients WebSocket connectés.
    """

    def __init__(self, max_history: int = 500):
        self._subscribers: list[asyncio.Queue] = []
        self._history: deque = deque(maxlen=max_history)

    async def emit(self, event_type: str, data: dict):
        """Émet un événement vers tous les abonnés."""
        event = {
            "type": event_type,
            "data": data,
            "ts": time.time()
        }
        self._history.append(event)

        dead = []
        for q in self._subscribers:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                dead.append(q)

        for q in dead:
            self._subscribers.remove(q)

    def subscribe(self) -> asyncio.Queue:
        """S'abonner aux événements. Retourne une queue."""
        q = asyncio.Queue(maxsize=200)
        self._subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        if q in self._subscribers:
            self._subscribers.remove(q)

    def get_history(self) -> list:
        return list(self._history)


class WebSocketServer:
    """
    Serveur WebSocket minimal (sans dépendance externe lourde).
    Utilise asyncio + websockets.

    Pour lancer : python gui/server.py
    """

    def __init__(self, event_bus: EventBus, host="localhost", port=8765):
        self.event_bus = event_bus
        self.host = host
        self.port = port
        self._raft_cluster = None
        self._pbft_cluster = None
        self._chaos_engine = None

    def set_clusters(self, raft, pbft, chaos):
        self._raft_cluster = raft
        self._pbft_cluster = pbft
        self._chaos_engine = chaos

    async def handler(self, websocket, path=None):
        """Gère une connexion WebSocket cliente."""
        logger.info(f"[WS] Client connected from {websocket.remote_address}")

        # Envoyer l'historique des événements récents
        history = self.event_bus.get_history()[-50:]
        for event in history:
            try:
                await websocket.send(json.dumps(event))
            except Exception:
                break

        # S'abonner aux nouveaux événements
        queue = self.event_bus.subscribe()

        # Lancer le listener pour les commandes entrantes
        recv_task = asyncio.create_task(self._receive_commands(websocket))

        try:
            while True:
                event = await asyncio.wait_for(queue.get(), timeout=30.0)
                await websocket.send(json.dumps(event))
        except asyncio.TimeoutError:
            # Ping pour garder la connexion vivante
            try:
                await websocket.ping()
            except Exception:
                pass
        except Exception as e:
            logger.debug(f"[WS] Client disconnected: {e}")
        finally:
            recv_task.cancel()
            self.event_bus.unsubscribe(queue)

    async def _receive_commands(self, websocket):
        """Reçoit les commandes de la GUI (boutons du Chaos Engine)."""
        try:
            async for message in websocket:
                try:
                    cmd = json.loads(message)
                    await self._handle_command(cmd, websocket)
                except json.JSONDecodeError:
                    pass
        except Exception:
            pass

    async def _handle_command(self, cmd: dict, websocket):
        """Dispatch des commandes GUI vers le Chaos Engine."""
        action = cmd.get("action")
        target = cmd.get("target", "raft")
        node_id = cmd.get("node_id")

        if not self._chaos_engine:
            return

        response = {"type": "command_ack", "action": action, "success": True}

        try:
            if action == "crash":
                await self._chaos_engine.crash_node(target, node_id)
            elif action == "revive":
                await self._chaos_engine.revive_node(target, node_id)
            elif action == "leader_attack":
                await self._chaos_engine.leader_attack(target)
            elif action == "partition":
                g1 = cmd.get("group1", [])
                g2 = cmd.get("group2", [])
                await self._chaos_engine.network_partition(target, g1, g2)
            elif action == "heal":
                await self._chaos_engine.heal_partition(target)
            elif action == "delay":
                await self._chaos_engine.add_delay(target, node_id, cmd.get("delay_ms", 100))
            elif action == "byzantine":
                await self._chaos_engine.set_byzantine(node_id)
            elif action == "scenario":
                scenario = cmd.get("scenario", "leader_failure")
                asyncio.create_task(self._chaos_engine.run_scenario(scenario))
            elif action == "client_request":
                leader = self._chaos_engine._get_leader_node(target)
                if leader:
                    result = await leader.client_request(
                        cmd.get("op", "set"),
                        cmd.get("value", 0)
                    )
                    response["result"] = result
            elif action == "get_status":
                response["status"] = self._get_full_status()
        except Exception as e:
            response["success"] = False
            response["error"] = str(e)

        await websocket.send(json.dumps(response))

    def _get_full_status(self) -> dict:
        status = {"raft": [], "pbft": []}
        if self._raft_cluster:
            for node in self._raft_cluster.values():
                status["raft"].append(node.get_status())
        if self._pbft_cluster:
            for node in self._pbft_cluster.values():
                status["pbft"].append(node.get_status())
        return status

    async def start(self):
        try:
            import websockets
            logger.info(f"[WS] Server starting on ws://{self.host}:{self.port}")
            async with websockets.serve(self.handler, self.host, self.port):
                logger.info(f"[WS] ✅ WebSocket server running")
                await asyncio.Future()
        except ImportError:
            logger.warning("[WS] 'websockets' package not installed. Run: pip install websockets")
        except Exception as e:
            logger.error(f"[WS] Failed to start: {e}")