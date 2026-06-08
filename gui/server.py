"""
EVENT BUS & WEBSOCKET SERVER
=============================
Bridge between the Python backend and the JavaScript GUI.
Broadcast live events through WebSocket.
"""

import asyncio
import json
import logging
import time
from collections import deque

logger = logging.getLogger(__name__)


class EventBus:
    """
    Central event bus.
    Collects events from Raft, PBFT, and the Chaos Engine and broadcasts
    them to connected WebSocket clients.
    """

    def __init__(self, max_history: int = 500):
        self._subscribers: list[asyncio.Queue] = []
        self._history: deque = deque(maxlen=max_history)

    async def emit(self, event_type: str, data: dict):
        """Emit an event to all subscribers."""
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
        """Subscribe to events and return the subscriber queue."""
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
    Minimal WebSocket server using asyncio and websockets.

    Run with: python gui/server.py
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
        """Handle one WebSocket client connection."""
        logger.info(f"[WS] Client connected from {websocket.remote_address}")

        # Send recent event history.
        history = self.event_bus.get_history()[-50:]
        for event in history:
            try:
                await websocket.send(json.dumps(event))
            except Exception:
                break

        # Subscribe to new events.
        queue = self.event_bus.subscribe()

        # Start the incoming command listener.
        recv_task = asyncio.create_task(self._receive_commands(websocket))

        try:
            while True:
                event = await asyncio.wait_for(queue.get(), timeout=30.0)
                await websocket.send(json.dumps(event))
        except asyncio.TimeoutError:
            # Send a ping to keep the connection alive.
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
        """Receive GUI commands from the Chaos Engine buttons."""
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
        """Dispatch GUI commands to the Chaos Engine."""
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
                logger.info("[WS] WebSocket server running")
                await asyncio.Future()
        except ImportError:
            logger.warning("[WS] 'websockets' package not installed. Run: pip install websockets")
        except Exception as e:
            logger.error(f"[WS] Failed to start: {e}")
