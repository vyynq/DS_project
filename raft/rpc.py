import asyncio
import random
import logging

logger = logging.getLogger(__name__)

class RPCManager:
    """Simulate an asynchronous network layer with latency and failures."""
    def __init__(self, event_bus=None):
        self.nodes = {}
        self.event_bus = event_bus
        self.latency_min = 0.01  # 10ms min
        self.latency_max = 0.05  # 50ms max

    def register_node(self, node_id, node_instance):
        self.nodes[node_id] = node_instance

    async def send(self, from_id, to_id, method, args):
        """Send an RPC message with simulated network delay."""
        if to_id not in self.nodes:
            return None
        
        # Simulate network latency.
        delay = random.uniform(self.latency_min, self.latency_max)
        await asyncio.sleep(delay)
        
        target_node = self.nodes[to_id]
        
        # Check whether the target node is alive.
        if hasattr(target_node, 'state') and target_node.state.value == "dead":
            return None

        # Call the requested method on the remote node.
        func = getattr(target_node, method, None)
        if func:
            if asyncio.iscoroutinefunction(func):
                return await func(args)
            return func(args)
        return None
