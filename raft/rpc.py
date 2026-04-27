import asyncio
import random
import logging

logger = logging.getLogger(__name__)

class RPCManager:
    """Simule une couche réseau asynchrone avec latence et pannes."""
    def __init__(self, event_bus=None):
        self.nodes = {}
        self.event_bus = event_bus
        self.latency_min = 0.01  # 10ms min
        self.latency_max = 0.05  # 50ms max

    def register_node(self, node_id, node_instance):
        self.nodes[node_id] = node_instance

    async def send(self, from_id, to_id, method, args):
        """Envoie un message RPC avec une simulation de délai réseau."""
        if to_id not in self.nodes:
            return None
        
        # Simulation de la latence réseau
        delay = random.uniform(self.latency_min, self.latency_max)
        await asyncio.sleep(delay)
        
        target_node = self.nodes[to_id]
        
        # Vérification si le nœud destinataire est "vivant"
        if hasattr(target_node, 'state') and target_node.state.value == "dead":
            return None

        # Appel de la méthode sur le nœud distant
        func = getattr(target_node, method, None)
        if func:
            if asyncio.iscoroutinefunction(func):
                return await func(args)
            return func(args)
        return None