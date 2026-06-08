import asyncio
import logging
import random
import time
from pbft.node import PBFTNode, PBFTNodeState
from raft.node import RaftNode, NodeState

# Keep logs at WARNING level so the final table remains readable.
logging.basicConfig(level=logging.WARNING, format='%(asctime)s  %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)

# =========================================================================
# BYZANTINE EXTENSION FOR RAFT
# =========================================================================
class ByzantineRaftNode(RaftNode):
    """
    Simulate a compromised Raft leader that modifies client data before
    replicating it to followers.
    """
    async def client_request(self, command, value):
        corrupted_value = "HACKED_9999"
        print(f"  [ATTACK] Compromised Raft leader changed {value} to {corrupted_value}")
        # Force replication of the corrupted value.
        return await super().client_request(command, corrupted_value)

# =========================================================================
# BENCHMARK ENGINE
# =========================================================================
async def run_scenario(scenario_name, protocol_name, nodes, duration=8):
    """
    Simulate client request load for a fixed duration and collect performance data.
    """
    start_time = time.time()
    requests_sent = 0
    successful_commits = 0

    while time.time() - start_time < duration:
        # 1. Find the current leader or primary.
        leader_node = None
        if protocol_name == "PBFT":
            leader_node = next((n for n in nodes if n.is_primary() and n.state != PBFTNodeState.DEAD), None)
        else:  # RAFT
            leader_node = next((n for n in nodes if n.state == NodeState.LEADER), None)

        # 2. Send a request when a leader is available.
        if leader_node:
            val = f"energy_{random.randint(100, 500)}"
            task = asyncio.create_task(leader_node.client_request("set", val))
            
            try:
                # Use a short timeout so the benchmark loop keeps progressing.
                res = await asyncio.wait_for(task, timeout=1.5)
                if res and res.get("success"):
                    successful_commits += 1
            except asyncio.TimeoutError:
                pass
            
            requests_sent += 1

        await asyncio.sleep(0.05)  # Request interval of about 20 req/s.

    tps = successful_commits / duration
    return {
        "scenario": scenario_name,
        "protocol": protocol_name,
        "commits": successful_commits,
        "tps": round(tps, 1)
    }

async def setup_cluster(protocol, num_nodes, delay_ms, drop_rate, byzantine_ids=[]):
    """Initialize a cluster with network chaos parameters."""
    nodes = []
    if protocol == "PBFT":
        # PBFTNode handles Byzantine node initialization directly.
        nodes = [PBFTNode(node_id=i, is_byzantine=(i in byzantine_ids)) for i in range(num_nodes)]
    elif protocol == "RAFT":
        if byzantine_ids:
            # Node 0 is compromised; the remaining nodes are normal.
            nodes = [ByzantineRaftNode(node_id=0, election_timeout_range=(150, 300))] + \
                    [RaftNode(node_id=i, election_timeout_range=(150, 300)) for i in range(1, num_nodes)]
        else:
            nodes = [RaftNode(node_id=i, election_timeout_range=(150, 300)) for i in range(num_nodes)]
    
    # Configure peer links and chaos parameters.
    for node in nodes:
        node.peers = nodes
        node.chaos_delay_ms = delay_ms
        node.chaos_drop_rate = drop_rate

    # Start all nodes.
    for node in nodes:
        await node.start()
        
    await asyncio.sleep(1.5)  # Stabilization period for the initial election.
    
    # Force the compromised node to lead for scenario 3.
    if protocol == "RAFT" and byzantine_ids:
        nodes[0].state = NodeState.LEADER

    return nodes

async def stop_cluster(nodes):
    for node in nodes:
        await node.stop()

# =========================================================================
# SCENARIO EXECUTION
# =========================================================================
async def main():
    print(f"\n{'='*65}")
    print("  STARTING INDUSTRIAL COMPARATIVE BENCHMARK (SMART GRID)")
    print(f"{'='*65}\n")
    
    results = []
    duration_test = 6  # Seconds per test.

    # -----------------------------------------------------------------
    # SCENARIO 1: Internal LAN network (trusted and fast).
    # -----------------------------------------------------------------
    print("Scenario 1: Internal plant LAN (Latency: 5 ms, Loss: 0%)")
    
    raft_lan = await setup_cluster("RAFT", num_nodes=5, delay_ms=5.0, drop_rate=0.0)
    res_raft_lan = await run_scenario("Internal LAN", "RAFT", raft_lan, duration_test)
    results.append(res_raft_lan)
    await stop_cluster(raft_lan)

    pbft_lan = await setup_cluster("PBFT", num_nodes=4, delay_ms=5.0, drop_rate=0.0)
    res_pbft_lan = await run_scenario("Internal LAN", "PBFT", pbft_lan, duration_test)
    results.append(res_pbft_lan)
    await stop_cluster(pbft_lan)

    # -----------------------------------------------------------------
    # SCENARIO 2: Microgrid WAN (unstable peer-to-peer network).
    # -----------------------------------------------------------------
    print("\nScenario 2: Public microgrid WAN (Latency: 40 ms, Loss: 2%)")
    
    raft_wan = await setup_cluster("RAFT", num_nodes=5, delay_ms=40.0, drop_rate=0.02)
    res_raft_wan = await run_scenario("Microgrid WAN", "RAFT", raft_wan, duration_test)
    results.append(res_raft_wan)
    await stop_cluster(raft_wan)

    # PBFT also receives 2% loss in WAN mode to keep the comparison consistent.
    pbft_wan = await setup_cluster("PBFT", num_nodes=4, delay_ms=40.0, drop_rate=0.02)
    res_pbft_wan = await run_scenario("Microgrid WAN", "PBFT", pbft_wan, duration_test)
    results.append(res_pbft_wan)
    await stop_cluster(pbft_wan)

    # -----------------------------------------------------------------
    # SCENARIO 3: cyber attack against the leader or primary.
    # -----------------------------------------------------------------
    print("\nScenario 3: Cyber attack against leader or primary")
    
    print("  ... Running attack test on RAFT")
    raft_hack = await setup_cluster("RAFT", num_nodes=5, delay_ms=5.0, drop_rate=0.0, byzantine_ids=[0])
    res_raft_hack = await run_scenario("Cyber Attack", "RAFT", raft_hack, duration_test)
    results.append(res_raft_hack)
    await stop_cluster(raft_hack)

    print("  ... Running attack test on PBFT")
    pbft_hack = await setup_cluster("PBFT", num_nodes=4, delay_ms=5.0, drop_rate=0.0, byzantine_ids=[0])
    res_pbft_hack = await run_scenario("Cyber Attack", "PBFT", pbft_hack, duration_test)
    results.append(res_pbft_hack)
    await stop_cluster(pbft_hack)

    # -----------------------------------------------------------------
    # Final report output.
    # -----------------------------------------------------------------
    print(f"\n{'='*65}")
    print(f"{'Scenario':<18} | {'Protocol':<10} | {'Validated Commits':<18} | {'TPS':<6}")
    print(f"{'-'*65}")
    for r in results:
        print(f"{r['scenario']:<18} | {r['protocol']:<10} | {r['commits']:<18} | {r['tps']:<6}")
    print(f"{'='*65}\n")

if __name__ == "__main__":
    asyncio.run(main())
