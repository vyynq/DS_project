import asyncio
import logging
import random
import time
from pbft.node import PBFTNode, PBFTNodeState
from raft.node import RaftNode, NodeState  # Uses the same imports as demo.py.

logging.basicConfig(level=logging.WARNING)  # Keep benchmark output focused on final results.
logger = logging.getLogger(__name__)

async def run_scenario(scenario_name, protocol_name, nodes, duration, req_interval=0.05):
    """
    Simulate network load with continuous energy-production requests.
    """
    start_time = time.time()
    requests_sent = 0
    successful_commits = 0
    total_latency = 0

    while time.time() - start_time < duration:
        # Find the current leader or primary.
        primary = None
        if protocol_name == "PBFT":
            primary = next((n for n in nodes if n.is_primary() and n.state != PBFTNodeState.DEAD), None)
        else:  # RAFT
            primary = next((n for n in nodes if n.state == NodeState.LEADER), None)

        if primary:
            # Simulate a smart meter reading.
            val = random.randint(100, 500)
            task = asyncio.create_task(primary.client_request("record_energy", val))
            
            # Wait for the response and record benchmark statistics.
            try:
                res = await asyncio.wait_for(task, timeout=2.0)
                if res and res.get("success"):
                    successful_commits += 1
                    total_latency += res.get("latency_ms", 0)
            except asyncio.TimeoutError:
                pass
            
            requests_sent += 1

        await asyncio.sleep(req_interval)

    avg_latency = (total_latency / successful_commits) if successful_commits > 0 else 0
    tps = successful_commits / duration
    
    return {
        "scenario": scenario_name,
        "protocol": protocol_name,
        "sent": requests_sent,
        "commits": successful_commits,
        "tps": round(tps, 2),
        "avg_latency_ms": round(avg_latency, 2)
    }

async def setup_cluster(protocol, num_nodes, delay_ms, drop_rate, byzantine_ids=[]):
    nodes = []
    if protocol == "PBFT":
        nodes = [PBFTNode(i, is_byzantine=(i in byzantine_ids)) for i in range(num_nodes)]
    elif protocol == "RAFT":
        nodes = [RaftNode(node_id=i, election_timeout_range=(150, 300)) for i in range(num_nodes)]
    
    # Connect peers and apply chaos parameters.
    for node in nodes:
        node.peers = nodes
        # Apply the chaos parameters supported by each node implementation.
        node.chaos_delay_ms = delay_ms
        node.chaos_drop_rate = drop_rate 

    for node in nodes:
        await node.start()
    await asyncio.sleep(1)  # Stabilization period for Raft leader election.
    return nodes

async def stop_cluster(nodes):
    for node in nodes:
        await node.stop()

async def main():
    print(f"\n{'='*60}")
    print("STARTING INDUSTRIAL BENCHMARK: SMART GRID")
    print(f"{'='*60}\n")
    
    duration = 5  # Seconds per test.
    results = []

    # ---------------------------------------------------------
    # SCENARIO 1: Internal plant network (LAN, trusted, fast).
    # ---------------------------------------------------------
    print("Scenario 1: Internal LAN network (0% loss, 5 ms latency, 0 Byzantine nodes)")
    print("  ... Running Raft test")
    raft_lan = await setup_cluster("RAFT", 5, delay_ms=5.0, drop_rate=0.0)
    res_raft_lan = await run_scenario("Internal LAN", "RAFT", raft_lan, duration)
    results.append(res_raft_lan)
    await stop_cluster(raft_lan)

    print("  ... Running PBFT test")
    pbft_lan = await setup_cluster("PBFT", 4, delay_ms=5.0, drop_rate=0.0)
    res_pbft_lan = await run_scenario("Internal LAN", "PBFT", pbft_lan, duration)
    results.append(res_pbft_lan)
    await stop_cluster(pbft_lan)

    # ---------------------------------------------------------
    # SCENARIO 2: Peer-to-peer market network (WAN, hostile, lossy).
    # ---------------------------------------------------------
    print("\nScenario 2: Microgrid WAN (5% loss, 40 ms latency, 1 Byzantine node)")
    # Raft is not modeled as Byzantine because the protocol is not designed for that threat model.
    # Higher latency and packet loss are still applied to measure their impact on elections.
    print("  ... Running Raft test")
    raft_wan = await setup_cluster("RAFT", 5, delay_ms=40.0, drop_rate=0.05)
    res_raft_wan = await run_scenario("Microgrid WAN", "RAFT", raft_wan, duration)
    results.append(res_raft_wan)
    await stop_cluster(raft_wan)

    print("  ... Running PBFT test")
    pbft_wan = await setup_cluster("PBFT", 4, delay_ms=40.0, drop_rate=0.05, byzantine_ids=[3])
    res_pbft_wan = await run_scenario("Microgrid WAN", "PBFT", pbft_wan, duration)
    results.append(res_pbft_wan)
    await stop_cluster(pbft_wan)

    # ---------------------------------------------------------
    # Results output.
    # ---------------------------------------------------------
    print(f"\n{'='*75}")
    print(f"{'Scenario':<15} | {'Protocol':<10} | {'Commits':<8} | {'TPS':<8} | {'Latency (ms)':<15}")
    print(f"{'-'*75}")
    for r in results:
        print(f"{r['scenario']:<15} | {r['protocol']:<10} | {r['commits']:<8} | {r['tps']:<8} | {r['avg_latency_ms']:<15}")
    print(f"{'='*75}")

if __name__ == "__main__":
    asyncio.run(main())
