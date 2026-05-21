import asyncio
import logging
import random
import time
from pbft.node import PBFTNode, PBFTNodeState
from raft.node import RaftNode, NodeState # Basé sur tes imports dans demo.py

logging.basicConfig(level=logging.WARNING) # On réduit les logs pour la clarté du résultat final
logger = logging.getLogger(__name__)

async def run_scenario(scenario_name, protocol_name, nodes, duration, req_interval=0.05):
    """
    Simule une charge réseau (envoi continu de requêtes de 'production_energie').
    """
    start_time = time.time()
    requests_sent = 0
    successful_commits = 0
    total_latency = 0

    while time.time() - start_time < duration:
        # Trouver le leader/primary
        primary = None
        if protocol_name == "PBFT":
            primary = next((n for n in nodes if n.is_primary() and n.state != PBFTNodeState.DEAD), None)
        else: # RAFT
            primary = next((n for n in nodes if n.state == NodeState.LEADER), None)

        if primary:
            # Simulation d'une mesure de compteur électrique
            val = random.randint(100, 500)
            task = asyncio.create_task(primary.client_request("record_energy", val))
            
            # Attendre la réponse pour les stats (simplifié pour le benchmark)
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
    
    # Interconnexion et Chaos
    for node in nodes:
        node.peers = nodes
        # Utilisation des paramètres de chaos de ton node.py
        node.chaos_delay_ms = delay_ms
        node.chaos_drop_rate = drop_rate 

    for node in nodes:
        await node.start()
    await asyncio.sleep(1) # Stabilisation (élection Raft)
    return nodes

async def stop_cluster(nodes):
    for node in nodes:
        await node.stop()

async def main():
    print(f"\n{'='*60}")
    print("🌍 DÉMARRAGE DU BENCHMARK INDUSTRIEL : SMART GRID")
    print(f"{'='*60}\n")
    
    duration = 5 # Secondes par test
    results = []

    # ---------------------------------------------------------
    # SCÉNARIO 1 : Centrale Interne (LAN, Confiance, Rapide)
    # ---------------------------------------------------------
    print("▶ Scénario 1 : Réseau LAN Interne (0% perte, 5ms latence, 0 Byzantin)")
    print("  ... Test de Raft (En cours)")
    raft_lan = await setup_cluster("RAFT", 5, delay_ms=5.0, drop_rate=0.0)
    res_raft_lan = await run_scenario("LAN Interne", "RAFT", raft_lan, duration)
    results.append(res_raft_lan)
    await stop_cluster(raft_lan)

    print("  ... Test de PBFT (En cours)")
    pbft_lan = await setup_cluster("PBFT", 4, delay_ms=5.0, drop_rate=0.0)
    res_pbft_lan = await run_scenario("LAN Interne", "PBFT", pbft_lan, duration)
    results.append(res_pbft_lan)
    await stop_cluster(pbft_lan)

    # ---------------------------------------------------------
    # SCÉNARIO 2 : Marché P2P (WAN, Hostile, Pertes)
    # ---------------------------------------------------------
    print("\n▶ Scénario 2 : Microgrid WAN (5% perte, 40ms latence, 1 Byzantin)")
    # NB: On ne met pas Raft en "byzantin" car il n'est pas conçu pour ça, 
    # mais on applique la forte latence/perte pour voir l'impact sur les élections.
    print("  ... Test de Raft (En cours)")
    raft_wan = await setup_cluster("RAFT", 5, delay_ms=40.0, drop_rate=0.05)
    res_raft_wan = await run_scenario("Microgrid WAN", "RAFT", raft_wan, duration)
    results.append(res_raft_wan)
    await stop_cluster(raft_wan)

    print("  ... Test de PBFT (En cours)")
    pbft_wan = await setup_cluster("PBFT", 4, delay_ms=40.0, drop_rate=0.05, byzantine_ids=[3])
    res_pbft_wan = await run_scenario("Microgrid WAN", "PBFT", pbft_wan, duration)
    results.append(res_pbft_wan)
    await stop_cluster(pbft_wan)

    # ---------------------------------------------------------
    # AFFICHAGE DES RÉSULTATS
    # ---------------------------------------------------------
    print(f"\n{'='*75}")
    print(f"{'Scénario':<15} | {'Protocole':<10} | {'Commits':<8} | {'TPS':<8} | {'Latence (ms)':<15}")
    print(f"{'-'*75}")
    for r in results:
        print(f"{r['scenario']:<15} | {r['protocol']:<10} | {r['commits']:<8} | {r['tps']:<8} | {r['avg_latency_ms']:<15}")
    print(f"{'='*75}")

if __name__ == "__main__":
    asyncio.run(main())