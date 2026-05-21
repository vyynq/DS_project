import asyncio
import logging
import random
import time
from pbft.node import PBFTNode, PBFTNodeState
from raft.node import RaftNode, NodeState

# Configuration des logs : on met en WARNING pour ne pas polluer le tableau final
logging.basicConfig(level=logging.WARNING, format='%(asctime)s  %(levelname)-8s %(message)s')
logger = logging.getLogger(__name__)

# =========================================================================
# EXTENSION BYZANTINE POUR RAFT
# =========================================================================
class ByzantineRaftNode(RaftNode):
    """
    Simule un leader Raft corrompu (hacké) qui modifie les données 
    reçues du client avant de les répliquer aux followers.
    """
    async def client_request(self, command, value):
        valeur_corrompue = "HACKED_9999"
        print(f"  [ATTACK] Leader Raft piraté ! Modifie discrètement {value} en {valeur_corrompue}")
        # Il force la réplication de la fausse donnée
        return await super().client_request(command, valeur_corrompue)

# =========================================================================
# MOTEUR DE BENCHMARK
# =========================================================================
async def run_scenario(scenario_name, protocol_name, nodes, duration=8):
    """
    Simule une charge de requêtes client ("record_energy") pendant une durée donnée
    et comptabilise les performances.
    """
    start_time = time.time()
    requests_sent = 0
    successful_commits = 0

    while time.time() - start_time < duration:
        # 1. Trouver le leader ou le primary actuel
        leader_node = None
        if protocol_name == "PBFT":
            leader_node = next((n for n in nodes if n.is_primary() and n.state != PBFTNodeState.DEAD), None)
        else: # RAFT
            leader_node = next((n for n in nodes if n.state == NodeState.LEADER), None)

        # 2. Envoyer une requête si un leader est disponible
        if leader_node:
            val = f"energy_{random.randint(100, 500)}"
            task = asyncio.create_task(leader_node.client_request("set", val))
            
            try:
                # Timeout court pour ne pas bloquer la boucle du benchmark
                res = await asyncio.wait_for(task, timeout=1.5)
                if res and res.get("success"):
                    successful_commits += 1
            except asyncio.TimeoutError:
                pass
            
            requests_sent += 1

        await asyncio.sleep(0.05) # Intervalle entre requêtes (~20 req/s)

    tps = successful_commits / duration
    return {
        "scenario": scenario_name,
        "protocol": protocol_name,
        "commits": successful_commits,
        "tps": round(tps, 1)
    }

async def setup_cluster(protocol, num_nodes, delay_ms, drop_rate, byzantine_ids=[]):
    """Initialise un cluster avec les paramètres de chaos réseau."""
    nodes = []
    if protocol == "PBFT":
        # Utilise l'initialisation de PBFTNode détectant les noeuds byzantins
        nodes = [PBFTNode(node_id=i, is_byzantine=(i in byzantine_ids)) for i in range(num_nodes)]
    elif protocol == "RAFT":
        if byzantine_ids:
            # Le nœud 0 est instancié comme piraté, les autres sont normaux
            nodes = [ByzantineRaftNode(node_id=0, election_timeout_range=(150, 300))] + \
                    [RaftNode(node_id=i, election_timeout_range=(150, 300)) for i in range(1, num_nodes)]
        else:
            nodes = [RaftNode(node_id=i, election_timeout_range=(150, 300)) for i in range(num_nodes)]
    
    # Configuration des interconnexions et du Chaos
    for node in nodes:
        node.peers = nodes
        node.chaos_delay_ms = delay_ms
        node.chaos_drop_rate = drop_rate

    # Démarrage
    for node in nodes:
        await node.start()
        
    await asyncio.sleep(1.5) # Temps de stabilisation / élection initiale
    
    # Forcer le nœud piraté à être leader pour la démonstration du scénario 3
    if protocol == "RAFT" and byzantine_ids:
        nodes[0].state = NodeState.LEADER

    return nodes

async def stop_cluster(nodes):
    for node in nodes:
        await node.stop()

# =========================================================================
# EXÉCUTION DES SCÉNARIOS
# =========================================================================
async def main():
    print(f"\n{'='*65}")
    print("  LANCEMENT DU BENCHMARK COMPARATIF INDUSTRIEL (SMART GRID)")
    print(f"{'='*65}\n")
    
    results = []
    duration_test = 6 # Secondes par test

    # -----------------------------------------------------------------
    # SCÉNARIO 1 : Réseau LAN Interne (Confiance totale, Réseau rapide)
    # -----------------------------------------------------------------
    print("Scénario 1 : LAN Interne de la Centrale (Latence: 5ms, Pertes: 0%)")
    
    raft_lan = await setup_cluster("RAFT", num_nodes=5, delay_ms=5.0, drop_rate=0.0)
    res_raft_lan = await run_scenario("LAN Interne", "RAFT", raft_lan, duration_test)
    results.append(res_raft_lan)
    await stop_cluster(raft_lan)

    pbft_lan = await setup_cluster("PBFT", num_nodes=4, delay_ms=5.0, drop_rate=0.0)
    res_pbft_lan = await run_scenario("LAN Interne", "PBFT", pbft_lan, duration_test)
    results.append(res_pbft_lan)
    await stop_cluster(pbft_lan)

    # -----------------------------------------------------------------
    # SCÉNARIO 2 : Microgrid WAN (Réseau instable, Voisinage P2P)
    # -----------------------------------------------------------------
    print("\nScénario 2 : Microgrid WAN Public (Latence: 40ms, Pertes: 2%)")
    
    raft_wan = await setup_cluster("RAFT", num_nodes=5, delay_ms=40.0, drop_rate=0.02)
    res_raft_wan = await run_scenario("Microgrid WAN", "RAFT", raft_wan, duration_test)
    results.append(res_raft_wan)
    await stop_cluster(raft_wan)

    # Pour PBFT en WAN, on met 2% de pertes pour lui laisser une chance de faire des commits
    pbft_wan = await setup_cluster("PBFT", num_nodes=4, delay_ms=40.0, drop_rate=0.02)
    res_pbft_wan = await run_scenario("Microgrid WAN", "PBFT", pbft_wan, duration_test)
    results.append(res_pbft_wan)
    await stop_cluster(pbft_wan)

    # -----------------------------------------------------------------
    # SCÉNARIO 3 : Cyber-attaque (Leader / Primary Piraté)
    # -----------------------------------------------------------------
    print("\nScénario 3 : Cyber-attaque (Leader / Primary Piraté)")
    
    print("  ... Test de l'attaque sur RAFT")
    raft_hack = await setup_cluster("RAFT", num_nodes=5, delay_ms=5.0, drop_rate=0.0, byzantine_ids=[0])
    res_raft_hack = await run_scenario("Cyber-Attaque", "RAFT", raft_hack, duration_test)
    results.append(res_raft_hack)
    await stop_cluster(raft_hack)

    print("  ... Test de l'attaque sur PBFT")
    pbft_hack = await setup_cluster("PBFT", num_nodes=4, delay_ms=5.0, drop_rate=0.0, byzantine_ids=[0])
    res_pbft_hack = await run_scenario("Cyber-Attaque", "PBFT", pbft_hack, duration_test)
    results.append(res_pbft_hack)
    await stop_cluster(pbft_hack)

    # -----------------------------------------------------------------
    # AFFICHAGE DU RAPPORT FINAL
    # -----------------------------------------------------------------
    print(f"\n{'='*65}")
    print(f"{'Scénario':<18} | {'Protocole':<10} | {'Commits Validés':<18} | {'TPS':<6}")
    print(f"{'-'*65}")
    for r in results:
        print(f"{r['scenario']:<18} | {r['protocol']:<10} | {r['commits']:<18} | {r['tps']:<6}")
    print(f"{'='*65}\n")

if __name__ == "__main__":
    asyncio.run(main())