import asyncio
import logging
import random
from pbft.node import PBFTNode
# Importe ton RaftNode ici si nécessaire
# from raft.node import RaftNode 

# Configuration des logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s  %(levelname)-8s %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

async def run_benchmark(name, nodes, duration=10):
    logger.info(f"🔵 [{name.upper()}] Starting Realistic Benchmark ({duration}s)...")
    start_time = asyncio.get_event_loop().time()
    count = 0
    
    # On choisit le premier nœud comme point d'entrée
    primary = nodes[0] 

    while asyncio.get_event_loop().time() - start_time < duration:
        # Envoi d'une requête asynchrone
        # On simule un client qui envoie des requêtes à intervalle régulier
        task = asyncio.create_task(primary.client_request("set", random.randint(1, 100)))
        
        # On n'attend pas forcément la fin pour en envoyer une autre (concurrence)
        await asyncio.sleep(0.05) # ~20 requêtes par seconde envoyées
        count += 1

    logger.info(f"🏁 [{name.upper()}] Benchmark finished.")

async def main():
    # --- CONFIGURATION PBFT ---
    nb_pbft = 4
    pbft_nodes = [PBFTNode(i) for i in range(nb_pbft)]
    
    # Interconnexion des nœuds
    for node in pbft_nodes:
        node.peers = pbft_nodes
        
        # --- ACTIVATION DU CHAOS ENGINE (Simulation Réelle) ---
        node.chaos_delay_ms = 30.0   # 30ms de latence de base
        node.chaos_drop_rate = 0.03  # 3% de perte de paquets
        # Note : Le jitter est géré à l'intérieur de node.py via random.uniform

    # Démarrage des nœuds
    for node in pbft_nodes:
        await node.start()

    # Petit temps de stabilisation
    await asyncio.sleep(1)

    # --- LANCEMENT DU BENCHMARK ---
    try:
        await run_benchmark("pbft_realistic", pbft_nodes, duration=15)
    except KeyboardInterrupt:
        pass
    finally:
        # Affichage des résultats finaux
        print("\n" + "="*40)
        print("📊 RÉSULTATS DE LA SIMULATION RÉELLE")
        print("="*40)
        for node in pbft_nodes:
            status = node.get_status()
            metrics = status.get("metrics", {})
            print(f"Node {node.node_id} ({status['state']}):")
            print(f"  - Commits validés: {status['commits']}")
            print(f"  - Requêtes gérées (Primary): {metrics.get('requests_handled', 0)}")
        print("="*40)

        # Arrêt des nœuds
        for node in pbft_nodes:
            await node.stop()

if __name__ == "__main__":
    asyncio.run(main())