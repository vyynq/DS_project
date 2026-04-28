"""
BENCHMARK RUNNER 📊
===================
Mesure automatisée des performances de Raft et PBFT
sous différentes conditions. Génère des données CSV
pour SageMath.

Tests disponibles :
  - baseline          : performances normales
  - fault_tolerance   : performances avec f pannes
  - recovery_time     : temps de recovery après une panne
  - latency_vs_nodes  : latence en fonction du nombre de nœuds
  - throughput_vs_load: throughput en fonction de la charge
  - byzantine_impact  : impact des nœuds byzantins sur PBFT
"""

import asyncio
import csv
import json
import logging
import os
import time
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class BenchmarkResult:
    test_name: str
    algo: str
    params: dict
    throughput_rps: float
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    error_rate: float
    duration_s: float
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "test": self.test_name,
            "algo": self.algo,
            **self.params,
            "throughput_rps": round(self.throughput_rps, 2),
            "avg_latency_ms": round(self.avg_latency_ms, 2),
            "p50_latency_ms": round(self.p50_latency_ms, 2),
            "p95_latency_ms": round(self.p95_latency_ms, 2),
            "p99_latency_ms": round(self.p99_latency_ms, 2),
            "error_rate": round(self.error_rate, 4),
        }


class BenchmarkRunner:
    """
    Exécute tous les benchmarks et exporte les résultats en CSV.

    Usage:
        runner = BenchmarkRunner(raft_cluster, pbft_cluster, chaos_engine)
        await runner.run_all()
        runner.export_csv("results/")
    """

    def __init__(self, raft_cluster, pbft_cluster, chaos_engine, output_dir="analysis"):
        self.raft = raft_cluster
        self.pbft = pbft_cluster
        self.chaos = chaos_engine
        self.output_dir = output_dir
        self.results: list[BenchmarkResult] = []
        os.makedirs(output_dir, exist_ok=True)

    # ─────────────────────────────────────────────
    # TESTS
    # ─────────────────────────────────────────────

    async def run_all(self):
        """Exécute tous les benchmarks dans l'ordre."""
        logger.info("📊 Starting full benchmark suite...")

        await self.test_baseline()
        await asyncio.sleep(0.5)

        await self.test_fault_tolerance()
        await asyncio.sleep(0.5)

        await self.test_recovery_time()
        await asyncio.sleep(0.5)

        await self.test_throughput_vs_load()
        await asyncio.sleep(0.5)

        await self.test_byzantine_impact()

        self.export_csv()
        logger.info("📊 Benchmark suite complete. Results exported.")
        return self.results

    async def test_baseline(self, num_requests: int = 100, rps: int = 20):
        """
        Test de référence : performances sans aucune panne.
        """
        logger.info(" [Baseline] Starting...")

        for algo_name, cluster in [("raft", self.raft), ("pbft", self.pbft)]:
            result = await self._run_load(algo_name, cluster, num_requests, rps)
            br = self._make_result("baseline", algo_name, {}, result)
            self.results.append(br)
            logger.info(f"  {algo_name}: {br.throughput_rps} rps, {br.avg_latency_ms}ms avg")

    async def test_fault_tolerance(self):
        """
        Mesure les performances avec f=0, 1, 2 pannes simultanées.
        """
        logger.info(" [Fault Tolerance] Starting...")

        # Raft : peut tolérer (n-1)//2 pannes
        for f in range(3):
            # Crasher f nœuds (pas le leader)
            crashed = []
            raft_nodes = list(self.raft.values())
            leader = self.chaos._find_raft_leader()

            for node in raft_nodes:
                if len(crashed) >= f:
                    break
                if node != leader:
                    await self.chaos.crash_node("raft", node.node_id)
                    crashed.append(node.node_id)

            await asyncio.sleep(0.3)
            result = await self._run_load("raft", self.raft, 50, 10)
            br = self._make_result("fault_tolerance", "raft", {"f": f}, result)
            self.results.append(br)
            logger.info(f"  Raft f={f}: {br.throughput_rps} rps, error_rate={br.error_rate}")

            # Revive
            for nid in crashed:
                await self.chaos.revive_node("raft", nid)
            await asyncio.sleep(0.5)

        # PBFT : peut tolérer (n-1)//3 pannes
        for f in range(2):
            crashed = []
            pbft_nodes = list(self.pbft.values())
            primary = self.chaos._find_pbft_primary()

            for node in pbft_nodes:
                if len(crashed) >= f:
                    break
                if node != primary:
                    await self.chaos.crash_node("pbft", node.node_id)
                    crashed.append(node.node_id)

            await asyncio.sleep(0.3)
            result = await self._run_load("pbft", self.pbft, 30, 5)
            br = self._make_result("fault_tolerance", "pbft", {"f": f}, result)
            self.results.append(br)

            for nid in crashed:
                await self.chaos.revive_node("pbft", nid)
            await asyncio.sleep(0.5)

    async def test_recovery_time(self):
        """
        Mesure le temps de recovery après une panne du leader/primary.
        C'est une métrique clé souvent absente des implémentations naïves.
        """
        logger.info("  [Recovery Time] Starting...")

        # Raft recovery
        leader = self.chaos._find_raft_leader()
        if leader:
            kill_time = time.time()
            await self.chaos.crash_node("raft", leader.node_id)

            # Attendre qu'un nouveau leader soit élu
            recovery_time = None
            for _ in range(100):
                await asyncio.sleep(0.05)
                new_leader = self.chaos._find_raft_leader()
                if new_leader and new_leader.node_id != leader.node_id:
                    recovery_time = (time.time() - kill_time) * 1000
                    break

            br = BenchmarkResult(
                test_name="recovery_time", algo="raft",
                params={"event": "leader_crash"},
                throughput_rps=0,
                avg_latency_ms=recovery_time or 5000,
                p50_latency_ms=0, p95_latency_ms=0, p99_latency_ms=0,
                error_rate=0, duration_s=recovery_time / 1000 if recovery_time else 5
            )
            self.results.append(br)
            logger.info(f"  Raft recovery: {recovery_time:.0f}ms")
            await self.chaos.revive_node("raft", leader.node_id)

        # PBFT recovery (view-change)
        primary = self.chaos._find_pbft_primary()
        if primary:
            kill_time = time.time()
            await self.chaos.crash_node("pbft", primary.node_id)

            recovery_time = None
            old_view = primary.view
            for _ in range(100):
                await asyncio.sleep(0.1)
                for node in self.pbft.values():
                    if node.node_id != primary.node_id and node.view > old_view:
                        recovery_time = (time.time() - kill_time) * 1000
                        break
                if recovery_time:
                    break

            br = BenchmarkResult(
                test_name="recovery_time", algo="pbft",
                params={"event": "primary_crash"},
                throughput_rps=0,
                avg_latency_ms=recovery_time or 5000,
                p50_latency_ms=0, p95_latency_ms=0, p99_latency_ms=0,
                error_rate=0, duration_s=recovery_time / 1000 if recovery_time else 5
            )
            self.results.append(br)
            logger.info(f"  PBFT recovery (view-change): {recovery_time:.0f}ms")
            await self.chaos.revive_node("pbft", primary.node_id)

    async def test_throughput_vs_load(self):
        """
        Courbe throughput en fonction du nombre de requêtes/s.
        Permet d'identifier le point de saturation.
        """
        logger.info(" [Throughput vs Load] Starting...")

        for rps in [5, 10, 20, 50, 100]:
            for algo_name, cluster in [("raft", self.raft), ("pbft", self.pbft)]:
                result = await self._run_load(algo_name, cluster, rps * 3, rps)
                br = self._make_result("throughput_vs_load", algo_name, {"target_rps": rps}, result)
                self.results.append(br)
                logger.info(f"  {algo_name} @ {rps} rps → actual {br.throughput_rps} rps")

    async def test_byzantine_impact(self):
        """
        Mesure l'impact des nœuds byzantins sur PBFT.
        Compare : 0 byzantin, 1 byzantin (toléré), 2 byzantins (limite dépassée).
        """
        logger.info(" [Byzantine Impact] Starting...")

        pbft_nodes = list(self.pbft.values())

        for nb_byz in [0, 1]:
            # Activer nb_byz nœuds byzantins
            byzantine_ids = []
            primary_id = self.chaos._find_pbft_primary()

            for node in pbft_nodes:
                if len(byzantine_ids) >= nb_byz:
                    break
                if primary_id and node.node_id != primary_id.node_id:
                    await self.chaos.set_byzantine(node.node_id)
                    byzantine_ids.append(node.node_id)

            await asyncio.sleep(0.2)
            result = await self._run_load("pbft", self.pbft, 30, 5)
            br = self._make_result("byzantine_impact", "pbft",
                                   {"byzantine_nodes": nb_byz}, result)
            self.results.append(br)
            logger.info(f"  PBFT with {nb_byz} byzantine: {br.throughput_rps} rps, "
                        f"errors={br.error_rate}")

            # Guérir
            for nid in byzantine_ids:
                await self.chaos.cure_byzantine(nid)
            await asyncio.sleep(0.3)

    # ─────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────

    async def _run_load(self, algo: str, cluster: dict, num_requests: int,
                        rps: int) -> dict:
        """Envoie num_requests à rps req/s. Retourne les stats."""
        from raft.node import NodeState
        from pbft.node import PBFTNodeState

        latencies = []
        errors = 0
        interval = 1.0 / rps
        start = time.time()

        for i in range(num_requests):
            # Trouver le leader/primary
            leader = None
            if algo == "raft":
                leader = self.chaos._find_raft_leader()
            elif algo == "pbft":
                leader = self.chaos._find_pbft_primary()

            if not leader:
                errors += 1
                await asyncio.sleep(interval)
                continue

            t0 = time.time()
            result = await leader.client_request("set", i)
            latency = (time.time() - t0) * 1000

            if result.get("success"):
                latencies.append(latency)
            else:
                errors += 1

            await asyncio.sleep(max(0, interval - (time.time() - t0)))

        duration = time.time() - start
        return {
            "latencies": latencies,
            "errors": errors,
            "num_requests": num_requests,
            "duration_s": duration,
        }

    def _make_result(self, test: str, algo: str, params: dict, raw: dict) -> BenchmarkResult:
        lats = sorted(raw["latencies"])
        n = len(lats)

        def percentile(p):
            if not lats:
                return 0
            idx = int(n * p / 100)
            return lats[min(idx, n - 1)]

        total = raw["num_requests"]
        errors = raw["errors"]
        duration = raw["duration_s"]

        return BenchmarkResult(
            test_name=test, algo=algo, params=params,
            throughput_rps=(total - errors) / duration if duration > 0 else 0,
            avg_latency_ms=sum(lats) / n if lats else 0,
            p50_latency_ms=percentile(50),
            p95_latency_ms=percentile(95),
            p99_latency_ms=percentile(99),
            error_rate=errors / total if total > 0 else 0,
            duration_s=duration
        )

    def export_csv(self, path="analysis"):
        """Exporte les résultats au format CSV de manière robuste."""
        import os
        import csv
        
        # 1. Créer le dossier s'il n'existe pas
        os.makedirs(path, exist_ok=True)
        
        if not self.results:
            logger.warning("No results to export to CSV.")
            return
        
        # 2. Déterminer dynamiquement tous les champs (fieldnames)
        # On regarde tous les dictionnaires pour ne rater aucune clé (comme 'f')
        all_keys = set()
        dict_results = []
        for r in self.results:
            d = r.to_dict()
            all_keys.update(d.keys())
            dict_results.append(d)
        
        fieldnames = sorted(list(all_keys))
        global_path = os.path.join(path, "results.csv")

        # 3. Écriture du fichier global
        try:
            with open(global_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for row in dict_results:
                    writer.writerow(row)
            logger.info(f" Results exported to {global_path}")
        except Exception as e:
            logger.error(f"Failed to export CSV: {e}")
            
        return global_path

    def print_summary(self):
        """Affiche un résumé lisible dans le terminal."""
        print("\n" + "="*60)
        print("BENCHMARK RESULTS SUMMARY")
        print("="*60)
        for r in self.results:
            print(f"\n[{r.test_name}] {r.algo.upper()} | params={r.params}")
            print(f"  Throughput : {r.throughput_rps:.1f} rps")
            print(f"  Latency    : avg={r.avg_latency_ms:.1f}ms  "
                  f"p95={r.p95_latency_ms:.1f}ms  p99={r.p99_latency_ms:.1f}ms")
            print(f"  Error rate : {r.error_rate*100:.1f}%")
        print("="*60 + "\n")