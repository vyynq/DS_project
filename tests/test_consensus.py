import pytest
import asyncio
from raft.node import RaftNode
from pbft.node import PBFTNode

@pytest.mark.asyncio
async def test_raft_election():
    """Vérifie qu'un leader est élu dans un cluster Raft sain."""
    nodes = [RaftNode(i) for i in range(3)]
    for n in nodes: n.peers = [p for p in nodes if p.node_id != n.node_id]
    
    # On laisse le temps aux timeouts aléatoires de déclencher une élection
    await asyncio.sleep(2)
    
    leaders = [n for n in nodes if n.state.value == "leader"]
    assert len(leaders) >= 1, "Raft devrait avoir élu au moins un leader"

@pytest.mark.asyncio
async def test_pbft_consensus():
    """Vérifie que PBFT valide une requête avec 4 nœuds (f=1)."""
    nodes = [PBFTNode(i) for i in range(4)]
    # Simulation d'une requête client sur le primary (node 0)
    success = await nodes[0].client_request("set", 100)
    assert success is True, "PBFT devrait atteindre un consensus sur la valeur"