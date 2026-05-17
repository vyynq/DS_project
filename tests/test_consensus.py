"""
TESTS — Raft et PBFT
====================
Lance avec : pytest tests/test_consensus.py -v
"""

import pytest
import asyncio
from raft.node import RaftNode, NodeState
from pbft.node import PBFTNode


@pytest.mark.asyncio
async def test_raft_election():
    """Un leader doit etre elu dans un cluster Raft sain."""
    nodes = [RaftNode(i) for i in range(3)]
    for n in nodes:
        n.peers = nodes

    # Demarrer les noeuds avant d'attendre les timeouts
    for n in nodes:
        await n.start()

    await asyncio.sleep(2)

    leaders = [n for n in nodes if n.state == NodeState.LEADER]
    assert len(leaders) >= 1, "Raft devrait avoir elu au moins un leader"

    for n in nodes:
        await n.stop()


@pytest.mark.asyncio
async def test_raft_single_leader():
    """Il ne doit jamais y avoir plus d'un leader au meme term."""
    nodes = [RaftNode(i) for i in range(5)]
    for n in nodes:
        n.peers = nodes
        await n.start()

    await asyncio.sleep(2)

    leaders = [n for n in nodes if n.state == NodeState.LEADER]
    assert len(leaders) <= 1, "Il ne doit pas y avoir plus d'un leader simultanement"

    for n in nodes:
        await n.stop()


@pytest.mark.asyncio
async def test_pbft_consensus():
    """PBFT doit valider une requete avec 4 noeuds (f=1)."""
    nodes = [PBFTNode(i) for i in range(4)]
    for node in nodes:
        node.peers = nodes
        await node.start()

    await asyncio.sleep(0.2)

    # Envoyer au primary (node 0 par convention, view=0)
    result = await nodes[0].client_request("set", 100)

    assert result.get("success") is True, \
        f"PBFT devrait atteindre un consensus. Resultat : {result}"

    for node in nodes:
        await node.stop()


@pytest.mark.asyncio
async def test_pbft_tolerates_one_byzantine():
    """PBFT avec 4 noeuds doit resister a 1 noeud byzantin."""
    nodes = [PBFTNode(i) for i in range(4)]
    for node in nodes:
        node.peers = nodes
        await node.start()

    # Rendre le noeud 3 byzantin (pas le primary)
    nodes[3].is_byzantine = True
    from pbft.node import PBFTNodeState
    nodes[3].state = PBFTNodeState.BYZANTINE

    await asyncio.sleep(0.2)

    result = await nodes[0].client_request("transfer", 500)

    assert result.get("success") is True, \
        f"PBFT devrait tenir avec 1 byzantin. Resultat : {result}"

    for node in nodes:
        await node.stop()