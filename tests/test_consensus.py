"""
TESTS - Raft and PBFT
====================
Run with: pytest tests/test_consensus.py -v
"""

import pytest
import asyncio
from raft.node import RaftNode, NodeState
from pbft.node import PBFTNode


@pytest.mark.asyncio
async def test_raft_election():
    """A healthy Raft cluster should elect a leader."""
    nodes = [RaftNode(i) for i in range(3)]
    for n in nodes:
        n.peers = nodes

    # Start nodes before waiting for election timeouts.
    for n in nodes:
        await n.start()

    await asyncio.sleep(2)

    leaders = [n for n in nodes if n.state == NodeState.LEADER]
    assert len(leaders) >= 1, "Raft should have elected at least one leader"

    for n in nodes:
        await n.stop()


@pytest.mark.asyncio
async def test_raft_single_leader():
    """There should never be more than one leader at the same time."""
    nodes = [RaftNode(i) for i in range(5)]
    for n in nodes:
        n.peers = nodes
        await n.start()

    await asyncio.sleep(2)

    leaders = [n for n in nodes if n.state == NodeState.LEADER]
    assert len(leaders) <= 1, "There should not be more than one leader simultaneously"

    for n in nodes:
        await n.stop()


@pytest.mark.asyncio
async def test_pbft_consensus():
    """PBFT should validate a request with 4 nodes (f=1)."""
    nodes = [PBFTNode(i) for i in range(4)]
    for node in nodes:
        node.peers = nodes
        await node.start()

    await asyncio.sleep(0.2)

    # Send to the primary, node 0 by convention in view 0.
    result = await nodes[0].client_request("set", 100)

    assert result.get("success") is True, \
        f"PBFT should reach consensus. Result: {result}"

    for node in nodes:
        await node.stop()


@pytest.mark.asyncio
async def test_pbft_tolerates_one_byzantine():
    """PBFT with 4 nodes should tolerate 1 Byzantine node."""
    nodes = [PBFTNode(i) for i in range(4)]
    for node in nodes:
        node.peers = nodes
        await node.start()

    # Mark node 3 as Byzantine, leaving the primary normal.
    nodes[3].is_byzantine = True
    from pbft.node import PBFTNodeState
    nodes[3].state = PBFTNodeState.BYZANTINE

    await asyncio.sleep(0.2)

    result = await nodes[0].client_request("transfer", 500)

    assert result.get("success") is True, \
        f"PBFT should tolerate 1 Byzantine node. Result: {result}"

    for node in nodes:
        await node.stop()
