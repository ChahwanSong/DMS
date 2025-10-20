"""Tests for the agent CLI helpers."""
from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("yaml")

from control_plane import agent_cli
from dms_agent.config import AgentConfig, AgentDataPlaneEndpoint, AgentNetworkConfig
from dms_master.models import Assignment, WorkerStatus


def _make_config() -> AgentConfig:
    network = AgentNetworkConfig(
        control_plane_iface="eth0",
        control_plane_address="10.0.0.10",
        data_plane_endpoints=[
            AgentDataPlaneEndpoint(iface="ib0", address="192.168.0.10"),
            AgentDataPlaneEndpoint(iface="ib1", address="192.168.0.11"),
        ],
    )
    return AgentConfig(master_url="http://127.0.0.1:8000", worker_id="worker-a", network=network)


def test_heartbeat_factory_tracks_status() -> None:
    config = _make_config()
    tracker = agent_cli.AgentStatusTracker()
    factory = agent_cli.build_heartbeat_factory(config, tracker, free_bytes=1234)

    hb = factory()
    assert hb.status == WorkerStatus.IDLE
    assert len(hb.data_plane_endpoints) == 2

    tracker.status = WorkerStatus.TRANSFERRING
    hb2 = factory()
    assert hb2.status == WorkerStatus.TRANSFERRING


@pytest.mark.asyncio
async def test_assignment_handler_success() -> None:
    config = _make_config()
    tracker = agent_cli.AgentStatusTracker()

    async def runner(_: Assignment) -> None:
        await asyncio.sleep(0)

    handler = agent_cli.build_assignment_handler(config, tracker, transfer_runner=runner)
    assignment = Assignment(
        request_id="r1",
        worker_id="worker-a",
        file_path="/tmp/file",
        chunk_offset=0,
        chunk_size=1024,
        data_plane_iface="ib0",
        data_plane_address="192.168.0.10",
    )

    result = await handler(assignment)
    assert result.success is True
    assert tracker.status == WorkerStatus.IDLE


@pytest.mark.asyncio
async def test_assignment_handler_failure() -> None:
    config = _make_config()
    tracker = agent_cli.AgentStatusTracker()

    async def runner(_: Assignment) -> None:
        raise RuntimeError("boom")

    handler = agent_cli.build_assignment_handler(config, tracker, transfer_runner=runner)
    assignment = Assignment(
        request_id="r1",
        worker_id="worker-a",
        file_path="/tmp/file",
        chunk_offset=0,
        chunk_size=1024,
        data_plane_iface="ib1",
        data_plane_address="192.168.0.11",
    )

    result = await handler(assignment)
    assert result.success is False
    assert "boom" in result.message
    assert tracker.status == WorkerStatus.IDLE
