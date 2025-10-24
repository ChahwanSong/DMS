"""Tests for the agent CLI helpers."""
from __future__ import annotations

import argparse
import asyncio
import logging

import pytest

pytest.importorskip("yaml")

from control_plane import agent_cli
from dms_agent.config import AgentConfig, AgentNetworkConfig
from dms_master.models import Assignment, WorkerStatus


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def _make_config() -> AgentConfig:
    network = AgentNetworkConfig(
        control_plane_address="10.0.0.10",
        data_plane_endpoints=[
            "192.168.0.10",
            "192.168.0.11",
        ],
    )
    return AgentConfig(
        master_url="http://127.0.0.1:8000",
        worker_id="worker-a",
        network=network,
        storage_paths=["/mnt/source", "/mnt/destination"],
    )


def test_heartbeat_factory_tracks_status() -> None:
    config = _make_config()
    tracker = agent_cli.AgentStatusTracker()
    factory = agent_cli.build_heartbeat_factory(config, tracker)

    hb = factory()
    assert hb.status == WorkerStatus.IDLE
    assert len(hb.data_plane_endpoints) == 2
    assert hb.storage_paths == ["/mnt/source", "/mnt/destination"]

    tracker.status = WorkerStatus.TRANSFERRING
    hb2 = factory()
    assert hb2.status == WorkerStatus.TRANSFERRING


@pytest.fixture(scope="module")
async def test_assignment_handler_success() -> None:
    config = _make_config()
    tracker = agent_cli.AgentStatusTracker()

    async def runner(_: Assignment) -> None:
        await asyncio.sleep(0)

    handler = agent_cli.build_assignment_handler(config, tracker, transfer_runner=runner)
    assignment = Assignment(
        request_id="r1",
        worker_id="worker-a",
        source_path="/tmp/source",
        destination_path="/tmp/destination",
        chunk_offset=0,
        chunk_size=1024,
    )

    result = await handler(assignment)
    assert result.success is True
    assert tracker.status == WorkerStatus.IDLE


@pytest.fixture(scope="module")
async def test_assignment_handler_failure() -> None:
    config = _make_config()
    tracker = agent_cli.AgentStatusTracker()

    async def runner(_: Assignment) -> None:
        raise RuntimeError("boom")

    handler = agent_cli.build_assignment_handler(config, tracker, transfer_runner=runner)
    assignment = Assignment(
        request_id="r1",
        worker_id="worker-a",
        source_path="/tmp/source",
        destination_path="/tmp/destination",
        chunk_offset=0,
        chunk_size=1024,
    )

    result = await handler(assignment)
    assert result.success is False
    assert "boom" in result.message
    assert tracker.status == WorkerStatus.IDLE


@pytest.fixture(scope="module")
async def test_run_exits_with_message_on_communication_error(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    args = argparse.Namespace(
        config="example.yml",
        worker_id="worker-a",
        heartbeat_interval=5.0,
        log_level="INFO",
    )

    config = _make_config()
    monkeypatch.setattr(agent_cli, "load_agent_config", lambda *_: config)

    class DummyClient:
        pass

    dummy_client = DummyClient()

    def fake_client(*_args, **_kwargs):
        return dummy_client

    monkeypatch.setattr(agent_cli, "AgentClient", fake_client)

    async def fake_run_agent(*_args, **_kwargs):
        raise agent_cli.AgentCommunicationError(
            "Failed to communicate with master at http://master.local: boom"
        )

    monkeypatch.setattr(agent_cli, "run_agent", fake_run_agent)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(SystemExit) as excinfo:
            await agent_cli._run(args)

    assert excinfo.value.code == 1
    assert any("Failed to communicate with master" in message for message in caplog.messages)
