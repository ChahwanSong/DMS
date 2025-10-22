"""Runtime behaviour tests for the agent client loop."""
from __future__ import annotations

import asyncio

import httpx
import pytest

from dms_agent import AgentCommunicationError, run_agent
from dms_master.models import WorkerHeartbeat, WorkerStatus


class _FailingClient:
    """Minimal client stub that fails all HTTP interactions."""

    def __init__(self) -> None:
        self.master_url = "http://master.local"
        self.closed = False
        self._request = httpx.Request("POST", f"{self.master_url}/workers/heartbeat")

    async def send_heartbeat(self, heartbeat: WorkerHeartbeat) -> None:  # noqa: ARG002
        raise httpx.ConnectError("boom", request=self._request)

    async def poll_assignment(self, interval: float = 1.0) -> None:  # noqa: ARG002
        pytest.fail("poll_assignment should not be called after send_heartbeat failure")

    async def report_result(self, result) -> None:  # noqa: ARG002
        pytest.fail("report_result should not be called after send_heartbeat failure")

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_run_agent_raises_communication_error_and_closes_client() -> None:
    heartbeat = WorkerHeartbeat(worker_id="worker-a", status=WorkerStatus.IDLE)

    def heartbeat_factory() -> WorkerHeartbeat:
        return heartbeat

    async def assignment_handler(_):  # pragma: no cover - not expected to run
        await asyncio.sleep(0)

    client = _FailingClient()

    with pytest.raises(AgentCommunicationError) as excinfo:
        await run_agent(client, heartbeat_factory, assignment_handler)

    assert "Failed to communicate with master" in str(excinfo.value)
    assert "boom" in str(excinfo.value)
    assert client.closed is True


