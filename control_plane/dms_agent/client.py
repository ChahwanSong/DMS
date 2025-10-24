"""Async worker agent client interacting with the master."""

from __future__ import annotations

import asyncio
from datetime import datetime, UTC
from enum import Enum
from typing import Any, Optional

import httpx

from dms_master.models import Assignment, SyncResult, WorkerHeartbeat


def _jsonable_payload(model: Any) -> Any:
    """Convert Pydantic-like models into JSON-serialisable payloads."""

    def _convert(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, Enum):
            return value.value
        if hasattr(value, "model_dump"):
            return _convert(value.model_dump())
        if isinstance(value, dict):
            return {key: _convert(inner) for key, inner in value.items()}
        if isinstance(value, (list, tuple)):
            return [_convert(inner) for inner in value]
        return value

    return _convert(model)


class AgentCommunicationError(RuntimeError):
    """Raised when the agent cannot communicate with the master."""


class AgentClient:
    def __init__(
        self,
        master_url: str,
        worker_id: str,
        *,
        control_plane_bind: Optional[str] = None,
    ) -> None:
        self.master_url = master_url.rstrip("/")
        self.worker_id = worker_id
        transport = None
        if control_plane_bind:
            transport = httpx.AsyncHTTPTransport(local_address=control_plane_bind)
        self._client = httpx.AsyncClient(timeout=30.0, transport=transport)

    async def send_heartbeat(self, heartbeat: WorkerHeartbeat) -> None:
        payload = _jsonable_payload(heartbeat)
        await self._client.post(f"{self.master_url}/workers/heartbeat", json=payload)

    async def poll_assignment(self, interval: float = 1.0) -> Optional[Assignment]:
        response = await self._client.post(
            f"{self.master_url}/workers/{self.worker_id}/assignment"
        )
        if response.status_code == 200 and response.json() is not None:
            return Assignment.parse_obj(response.json())
        await asyncio.sleep(interval)
        return None

    async def report_result(self, result: SyncResult) -> None:
        payload = _jsonable_payload(result)
        await self._client.post(f"{self.master_url}/workers/result", json=payload)

    async def close(self) -> None:
        await self._client.aclose()


async def run_agent(
    client: AgentClient,
    heartbeat_factory,
    assignment_handler,
    interval: float = 5.0,
) -> None:
    try:
        while True:
            heartbeat = heartbeat_factory()
            await client.send_heartbeat(heartbeat)
            assignment = await client.poll_assignment()
            if assignment:
                result = await assignment_handler(assignment)
                await client.report_result(result)
            await asyncio.sleep(interval)
    except httpx.HTTPError as exc:  # pragma: no cover - exercised via async tests
        raise AgentCommunicationError(
            f"Failed to communicate with master at {client.master_url}: {exc}"
        ) from exc
    finally:
        await client.close()
