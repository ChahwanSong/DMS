"""Redis-backed metadata persistence for the DMS master."""
from __future__ import annotations

import json
from typing import Protocol

try:  # pragma: no cover - allow running unit tests without redis client installed
    from redis.asyncio import Redis  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - surfaced via RuntimeError when used
    Redis = None  # type: ignore

from .config import RedisConfig
from .models import SyncProgress, SyncResult, WorkerHeartbeat


class MetadataStore(Protocol):
    async def store_request(self, progress: SyncProgress) -> None:
        ...

    async def update_progress(self, progress: SyncProgress) -> None:
        ...

    async def append_result(self, result: SyncResult) -> None:
        ...

    async def record_worker(self, heartbeat: WorkerHeartbeat) -> None:
        ...

    async def delete_request(self, request_id: str) -> None:
        ...

    async def health_check(self) -> None:
        """Verify that the backing store is reachable."""
        ...


class RedisMetadataStore:
    """Simple JSON-based metadata persistence layer stored in Redis."""

    def __init__(self, client: Redis, namespace: str = "dms") -> None:
        self._client = client
        self._namespace = namespace

    @classmethod
    def from_config(cls, config: RedisConfig, namespace: str = "dms") -> "RedisMetadataStore":
        if Redis is None:  # pragma: no cover - handled in runtime environments
            raise RuntimeError(
                "redis-py is required to use the Redis metadata store. Install the 'redis' package."
            )
        client = Redis(
            host=config.host,
            port=config.port,
            db=config.db,
            encoding="utf-8",
            decode_responses=True,
        )
        return cls(client, namespace=namespace)

    def _request_key(self, request_id: str) -> str:
        return f"{self._namespace}:requests:{request_id}"

    def _result_key(self, request_id: str) -> str:
        return f"{self._namespace}:results:{request_id}"

    def _heartbeat_key(self, worker_id: str) -> str:
        return f"{self._namespace}:workers:{worker_id}"

    async def store_request(self, progress: SyncProgress) -> None:
        await self._client.set(self._request_key(progress.request_id), self._dump(progress))

    async def update_progress(self, progress: SyncProgress) -> None:
        await self._client.set(self._request_key(progress.request_id), self._dump(progress))

    async def append_result(self, result: SyncResult) -> None:
        await self._client.rpush(self._result_key(result.request_id), self._dump(result))

    async def record_worker(self, heartbeat: WorkerHeartbeat) -> None:
        await self._client.set(self._heartbeat_key(heartbeat.worker_id), self._dump(heartbeat))

    async def delete_request(self, request_id: str) -> None:
        await self._client.delete(
            self._request_key(request_id),
            self._result_key(request_id),
        )

    async def health_check(self) -> None:
        await self._client.ping()

    @staticmethod
    def _dump(model) -> str:
        if hasattr(model, "dict"):
            payload = model.dict()
        else:
            raise TypeError("Model does not support dict() serialization")
        return json.dumps(payload, default=str)
