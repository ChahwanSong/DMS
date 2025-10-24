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

    def __init__(
        self,
        client: Redis,
        namespace: str = "dms",
        expiry_seconds: int | None = 60 * 24 * 3600,
    ) -> None:
        self._client = client
        self._namespace = namespace
        self._expiry_seconds = expiry_seconds

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
        expiry_seconds = int(config.expiry_days * 24 * 3600)
        if expiry_seconds <= 0:
            expiry_seconds = None
        return cls(client, namespace=namespace, expiry_seconds=expiry_seconds)

    def _request_key(self, request_id: str) -> str:
        return f"{self._namespace}:requests:{request_id}"

    def _result_key(self, request_id: str) -> str:
        return f"{self._namespace}:results:{request_id}"

    def _heartbeat_key(self, worker_id: str) -> str:
        return f"{self._namespace}:workers:{worker_id}"

    async def store_request(self, progress: SyncProgress) -> None:
        await self._client.set(
            self._request_key(progress.request_id),
            self._dump(progress),
            ex=self._expiry_seconds,
        )

    async def update_progress(self, progress: SyncProgress) -> None:
        await self._client.set(
            self._request_key(progress.request_id),
            self._dump(progress),
            ex=self._expiry_seconds,
        )

    async def append_result(self, result: SyncResult) -> None:
        key = self._result_key(result.request_id)
        await self._client.rpush(key, self._dump(result))
        if self._expiry_seconds is not None:
            await self._client.expire(key, self._expiry_seconds)

    async def record_worker(self, heartbeat: WorkerHeartbeat) -> None:
        await self._client.set(
            self._heartbeat_key(heartbeat.worker_id),
            self._dump(heartbeat),
            ex=self._expiry_seconds,
        )

    async def delete_request(self, request_id: str) -> None:
        await self._client.delete(
            self._request_key(request_id),
            self._result_key(request_id),
        )

    async def health_check(self) -> None:
        await self._client.ping()

    @staticmethod
    def _dump(model) -> str:
        if hasattr(model, "model_dump"):
            payload = model.model_dump()
        elif isinstance(model, dict):
            payload = model
        else:
            raise TypeError("Model does not support model_dump() serialization")
        return json.dumps(payload, default=str)
