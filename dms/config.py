"""DMS configuration defaults."""
from __future__ import annotations

from dataclasses import dataclass


DEFAULT_CHUNK_SIZE_BYTES: int = 64 * 1024 * 1024  # 64 MiB
DEFAULT_TCP_PORT: int = 50051
LOG_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


@dataclass(frozen=True)
class AgentEndpoint:
    """Represents an agent reachable by the master controller."""

    agent_id: str
    host: str
    control_port: int
    data_port: int
    is_source: bool


@dataclass(frozen=True)
class SyncRequest:
    """User facing sync request captured by the master server."""

    request_id: str
    source_path: str
    dest_path: str
    transfer_mode: str = "TCP"
    chunk_size: int = DEFAULT_CHUNK_SIZE_BYTES
    policy: str = "round_robin"


SUPPORTED_TRANSFER_MODES = {"TCP", "RDMA"}
