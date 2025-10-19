"""Lightweight protobuf replacements for offline unit tests."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class SyncRequest:
    request_id: str = ""
    source_path: str = ""
    dest_path: str = ""
    transfer_mode: str = ""
    chunk_size: int = 0


@dataclass
class SyncResponse:
    accepted: bool = False
    detail: str = ""


@dataclass
class StatusRequest:
    request_id: str = ""


@dataclass
class ProgressUpdate:
    request_id: str = ""
    agent_id: str = ""
    bytes_transferred: int = 0
    total_bytes: int = 0
    state: str = ""
    detail: str = ""
    timestamp_ms: int = 0


@dataclass
class StatusResponse:
    updates: List[ProgressUpdate] = field(default_factory=list)


@dataclass
class ChunkInfo:
    relative_path: str = ""
    offset: int = 0
    length: int = 0
    peer_host: str = ""
    peer_port: int = 0


@dataclass
class AgentTask:
    request_id: str = ""
    agent_id: str = ""
    source_root: str = ""
    dest_root: str = ""
    transfer_mode: str = ""
    chunks: List[ChunkInfo] = field(default_factory=list)
    total_bytes: int = 0


@dataclass
class Ack:
    ok: bool = True
    detail: str = ""


__all__ = [
    "SyncRequest",
    "SyncResponse",
    "StatusRequest",
    "ProgressUpdate",
    "StatusResponse",
    "ChunkInfo",
    "AgentTask",
    "Ack",
]
