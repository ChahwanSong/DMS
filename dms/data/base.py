"""Abstract data plane implementations."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from ..common.chunker import FileChunk


@dataclass(frozen=True)
class TransferContext:
    request_id: str
    agent_id: str
    relative_path: str
    chunk: FileChunk
    peer_host: str
    peer_port: int
    dest_root: Path


class DataPlane(Protocol):
    """Protocol for data plane implementations."""

    def transfer(self, ctx: TransferContext) -> None:
        """Transfer ``ctx.chunk`` to ``ctx.dest_root / ctx.relative_path``."""


class DataPlaneError(RuntimeError):
    """Generic data plane error."""


__all__ = ["TransferContext", "DataPlane", "DataPlaneError"]
