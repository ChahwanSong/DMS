"""Chunking utilities for large files."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator


@dataclass(frozen=True)
class FileChunk:
    """Chunk metadata for a portion of a file."""

    path: Path
    offset: int
    length: int


def chunk_file(path: Path, chunk_size: int) -> Iterator[FileChunk]:
    """Yield ``FileChunk`` objects for *path* using *chunk_size* boundaries."""

    file_size = path.stat().st_size
    if file_size == 0:
        yield FileChunk(path=path, offset=0, length=0)
        return

    offset = 0
    while offset < file_size:
        length = min(chunk_size, file_size - offset)
        yield FileChunk(path=path, offset=offset, length=length)
        offset += length


@dataclass(frozen=True)
class FileAssignment:
    """Represents a chunk assigned to a specific agent."""

    relative_path: str
    chunk: FileChunk
    agent_id: str
    peer_host: str
    peer_port: int
    is_sender: bool


def round_robin_assignments(
    chunks: Iterable[FileChunk],
    agent_cycle: Iterable[str],
    *,
    relative_root: Path,
    peer_host: str,
    peer_port: int,
    is_sender: bool,
) -> Iterator[FileAssignment]:
    """Assign chunks to agents using round robin scheduling."""

    agents = list(agent_cycle)
    if not agents:
        raise ValueError("At least one agent is required for assignment")

    for idx, chunk in enumerate(chunks):
        agent_id = agents[idx % len(agents)]
        rel_path = chunk.path.relative_to(relative_root).as_posix()
        yield FileAssignment(
            relative_path=rel_path,
            chunk=chunk,
            agent_id=agent_id,
            peer_host=peer_host,
            peer_port=peer_port,
            is_sender=is_sender,
        )
