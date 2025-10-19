"""Test policy for plugin validation."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

from ...common.chunker import FileAssignment, FileChunk
from ...config import AgentEndpoint, SyncRequest
from .base import SchedulingPolicy


class TestFixedPolicy(SchedulingPolicy):
    """Assign the entire workload to the first agent for testing purposes."""

    name = "test_fixed"

    def assign(
        self,
        *,
        request: SyncRequest,
        source_agents: Iterable[AgentEndpoint],
        dest_agents: Iterable[AgentEndpoint],
        files: Iterable[Path],
    ) -> Dict[str, List[FileAssignment]]:
        agents = list(source_agents)
        destinations = list(dest_agents)
        if not agents or not destinations:
            raise ValueError("test policy requires at least one source and destination agent")
        primary = agents[0]
        dest = destinations[0]
        source_root = Path(request.source_path).resolve()
        assignments: List[FileAssignment] = []
        for path in files:
            chunk = FileChunk(path=path, offset=0, length=path.stat().st_size)
            assignments.append(
                FileAssignment(
                    relative_path=path.relative_to(source_root).as_posix(),
                    chunk=chunk,
                    agent_id=primary.agent_id,
                    peer_host=dest.host,
                    peer_port=dest.data_port,
                    is_sender=True,
                )
            )
        return {primary.agent_id: assignments}


__all__ = ["TestFixedPolicy"]
