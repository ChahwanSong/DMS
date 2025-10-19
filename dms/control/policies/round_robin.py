"""Round robin scheduling policy."""
from __future__ import annotations

import itertools
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List

from ...common.chunker import FileAssignment, chunk_file
from ...config import AgentEndpoint, SyncRequest
from .base import SchedulingPolicy


class RoundRobinPolicy(SchedulingPolicy):
    """Assign chunks by alternating through source and destination agents."""

    name = "round_robin"

    def assign(
        self,
        *,
        request: SyncRequest,
        source_agents: Iterable[AgentEndpoint],
        dest_agents: Iterable[AgentEndpoint],
        files: Iterable[Path],
    ) -> Dict[str, List[FileAssignment]]:
        source_agents = list(source_agents)
        dest_agents = list(dest_agents)
        if not source_agents:
            raise ValueError("At least one source agent is required")
        if not dest_agents:
            raise ValueError("At least one destination agent is required")

        plans: Dict[str, List[FileAssignment]] = defaultdict(list)
        dest_cycle = itertools.cycle(dest_agents)
        source_cycle = itertools.cycle(source_agents)
        source_root = Path(request.source_path).resolve()

        for path in files:
            for chunk in chunk_file(path, request.chunk_size):
                source_agent = next(source_cycle)
                dest_agent = next(dest_cycle)
                rel_path = path.relative_to(source_root).as_posix()
                plans[source_agent.agent_id].append(
                    FileAssignment(
                        relative_path=rel_path,
                        chunk=chunk,
                        agent_id=source_agent.agent_id,
                        peer_host=dest_agent.host,
                        peer_port=dest_agent.data_port,
                        is_sender=True,
                    )
                )
        return plans


__all__ = ["RoundRobinPolicy"]
