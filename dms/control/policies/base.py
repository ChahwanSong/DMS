"""Base interfaces for scheduler policies."""
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

from ...common.chunker import FileAssignment
from ...config import AgentEndpoint, SyncRequest


class SchedulingPolicy(ABC):
    """Abstract policy definition for assigning file chunks."""

    name: str

    @abstractmethod
    def assign(
        self,
        *,
        request: SyncRequest,
        source_agents: Sequence[AgentEndpoint],
        dest_agents: Sequence[AgentEndpoint],
        files: Iterable[Path],
    ) -> Dict[str, List[FileAssignment]]:
        """Return assignments keyed by source agent id."""


__all__ = ["SchedulingPolicy"]
