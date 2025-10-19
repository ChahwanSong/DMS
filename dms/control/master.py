"""Master controller logic for DMS."""
from __future__ import annotations

import itertools
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Deque, Dict, List, MutableMapping, Sequence

from ..common.chunker import FileAssignment, chunk_file
from ..common.filesystem import list_files, total_size
from ..config import AgentEndpoint, SUPPORTED_TRANSFER_MODES, SyncRequest
from ..logging_utils import log_progress, setup_logging


@dataclass(frozen=True)
class ProgressRecord:
    request_id: str
    agent_id: str
    bytes_transferred: int
    total_bytes: int
    state: str
    detail: str
    timestamp_ms: int


@dataclass
class AgentTaskPlan:
    agent: AgentEndpoint
    assignments: List[FileAssignment] = field(default_factory=list)
    total_bytes: int = 0


class MasterScheduler:
    """Plan sync jobs and track progress for them."""

    def __init__(
        self,
        source_agents: Sequence[AgentEndpoint],
        dest_agents: Sequence[AgentEndpoint],
    ) -> None:
        if not source_agents:
            raise ValueError("At least one source agent is required")
        if not dest_agents:
            raise ValueError("At least one destination agent is required")
        self.source_agents = list(source_agents)
        self.dest_agents = list(dest_agents)
        self._logger = setup_logging(self.__class__.__name__)
        self._status_store: MutableMapping[str, Deque[ProgressRecord]] = defaultdict(deque)

    def plan(self, request: SyncRequest) -> Dict[str, AgentTaskPlan]:
        if request.transfer_mode not in SUPPORTED_TRANSFER_MODES:
            raise ValueError(f"Unsupported transfer mode {request.transfer_mode}")

        source_root = Path(request.source_path).resolve()
        dest_root = Path(request.dest_path).resolve()
        files = list_files(source_root)
        total_bytes = total_size(files)
        self._logger.info(
            "planning sync",
            extra={
                "_dms_request_id": request.request_id,
                "_dms_total_bytes": total_bytes,
                "_dms_source_root": str(source_root),
                "_dms_dest_root": str(dest_root),
            },
        )

        plans: Dict[str, AgentTaskPlan] = {
            agent.agent_id: AgentTaskPlan(agent=agent) for agent in self.source_agents
        }

        dest_cycle = itertools.cycle(self.dest_agents)
        source_cycle = itertools.cycle(self.source_agents)

        for path in files:
            for chunk in chunk_file(path, request.chunk_size):
                source_agent = next(source_cycle)
                dest_agent = next(dest_cycle)
                assignment = FileAssignment(
                    relative_path=path.relative_to(source_root).as_posix(),
                    chunk=chunk,
                    agent_id=source_agent.agent_id,
                    peer_host=dest_agent.host,
                    peer_port=dest_agent.data_port,
                    is_sender=True,
                )
                plans[source_agent.agent_id].assignments.append(assignment)
                plans[source_agent.agent_id].total_bytes += chunk.length

        for plan in plans.values():
            self._logger.info(
                "agent plan",
                extra={
                    "_dms_request_id": request.request_id,
                    "_dms_agent_id": plan.agent.agent_id,
                    "_dms_total_bytes": plan.total_bytes,
                    "_dms_chunks": len(plan.assignments),
                },
            )
        return plans

    def record_progress(
        self,
        request_id: str,
        agent_id: str,
        *,
        bytes_transferred: int,
        total_bytes: int,
        state: str,
        detail: str = "",
    ) -> ProgressRecord:
        timestamp_ms = int(time.time() * 1000)
        record = ProgressRecord(
            request_id=request_id,
            agent_id=agent_id,
            bytes_transferred=bytes_transferred,
            total_bytes=total_bytes,
            state=state,
            detail=detail,
            timestamp_ms=timestamp_ms,
        )
        store = self._status_store[request_id]
        store.append(record)
        while len(store) > 1000:
            store.popleft()
        log_progress(
            self._logger,
            request_id=request_id,
            agent_id=agent_id,
            bytes_transferred=bytes_transferred,
            total_bytes=total_bytes,
            state=state,
            detail=detail or None,
        )
        return record

    def get_status(self, request_id: str) -> List[ProgressRecord]:
        return list(self._status_store.get(request_id, ()))


__all__ = ["MasterScheduler", "AgentTaskPlan", "ProgressRecord"]
