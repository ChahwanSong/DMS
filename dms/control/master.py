"""Master controller logic for DMS."""
from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Deque, Dict, List, MutableMapping, Sequence

from ..common.chunker import FileAssignment
from ..common.filesystem import list_files, total_size
from ..config import AgentEndpoint, SUPPORTED_TRANSFER_MODES, SyncRequest
from ..logging_utils import log_progress, setup_logging
from .policies.registry import get_policy, SchedulingPolicy


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
        *,
        policy_name: str = "round_robin",
    ) -> None:
        if not source_agents:
            raise ValueError("At least one source agent is required")
        if not dest_agents:
            raise ValueError("At least one destination agent is required")
        self.source_agents = list(source_agents)
        self.dest_agents = list(dest_agents)
        self._logger = setup_logging(self.__class__.__name__)
        self._status_store: MutableMapping[str, Deque[ProgressRecord]] = defaultdict(deque)
        self._policy_name = policy_name
        self._policy: SchedulingPolicy = get_policy(policy_name)

    def _ensure_policy(self, policy_name: str) -> SchedulingPolicy:
        if policy_name != self._policy_name:
            self._policy_name = policy_name
            self._policy = get_policy(policy_name)
        return self._policy

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

        policy = self._ensure_policy(getattr(request, "policy", self._policy_name))
        raw_assignments = policy.assign(
            request=request,
            source_agents=self.source_agents,
            dest_agents=self.dest_agents,
            files=files,
        )

        plans: Dict[str, AgentTaskPlan] = {
            agent.agent_id: AgentTaskPlan(agent=agent) for agent in self.source_agents
        }
        for agent_id, assignments in raw_assignments.items():
            if agent_id not in plans:
                continue
            plan = plans[agent_id]
            plan.assignments.extend(assignments)
            plan.total_bytes += sum(assignment.chunk.length for assignment in assignments)

        for plan in plans.values():
            self._logger.info(
                "agent plan",
                extra={
                    "_dms_request_id": request.request_id,
                    "_dms_agent_id": plan.agent.agent_id,
                    "_dms_total_bytes": plan.total_bytes,
                    "_dms_chunks": len(plan.assignments),
                    "_dms_policy": self._policy_name,
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
