"""Agent execution logic."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from ..common.chunker import FileAssignment
from ..data.base import DataPlane, TransferContext
from ..logging_utils import log_progress, setup_logging


@dataclass
class AgentExecutionResult:
    request_id: str
    agent_id: str
    bytes_transferred: int
    total_bytes: int
    success: bool
    detail: str = ""


class AgentWorker:
    """Executes assignments using a data plane implementation."""

    def __init__(
        self,
        agent_id: str,
        *,
        source_root: Path,
        dest_root: Path,
        data_plane: DataPlane,
    ) -> None:
        self.agent_id = agent_id
        self.source_root = source_root
        self.dest_root = dest_root
        self.data_plane = data_plane
        self._logger = setup_logging(f"AgentWorker[{agent_id}]")

    def execute(self, request_id: str, assignments: Iterable[FileAssignment]) -> AgentExecutionResult:
        assignment_list = list(assignments)
        total_bytes = sum(assignment.chunk.length for assignment in assignment_list)
        transferred = 0
        success = True
        detail = ""

        for assignment in assignment_list:
            ctx = TransferContext(
                request_id=request_id,
                agent_id=self.agent_id,
                relative_path=assignment.relative_path,
                chunk=assignment.chunk,
                peer_host=assignment.peer_host,
                peer_port=assignment.peer_port,
                dest_root=self.dest_root,
            )
            log_progress(
                self._logger,
                request_id=request_id,
                agent_id=self.agent_id,
                bytes_transferred=transferred,
                total_bytes=total_bytes,
                state="IN_PROGRESS",
                detail=f"dispatching {assignment.relative_path}:{assignment.chunk.offset}",
            )
            try:
                self.data_plane.transfer(ctx)
                transferred += assignment.chunk.length
                log_progress(
                    self._logger,
                    request_id=request_id,
                    agent_id=self.agent_id,
                    bytes_transferred=transferred,
                    total_bytes=total_bytes,
                    state="IN_PROGRESS",
                    detail=f"completed {assignment.relative_path}:{assignment.chunk.offset}",
                )
            except Exception as exc:  # pragma: no cover - defensive logging
                success = False
                detail = str(exc)
                log_progress(
                    self._logger,
                    request_id=request_id,
                    agent_id=self.agent_id,
                    bytes_transferred=transferred,
                    total_bytes=total_bytes,
                    state="FAILED",
                    detail=detail,
                )
                break

        final_state = "SUCCESS" if success else "FAILED"
        log_progress(
            self._logger,
            request_id=request_id,
            agent_id=self.agent_id,
            bytes_transferred=transferred,
            total_bytes=total_bytes,
            state=final_state,
            detail=detail or "job finished",
        )
        return AgentExecutionResult(
            request_id=request_id,
            agent_id=self.agent_id,
            bytes_transferred=transferred,
            total_bytes=total_bytes,
            success=success,
            detail=detail,
        )


__all__ = ["AgentWorker", "AgentExecutionResult"]
