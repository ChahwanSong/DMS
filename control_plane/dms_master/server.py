"""Core master server orchestration logic."""
from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from typing import Deque, Dict, List, Optional

from .config import MasterConfig
from .logging_utils import configure_logging
from .models import (
    Assignment,
    SyncProgress,
    SyncRequest,
    SyncResult,
    WorkerHeartbeat,
    WorkerStatus,
)
from .scheduler.base import SchedulerPolicy, WorkerInterface, registry as scheduler_registry
from .scheduler import round_robin  # noqa: F401 ensure registration
from .metadata import MetadataStore, RedisMetadataStore


@dataclass
class RequestState:
    request: SyncRequest
    progress: SyncProgress
    pending_files: Deque[str]
    active_assignments: Dict[str, Assignment]


def _endpoint_key(worker_id: str, iface: Optional[str]) -> str:
    if iface:
        return f"{worker_id}::{iface}"
    return worker_id


class DMSMaster:
    """In-memory orchestration of sync requests and worker assignments."""

    def __init__(self, config: MasterConfig, metadata_store: MetadataStore | None = None) -> None:
        self.config = config
        self.logger = configure_logging("dms.master")
        self.scheduler: SchedulerPolicy = scheduler_registry.create(config.scheduler)
        self._requests: Dict[str, RequestState] = {}
        self._worker_status: Dict[str, WorkerHeartbeat] = {}
        self._result_log: Dict[str, List[SyncResult]] = defaultdict(list)
        self._assignment_queue: asyncio.Queue[Assignment] = asyncio.Queue()
        self._lock = asyncio.Lock()
        if config.redis is None:
            raise ValueError("Redis configuration must be provided for the master server")
        self.metadata: MetadataStore = metadata_store or RedisMetadataStore.from_config(config.redis)

    async def submit_request(self, request: SyncRequest) -> None:
        async with self._lock:
            if request.request_id in self._requests:
                raise ValueError(f"Request {request.request_id} already exists")
            pending_files = deque(request.file_list or [request.source_path])
            progress = SyncProgress(
                request_id=request.request_id,
                transferred_bytes=0,
                total_bytes=0,
                started_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                state="QUEUED",
            )
            state = RequestState(
                request=request,
                progress=progress,
                pending_files=pending_files,
                active_assignments={},
            )
            self._requests[request.request_id] = state
            self.logger.info("Request %s queued with %d files", request.request_id, len(pending_files))
            await self.metadata.store_request(progress)
        await self._schedule_work()

    async def _schedule_work(self) -> None:
        async with self._lock:
            busy_endpoints = {
                key
                for state in self._requests.values()
                for key in state.active_assignments.keys()
            }
            for state in self._requests.values():
                if not state.pending_files:
                    continue
                needed = max(1, state.request.parallelism)
                available_endpoints: List[WorkerInterface] = []
                for worker_id, heartbeat in self._worker_status.items():
                    if heartbeat.status == WorkerStatus.ERROR:
                        continue
                    for endpoint in heartbeat.data_plane_endpoints:
                        interface = WorkerInterface(
                            worker_id=worker_id,
                            iface=endpoint.iface,
                            address=endpoint.address,
                        )
                        if interface.key in busy_endpoints:
                            continue
                        available_endpoints.append(interface)
                if not available_endpoints:
                    continue
                chosen = self.scheduler.select_workers(available_endpoints, needed)
                for interface in chosen:
                    if not state.pending_files:
                        break
                    endpoint_id = interface.key
                    if endpoint_id in state.active_assignments:
                        continue
                    file_path = state.pending_files.popleft()
                    assignment = Assignment(
                        request_id=state.request.request_id,
                        worker_id=interface.worker_id,
                        file_path=file_path,
                        chunk_offset=0,
                        chunk_size=state.request.chunk_size_mb * 1024 * 1024,
                        data_plane_iface=interface.iface,
                        data_plane_address=interface.address,
                    )
                    state.active_assignments[endpoint_id] = assignment
                    busy_endpoints.add(endpoint_id)
                    await self._assignment_queue.put(assignment)
                    self.logger.info(
                        "Assigned %s to worker %s (%s) for request %s",
                        file_path,
                        interface.worker_id,
                        interface.iface,
                        state.request.request_id,
                    )

    async def worker_heartbeat(self, heartbeat: WorkerHeartbeat) -> None:
        async with self._lock:
            self._worker_status[heartbeat.worker_id] = heartbeat
        await self.metadata.record_worker(heartbeat)
        await self._schedule_work()

    async def next_assignment(self, worker_id: str, timeout: float = 1.0) -> Optional[Assignment]:
        try:
            assignment = await asyncio.wait_for(self._assignment_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        if assignment.worker_id != worker_id:
            await self._assignment_queue.put(assignment)
            return None
        return assignment

    async def report_result(self, result: SyncResult) -> None:
        async with self._lock:
            state = self._requests.get(result.request_id)
            if not state:
                self.logger.warning("Received result for unknown request %s", result.request_id)
                return
            self._result_log[result.request_id].append(result)
            state.progress.updated_at = datetime.utcnow()
            detail_key = _endpoint_key(result.worker_id, result.data_plane_iface)
            if result.success:
                state.progress.detail[detail_key] = "COMPLETED"
            else:
                state.progress.state = "FAILED"
                state.progress.detail[detail_key] = result.message
                self.logger.error(
                    "Request %s failed on worker %s (%s): %s",
                    result.request_id,
                    result.worker_id,
                    result.data_plane_iface or "unknown-iface",
                    result.message,
                )
            assignment_key = _endpoint_key(result.worker_id, result.data_plane_iface)
            if assignment_key not in state.active_assignments:
                # Fallback to worker_id only if provided interface is missing
                assignment_key = result.worker_id
            state.active_assignments.pop(assignment_key, None)
            if not state.pending_files and not state.active_assignments:
                if state.progress.state != "FAILED":
                    state.progress.state = "COMPLETED"
                self.logger.info("Request %s completed", result.request_id)
            await self.metadata.append_result(result)
            await self.metadata.update_progress(state.progress)

    async def query_progress(self, request_id: str) -> Optional[SyncProgress]:
        async with self._lock:
            state = self._requests.get(request_id)
            return state.progress if state else None

    async def list_requests(self) -> List[SyncProgress]:
        async with self._lock:
            return [state.progress for state in self._requests.values()]

    async def forget_request(self, request_id: str) -> None:
        async with self._lock:
            self._requests.pop(request_id, None)
            self._result_log.pop(request_id, None)
            self.logger.info("Request %s removed from master state", request_id)
        await self.metadata.delete_request(request_id)
