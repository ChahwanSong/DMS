"""Core master server orchestration logic."""
from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from typing import Deque, Dict, List, Optional, Set

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
    preferred_worker: Optional[str] = None


def _endpoint_key(worker_id: str, address: Optional[str]) -> str:
    if address:
        return f"{worker_id}::{address}"
    return worker_id


def _path_in_mount(path: str, mount: str) -> bool:
    """Return True if *path* is contained within *mount*."""

    path_obj = PurePosixPath(path)
    mount_obj = PurePosixPath(mount)
    return mount_obj == path_obj or mount_obj in path_obj.parents


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

    def _worker_pool_for_path(self, path: str) -> List[str]:
        """Return worker IDs that can access the provided path."""

        pool: list[str] = []
        for worker_id, heartbeat in self._worker_status.items():
            mounts = getattr(heartbeat, "storage_paths", []) or []
            for mount in mounts:
                if _path_in_mount(path, mount):
                    pool.append(worker_id)
                    break
        # Deduplicate while preserving order to provide deterministic pools.
        seen: set[str] = set()
        ordered_pool: list[str] = []
        for worker_id in pool:
            if worker_id not in seen:
                ordered_pool.append(worker_id)
                seen.add(worker_id)
        return ordered_pool

    async def _fail_request(self, state: RequestState, message: str) -> None:
        """Transition *state* to FAILED and persist the provided *message*."""

        if state.progress.state == "FAILED":
            return

        self.logger.error("Request %s failed: %s", state.request.request_id, message)
        state.progress.state = "FAILED"
        state.progress.detail[_endpoint_key("master", None)] = message
        state.pending_files.clear()
        state.active_assignments.clear()

        result = SyncResult(
            request_id=state.request.request_id,
            worker_id="master",
            success=False,
            message=message,
        )
        self._result_log[state.request.request_id].append(result)
        await self.metadata.append_result(result)
        await self.metadata.update_progress(state.progress)

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
                source_pool = self._worker_pool_for_path(state.request.source_path)
                destination_pool = self._worker_pool_for_path(
                    state.request.destination_path
                )
                if not source_pool:
                    if self._worker_status:
                        await self._fail_request(
                            state,
                            "No workers have access to source path "
                            f"{state.request.source_path}",
                        )
                    continue
                if not destination_pool:
                    if self._worker_status:
                        await self._fail_request(
                            state,
                            "No workers have access to destination path "
                            f"{state.request.destination_path}",
                        )
                    continue
                candidate_workers: Set[str] = set(source_pool)
                if state.preferred_worker:
                    if state.preferred_worker in candidate_workers:
                        candidate_workers = {state.preferred_worker}
                    else:
                        self.logger.warning(
                            "Preferred worker %s unavailable for request %s",
                            state.preferred_worker,
                            state.request.request_id,
                        )
                        continue
                if not candidate_workers:
                    continue

                available_endpoints: List[WorkerInterface] = []
                for worker_id, heartbeat in self._worker_status.items():
                    if worker_id not in candidate_workers:
                        continue
                    if heartbeat.status == WorkerStatus.ERROR:
                        continue
                    for endpoint in heartbeat.data_plane_endpoints:
                        interface = WorkerInterface(
                            worker_id=worker_id,
                            address=endpoint.address,
                        )
                        if interface.key in busy_endpoints:
                            continue
                        available_endpoints.append(interface)
                if not available_endpoints:
                    continue
                needed = min(len(available_endpoints), len(state.pending_files))
                if needed <= 0:
                    continue
                chosen = self.scheduler.select_workers(available_endpoints, needed)
                for interface in chosen:
                    if not state.pending_files:
                        break
                    endpoint_id = interface.key
                    if endpoint_id in state.active_assignments:
                        continue
                    source_path = state.pending_files.popleft()
                    assignment = Assignment(
                        request_id=state.request.request_id,
                        worker_id=interface.worker_id,
                        source_path=source_path,
                        destination_path=state.request.destination_path,
                        chunk_offset=0,
                        chunk_size=state.request.chunk_size_mb * 1024 * 1024,
                        source_worker_pool=source_pool,
                        destination_worker_pool=destination_pool,
                    )
                    state.active_assignments[endpoint_id] = assignment
                    busy_endpoints.add(endpoint_id)
                    await self._assignment_queue.put(assignment)
                    self.logger.info(
                        "Assigned %s to worker %s (%s) for request %s",
                        source_path,
                        interface.worker_id,
                        interface.address,
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
        progress_to_update: Optional[SyncProgress] = None
        async with self._lock:
            state = self._requests.get(assignment.request_id)
            if state:
                state.progress.updated_at = datetime.utcnow()
                if state.progress.state == "QUEUED":
                    state.progress.state = "PROGRESS"
                detail_key = _endpoint_key(assignment.worker_id, None)
                for key, active in state.active_assignments.items():
                    if active is assignment:
                        detail_key = key
                        break
                state.progress.detail[detail_key] = "PROGRESS"
                progress_to_update = state.progress
        if progress_to_update:
            await self.metadata.update_progress(progress_to_update)
        return assignment

    async def _drain_assignments_for_request(self, request_id: str) -> None:
        """Remove any queued assignments belonging to *request_id*."""

        retained: list[Assignment] = []
        while True:
            try:
                queued = self._assignment_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if queued.request_id != request_id:
                retained.append(queued)
        for assignment in retained:
            await self._assignment_queue.put(assignment)

    async def reassign_request(self, request_id: str, worker_id: str) -> None:
        async with self._lock:
            state = self._requests.get(request_id)
            if not state:
                raise ValueError(f"Request {request_id} not found")
            if state.progress.state not in {"QUEUED", "FAILED"}:
                raise ValueError(
                    "Reassignment is only supported for requests in QUEUED or FAILED state"
                )
            if worker_id not in self._worker_status:
                raise ValueError(f"Worker {worker_id} is not registered with the master")
            source_pool = self._worker_pool_for_path(state.request.source_path)
            if worker_id not in source_pool:
                raise ValueError(
                    f"Worker {worker_id} does not have access to {state.request.source_path}"
                )

            reassigned_sources = [assignment.source_path for assignment in state.active_assignments.values()]
            state.active_assignments.clear()
            for path in reversed(reassigned_sources):
                state.pending_files.appendleft(path)

            if not state.pending_files:
                state.pending_files = deque(
                    state.request.file_list or [state.request.source_path]
                )

            await self._drain_assignments_for_request(request_id)

            state.preferred_worker = worker_id
            state.progress.state = "QUEUED"
            state.progress.updated_at = datetime.utcnow()
            detail_key = _endpoint_key("master", None)
            if detail_key in state.progress.detail and state.progress.detail[detail_key].startswith(
                "No workers have access"
            ):
                state.progress.detail.pop(detail_key)
            progress_to_update = state.progress

        await self.metadata.update_progress(progress_to_update)
        await self._schedule_work()

    async def list_requests_for_worker(self, worker_id: str) -> List[SyncProgress]:
        async with self._lock:
            results: list[SyncProgress] = []
            for state in self._requests.values():
                if any(assignment.worker_id == worker_id for assignment in state.active_assignments.values()):
                    results.append(state.progress)
            return results

    async def report_result(self, result: SyncResult) -> None:
        async with self._lock:
            state = self._requests.get(result.request_id)
            if not state:
                self.logger.warning("Received result for unknown request %s", result.request_id)
                return
            self._result_log[result.request_id].append(result)
            state.progress.updated_at = datetime.utcnow()
            detail_key = _endpoint_key(result.worker_id, result.data_plane_address)
            if result.data_plane_address is None:
                for key, active in state.active_assignments.items():
                    if active.worker_id == result.worker_id:
                        detail_key = key
                        break
            if result.success:
                state.progress.detail[detail_key] = "COMPLETED"
            else:
                state.progress.state = "FAILED"
                state.progress.detail[detail_key] = result.message
                self.logger.error(
                    "Request %s failed on worker %s (%s): %s",
                    result.request_id,
                    result.worker_id,
                    result.data_plane_address or "unknown-address",
                    result.message,
                )
            assignment_key = _endpoint_key(result.worker_id, result.data_plane_address)
            if assignment_key not in state.active_assignments:
                for key, active in state.active_assignments.items():
                    if active.worker_id == result.worker_id:
                        assignment_key = key
                        break
                else:
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
