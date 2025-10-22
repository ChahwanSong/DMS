import asyncio

import pytest

from dms_master.config import MasterConfig
from dms_master.models import (
    DataPlaneEndpoint,
    SyncRequest,
    WorkerHeartbeat,
    WorkerStatus,
    SyncResult,
)
from dms_master.server import DMSMaster

try:
    from pydantic import ValidationError
except ModuleNotFoundError:  # pragma: no cover - fallback paths during minimal testing
    ValidationError = ValueError


class DummyMetadataStore:
    def __init__(self):
        self.requests = {}
        self.results = {}
        self.heartbeats = {}

    async def store_request(self, progress):
        self.requests[progress.request_id] = progress

    async def update_progress(self, progress):
        self.requests[progress.request_id] = progress

    async def append_result(self, result):
        self.results.setdefault(result.request_id, []).append(result)

    async def record_worker(self, heartbeat):
        self.heartbeats[heartbeat.worker_id] = heartbeat

    async def delete_request(self, request_id: str) -> None:
        self.requests.pop(request_id, None)
        self.results.pop(request_id, None)

    async def health_check(self) -> None:
        return None


def test_submit_and_assignments():
    async def scenario():
        master = DMSMaster(MasterConfig(), metadata_store=DummyMetadataStore())
        request = SyncRequest(
            request_id="req-1",
            source_path="/home/clusterA/foo",
            destination_path="/home/clusterB",
            file_list=["/home/clusterA/foo/file1", "/home/clusterA/foo/file2"],
            parallelism=2,
        )
        await master.submit_request(request)
        await master.worker_heartbeat(
            WorkerHeartbeat(
                worker_id="worker-1",
                status=WorkerStatus.IDLE,
                data_plane_endpoints=[
                    DataPlaneEndpoint(iface="ib0", address="192.168.1.10"),
                    DataPlaneEndpoint(iface="ib1", address="192.168.1.11"),
                ],
            )
        )
        assignment1 = await master.next_assignment("worker-1", timeout=1.0)
        assignment2 = await master.next_assignment("worker-1", timeout=1.0)
        assert assignment1 is not None
        assert assignment2 is not None
        assert {assignment1.data_plane_iface, assignment2.data_plane_iface} == {"ib0", "ib1"}
        await master.report_result(
            SyncResult(
                request_id="req-1",
                worker_id="worker-1",
                success=True,
                message="done",
                data_plane_iface=assignment1.data_plane_iface,
            )
        )
        await master.report_result(
            SyncResult(
                request_id="req-1",
                worker_id="worker-1",
                success=True,
                message="done",
                data_plane_iface=assignment2.data_plane_iface,
            )
        )
        progress = await master.query_progress("req-1")
        assert progress is not None
        assert progress.state == "COMPLETED"
        assert set(progress.detail.keys()) == {
            f"worker-1::{assignment1.data_plane_iface}",
            f"worker-1::{assignment2.data_plane_iface}",
        }

    asyncio.run(scenario())


def test_sync_request_requires_absolute_paths():
    with pytest.raises(ValidationError) as excinfo:
        SyncRequest(
            request_id="req-abs", source_path="relative/path", destination_path="/abs/path"
        )

    assert "source_path must be an absolute path" in str(excinfo.value)

    with pytest.raises(ValidationError) as excinfo:
        SyncRequest(
            request_id="req-abs", source_path="/abs/path", destination_path="relative/path"
        )

    assert "destination_path must be an absolute path" in str(excinfo.value)
