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
        )
        await master.submit_request(request)
        await master.worker_heartbeat(
            WorkerHeartbeat(
                worker_id="worker-1",
                status=WorkerStatus.IDLE,
                storage_paths=["/home/clusterA", "/home/clusterB"],
                data_plane_endpoints=[
                    DataPlaneEndpoint(address="192.168.1.10"),
                    DataPlaneEndpoint(address="192.168.1.11"),
                ],
            )
        )
        assignment1 = await master.next_assignment("worker-1", timeout=1.0)
        assignment2 = await master.next_assignment("worker-1", timeout=1.0)
        assert assignment1 is not None
        assert assignment2 is not None
        assert {
            assignment1.data_plane_address,
            assignment2.data_plane_address,
        } == {"192.168.1.10", "192.168.1.11"}
        assert assignment1.source_worker_pool == ["worker-1"]
        assert assignment1.destination_worker_pool == ["worker-1"]
        await master.report_result(
            SyncResult(
                request_id="req-1",
                worker_id="worker-1",
                success=True,
                message="done",
                data_plane_address=assignment1.data_plane_address,
            )
        )
        await master.report_result(
            SyncResult(
                request_id="req-1",
                worker_id="worker-1",
                success=True,
                message="done",
                data_plane_address=assignment2.data_plane_address,
            )
        )
        progress = await master.query_progress("req-1")
        assert progress is not None
        assert progress.state == "COMPLETED"
        assert set(progress.detail.keys()) == {
            f"worker-1::{assignment1.data_plane_address}",
            f"worker-1::{assignment2.data_plane_address}",
        }

    asyncio.run(scenario())


def test_assignment_progress_and_reassign():
    async def scenario():
        store = DummyMetadataStore()
        master = DMSMaster(MasterConfig(), metadata_store=store)
        request = SyncRequest(
            request_id="req-reassign",
            source_path="/data/source",
            destination_path="/data/dest",
            file_list=["/data/source/file1"],
        )
        await master.submit_request(request)
        await master.worker_heartbeat(
            WorkerHeartbeat(
                worker_id="worker-a",
                status=WorkerStatus.IDLE,
                storage_paths=["/data", "/data/source", "/data/dest"],
                data_plane_endpoints=[DataPlaneEndpoint(address="10.0.0.1")],
            )
        )
        await master.worker_heartbeat(
            WorkerHeartbeat(
                worker_id="worker-b",
                status=WorkerStatus.IDLE,
                storage_paths=["/data", "/data/source", "/data/dest"],
                data_plane_endpoints=[DataPlaneEndpoint(address="10.0.0.2")],
            )
        )

        assignment = await master.next_assignment("worker-a", timeout=1.0)
        assert assignment is not None

        progress = await master.query_progress("req-reassign")
        assert progress is not None
        assert progress.state == "PROGRESS"
        detail_key = f"worker-a::{assignment.data_plane_address}"
        assert progress.detail[detail_key] == "PROGRESS"

        await master.report_result(
            SyncResult(
                request_id="req-reassign",
                worker_id="worker-a",
                success=False,
                message="transfer failed",
                data_plane_address=assignment.data_plane_address,
            )
        )

        failed_progress = await master.query_progress("req-reassign")
        assert failed_progress is not None
        assert failed_progress.state == "FAILED"

        await master.reassign_request("req-reassign", "worker-b")

        requeued_progress = await master.query_progress("req-reassign")
        assert requeued_progress is not None
        assert requeued_progress.state == "QUEUED"

        reassigned = await master.next_assignment("worker-b", timeout=1.0)
        assert reassigned is not None
        assert reassigned.worker_id == "worker-b"

        worker_requests = await master.list_requests_for_worker("worker-b")
        assert any(progress.request_id == "req-reassign" for progress in worker_requests)
        assert await master.next_assignment("worker-a", timeout=0.1) is None

    asyncio.run(scenario())


def test_assignment_respects_storage_paths():
    async def scenario():
        master = DMSMaster(MasterConfig(), metadata_store=DummyMetadataStore())
        await master.worker_heartbeat(
            WorkerHeartbeat(
                worker_id="worker-3",
                status=WorkerStatus.IDLE,
                storage_paths=["/data/destination"],
                data_plane_endpoints=[
                    DataPlaneEndpoint(address="192.168.10.3"),
                ],
            )
        )
        await master.worker_heartbeat(
            WorkerHeartbeat(
                worker_id="worker-2",
                status=WorkerStatus.IDLE,
                storage_paths=["/data/source"],
                data_plane_endpoints=[
                    DataPlaneEndpoint(address="192.168.10.2"),
                ],
            )
        )
        request = SyncRequest(
            request_id="req-storage",
            source_path="/data/source/project",
            destination_path="/data/destination",
            file_list=["/data/source/project/file1"],
        )
        await master.submit_request(request)

        assignment = await master.next_assignment("worker-2", timeout=1.0)
        assert assignment is not None
        assert assignment.worker_id == "worker-2"
        assert assignment.source_worker_pool == ["worker-2"]
        assert assignment.destination_worker_pool == ["worker-3"]

        # Workers without source access should not receive assignments for this request.
        assert await master.next_assignment("worker-3", timeout=0.1) is None

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


def test_request_fails_when_worker_pools_missing():
    async def scenario():
        source_store = DummyMetadataStore()
        master_no_workers = DMSMaster(MasterConfig(), metadata_store=source_store)
        await master_no_workers.worker_heartbeat(
            WorkerHeartbeat(
                worker_id="dest-only",
                status=WorkerStatus.IDLE,
                storage_paths=["/cluster/dest"],
                data_plane_endpoints=[
                    DataPlaneEndpoint(address="10.0.0.2"),
                ],
            )
        )
        request_no_source = SyncRequest(
            request_id="req-no-source",
            source_path="/cluster/source",
            destination_path="/cluster/dest",
        )

        await master_no_workers.submit_request(request_no_source)
        progress = await master_no_workers.query_progress("req-no-source")
        assert progress is not None
        assert progress.state == "FAILED"
        assert progress.detail.get("master", "").startswith(
            "No workers have access to source path"
        )
        result_entries = source_store.results.get("req-no-source", [])
        assert result_entries and not result_entries[0].success
        assert "source path" in result_entries[0].message

        dest_store = DummyMetadataStore()
        master_no_dest = DMSMaster(MasterConfig(), metadata_store=dest_store)
        await master_no_dest.worker_heartbeat(
            WorkerHeartbeat(
                worker_id="source-worker",
                status=WorkerStatus.IDLE,
                storage_paths=["/cluster/source"],
                data_plane_endpoints=[
                    DataPlaneEndpoint(address="10.0.0.1"),
                ],
            )
        )
        request_no_dest = SyncRequest(
            request_id="req-no-dest",
            source_path="/cluster/source",
            destination_path="/cluster/dest",
        )

        await master_no_dest.submit_request(request_no_dest)
        progress = await master_no_dest.query_progress("req-no-dest")
        assert progress is not None
        assert progress.state == "FAILED"
        assert progress.detail.get("master", "").startswith(
            "No workers have access to destination path"
        )
        dest_results = dest_store.results.get("req-no-dest", [])
        assert dest_results and not dest_results[0].success
        assert "destination path" in dest_results[0].message

    asyncio.run(scenario())
