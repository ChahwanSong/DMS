import asyncio

from dms_master.config import MasterConfig
from dms_master.models import SyncRequest, WorkerHeartbeat, WorkerStatus, SyncResult
from dms_master.server import DMSMaster


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
            WorkerHeartbeat(worker_id="worker-1", status=WorkerStatus.IDLE, free_bytes=10**9)
        )
        await master.worker_heartbeat(
            WorkerHeartbeat(worker_id="worker-2", status=WorkerStatus.IDLE, free_bytes=10**9)
        )
        assignment1 = await master.next_assignment("worker-1", timeout=1.0)
        assignment2 = await master.next_assignment("worker-2", timeout=1.0)
        assert assignment1 is not None
        assert assignment2 is not None
        await master.report_result(
            SyncResult(
                request_id="req-1",
                worker_id="worker-1",
                success=True,
                message="done",
            )
        )
        await master.report_result(
            SyncResult(
                request_id="req-1",
                worker_id="worker-2",
                success=True,
                message="done",
            )
        )
        progress = await master.query_progress("req-1")
        assert progress is not None
        assert progress.state == "COMPLETED"

    asyncio.run(scenario())
