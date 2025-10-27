from __future__ import annotations
import asyncio
from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

from dms_master.app import app, master_dependency
from dms_master.config import MasterConfig
from dms_master.models import WorkerHeartbeat, WorkerStatus
from dms_master.server import DMSMaster


class DummyMetadataStore:
    async def store_request(self, progress):  # pragma: no cover - simple stub
        return None

    async def update_progress(self, progress):  # pragma: no cover - simple stub
        return None

    async def append_result(self, result):  # pragma: no cover - simple stub
        return None

    async def record_worker(self, heartbeat):  # pragma: no cover - simple stub
        return None

    async def delete_request(self, request_id: str):  # pragma: no cover - simple stub
        return None

    async def health_check(self):  # pragma: no cover - simple stub
        return None


def test_duplicate_request_returns_conflict_status():
    master = DMSMaster(MasterConfig(), metadata_store=DummyMetadataStore())

    def override_master_dependency() -> DMSMaster:
        return master

    app.dependency_overrides[master_dependency] = override_master_dependency
    payload = {
        "request_id": "demo-1",
        "source_path": "/data/source",
        "destination_path": "/data/destination",
        "file_list": ["/data/source/file.txt"],
    }

    try:
        with TestClient(app) as client:
            first_response = client.post("/sync", json=payload)
            assert first_response.status_code == 202

            second_response = client.post("/sync", json=payload)
            assert second_response.status_code == 409
            assert second_response.json() == {"detail": "Request demo-1 already exists"}
    finally:
        app.dependency_overrides.clear()


def test_submit_request_still_succeeds_without_duplicate():
    master = DMSMaster(MasterConfig(), metadata_store=DummyMetadataStore())

    def override_master_dependency() -> DMSMaster:
        return master

    app.dependency_overrides[master_dependency] = override_master_dependency
    payload = {
        "request_id": "demo-unique",
        "source_path": "/data/source",
        "destination_path": "/data/destination",
        "file_list": ["/data/source/file.txt"],
    }

    try:
        with TestClient(app) as client:
            response = client.post("/sync", json=payload)
            assert response.status_code == 202
            assert response.json() == {"status": "queued", "request_id": "demo-unique"}
    finally:
        app.dependency_overrides.clear()


def test_list_workers_endpoint_supports_status_filtering():
    master = DMSMaster(
        MasterConfig(worker_heartbeat_timeout=0.1), metadata_store=DummyMetadataStore()
    )

    def override_master_dependency() -> DMSMaster:
        return master

    app.dependency_overrides[master_dependency] = override_master_dependency

    async def seed_workers():
        await master.worker_heartbeat(
            WorkerHeartbeat(
                worker_id="worker-active",
                status=WorkerStatus.IDLE,
                storage_paths=["/data"],
            )
        )
        await master.worker_heartbeat(
            WorkerHeartbeat(
                worker_id="worker-inactive",
                status=WorkerStatus.IDLE,
                timestamp=datetime.now(UTC) - timedelta(seconds=5),
                storage_paths=["/data"],
            )
        )

    asyncio.run(seed_workers())

    try:
        with TestClient(app) as client:
            response = client.get("/workers")
            assert response.status_code == 200
            payload = response.json()
            assert [worker["worker_id"] for worker in payload["active"]] == [
                "worker-active"
            ]
            assert [worker["worker_id"] for worker in payload["inactive"]] == [
                "worker-inactive"
            ]

            active_only = client.get("/workers", params={"status": "active"})
            assert active_only.status_code == 200
            assert [
                worker["worker_id"] for worker in active_only.json()["active"]
            ] == ["worker-active"]
            assert not active_only.json()["inactive"]

            inactive_only = client.get("/workers", params={"status": "inactive"})
            assert inactive_only.status_code == 200
            assert [
                worker["worker_id"] for worker in inactive_only.json()["inactive"]
            ] == ["worker-inactive"]
            assert not inactive_only.json()["active"]
    finally:
        app.dependency_overrides.clear()
