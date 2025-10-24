from __future__ import annotations
from fastapi.testclient import TestClient

from dms_master.app import app, master_dependency
from dms_master.config import MasterConfig
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
