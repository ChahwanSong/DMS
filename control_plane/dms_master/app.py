"""FastAPI wrapper exposing DMS master functionality."""
from __future__ import annotations

import asyncio
from functools import lru_cache
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException

from .config import MasterConfig, load_config
from .models import (
    Assignment,
    ReassignRequest,
    SyncProgress,
    SyncRequest,
    SyncResult,
    WorkerHeartbeat,
)
from .server import DMSMaster

app = FastAPI(title="DMS Master", version="0.1.0")


@lru_cache(maxsize=1)
def get_master(config_path: Optional[str] = None) -> DMSMaster:
    config: MasterConfig = load_config(config_path)
    return DMSMaster(config)


def master_dependency() -> DMSMaster:
    return get_master()


@app.post("/sync", status_code=202)
async def submit_sync(request: SyncRequest, master: DMSMaster = Depends(master_dependency)) -> dict:
    await master.submit_request(request)
    return {"status": "queued", "request_id": request.request_id}


@app.get("/sync/{request_id}", response_model=SyncProgress)
async def get_progress(request_id: str, master: DMSMaster = Depends(master_dependency)) -> SyncProgress:
    progress = await master.query_progress(request_id)
    if not progress:
        raise HTTPException(status_code=404, detail="request not found")
    return progress


@app.get("/sync", response_model=list[SyncProgress])
async def list_requests(master: DMSMaster = Depends(master_dependency)) -> list[SyncProgress]:
    return await master.list_requests()


@app.post("/workers/heartbeat")
async def worker_heartbeat(heartbeat: WorkerHeartbeat, master: DMSMaster = Depends(master_dependency)) -> dict:
    await master.worker_heartbeat(heartbeat)
    return {"status": "ok"}


@app.post("/workers/{worker_id}/assignment", response_model=Optional[Assignment])
async def next_assignment(worker_id: str, master: DMSMaster = Depends(master_dependency)) -> Optional[Assignment]:
    assignment = await master.next_assignment(worker_id)
    return assignment


@app.post("/workers/result")
async def report_result(result: SyncResult, master: DMSMaster = Depends(master_dependency)) -> dict:
    await master.report_result(result)
    return {"status": "ack"}


@app.delete("/sync/{request_id}")
async def delete_request(request_id: str, master: DMSMaster = Depends(master_dependency)) -> dict:
    await master.forget_request(request_id)
    return {"status": "deleted"}


@app.post("/sync/{request_id}/reassign")
async def reassign_request(
    request_id: str,
    payload: ReassignRequest,
    master: DMSMaster = Depends(master_dependency),
) -> dict:
    try:
        await master.reassign_request(request_id, payload.worker_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"status": "requeued", "request_id": request_id, "worker_id": payload.worker_id}


@app.get("/workers/{worker_id}/requests", response_model=list[SyncProgress])
async def list_worker_requests(
    worker_id: str, master: DMSMaster = Depends(master_dependency)
) -> list[SyncProgress]:
    return await master.list_requests_for_worker(worker_id)


@app.on_event("startup")
async def startup_event() -> None:
    await asyncio.sleep(0)
