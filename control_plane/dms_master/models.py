"""Shared Pydantic models used by the control plane."""

from __future__ import annotations

from datetime import datetime, UTC
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from pydantic import BaseModel, Field, ValidationError, model_validator
except ImportError:  # pragma: no cover - tests still run even if model_validator missing
    from pydantic import BaseModel, Field, ValidationError

    def model_validator(*_args: Any, **_kwargs: Any):
        def decorator(func):
            return func

        return decorator


try:  # pragma: no cover - keep compatibility with environments lacking model_validator
    from pydantic.error_wrappers import ErrorWrapper
except ImportError:  # pragma: no cover - pydantic v2 path
    ErrorWrapper = None  # type: ignore[assignment]


class _AbsolutePathError(ValueError):
    def __init__(self, field_name: str) -> None:
        self.field_name = field_name
        super().__init__(f"{field_name} must be an absolute path")


class SyncRequest(BaseModel):
    request_id: str = Field(..., description="Unique identifier for the sync request")
    source_path: str
    destination_path: str
    file_list: Optional[List[str]] = Field(
        None, description="Optional subset of files to sync instead of full directory"
    )
    chunk_size_mb: int = Field(64, ge=1, le=1024)

    def __init__(self, **data: Any) -> None:  # type: ignore[override]
        try:
            super().__init__(**data)
            self._enforce_absolute_paths()
        except _AbsolutePathError as exc:
            raise self._as_validation_error(exc) from exc

    @model_validator(mode="after")
    def _validate_absolute_paths(self) -> "SyncRequest":
        self._check_absolute_path("source_path", getattr(self, "source_path", None))
        self._check_absolute_path(
            "destination_path", getattr(self, "destination_path", None)
        )
        return self

    def _enforce_absolute_paths(self) -> None:
        for field_name in ("source_path", "destination_path"):
            self._check_absolute_path(field_name, getattr(self, field_name, None))

    @staticmethod
    def _check_absolute_path(field_name: str, value: Optional[str]) -> None:
        if value is None:
            return
        if not Path(value).is_absolute():
            raise _AbsolutePathError(field_name)

    @classmethod
    def _as_validation_error(cls, exc: _AbsolutePathError) -> ValidationError:
        if ErrorWrapper is not None:
            return ValidationError([ErrorWrapper(exc, loc=exc.field_name)], cls)
        return ValidationError.from_exception_data(  # type: ignore[attr-defined]
            cls.__name__,
            [
                {
                    "type": "value_error",
                    "loc": (exc.field_name,),
                    "msg": str(exc),
                    "input": None,
                }
            ],
        )


class WorkerStatus(str, Enum):
    IDLE = "IDLE"
    TRANSFERRING = "TRANSFERRING"
    ERROR = "ERROR"


class DataPlaneEndpoint(BaseModel):
    address: str


class WorkerHeartbeat(BaseModel):
    worker_id: str
    status: WorkerStatus
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    control_plane_address: Optional[str] = None
    data_plane_endpoints: List[DataPlaneEndpoint] = Field(default_factory=list)
    storage_paths: List[str] = Field(
        default_factory=list,
        description="List of directories accessible from this worker",
    )


class SyncProgress(BaseModel):
    request_id: str
    transferred_bytes: int
    total_bytes: int
    started_at: datetime
    updated_at: datetime
    state: str
    detail: Dict[str, str] = Field(default_factory=dict)


class Assignment(BaseModel):
    request_id: str
    worker_id: str
    source_path: str
    destination_path: str
    chunk_offset: int = Field(
        ...,
        description="Byte offset within the source object where this transfer chunk begins",
    )
    chunk_size: int = Field(
        ...,
        description="Number of bytes to transfer for this chunk",
    )
    source_worker_pool: List[str] = Field(
        default_factory=list,
        description="Workers that can access the sync source",
    )
    destination_worker_pool: List[str] = Field(
        default_factory=list,
        description="Workers that can access the sync destination",
    )


class SyncResult(BaseModel):
    request_id: str
    worker_id: str
    success: bool
    message: str = ""
    completed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    data_plane_address: Optional[str] = None


class ReassignRequest(BaseModel):
    worker_id: str
