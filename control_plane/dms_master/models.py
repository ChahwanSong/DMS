"""Shared Pydantic models used by the control plane."""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

try:  # pragma: no cover - prefer real pydantic when available
    from pydantic import BaseModel, Field
except ModuleNotFoundError:  # pragma: no cover - minimal fallback for test environments

    _UNSET = object()

    class _FieldInfo:
        __slots__ = ("default", "default_factory")

        def __init__(self, default: Any = _UNSET, default_factory=None, **_: Any) -> None:
            self.default = default
            self.default_factory = default_factory

    def Field(default: Any = _UNSET, *, default_factory=None, **_: Any) -> Any:
        return _FieldInfo(default=default, default_factory=default_factory)

    class BaseModel:
        """Very small subset of the Pydantic interface used in tests."""

        def __init__(self, **data: Any) -> None:
            for name in self.__annotations__:
                value = data.get(name, None)
                default = getattr(self.__class__, name, None)
                if isinstance(default, _FieldInfo):
                    if value is None:
                        if default.default_factory is not None:
                            value = default.default_factory()
                        elif default.default is _UNSET:
                            raise TypeError(f"Missing required field '{name}'")
                        else:
                            value = default.default
                    setattr(self, name, value)
                else:
                    if value is None and default is not None:
                        value = default
                    setattr(self, name, value)

        def dict(self) -> Dict[str, Any]:
            return {name: getattr(self, name) for name in self.__annotations__}

        @classmethod
        def parse_obj(cls, obj: Dict[str, Any]) -> "BaseModel":
            return cls(**obj)


class SyncDirection(str, Enum):
    A_TO_B = "A_TO_B"
    B_TO_A = "B_TO_A"


class SyncRequest(BaseModel):
    request_id: str = Field(..., description="Unique identifier for the sync request")
    source_path: str
    destination_path: str
    file_list: Optional[List[str]] = Field(
        None, description="Optional subset of files to sync instead of full directory"
    )
    parallelism: int = Field(4, ge=1, le=64)
    chunk_size_mb: int = Field(64, ge=1, le=1024)
    direction: SyncDirection = SyncDirection.A_TO_B


class WorkerStatus(str, Enum):
    IDLE = "IDLE"
    TRANSFERRING = "TRANSFERRING"
    ERROR = "ERROR"


class DataPlaneEndpoint(BaseModel):
    iface: str
    address: str


class WorkerHeartbeat(BaseModel):
    worker_id: str
    status: WorkerStatus
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    control_plane_iface: Optional[str] = None
    control_plane_address: Optional[str] = None
    data_plane_endpoints: List[DataPlaneEndpoint] = Field(default_factory=list)


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
    file_path: str
    chunk_offset: int
    chunk_size: int
    data_plane_iface: Optional[str] = None
    data_plane_address: Optional[str] = None


class SyncResult(BaseModel):
    request_id: str
    worker_id: str
    success: bool
    message: str = ""
    completed_at: datetime = Field(default_factory=datetime.utcnow)
    data_plane_iface: Optional[str] = None
    data_plane_address: Optional[str] = None
