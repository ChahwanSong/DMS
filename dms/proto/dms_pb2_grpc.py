"""gRPC service stubs with optional runtime dependency."""
from __future__ import annotations

try:  # pragma: no cover - import guard
    import grpc  # type: ignore
except Exception:  # pragma: no cover - import guard
    grpc = None  # type: ignore

from . import dms_pb2 as dms__pb2  # noqa: F401  (re-export for compatibility)


class MasterServiceServicer:
    """Base servicer for MasterService."""

    def SubmitSync(self, request, context):  # pragma: no cover - to be implemented by user
        raise NotImplementedError("SubmitSync is not implemented")

    def GetStatus(self, request, context):  # pragma: no cover
        raise NotImplementedError("GetStatus is not implemented")


class AgentServiceServicer:
    """Base servicer for AgentService."""

    def ExecuteTask(self, request, context):  # pragma: no cover
        raise NotImplementedError("ExecuteTask is not implemented")


def add_MasterServiceServicer_to_server(servicer, server):  # pragma: no cover - gRPC integration
    if grpc is None:
        raise RuntimeError("grpcio must be installed to register services")
    rpc_method_handlers = {
        "SubmitSync": grpc.unary_unary_rpc_method_handler(
            servicer.SubmitSync,
        ),
        "GetStatus": grpc.unary_unary_rpc_method_handler(
            servicer.GetStatus,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler("dms.MasterService", rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


def add_AgentServiceServicer_to_server(servicer, server):  # pragma: no cover - gRPC integration
    if grpc is None:
        raise RuntimeError("grpcio must be installed to register services")
    rpc_method_handlers = {
        "ExecuteTask": grpc.unary_stream_rpc_method_handler(
            servicer.ExecuteTask,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler("dms.AgentService", rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


__all__ = [
    "MasterServiceServicer",
    "AgentServiceServicer",
    "add_MasterServiceServicer_to_server",
    "add_AgentServiceServicer_to_server",
]
