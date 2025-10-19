"""RDMA data plane skeleton using RoCEv2."""
from __future__ import annotations

from .base import DataPlane, DataPlaneError, TransferContext


class RDMADataplane(DataPlane):
    """Placeholder RDMA implementation.

    The class documents the interface expected by the rest of the system. A
    production environment can replace this stub with a pyverbs based RDMA RC
    implementation that manages queue pairs and memory regions. Attempting to
    use the stub will raise ``DataPlaneError`` to avoid silent fallbacks.
    """

    def __init__(self) -> None:
        self._available = self._check_pyverbs()

    @staticmethod
    def _check_pyverbs() -> bool:
        try:  # pragma: no cover - optional dependency
            import pyverbs  # type: ignore  # noqa: F401
        except Exception:  # pragma: no cover - import guard
            return False
        return True

    def transfer(self, ctx: TransferContext) -> None:  # type: ignore[override]
        if not self._available:
            raise DataPlaneError(
                "pyverbs is required for RDMA transfers but is not installed or failed to load",
            )
        raise DataPlaneError(
            "RDMA data plane is not implemented in this environment; install a backend and replace the stub",
        )


__all__ = ["RDMADataplane"]
