"""DMS worker agent package."""
from __future__ import annotations

from importlib import import_module
from typing import Any

from .config import (
    AgentConfig,
    AgentDataPlaneEndpoint,
    AgentNetworkConfig,
    load_agent_config,
)

__all__ = [
    "AgentClient",
    "AgentCommunicationError",
    "AgentConfig",
    "AgentDataPlaneEndpoint",
    "AgentNetworkConfig",
    "load_agent_config",
    "run_agent",
]


def __getattr__(name: str) -> Any:
    if name in {"AgentClient", "AgentCommunicationError", "run_agent"}:
        module = import_module(".client", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
