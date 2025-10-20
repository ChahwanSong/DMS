"""Configuration helpers for DMS worker agents."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class AgentDataPlaneEndpoint:
    iface: str
    address: str


@dataclass
class AgentNetworkConfig:
    control_plane_iface: str
    control_plane_address: str
    data_plane_endpoints: List[AgentDataPlaneEndpoint]


@dataclass
class AgentConfig:
    master_url: str
    worker_id: str
    network: AgentNetworkConfig


def load_agent_config(path: str | Path) -> AgentConfig:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(path)
    with config_path.open("r", encoding="utf-8") as fh:
        data: Dict[str, Any] = yaml.safe_load(fh) or {}

    network_data: Optional[Dict[str, Any]] = data.get("network")
    if not network_data:
        raise ValueError("Agent configuration requires a network section")
    required_network_fields = {
        "control_plane_iface",
        "control_plane_address",
        "data_plane_endpoints",
    }
    missing = required_network_fields - network_data.keys()
    if missing:
        raise ValueError(f"Agent network configuration missing fields: {', '.join(sorted(missing))}")
    endpoints_raw = network_data.get("data_plane_endpoints", [])
    if not isinstance(endpoints_raw, list) or not endpoints_raw:
        raise ValueError("Agent network configuration requires at least one data plane endpoint")
    endpoints = [AgentDataPlaneEndpoint(**item) for item in endpoints_raw]
    network = AgentNetworkConfig(
        control_plane_iface=network_data["control_plane_iface"],
        control_plane_address=network_data["control_plane_address"],
        data_plane_endpoints=endpoints,
    )
    master_url = data.get("master_url")
    if not master_url:
        raise ValueError("Agent configuration requires master_url")
    worker_id = data.get("worker_id")
    if not worker_id:
        raise ValueError("Agent configuration requires worker_id")
    return AgentConfig(master_url=master_url, worker_id=worker_id, network=network)
