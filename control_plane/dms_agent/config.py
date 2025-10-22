"""Configuration helpers for DMS worker agents."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

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
    storage_paths: List[str]


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _as_list(value: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(value, Mapping):
        return [dict(worker_id=key, **(cfg or {})) for key, cfg in value.items()]
    if isinstance(value, list):
        return value
    raise ValueError("Agent configuration 'workers' must be a list or mapping")


def _select_worker_config(workers: Iterable[Dict[str, Any]], worker_id: str) -> Dict[str, Any]:
    for entry in workers:
        if entry.get("worker_id") == worker_id:
            return entry
    raise KeyError(f"Worker '{worker_id}' not found in agent configuration")


def _build_network(network_data: Optional[Dict[str, Any]]) -> AgentNetworkConfig:
    if not network_data:
        raise ValueError("Agent configuration requires a network section for each worker")
    required_fields = {"control_plane_iface", "control_plane_address", "data_plane_endpoints"}
    missing = required_fields - network_data.keys()
    if missing:
        raise ValueError(
            "Agent network configuration missing fields: " + ", ".join(sorted(missing))
        )
    endpoints_raw = network_data.get("data_plane_endpoints", [])
    if not isinstance(endpoints_raw, list) or not endpoints_raw:
        raise ValueError(
            "Agent network configuration requires at least one data plane endpoint"
        )
    endpoints = [AgentDataPlaneEndpoint(**item) for item in endpoints_raw]
    return AgentNetworkConfig(
        control_plane_iface=network_data["control_plane_iface"],
        control_plane_address=network_data["control_plane_address"],
        data_plane_endpoints=endpoints,
    )


def load_agent_config(path: str | Path, worker_id: str) -> AgentConfig:
    config_path = Path(path)
    data = _load_yaml(config_path)

    master_url = data.get("master_url")
    if not master_url:
        raise ValueError("Agent configuration requires master_url")

    workers_raw = data.get("workers")
    if not workers_raw:
        raise ValueError("Agent configuration requires a workers section")
    workers = _as_list(workers_raw)
    worker_data = _select_worker_config(workers, worker_id)

    network = _build_network(worker_data.get("network"))
    worker_master_url = worker_data.get("master_url", master_url)
    storage_paths_raw = worker_data.get("storage_paths")
    if not storage_paths_raw or not isinstance(storage_paths_raw, list):
        raise ValueError(
            "Agent configuration requires a non-empty list of storage_paths for each worker"
        )
    storage_paths = []
    for entry in storage_paths_raw:
        if not isinstance(entry, str):
            raise ValueError("Each storage_paths entry must be a string")
        if not Path(entry).is_absolute():
            raise ValueError(f"storage path '{entry}' must be an absolute path")
        storage_paths.append(entry)

    return AgentConfig(
        master_url=worker_master_url,
        worker_id=worker_id,
        network=network,
        storage_paths=storage_paths,
    )
