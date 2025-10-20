"""Configuration utilities for the DMS master server."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

try:  # pragma: no cover - optional dependency
    import yaml  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback when PyYAML unavailable
    yaml = None


@dataclass
class NetworkInterfaceConfig:
    control_plane_iface: str
    data_plane_iface: str | None = None


@dataclass
class RedisConfig:
    host: str = "localhost"
    port: int = 6379
    db: int = 0


@dataclass
class MasterConfig:
    scheduler: str = "round_robin"
    network: NetworkInterfaceConfig | None = None
    redis: RedisConfig = field(default_factory=RedisConfig)
    worker_heartbeat_timeout: float = 30.0


DEFAULT_CONFIG = MasterConfig()


def load_config(path: str | Path | None) -> MasterConfig:
    if path is None:
        return DEFAULT_CONFIG
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(path)
    if yaml is None:
        raise RuntimeError(
            "PyYAML is required to parse configuration files. Install pyyaml or omit the --config flag."
        )
    with config_path.open("r", encoding="utf-8") as fh:
        data: Dict[str, Any] = yaml.safe_load(fh) or {}
    network_cfg = data.get("network")
    network = None
    if network_cfg:
        network = NetworkInterfaceConfig(**network_cfg)
    redis_cfg = data.get("redis")
    if not redis_cfg:
        raise ValueError("Redis configuration is required in the master configuration file")
    redis = RedisConfig(**redis_cfg)
    scheduler = data.get("scheduler", DEFAULT_CONFIG.scheduler)
    heartbeat_timeout = data.get(
        "worker_heartbeat_timeout", DEFAULT_CONFIG.worker_heartbeat_timeout
    )
    return MasterConfig(
        scheduler=scheduler,
        network=network,
        redis=redis,
        worker_heartbeat_timeout=heartbeat_timeout,
    )
