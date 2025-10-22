from pathlib import Path

import pytest

pytest.importorskip("yaml")

from dms_agent import load_agent_config


def test_load_agent_config(tmp_path: Path) -> None:
    cfg = tmp_path / "agent.yml"
    cfg.write_text(
        """
master_url: http://localhost:8000
workers:
  - worker_id: worker-1
    storage_paths:
      - /mnt/clusterA
    network:
      control_plane_iface: eth0
      control_plane_address: 10.0.0.10
      data_plane_endpoints:
        - iface: ib0
          address: 192.168.1.10
        - iface: ib1
          address: 192.168.1.11
  - worker_id: worker-2
    master_url: http://localhost:9000
    storage_paths:
      - /mnt/clusterB
      - /scratch
    network:
      control_plane_iface: eth1
      control_plane_address: 10.0.0.11
      data_plane_endpoints:
        - iface: ib2
          address: 192.168.1.12
"""
    )
    config = load_agent_config(cfg, "worker-2")
    assert config.worker_id == "worker-2"
    assert config.master_url == "http://localhost:9000"
    assert config.network.control_plane_iface == "eth1"
    assert len(config.network.data_plane_endpoints) == 1
    assert config.network.data_plane_endpoints[0].iface == "ib2"
    assert config.storage_paths == ["/mnt/clusterB", "/scratch"]


def test_load_agent_config_missing_network(tmp_path: Path) -> None:
    cfg = tmp_path / "agent.yml"
    cfg.write_text(
        """
master_url: http://localhost:8000
workers:
  - worker_id: worker-1
    storage_paths:
      - /mnt/storage
"""
    )
    with pytest.raises(ValueError):
        load_agent_config(cfg, "worker-1")


def test_load_agent_config_requires_data_plane_endpoints(tmp_path: Path) -> None:
    cfg = tmp_path / "agent.yml"
    cfg.write_text(
        """
master_url: http://localhost:8000
workers:
  - worker_id: worker-1
    storage_paths:
      - /mnt/storage
    network:
      control_plane_iface: eth0
      control_plane_address: 10.0.0.10
      data_plane_endpoints: []
"""
    )
    with pytest.raises(ValueError):
        load_agent_config(cfg, "worker-1")


def test_load_agent_config_missing_worker(tmp_path: Path) -> None:
    cfg = tmp_path / "agent.yml"
    cfg.write_text(
        """
master_url: http://localhost:8000
workers:
  - worker_id: worker-1
    storage_paths:
      - /mnt/storage
    network:
      control_plane_iface: eth0
      control_plane_address: 10.0.0.10
      data_plane_endpoints:
        - iface: ib0
          address: 192.168.1.10
"""
    )
    with pytest.raises(KeyError):
        load_agent_config(cfg, "worker-unknown")


def test_load_agent_config_requires_storage_paths(tmp_path: Path) -> None:
    cfg = tmp_path / "agent.yml"
    cfg.write_text(
        """
master_url: http://localhost:8000
workers:
  - worker_id: worker-1
    network:
      control_plane_iface: eth0
      control_plane_address: 10.0.0.10
      data_plane_endpoints:
        - iface: ib0
          address: 192.168.1.10
"""
    )
    with pytest.raises(ValueError):
        load_agent_config(cfg, "worker-1")
