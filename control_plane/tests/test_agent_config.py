from pathlib import Path

import pytest

pytest.importorskip("yaml")

from dms_agent import load_agent_config


def test_load_agent_config(tmp_path: Path) -> None:
    cfg = tmp_path / "agent.yml"
    cfg.write_text(
        """
master_url: http://localhost:8000
worker_id: worker-1
network:
  control_plane_iface: eth0
  control_plane_address: 10.0.0.10
  data_plane_iface: ib0
  data_plane_address: 192.168.1.10
"""
    )
    config = load_agent_config(cfg)
    assert config.worker_id == "worker-1"
    assert config.network.control_plane_iface == "eth0"
    assert config.network.data_plane_iface == "ib0"


def test_load_agent_config_missing_network(tmp_path: Path) -> None:
    cfg = tmp_path / "agent.yml"
    cfg.write_text(
        """
master_url: http://localhost:8000
worker_id: worker-1
"""
    )
    with pytest.raises(ValueError):
        load_agent_config(cfg)
