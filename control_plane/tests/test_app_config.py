from __future__ import annotations

from pathlib import Path

import pytest

from dms_master import app as master_app


@pytest.fixture(autouse=True)
def clear_master_cache():
    master_app.get_master.cache_clear()  # type: ignore[attr-defined]
    try:
        yield
    finally:
        master_app.get_master.cache_clear()  # type: ignore[attr-defined]


def test_get_master_reuses_provided_config_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    config_path = tmp_path / "master.yml"
    config_path.write_text(
        """
worker_heartbeat_timeout: 45.0
redis:
  host: localhost
  port: 6379
  db: 0
  expiry_days: 1
""".strip()
    )

    created_configs: list = []

    class DummyMaster:
        def __init__(self, config):
            created_configs.append(config)

    monkeypatch.setattr(master_app, "DMSMaster", DummyMaster)

    first = master_app.get_master(str(config_path))
    assert isinstance(first, DummyMaster)
    assert created_configs[0].worker_heartbeat_timeout == 45.0

    second = master_app.get_master()
    assert second is first
    assert len(created_configs) == 1
