import types

import pytest

from master_cli import ensure_redis_available, run_startup_checks


class DummyMetadata:
    def __init__(self, health_exc: Exception | None = None):
        self._health_exc = health_exc
        self.invocations = 0

    async def health_check(self) -> None:
        self.invocations += 1
        if self._health_exc:
            raise self._health_exc


class DummyMaster:
    def __init__(self, metadata):
        self.metadata = metadata


def test_ensure_redis_available_runs_health_check():
    metadata = DummyMetadata()
    master = DummyMaster(metadata)

    ensure_redis_available(master)

    assert metadata.invocations == 1


def test_ensure_redis_available_raises_runtime_error_on_failure():
    error = RuntimeError("ping failed")
    metadata = DummyMetadata(error)
    master = DummyMaster(metadata)

    with pytest.raises(RuntimeError) as exc:
        ensure_redis_available(master)

    assert "Redis dependency check failed" in str(exc.value)


def test_run_startup_checks_exits_when_pytest_fails(monkeypatch, capsys):
    metadata = DummyMetadata()
    master = DummyMaster(metadata)

    monkeypatch.setattr("master_cli.ensure_redis_available", lambda m: None)
    monkeypatch.setattr("master_cli.pytest", types.SimpleNamespace(main=lambda args: 1))

    with pytest.raises(SystemExit) as exc:
        run_startup_checks(master, ["tests"])

    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "tests failed" in captured.err


def test_run_startup_checks_exits_when_redis_unavailable(monkeypatch, capsys):
    metadata = DummyMetadata()
    master = DummyMaster(metadata)

    monkeypatch.setattr(
        "master_cli.ensure_redis_available", lambda m: (_ for _ in ()).throw(RuntimeError("no redis"))
    )
    monkeypatch.setattr("master_cli.pytest", types.SimpleNamespace(main=lambda args: 0))

    with pytest.raises(SystemExit) as exc:
        run_startup_checks(master, ["tests"])

    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "no redis" in captured.err


def test_run_startup_checks_passes_on_success(monkeypatch):
    metadata = DummyMetadata()
    master = DummyMaster(metadata)

    monkeypatch.setattr("master_cli.ensure_redis_available", lambda m: None)
    monkeypatch.setattr("master_cli.pytest", types.SimpleNamespace(main=lambda args: 0))

    run_startup_checks(master, ["tests"])
