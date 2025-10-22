"""CLI entry point for launching the DMS master."""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Awaitable, Iterable, Sequence

import pytest
import uvicorn

from dms_master.app import app, get_master
from dms_master.server import DMSMaster


DEFAULT_TEST_PATHS: Sequence[str] = ("control_plane/tests",)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _run_async(coro: Awaitable[None]) -> None:
    """Execute an asynchronous coroutine regardless of loop state."""

    try:
        asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()


def ensure_redis_available(master: DMSMaster) -> None:
    """Verify that the Redis dependency is reachable before starting the server."""

    metadata = getattr(master, "metadata", None)
    health_check = getattr(metadata, "health_check", None)
    if not callable(health_check):
        return
    try:
        _run_async(health_check())
    except Exception as exc:  # pragma: no cover - defensive guard
        raise RuntimeError(f"Redis dependency check failed: {exc}") from exc


def run_startup_checks(master: DMSMaster, test_paths: Iterable[str] | None = None) -> None:
    """Run Redis availability and test-suite checks before serving traffic."""

    try:
        ensure_redis_available(master)
    except RuntimeError as exc:
        print(f"Master startup aborted: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    paths = list(test_paths or DEFAULT_TEST_PATHS)
    result = pytest.main(paths)
    if result != 0:
        print(
            "Master startup aborted: control plane tests failed. See pytest output for details.",
            file=sys.stderr,
        )
        raise SystemExit(result)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the DMS master control plane server")
    parser.add_argument("--config", help="Path to YAML configuration", default=None)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    # Preload master with configuration.
    get_master.cache_clear()  # type: ignore[attr-defined]
    try:
        master = get_master(args.config)
    except Exception as exc:
        print(f"Master startup aborted: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    run_startup_checks(master)

    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
