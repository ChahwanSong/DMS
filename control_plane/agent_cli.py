"""Command line entry point for running a DMS worker agent."""
from __future__ import annotations

import argparse
import asyncio
import logging
from typing import Awaitable, Callable, Optional

from dms_agent import (
    AgentClient,
    AgentCommunicationError,
    AgentConfig,
    load_agent_config,
    run_agent,
)
from dms_master.models import (
    Assignment,
    DataPlaneEndpoint,
    SyncResult,
    WorkerHeartbeat,
    WorkerStatus,
)


class AgentStatusTracker:
    """Mutable container tracking the worker status for heartbeat emission."""

    __slots__ = ("status",)

    def __init__(self, initial: WorkerStatus = WorkerStatus.IDLE) -> None:
        self.status: WorkerStatus = initial


def build_heartbeat_factory(
    config: AgentConfig,
    status_tracker: AgentStatusTracker,
) -> Callable[[], WorkerHeartbeat]:
    """Create a heartbeat factory that reflects the latest worker status."""

    endpoints = [
        DataPlaneEndpoint(address=endpoint)
        for endpoint in config.network.data_plane_endpoints
    ]

    def heartbeat_factory() -> WorkerHeartbeat:
        return WorkerHeartbeat(
            worker_id=config.worker_id,
            status=status_tracker.status,
            control_plane_address=config.network.control_plane_address,
            data_plane_endpoints=endpoints,
            storage_paths=config.storage_paths,
        )

    return heartbeat_factory


async def _default_transfer_runner(assignment: Assignment) -> None:
    """Placeholder hook for integrating the C++ data plane."""

    logging.info("Received assignment payload: %s", assignment.dict())


def build_assignment_handler(
    config: AgentConfig,
    status_tracker: AgentStatusTracker,
    *,
    transfer_runner: Optional[Callable[[Assignment], Awaitable[None]]] = None,
) -> Callable[[Assignment], Awaitable[SyncResult]]:
    """Create an assignment handler that wraps the provided transfer runner."""

    runner = transfer_runner or _default_transfer_runner

    async def assignment_handler(assignment: Assignment) -> SyncResult:
        status_tracker.status = WorkerStatus.TRANSFERRING
        try:
            await runner(assignment)
        except Exception as exc:  # pragma: no cover - defensive logging path
            logging.exception("Transfer failed for request %s", assignment.request_id)
            return SyncResult(
                request_id=assignment.request_id,
                worker_id=config.worker_id,
                success=False,
                message=str(exc),
            )
        finally:
            status_tracker.status = WorkerStatus.IDLE

        return SyncResult(
            request_id=assignment.request_id,
            worker_id=config.worker_id,
            success=True,
            message="Transfer completed",
        )

    return assignment_handler


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a DMS worker agent")
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the agent YAML configuration set",
    )
    parser.add_argument(
        "--worker-id",
        required=True,
        help="Worker identifier to load from the configuration",
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=5.0,
        help="Seconds between heartbeat emissions",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging level for the agent process",
    )
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> None:
    config = load_agent_config(args.config, args.worker_id)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    status_tracker = AgentStatusTracker()
    heartbeat_factory = build_heartbeat_factory(
        config,
        status_tracker,
    )
    assignment_handler = build_assignment_handler(config, status_tracker)
    client = AgentClient(
        config.master_url,
        worker_id=config.worker_id,
        control_plane_bind=config.network.control_plane_address,
    )
    try:
        await run_agent(
            client,
            heartbeat_factory,
            assignment_handler,
            interval=args.heartbeat_interval,
        )
    except AgentCommunicationError as exc:
        logging.error("%s", exc)
        raise SystemExit(1)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        logging.info("Agent shutdown requested")


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    main()
