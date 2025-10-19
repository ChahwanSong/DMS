#!/usr/bin/env python3
"""Run an agent worker using a JSON plan."""
from __future__ import annotations

import argparse
import json
import signal
import sys
from pathlib import Path
from typing import Iterable

from dms.common.chunker import FileAssignment, FileChunk
from dms.control.agent import AgentWorker
from dms.data.tcp import TCPChunkServer, TCPDataPlane
from dms.data.rdma import RDMADataplane
from dms.logging_utils import setup_logging

_LOGGER = setup_logging("agent-server")


def _parse_assignments(root: Path, payload: Iterable[dict]) -> Iterable[FileAssignment]:
    assignments = []
    for entry in payload:
        chunk = FileChunk(
            path=root / entry["relative_path"],
            offset=int(entry["offset"]),
            length=int(entry["length"]),
        )
        assignments.append(
            FileAssignment(
                relative_path=entry["relative_path"],
                chunk=chunk,
                agent_id=entry["agent_id"],
                peer_host=entry["peer_host"],
                peer_port=int(entry["peer_port"]),
                is_sender=True,
            )
        )
    return assignments


def _select_data_plane(mode: str):
    if mode.upper() == "RDMA":
        return RDMADataplane()
    return TCPDataPlane()


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute a DMS agent plan")
    parser.add_argument("plan", type=Path, help="JSON file with assignments")
    parser.add_argument("agent_id")
    parser.add_argument("source_root", type=Path)
    parser.add_argument("dest_root", type=Path)
    parser.add_argument("--data-port", type=int, default=0)
    parser.add_argument("--mode", choices=["TCP", "RDMA"], default="TCP")
    args = parser.parse_args()

    payload = json.loads(args.plan.read_text())
    assignments = list(_parse_assignments(args.source_root, payload["assignments"]))

    data_plane = _select_data_plane(args.mode)

    server = None
    if args.mode == "TCP":
        server = TCPChunkServer("0.0.0.0", args.data_port, args.dest_root)
        server.start()
        _LOGGER.info("started TCP chunk server", extra={"_dms_agent_id": args.agent_id, "_dms_port": server.port})

    worker = AgentWorker(
        args.agent_id,
        source_root=args.source_root,
        dest_root=args.dest_root,
        data_plane=data_plane,
    )

    def _shutdown(signum, frame):  # pragma: no cover - signal handling
        _LOGGER.info("shutting down", extra={"_dms_agent_id": args.agent_id, "_dms_signal": signum})
        if server:
            server.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    worker.execute(payload["request_id"], assignments)

    if server:
        server.close()
        server.join(timeout=1)


if __name__ == "__main__":
    main()
