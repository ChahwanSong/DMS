#!/usr/bin/env python3
"""Minimal master service CLI for testing the scheduler."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List

from dms.config import AgentEndpoint, SyncRequest
from dms.control.master import MasterScheduler


def _load_agents(payload: List[dict]) -> List[AgentEndpoint]:
    agents: List[AgentEndpoint] = []
    for entry in payload:
        agents.append(
            AgentEndpoint(
                agent_id=entry["agent_id"],
                host=entry["host"],
                control_port=int(entry["control_port"]),
                data_port=int(entry["data_port"]),
                is_source=bool(entry["is_source"]),
            )
        )
    return agents


def main() -> None:
    parser = argparse.ArgumentParser(description="Plan a DMS sync request")
    parser.add_argument("config", type=Path, help="JSON file describing agents")
    parser.add_argument("source", type=Path, help="Source directory")
    parser.add_argument("dest", type=Path, help="Destination directory")
    parser.add_argument("request_id", help="Identifier for the sync request")
    parser.add_argument("--chunk-size", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--transfer-mode", default="TCP", choices=["TCP", "RDMA"])
    parser.add_argument(
        "--policy",
        default="round_robin",
        help="Scheduling policy to use (default: round_robin)",
    )
    args = parser.parse_args()

    config = json.loads(args.config.read_text())
    source_agents = _load_agents(config["source_agents"])
    dest_agents = _load_agents(config["dest_agents"])

    scheduler = MasterScheduler(source_agents, dest_agents, policy_name=args.policy)
    request = SyncRequest(
        request_id=args.request_id,
        source_path=str(args.source.resolve()),
        dest_path=str(args.dest.resolve()),
        transfer_mode=args.transfer_mode,
        chunk_size=args.chunk_size,
        policy=args.policy,
    )

    plans = scheduler.plan(request)
    summary = {
        agent_id: {
            "chunks": len(plan.assignments),
            "total_bytes": plan.total_bytes,
        }
        for agent_id, plan in plans.items()
    }
    print(json.dumps({"request_id": request.request_id, "plans": summary}, indent=2))


if __name__ == "__main__":
    main()
