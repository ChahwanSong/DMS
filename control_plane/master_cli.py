"""CLI entry point for launching the DMS master."""
from __future__ import annotations

import argparse
import uvicorn

from dms_master.app import app, get_master


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the DMS master control plane server")
    parser.add_argument("--config", help="Path to YAML configuration", default=None)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    # Preload master with configuration.
    get_master.cache_clear()  # type: ignore[attr-defined]
    get_master(args.config)

    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
