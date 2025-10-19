"""TCP data plane wrapper around the C++ implementation."""
from __future__ import annotations

import os
import select
import subprocess
import time
from pathlib import Path
from typing import Optional

from ..logging_utils import setup_logging
from .base import DataPlane, DataPlaneError, TransferContext

_DEFAULT_BINARY = Path(__file__).resolve().parents[2] / "cpp" / "build" / "dms_tcp_transfer"
_LOGGER = setup_logging(__name__)


class TCPReceiverProcess:
    """Launch the C++ receiver helper as a subprocess."""

    def __init__(
        self,
        dest_root: Path,
        *,
        binary_path: Optional[Path] = None,
        host: str = "0.0.0.0",
        port: int = 0,
        start_timeout: float = 10.0,
    ) -> None:
        self.dest_root = dest_root
        self.binary_path = Path(binary_path or _DEFAULT_BINARY)
        self.host = host
        self.requested_port = port
        self.start_timeout = start_timeout
        self._proc: Optional[subprocess.Popen[str]] = None
        self._port: Optional[int] = None

    @property
    def port(self) -> int:
        if self._port is None:
            return 0
        return self._port

    def start(self) -> int:
        if self._proc is not None:
            raise RuntimeError("receiver already started")
        if not self.binary_path.exists():
            raise FileNotFoundError(f"TCP data plane binary not found: {self.binary_path}")

        cmd = [
            str(self.binary_path),
            "receive",
            "--bind",
            self.host,
            "--port",
            str(self.requested_port),
            "--dest-root",
            str(self.dest_root),
        ]
        _LOGGER.debug("starting TCP receiver", extra={"cmd": cmd})
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self._proc = proc

        deadline = time.monotonic() + self.start_timeout
        stdout = proc.stdout
        assert stdout is not None
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                stderr = proc.stderr.read() if proc.stderr else ""
                raise DataPlaneError(f"receiver exited early: {stderr.strip()}")
            ready, _, _ = select.select([stdout], [], [], 0.1)
            if ready:
                line = stdout.readline().strip()
                if line.startswith("PORT="):
                    self._port = int(line.split("=", 1)[1])
                    return self._port
        raise DataPlaneError("receiver did not report listening port in time")

    def wait(self, timeout: Optional[float] = None) -> int:
        if self._proc is None:
            return 0
        try:
            return_code = self._proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:  # pragma: no cover - defensive
            raise DataPlaneError("receiver timed out waiting for completion") from exc
        if return_code != 0:
            stderr = self._proc.stderr.read() if self._proc.stderr else ""
            raise DataPlaneError(f"receiver failed with code {return_code}: {stderr.strip()}")
        return return_code

    def close(self) -> None:
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=2)
            except subprocess.TimeoutExpired:  # pragma: no cover - defensive
                self._proc.kill()
        self._proc = None
        self._port = None


class TCPDataPlane(DataPlane):
    """Python wrapper that delegates chunk movement to the C++ binary."""

    def __init__(self, *, binary_path: Optional[Path] = None) -> None:
        self.binary_path = Path(binary_path or _DEFAULT_BINARY)
        if not self.binary_path.exists():
            raise FileNotFoundError(
                f"TCP data plane binary not found: {self.binary_path}. Build it via cmake first."
            )

    def transfer(self, ctx: TransferContext) -> None:  # type: ignore[override]
        chunk = ctx.chunk
        cmd = [
            str(self.binary_path),
            "send",
            "--host",
            ctx.peer_host,
            "--port",
            str(ctx.peer_port),
            "--file",
            str(chunk.path),
            "--relative-path",
            ctx.relative_path,
            "--offset",
            str(chunk.offset),
            "--length",
            str(chunk.length),
        ]
        env = os.environ.copy()
        _LOGGER.debug("executing TCP send", extra={"cmd": cmd})
        try:
            subprocess.run(cmd, env=env, check=True)
        except subprocess.CalledProcessError as exc:
            raise DataPlaneError(f"TCP transfer failed with exit code {exc.returncode}") from exc


__all__ = ["TCPDataPlane", "TCPReceiverProcess"]
