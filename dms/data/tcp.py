"""TCP data plane implementation."""
from __future__ import annotations

import json
import os
import socket
import struct
from contextlib import closing
from pathlib import Path
from threading import Thread
from typing import Optional

from ..logging_utils import setup_logging
from .base import DataPlane, DataPlaneError, TransferContext

_HEADER_STRUCT = struct.Struct("!I")
_BUFFER_SIZE = 4 * 1024 * 1024
_LOGGER = setup_logging(__name__)


class TCPChunkServer(Thread):
    """Simple TCP server that receives chunk payloads."""

    def __init__(self, host: str, port: int, dest_root: Path, *, backlog: int = 128) -> None:
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.dest_root = dest_root
        self.backlog = backlog
        self._sock: Optional[socket.socket] = None
        self._running = False

    def run(self) -> None:  # type: ignore[override]
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_sock.bind((self.host, self.port))
            if self.port == 0:
                self.port = server_sock.getsockname()[1]
            server_sock.listen(self.backlog)
            self._sock = server_sock
            self._running = True
            while self._running:
                try:
                    conn, _ = server_sock.accept()
                except OSError:
                    break
                Thread(target=self._handle_connection, args=(conn,), daemon=True).start()

    def close(self) -> None:
        self._running = False
        if self._sock:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._sock.close()

    def _handle_connection(self, conn: socket.socket) -> None:
        with closing(conn) as sock:
            header_len_bytes = _recv_exact(sock, _HEADER_STRUCT.size)
            if not header_len_bytes:
                return
            (header_len,) = _HEADER_STRUCT.unpack(header_len_bytes)
            header_bytes = _recv_exact(sock, header_len)
            if not header_bytes:
                return
            header = json.loads(header_bytes.decode("utf-8"))
            rel_path = header["relative_path"]
            offset = int(header["offset"])
            length = int(header["length"])
            dest_path = self.dest_root / rel_path
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            flags = os.O_CREAT | os.O_WRONLY
            if hasattr(os, "O_BINARY"):
                flags |= os.O_BINARY
            fd = os.open(dest_path, flags, 0o644)
            try:
                os.lseek(fd, offset, os.SEEK_SET)
                remaining = length
                while remaining > 0:
                    chunk = sock.recv(min(_BUFFER_SIZE, remaining))
                    if not chunk:
                        raise DataPlaneError("unexpected EOF from sender")
                    os.write(fd, chunk)
                    remaining -= len(chunk)
            finally:
                os.close(fd)


class TCPDataPlane(DataPlane):
    """TCP implementation using per-chunk connections."""

    def __init__(self, *, connect_timeout: float = 30.0) -> None:
        self.connect_timeout = connect_timeout

    def transfer(self, ctx: TransferContext) -> None:  # type: ignore[override]
        chunk = ctx.chunk
        header = {
            "relative_path": ctx.relative_path,
            "offset": chunk.offset,
            "length": chunk.length,
            "request_id": ctx.request_id,
            "agent_id": ctx.agent_id,
        }
        try:
            with closing(socket.create_connection((ctx.peer_host, ctx.peer_port), timeout=self.connect_timeout)) as sock:
                header_bytes = json.dumps(header).encode("utf-8")
                sock.sendall(_HEADER_STRUCT.pack(len(header_bytes)))
                sock.sendall(header_bytes)
                with open(chunk.path, "rb", buffering=0) as fh:
                    fh.seek(chunk.offset)
                    remaining = chunk.length
                    while remaining > 0:
                        data = fh.read(min(_BUFFER_SIZE, remaining))
                        if not data:
                            raise DataPlaneError("unexpected EOF while reading source file")
                        sock.sendall(data)
                        remaining -= len(data)
        except OSError as exc:  # pragma: no cover - rarely triggered in tests
            raise DataPlaneError(f"TCP transfer failed: {exc}") from exc


def _recv_exact(sock: socket.socket, size: int) -> bytes:
    view = memoryview(bytearray(size))
    remaining = size
    offset = 0
    while remaining:
        n = sock.recv_into(view[offset:], remaining)
        if n == 0:
            return b""
        offset += n
        remaining -= n
    return view.tobytes()


__all__ = ["TCPChunkServer", "TCPDataPlane"]
