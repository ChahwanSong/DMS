from pathlib import Path

from dms.common.chunker import FileChunk
from dms.data.base import TransferContext
from dms.data.tcp import TCPDataPlane, TCPReceiverProcess


def test_tcp_data_plane_transfers_chunk(tmp_path: Path, tcp_binary: Path) -> None:
    dest_root = tmp_path / "dest"
    dest_root.mkdir()

    server = TCPReceiverProcess(dest_root, binary_path=tcp_binary, host="127.0.0.1", port=0)
    port = server.start()
    assert port != 0

    source_file = tmp_path / "source.bin"
    payload = b"abcdef" * 1024
    source_file.write_bytes(payload)

    chunk = FileChunk(path=source_file, offset=0, length=len(payload))
    ctx = TransferContext(
        request_id="req-1",
        agent_id="agent-A",
        relative_path="data.bin",
        chunk=chunk,
        peer_host="127.0.0.1",
        peer_port=port,
        dest_root=dest_root,
    )

    plane = TCPDataPlane(binary_path=tcp_binary)
    plane.transfer(ctx)
    server.wait(timeout=5)
    received_path = dest_root / ctx.relative_path
    assert received_path.exists()
    assert received_path.read_bytes() == payload

    server.close()
