from pathlib import Path

from dms.common.chunker import chunk_file


def test_chunk_file(tmp_path: Path) -> None:
    data = b"hello" * 10
    file_path = tmp_path / "file.bin"
    file_path.write_bytes(data)

    chunks = list(chunk_file(file_path, 7))
    assert len(chunks) == 8
    assert chunks[0].offset == 0
    assert chunks[0].length == 7
    assert chunks[-1].length == len(data) - 7 * 7


def test_chunk_file_zero_length(tmp_path: Path) -> None:
    file_path = tmp_path / "empty.bin"
    file_path.write_bytes(b"")

    chunks = list(chunk_file(file_path, 8))
    assert len(chunks) == 1
    assert chunks[0].length == 0
