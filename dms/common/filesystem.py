"""Filesystem helpers for DMS."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, List


def list_files(root: Path) -> List[Path]:
    """Return all files under *root* sorted lexicographically."""

    if not root.exists():
        raise FileNotFoundError(f"Root path {root} does not exist")

    files: List[Path] = []
    for dirpath, _, filenames in os.walk(root):
        dir_path = Path(dirpath)
        for name in sorted(filenames):
            files.append(dir_path / name)
    files.sort()
    return files


def total_size(paths: Iterable[Path]) -> int:
    """Return the combined size of *paths* in bytes."""

    return sum(path.stat().st_size for path in paths)
