import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def tcp_binary() -> Path:
    build_dir = ROOT / "cpp" / "build"
    source_dir = ROOT / "cpp"
    build_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run([
        "cmake",
        "-S",
        str(source_dir),
        "-B",
        str(build_dir),
        "-DCMAKE_BUILD_TYPE=Release",
    ], check=True)
    subprocess.run(["cmake", "--build", str(build_dir), "--target", "dms_tcp_transfer"], check=True)
    return build_dir / "dms_tcp_transfer"
