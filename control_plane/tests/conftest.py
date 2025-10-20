import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONTROL_PLANE = ROOT / "control_plane"
if str(CONTROL_PLANE) not in sys.path:
    sys.path.insert(0, str(CONTROL_PLANE))
