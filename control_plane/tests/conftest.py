import asyncio
import inspect
import sys
from functools import wraps
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CONTROL_PLANE = ROOT / "control_plane"
if str(CONTROL_PLANE) not in sys.path:
    sys.path.insert(0, str(CONTROL_PLANE))


def _wrap_async(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))

    return wrapper


def pytest_collection_modifyitems(items):
    for item in items:
        obj = getattr(item, "obj", None)
        if obj and inspect.iscoroutinefunction(obj):
            item.obj = _wrap_async(obj)


def pytest_configure(config):
    config.addinivalue_line("markers", "asyncio: mark a test to run on the default asyncio event loop")
