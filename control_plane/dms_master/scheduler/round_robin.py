"""Round robin scheduler implementation."""
from __future__ import annotations

from collections import deque
from typing import Iterable, List

from .base import SchedulerPolicy, registry


class RoundRobinPolicy(SchedulerPolicy):
    """Cycle through workers in a round-robin fashion."""

    def __init__(self) -> None:
        self._state: deque[str] | None = None

    def select_workers(self, workers: Iterable[str], required: int) -> List[str]:
        active = deque(sorted(workers))
        if self._state is None or set(self._state) != set(active):
            self._state = deque(active)
        result: List[str] = []
        if not self._state:
            return result
        for _ in range(min(required, len(self._state))):
            worker = self._state.popleft()
            result.append(worker)
            self._state.append(worker)
        return result


def register() -> None:
    registry.register("round_robin", RoundRobinPolicy)


register()
