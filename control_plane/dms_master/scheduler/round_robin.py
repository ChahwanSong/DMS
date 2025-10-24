"""Round robin scheduler implementation."""
from __future__ import annotations

from typing import Iterable, List

from .base import SchedulerPolicy, WorkerInterface, registry


class RoundRobinPolicy(SchedulerPolicy):
    """Cycle through workers in a round-robin fashion."""

    def __init__(self) -> None:
        self._last_key: str | None = None

    def select_workers(
        self, workers: Iterable[WorkerInterface], required: int
    ) -> List[WorkerInterface]:
        active = sorted(workers, key=lambda w: w.worker_id)
        if not active:
            return []

        result: List[WorkerInterface] = []
        count = min(required, len(active))
        start_index = 0
        if self._last_key is not None:
            for idx, worker in enumerate(active):
                if worker.key == self._last_key:
                    start_index = (idx + 1) % len(active)
                    break
        index = start_index
        for _ in range(count):
            worker = active[index]
            result.append(worker)
            self._last_key = worker.key
            index = (index + 1) % len(active)
        return result


def register() -> None:
    registry.register("round_robin", RoundRobinPolicy)


register()
