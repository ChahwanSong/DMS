"""Scheduler policy base classes for DMS master."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, List


@dataclass(frozen=True)
class WorkerInterface:
    """Identifies a worker's data-plane interface advertised to the master."""

    worker_id: str
    address: str

    @property
    def key(self) -> str:
        return f"{self.worker_id}::{self.address}"


class SchedulerPolicy(ABC):
    """Abstract base class for master scheduling policies.

    A policy receives a list of available worker identifiers and must return
    the ordered subset that should receive the next chunks of work. Policies
    are stateless unless they explicitly keep track of assignments.
    """

    @abstractmethod
    def select_workers(self, workers: Iterable[WorkerInterface], required: int) -> List[WorkerInterface]:
        """Pick workers to handle a piece of work.

        Args:
            workers: Iterable of currently available worker identifiers.
            required: Number of workers requested by the master. The policy may
                return fewer workers if insufficient workers are available.

        Returns:
            List of worker identifiers in the order they should be used.
        """


class SchedulerRegistry:
    """Runtime registry that maps string names to scheduler classes."""

    def __init__(self) -> None:
        self._registry: dict[str, type[SchedulerPolicy]] = {}

    def register(self, name: str, policy_cls: type[SchedulerPolicy]) -> None:
        if name in self._registry:
            raise ValueError(f"Policy '{name}' already registered")
        self._registry[name] = policy_cls

    def create(self, name: str, **kwargs) -> SchedulerPolicy:
        try:
            policy_cls = self._registry[name]
        except KeyError as exc:
            raise KeyError(f"Unknown policy '{name}'. Registered: {list(self._registry)}") from exc
        return policy_cls(**kwargs)

    def available(self) -> List[str]:
        return list(self._registry.keys())


registry = SchedulerRegistry()
