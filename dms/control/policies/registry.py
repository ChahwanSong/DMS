"""Simple plugin registry for scheduling policies."""
from __future__ import annotations

from typing import Dict, Type

from .base import SchedulingPolicy
from .round_robin import RoundRobinPolicy
from .test_policy import TestFixedPolicy

_REGISTRY: Dict[str, Type[SchedulingPolicy]] = {
    RoundRobinPolicy.name: RoundRobinPolicy,
    TestFixedPolicy.name: TestFixedPolicy,
}


def get_policy(name: str) -> SchedulingPolicy:
    try:
        policy_cls = _REGISTRY[name]
    except KeyError as exc:  # pragma: no cover - defensive
        raise ValueError(f"unknown scheduling policy '{name}'") from exc
    return policy_cls()


def register_policy(policy_cls: Type[SchedulingPolicy]) -> None:
    _REGISTRY[policy_cls.name] = policy_cls


__all__ = ["get_policy", "register_policy", "SchedulingPolicy"]
