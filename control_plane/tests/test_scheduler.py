from dms_master.scheduler.base import registry
import dms_master.scheduler.round_robin  # noqa: F401 ensure registration


def test_round_robin_registration():
    assert "round_robin" in registry.available()
    policy = registry.create("round_robin")
    workers = ["worker-a", "worker-b", "worker-c"]
    selection = policy.select_workers(workers, 2)
    assert selection == ["worker-a", "worker-b"]
    selection = policy.select_workers(workers, 2)
    assert selection == ["worker-c", "worker-a"]
