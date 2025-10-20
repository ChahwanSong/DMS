from dms_master.scheduler.base import WorkerInterface, registry
import dms_master.scheduler.round_robin  # noqa: F401 ensure registration


def test_round_robin_registration():
    assert "round_robin" in registry.available()
    policy = registry.create("round_robin")
    workers = [
        WorkerInterface("worker-a", "ib0", "192.168.1.10"),
        WorkerInterface("worker-b", "ib0", "192.168.1.11"),
        WorkerInterface("worker-c", "ib0", "192.168.1.12"),
    ]
    selection = policy.select_workers(workers, 2)
    assert [worker.worker_id for worker in selection] == ["worker-a", "worker-b"]
    selection = policy.select_workers(workers, 2)
    assert [worker.worker_id for worker in selection] == ["worker-c", "worker-a"]
