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


def test_round_robin_respects_rotation_with_changing_workers():
    policy = registry.create("round_robin")
    worker_a = WorkerInterface("worker-a", "ib0", "192.168.1.10")
    worker_b = WorkerInterface("worker-b", "ib0", "192.168.1.11")

    # First assignment goes to worker A
    selection = policy.select_workers([worker_a, worker_b], 1)
    assert [worker.worker_id for worker in selection] == ["worker-a"]

    # When only worker B is available, it should receive work
    selection = policy.select_workers([worker_b], 1)
    assert [worker.worker_id for worker in selection] == ["worker-b"]

    # Once worker A returns, it should be next in line
    selection = policy.select_workers([worker_a, worker_b], 1)
    assert [worker.worker_id for worker in selection] == ["worker-a"]
