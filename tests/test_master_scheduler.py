from pathlib import Path

from dms.config import AgentEndpoint, SyncRequest
from dms.control.master import MasterScheduler


def make_agent(idx: int, *, is_source: bool) -> AgentEndpoint:
    return AgentEndpoint(
        agent_id=f"agent-{idx}",
        host="127.0.0.1",
        control_port=6000 + idx,
        data_port=7000 + idx,
        is_source=is_source,
    )


def test_master_scheduler_plan(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    dest_root = tmp_path / "dest"
    source_root.mkdir()
    dest_root.mkdir()

    files = {
        "a.bin": b"a" * 10,
        "dir/b.bin": b"b" * 33,
    }
    for name, payload in files.items():
        path = source_root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)

    scheduler = MasterScheduler(
        [make_agent(1, is_source=True), make_agent(2, is_source=True)],
        [make_agent(3, is_source=False)],
    )
    request = SyncRequest(
        request_id="req-1",
        source_path=str(source_root),
        dest_path=str(dest_root),
        chunk_size=16,
    )

    plans = scheduler.plan(request)
    assert set(plans.keys()) == {"agent-1", "agent-2"}
    assert sum(plan.total_bytes for plan in plans.values()) == sum(len(v) for v in files.values())
    for plan in plans.values():
        assert all(assignment.peer_host == "127.0.0.1" for assignment in plan.assignments)
        assert all(assignment.chunk.length <= 16 for assignment in plan.assignments)
