# DMS (Data Moving Service)

This repository provides a reference implementation of a high-performance data
moving service that coordinates directory synchronisation between two HPC
clusters. The control plane is modelled around gRPC APIs while the data plane
focuses on zero-copy streaming over TCP with a placeholder RDMA implementation
that can be replaced with a pyverbs-based backend in production.

## Repository layout

```
dms/
  common/        # Filesystem helpers and chunk scheduling utilities
  control/       # Master scheduler and agent worker logic
  data/          # Data plane implementations (TCP + RDMA stub)
  logging_utils.py
  config.py
  proto/         # Lightweight protobuf-like structures and grpc stubs
scripts/
  agent_server.py
  master_server.py
```

Unit tests exercise the chunk planner, TCP data plane and scheduler.

## Running unit tests

```
pytest
```

All tests are self-contained and do not require network connectivity beyond the
loopback interface used by the TCP data-plane tests.

## Control plane overview

* **Master scheduler** (`dms.control.master`):
  * Accepts sync requests and assigns file chunks to source agents in a
    round-robin fashion while balancing destination agents.
  * Maintains progress logs for each request.
* **Agent worker** (`dms.control.agent`):
  * Executes chunk assignments using the configured data plane.
  * Emits structured progress logs and reports success/failure back to the
    master process.
* **Protocol buffers** (`dms.proto`):
  * Contains lightweight dataclasses mirroring the `.proto` schema so unit tests
    can run offline. When `grpcio` is available the generated services can be
    wired in without further changes.

## Data plane overview

* **TCP** (`dms.data.tcp`): chunk-based streaming with constant memory usage and
  a dedicated receiver that writes data directly to destination files.
* **RDMA** (`dms.data.rdma`): placeholder that validates environment support and
  raises descriptive errors when the pyverbs stack is unavailable.

## Logging

The `dms.logging_utils` module configures JSON-formatted logging with
progress-aware helpers so that master and agents can expose consistent runtime
information.

## Example usage

Pseudo-code for running a minimal transfer locally:

```python
from pathlib import Path

from dms.config import AgentEndpoint, SyncRequest
from dms.control.agent import AgentWorker
from dms.control.master import MasterScheduler
from dms.data.tcp import TCPChunkServer, TCPDataPlane

source_root = Path("/home/clusterA/targetDir")
dest_root = Path("/home/clusterB")

source_agent = AgentEndpoint("a1", "127.0.0.1", 6000, 7000, True)
dest_agent = AgentEndpoint("b1", "127.0.0.1", 6001, 7001, False)

scheduler = MasterScheduler([source_agent], [dest_agent])
request = SyncRequest("sync-001", str(source_root), str(dest_root))
plans = scheduler.plan(request)

server = TCPChunkServer("127.0.0.1", dest_agent.data_port, dest_root)
server.start()

worker = AgentWorker(
    source_agent.agent_id,
    source_root=source_root,
    dest_root=dest_root,
    data_plane=TCPDataPlane(),
)
worker.execute(request.request_id, plans[source_agent.agent_id].assignments)
```

The `scripts/master_server.py` and `scripts/agent_server.py` modules illustrate
how to wire these primitives into standalone processes once gRPC dependencies
are available.
