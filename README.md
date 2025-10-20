# Data Moving Service (DMS)

DMS is a reference implementation of a high-performance data synchronization service for
HPC environments. The system separates a Python-based control plane from a C++ data plane
to coordinate multi-cluster file transfers over dedicated control and data networks.

## Repository layout

```
control_plane/
  dms_master/            # Master orchestration service (FastAPI)
  dms_agent/             # Worker-side HTTP client helpers
  master_cli.py          # CLI entry point for running the master server
  requirements*.txt      # Python dependencies for runtime and testing
  tests/                 # pytest-based unit tests

data_plane/
  include/dms/           # C++20 public headers
  src/                   # Data plane implementation sources
  tests/                 # Executable unit tests built with CTest

LICENSE
README.md
```

## Control plane overview

The control plane exposes a REST API via FastAPI. The master server receives sync requests,
assigns file chunks to worker agents using a pluggable scheduler, and aggregates progress
and result logs. Worker agents send heartbeats over the control network, poll for
assignments, and report chunk completion or failures.

Key features:

- **Plugin scheduler:** Add new policies by registering subclasses of
  `dms_master.scheduler.base.SchedulerPolicy`. An example `round_robin` policy is provided.
- **Async orchestration:** Internal scheduling, heartbeats, and assignment queues leverage
  `asyncio` primitives to avoid blocking the master loop.
- **Logging utilities:** Centralized logging configuration for stream or file output.
- **Extensible agent client:** `dms_agent.AgentClient` implements the core HTTP contract to
  communicate with the master. Custom agents can combine it with site-specific data movers.

## Data plane overview

The data plane is written in C++20. It focuses on efficient chunking and transfer of files
with minimal memory overhead:

- `FileChunker` enumerates files and splits them into bounded-size chunks suitable for
  parallel transfer.
- `TransferManager` maintains a worker thread pool, converts transfer jobs into chunks, and
  streams data through an injectable `NetworkTransport` interface.
- `TcpTransport` implements a baseline TCP sender and can be replaced with future RDMA
  transports (e.g., RoCEv2) by implementing the same interface.

Unit tests validate the chunking logic and concurrent transfer manager behaviour.

## Prerequisites

- **Python:** 3.12 or newer.
- **C++ toolchain:** GCC ≥ 12 with CMake ≥ 3.20.
- **Redis (mandatory):** Redis backs all metadata in the master. Install it with your
  distribution package manager (`sudo apt install redis-server`) or refer to the official
  Redis documentation. Ensure the service is running before starting the DMS master.

## Python environment setup

```bash
cd control_plane
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

### Running the master server locally

```bash
# Ensure the virtual environment is active
export PYTHONPATH=$(pwd)
python master_cli.py --config example_master.yml --host 127.0.0.1 --port 8000
```

Example master configuration (`example_master.yml`):

```yaml
scheduler: round_robin
network:
  control_plane_iface: eth0
worker_heartbeat_timeout: 45.0
redis:
  host: localhost
  port: 6379
  db: 0
```

### Running a worker agent locally

Each agent has its own configuration describing which interfaces to use for control and data
plane communication. The YAML below (shipped as `example_agent.yml`) binds control-plane
traffic to `eth0` and advertises two InfiniBand adapters for the data plane. The master will
load balance assignments across the advertised interfaces:

```yaml
master_url: http://127.0.0.1:8000
worker_id: worker-a
network:
  control_plane_iface: eth0
  control_plane_address: 10.0.0.10
  data_plane_endpoints:
    - iface: ib0
      address: 192.168.100.10
    - iface: ib1
      address: 192.168.100.11
```

To start a basic asyncio worker that only acknowledges assignments you can run:

```python
import asyncio
from control_plane.dms_agent import (
    AgentClient,
    AgentConfig,
    load_agent_config,
    run_agent,
)
from control_plane.dms_master.models import (
    DataPlaneEndpoint,
    WorkerHeartbeat,
    WorkerStatus,
    SyncResult,
)

config = load_agent_config("example_agent.yml")

client = AgentClient(
    config.master_url,
    worker_id=config.worker_id,
    control_plane_bind=config.network.control_plane_address,
)


def heartbeat_factory():
    return WorkerHeartbeat(
        worker_id=config.worker_id,
        status=WorkerStatus.IDLE,
        free_bytes=10**12,
        control_plane_iface=config.network.control_plane_iface,
        control_plane_address=config.network.control_plane_address,
        data_plane_endpoints=[
            DataPlaneEndpoint(iface=ep.iface, address=ep.address)
            for ep in config.network.data_plane_endpoints
        ],
    )


async def assignment_handler(assignment):
    # Integrate with the C++ data plane here. For now we only acknowledge success.
    return SyncResult(
        request_id=assignment.request_id,
        worker_id=config.worker_id,
        success=True,
        data_plane_iface=assignment.data_plane_iface,
    )

asyncio.run(run_agent(client, heartbeat_factory, assignment_handler))
```

### Submitting a test sync request

```bash
curl -X POST http://127.0.0.1:8000/sync \
  -H 'Content-Type: application/json' \
  -d '{
        "request_id": "demo-1",
        "source_path": "/home/clusterA/demo",
        "destination_path": "/home/clusterB",
        "parallelism": 4,
        "chunk_size_mb": 64,
        "direction": "A_TO_B"
      }'
```

## C++ data plane build and test

```bash
cd data_plane
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

## End-to-end testing on a single node

1. Launch Redis locally (`sudo systemctl start redis-server`).
2. Start the master server with `python master_cli.py --config example_master.yml`.
3. In a second shell, start an agent using the sample script above. The agent will bind its
   control-plane client to the configured address and advertise the interfaces in heartbeats.
4. Submit a sync request with `curl` as shown earlier and observe the logs from both the master
   and the agent.
5. Use the C++ `TransferManager` in standalone mode to exercise the data-plane logic with the
   advertised data-plane interface when integrating into production.

## Production deployment notes

- Deploy dedicated master and worker nodes. Only workers require access to the data-plane
  network adapters.
- Configure interface bindings per worker using the agent YAML configuration so each process
  reports the correct interfaces and binds its control-plane client socket accordingly. The
  master only uses the control-plane adapter. You can advertise multiple data-plane
  interfaces per agent; the scheduler automatically load balances work across them.
- Implement a site-specific `NetworkTransport` derived from `dms::NetworkTransport` to map
  to the HPC fabric (e.g., RoCEv2) and plug it into worker agents.
- Extend `SchedulerPolicy` with policies tailored to server topology or storage locality.
- Request metadata is persisted in Redis. Provision a highly-available Redis deployment for
  production use and ensure the master can reach it over the management network.

## Running automated tests

```bash
# Python control plane tests
cd control_plane
PYTHONPATH=$(pwd) pytest

# C++ data plane tests
cd ../data_plane
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

## Failure isolation

The master maintains isolated request state, so a failed request (e.g., due to a corrupt
file) results in a logged failure without impacting other active requests. Logs are emitted
per request and can be aggregated using external tooling.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
