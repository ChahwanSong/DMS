# DMS (Data Moving Service)

This repository provides a reference implementation of a high-performance data
moving service that coordinates directory synchronisation between two HPC
clusters. The control plane is implemented in Python and exposes the core
scheduler/agent orchestration, while the data plane is implemented in C++ for
high-throughput chunk streaming over TCP. An RDMA placeholder remains so the
interface can be swapped with an actual RoCEv2 backend later on.

## Repository layout

```
dms/
  common/        # Filesystem helpers and chunk scheduling utilities
  control/       # Master scheduler and agent worker logic
  control/policies/  # Pluggable scheduling policies
  data/          # Python wrappers for C++ data plane helpers + RDMA stub
  logging_utils.py
  config.py
  proto/         # Lightweight protobuf-like structures and grpc stubs
cpp/
  CMakeLists.txt
  src/           # C++ sources for the TCP data-plane binary
scripts/
  agent_server.py
  master_server.py
```

Unit tests exercise the chunk planner, TCP data plane (through the C++ helper)
and scheduler, compiling the helper automatically via CMake.

## Running unit tests

```
cmake -S cpp -B cpp/build -DCMAKE_BUILD_TYPE=Release
cmake --build cpp/build --target dms_tcp_transfer
pytest
```

The C++ helper must be built before running the tests. The test harness invokes
CMake automatically, but pre-building avoids repeated compilation. All tests are
self-contained and require only loopback TCP connectivity.

## Control plane overview

* **Master scheduler** (`dms.control.master`):
  * Accepts sync requests and assigns file chunks to source agents using a
    pluggable policy framework (round-robin by default).
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

* **TCP** (`cpp/src/tcp_transfer.cpp`): native executable that streams file
  chunks with bounded memory usage. Python wrappers in `dms.data.tcp` invoke the
  helper for both sending and receiving.
* **RDMA** (`dms.data.rdma`): placeholder that validates environment support and
  raises descriptive errors when the pyverbs stack is unavailable.

Build the TCP helper with:

```
cmake -S cpp -B cpp/build -DCMAKE_BUILD_TYPE=Release
cmake --build cpp/build --target dms_tcp_transfer
```

## Logging

The `dms.logging_utils` module configures JSON-formatted logging with
progress-aware helpers so that master and agents can expose consistent runtime
information.

## Running the services

### Single-node smoke test (development)

1. Build the TCP helper: `cmake -S cpp -B cpp/build -DCMAKE_BUILD_TYPE=Release` and
   `cmake --build cpp/build --target dms_tcp_transfer`.
2. Start a receiver terminal:

   ```bash
   ./cpp/build/dms_tcp_transfer receive --bind 127.0.0.1 --port 0 --dest-root /tmp/dms_dest
   ```

   The program prints `PORT=<n>` once it is listening; note the port value.
3. In another terminal run the Python control-plane shim:

   ```bash
   python - <<'PY'
   from pathlib import Path

   from dms.config import AgentEndpoint, SyncRequest
   from dms.control.agent import AgentWorker
   from dms.control.master import MasterScheduler
   from dms.data.tcp import TCPDataPlane

   source_root = Path('/tmp/dms_source')
   dest_root = Path('/tmp/dms_dest')
   source_root.mkdir(parents=True, exist_ok=True)
   (source_root / 'file.bin').write_bytes(b'example payload')

   sender = AgentEndpoint('a1', '127.0.0.1', 6000, PORT_VALUE, True)
   receiver = AgentEndpoint('b1', '127.0.0.1', 6001, PORT_VALUE, False)

   scheduler = MasterScheduler([sender], [receiver])
   request = SyncRequest('local-sync', str(source_root), str(dest_root))
   plan = scheduler.plan(request)

   worker = AgentWorker(
       sender.agent_id,
       source_root=source_root,
       dest_root=dest_root,
       data_plane=TCPDataPlane(binary_path=Path('cpp/build/dms_tcp_transfer')),
   )
   worker.execute(request.request_id, plan[sender.agent_id].assignments)
   PY
   ```

   Replace `PORT_VALUE` with the port printed by the receiver. The destination
   file appears under `/tmp/dms_dest` once the transfer completes.

### Cluster deployment outline (production)

1. Build and distribute the `dms_tcp_transfer` binary to all source and
   destination agents (the binary has no Python dependency).
2. Deploy the Python control plane:
   * Run `scripts/master_server.py` on the management/master host.
   * Run `scripts/agent_server.py` on each DMS agent (source and destination).
3. Issue sync requests from the master. For example:

   ```bash
   python scripts/master_server.py agents.json /home/clusterA/dir /home/clusterB \
     sync-42 --policy round_robin --transfer-mode TCP
   ```

4. Agents invoke the C++ helper for each assigned chunk. Scheduling strategies
   can be tuned per request via the `policy` field or `--policy` CLI flag. The
   included `test_fixed` policy demonstrates how to add custom plugins.

Add a new policy by implementing `SchedulingPolicy` in
`dms/control/policies/` and calling `register_policy` to expose it by name.
