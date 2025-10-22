# Control Plane Development Guide

This document explains how to set up a Python environment for the control plane, run the
available unit tests, and execute the master/agent services locally.

## Prerequisites

- **Python 3.12** or newer (matches the version used in CI).
- **Redis** running locally or reachable over the network. The master process stores state
  in Redis, so the service must be running before you start the master server or the tests.

## Install dependencies

From the repository root, create (and activate) a virtual environment, then install the
control plane dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r control_plane/requirements-dev.txt
```

The development requirements include `pytest`, `httpx`, and other libraries used in the
control-plane test suite.

## Running the test suite

The control plane tests live under `control_plane/tests` and require the repository root to
be on `PYTHONPATH`. Run them with:

```bash
PYTHONPATH=. pytest control_plane/tests
```

A running Redis instance is required for the master scheduling tests. If Redis is running on
a different host or port, set the `REDIS_URL` environment variable before invoking `pytest`
(e.g. `export REDIS_URL=redis://redis-host:6380/0`).

## Running the master locally

```bash
cd control_plane
export PYTHONPATH=..
python master_cli.py --config example_master.yml --host 127.0.0.1 --port 8888
```

This launches the FastAPI master server bound to `127.0.0.1:8888` using the sample
configuration.

## Master API reference

The master exposes a small FastAPI surface that both operators and worker agents use. The
endpoints below are listed in the order they are typically exercised during a transfer.

### `POST /sync`

Queues a new sync request. The payload must match the `SyncRequest` schema. The master
returns `202 Accepted` when the request is accepted for scheduling.

```bash
curl -X POST http://127.0.0.1:8888/sync \
  -H 'Content-Type: application/json' \
-d '{
        "request_id": "demo-1",
        "source_path": "/data/source",
        "destination_path": "/data/dest",
        "chunk_size_mb": 64,
      }'
```

### `GET /sync/{request_id}`

Retrieves the latest `SyncProgress` document for the given request. It includes the current
state, timestamps, transferred bytes, and any failure details recorded by the master or
workers.

```bash
curl http://127.0.0.1:8888/sync/demo-1 | jq
```

### `GET /sync`

Lists every tracked request with the same `SyncProgress` payload returned by the previous
endpoint. Useful for dashboards or polling loops.

```bash
curl http://127.0.0.1:8888/sync | jq
```

### `DELETE /sync/{request_id}`

Removes a completed or failed request from the master's in-memory cache and Redis metadata.
Active transfers are cancelled, and any future progress queries return `404` after deletion.

```bash
curl -X DELETE http://127.0.0.1:8888/sync/demo-1
```

### `POST /workers/heartbeat`

Agents POST their `WorkerHeartbeat` documents to advertise health, storage mounts, and data
plane endpoints. The master responds with `{ "status": "ok" }` when the heartbeat is
accepted.

```bash
curl -X POST http://127.0.0.1:8888/workers/heartbeat \
  -H 'Content-Type: application/json' \
  -d '{
        "worker_id": "worker-a",
        "status": "READY",
        "storage_paths": ["/data/source", "/scratch"],
        "data_plane_endpoints": [
          {"iface": "ib0", "address": "192.168.100.10"}
        ]
      }'
```

### `POST /workers/{worker_id}/assignment`

Agents poll this endpoint to fetch the next `Assignment`. When work is available, the master
returns a JSON assignment document; otherwise the response body is `null` (HTTP 200).

```bash
curl -X POST http://127.0.0.1:8888/workers/worker-a/assignment | jq
```

### `POST /workers/result`

Agents report chunk success or failure by posting `SyncResult` objects to this endpoint. The
master acknowledges receipt with `{ "status": "ack" }`.

```bash
curl -X POST http://127.0.0.1:8888/workers/result \
  -H 'Content-Type: application/json' \
  -d '{
        "request_id": "demo-1",
        "worker_id": "worker-a",
        "success": true,
        "message": "chunk complete"
      }'
```

## Running an agent locally

Workers are defined in a shared YAML configuration. Choose a worker by ID when starting the
agent:

```bash
cd control_plane
export PYTHONPATH=..
python agent_cli.py --worker-id worker-a --config example_agent.yml
```

The CLI loads `worker-a` from `example_agent.yml`. Provide a different `--worker-id` to start
multiple agents from the same file.

Each worker entry must include a `storage_paths` list describing the root directories mounted on
that machine. The master uses this information to determine which workers can satisfy a sync
request. The sample configuration shows two workers:

```yaml
workers:
  - worker_id: worker-a
    storage_paths:
      - /home/gpu1
      - /home/cpu1
    network:
      ...
  - worker_id: worker-b
    storage_paths:
      - /home/gpu1
      - /scratch
    network:
      ...
```

When the agent sends heartbeats, these storage paths are included so the master can restrict
assignment policies to workers that have access to the requested source paths. The master also
shares both the source and destination worker pools with the agent for each assignment so the
data transfer implementation can contact the appropriate peers.

## Helpful environment variables

- `REDIS_URL`: override the Redis connection URL used by the tests and the master. Defaults
  to `redis://localhost:6379/0`.
- `AGENT_LOG_LEVEL`: increase the agent log verbosity when debugging local runs.

## Troubleshooting

- **`redis.exceptions.ConnectionError`:** ensure Redis is running and reachable from your
  machine. On Ubuntu/Debian install it with `sudo apt install redis-server` and verify with
  `redis-cli ping`.
- **Import errors when running pytest:** double-check that `PYTHONPATH` includes the
  repository root (e.g. run `PYTHONPATH=. pytest control_plane/tests` from the repo root).
