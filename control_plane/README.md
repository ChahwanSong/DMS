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
