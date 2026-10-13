# fastapi-cloud-queues

FastAPI canary for Laravel Cloud's Python web, queue, logging, network, runtime, and service behavior.
It mirrors `python-cloud-queues` check IDs and JSON responses while using async Redis, database, HTTP,
and queue clients from FastAPI's lifespan.

## Public demo warning

Every HTTP and WebSocket endpoint is public and unauthenticated by design. Deploy this canary only in
an isolated demo environment and protect it at the network layer. OpenAPI marks every HTTP operation as
public. Do not add secrets or production data.

JSON requests are limited to 64 KiB, reject unknown fields, and use bounded schemas. `/api/upload`
streams without buffering and caps uploads at 512 MiB. Mutating routes have per-process request limits.
Outbound probes use fixed hosts or Laravel Cloud
public hostnames only, with timeouts and validated response shapes. The app enables no debug mode or
credentialed wildcard CORS.

## Queue check

`POST /api/check` dispatches seven cases:

| Case | Pass condition |
|---|---|
| `quick` | sync job processes on attempt 1 |
| `async` | async job processes on attempt 1 |
| `delayed` | job starts after its 5 s delay |
| `flaky` | attempt 1 releases, attempt 2 processes |
| `failing` | attempt 1 releases, attempt 2 fails |
| `timeout` | worker exits on timeout, restarts, and redelivers |
| `burst` | 20 jobs each process exactly once |

The dashboard also verifies worker presence and Python-version parity. Throughput dispatches up to 10,000
jobs and reports processing rate, wait percentiles, loss, and duplicate deliveries.

## Run locally

Supports Python 3.11-3.14. Local queue checks require uv, Redis or Valkey on database 2, and MySQL database
`fastapi_cloud_queues`.

```sh
cp .env.example .env
set -a; source .env; set +a
uv sync
uv run python serve.py
```

Dashboard: `http://[::1]:8002` (`HOST=0.0.0.0` selects an IPv4 bind when needed).

Run a managed-style worker in another terminal. `[tool.laravel-cloud-queues]` supplies the target:

```sh
uv run laravel-cloud-queues work
```

The timeout case needs a process supervisor to restart its worker.

## Web servers

Only ASGI servers are supported:

- `python serve.py` or `python serve.py uvicorn`
- `python serve.py granian-asgi`
- `python serve.py hypercorn-asgi`
- `python serve.py daphne`

All bind to IPv6 wildcard and use `PORT` (`8002` locally). `python serve.py` reads the server from
`.web-server`, which is `uvicorn` on the default branch. Server-matrix branches change only `.web-server`;
supported values are `uvicorn`, `granian-asgi`, `hypercorn-asgi`, and `daphne`.

## Checks and scripts

- `GET /api/checks`: all Web, Logging, Runtime, and Services check groups
- `cloud_suite.py`: quick/full environment matrix and regression report
- `logs_check.py`: Cloud log rendering
- `upload_check.py`: proxy upload limits
- `scripts/ship.py`: branch deployment orchestration
- `scripts/deploy_drain.py`: in-flight request behavior during deploy
- `k6/run.py`: HTTP, probe, concurrency, and queue-throughput load tests

Cloud-facing scripts call Cloud APIs and may mutate deployments. Review them before running. Local tests
and self-checks never call Cloud APIs.

```sh
uv run python app.py --self-check
uv run python test_checks.py
uv run python test_throughput.py
uv run python test_servers.py
uv run --python 3.11 python test_servers.py
```
