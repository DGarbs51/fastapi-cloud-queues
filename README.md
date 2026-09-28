# fastapi-cloud-queues

A FastAPI demo of [`laravel-cloud-queues`](https://pypi.org/project/laravel-cloud-queues/),
installed from PyPI. The FastAPI app shows a live dashboard of jobs as they
are queued and processed on the Redis (Valkey) backend.

The plain-Python version with the same dashboard is
[`python-cloud-queues`](https://github.com/DGarbs51/python-cloud-queues).

## Run it

You need a Redis or Valkey server (Laravel Herd bundles Valkey on `127.0.0.1:6379`) and
[uv](https://docs.astral.sh/uv/).

```sh
uv sync
export LARAVEL_CLOUD_QUEUES_BACKEND=redis
export LARAVEL_CLOUD_QUEUES_REDIS_URL=redis://127.0.0.1:6379/0

uv run uvicorn main:app                     # dashboard on http://127.0.0.1:8000
uv run laravel-cloud-queues work main:app   # in a second terminal; run more to scale
```

Pass `--host` and `--port` to uvicorn to change where the dashboard listens.

## What the dashboard shows

- **Queue depth**: ready, delayed (including retry backoff) and reserved (in-flight) jobs,
  read from the package's Redis keys.
- **Totals and recent events**: `queued`, `started`, `processed`, `released` and `failed`.
  In `redis` mode the package emits no lifecycle events of its own, so each job records
  its telemetry into Redis (`lcq-demo:*`) using `current_job()`.
- **Workers**: which worker processes handled jobs in the last minute.

The dispatch buttons cover a quick sync job, an async job, a 3-second job, a 5-second
delay, a job that fails once and is retried, a job that always fails, and a burst of 25.

## Files

- `main.py`: the FastAPI app, `LaravelCloudQueues`, the jobs, their telemetry and the routes.
- `index.html`: the dashboard (Tailwind CSS from the CDN, no build step).

The dispatch endpoints have no authentication. Run this locally, not on a public host.
