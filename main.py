"""FastAPI demo for laravel-cloud-queues (Redis backend).

Web dashboard: uv run uvicorn main:app                      (http://127.0.0.1:8000)
Worker:        uv run laravel-cloud-queues work main:app

The package emits no lifecycle events in redis mode, so jobs record their own telemetry
into Redis under ``lcq-demo:``; queue depth is read from the package's Redis keys.
"""

from __future__ import annotations

import asyncio
import functools
import json
import os
import random
import socket
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import redis
from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse

from laravel_cloud_queues import __version__
from laravel_cloud_queues.fastapi import LaravelCloudQueues, current_job

app = FastAPI(title="Cloud Queues Telemetry")
queues = LaravelCloudQueues(app)
registry = queues.registry

EVENTS = "lcq-demo:events"
STATS = "lcq-demo:stats"
KEEP_EVENTS = 200
INDEX = Path(__file__).with_name("index.html")


@functools.cache
def store() -> redis.Redis:
    if registry.config.redis is None:
        raise RuntimeError("Set LARAVEL_CLOUD_QUEUES_BACKEND=redis; this demo targets Redis.")
    return redis.Redis.from_url(registry.config.redis.url, decode_responses=True)


def record(event: str, **fields: object) -> None:
    entry = {"event": event, "at": time.time(), **fields}
    pipe = store().pipeline()
    pipe.lpush(EVENTS, json.dumps(entry))
    pipe.ltrim(EVENTS, 0, KEEP_EVENTS - 1)
    pipe.hincrby(STATS, event, 1)
    pipe.execute()


@contextmanager
def tracked() -> Iterator[None]:
    """Record started/processed/released/failed for the current delivery."""
    job = current_job()
    base = {
        "job": job.job_name,
        "uuid": job.uuid,
        "attempt": job.attempt,
        "worker": f"{socket.gethostname()}:{os.getpid()}",
    }
    record("started", **base)
    start = time.monotonic()
    try:
        yield
    except Exception as exc:
        final = job.attempt >= job.max_tries
        ms = round((time.monotonic() - start) * 1000)
        record("failed" if final else "released", **base, ms=ms, error=str(exc)[:200])
        raise
    record("processed", **base, ms=round((time.monotonic() - start) * 1000))


@queues.job(name="demo.quick")
def quick() -> None:
    with tracked():
        time.sleep(random.uniform(0.05, 0.3))


@queues.job(name="demo.async")
async def async_job() -> None:
    with tracked():
        await asyncio.sleep(random.uniform(0.05, 0.3))


@queues.job(name="demo.slow")
def slow() -> None:
    with tracked():
        time.sleep(3)


@queues.job(name="demo.flaky", tries=3, backoff=[2])
def flaky() -> None:
    with tracked():
        if current_job().attempt == 1:
            raise RuntimeError("flaky job fails on its first attempt")


@queues.job(name="demo.failing", tries=2, backoff=[1])
def failing() -> None:
    with tracked():
        raise RuntimeError("this job always fails")


DISPATCHES = {  # kind: (job, delay seconds, how many)
    "quick": (quick, 0, 1),
    "async": (async_job, 0, 1),
    "slow": (slow, 0, 1),
    "delayed": (quick, 5, 1),
    "flaky": (flaky, 0, 1),
    "failing": (failing, 0, 1),
    "burst": (quick, 0, 25),
}


def require_json(content_type: str | None = Header(None)) -> None:
    # A JSON content type forces a CORS preflight, so other sites cannot trigger dispatches.
    if content_type != "application/json":
        raise HTTPException(415, "expected application/json")


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(INDEX)


@app.get("/api/stats")
def stats() -> dict[str, object]:
    cfg = registry.config.redis
    pending = f"{cfg.prefix}queues:{cfg.queue}"
    pipe = store().pipeline()
    pipe.llen(pending)
    pipe.zcard(f"{pending}:delayed")
    pipe.zcard(f"{pending}:reserved")
    pipe.hgetall(STATS)
    pipe.lrange(EVENTS, 0, 99)
    ready, delayed, reserved, counts, events = pipe.execute()
    parsed = sorted((json.loads(e) for e in events), key=lambda e: e["at"], reverse=True)
    return {
        "framework": "FastAPI",
        "version": __version__,
        "queue": cfg.queue,
        "depth": {"ready": ready, "delayed": delayed, "reserved": reserved},
        "counts": {k: int(v) for k, v in counts.items()},
        "events": parsed,
    }


@app.post("/api/dispatch/{kind}", dependencies=[Depends(require_json)])
async def dispatch(kind: str) -> dict[str, list[str]]:
    if kind not in DISPATCHES:
        raise HTTPException(404, "not found")
    job, delay, count = DISPATCHES[kind]
    uuids = []
    for _ in range(count):
        at = time.time()
        receipt = await job.options(delay=delay).dispatch_async()
        await run_in_threadpool(
            record, "queued", at=at, job=job.name, uuid=receipt.uuid, delay=delay
        )
        uuids.append(receipt.uuid)
    return {"uuids": uuids}


@app.post("/api/reset", dependencies=[Depends(require_json)])
def reset() -> dict[str, bool]:
    store().delete(EVENTS, STATS)
    return {"ok": True}
