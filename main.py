"""FastAPI demo for laravel-cloud-queues.

Web dashboard: uv run uvicorn main:app                      (http://127.0.0.1:8000)
Worker:        uv run laravel-cloud-queues work main:app
"""

from __future__ import annotations

import asyncio
import random
import time
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse

from laravel_cloud_queues.fastapi import LaravelCloudQueues, current_job
from telemetry import (
    BURST_SIZE,
    CHECK_KINDS,
    DELAY_SECONDS,
    TIMEOUT_SECONDS,
    Telemetry,
)

app = FastAPI(title="Cloud Queues Telemetry")
queues = LaravelCloudQueues(app)
telemetry = Telemetry(queues.registry, "FastAPI")
INDEX = Path(__file__).with_name("index.html")


@queues.job(name="demo.quick")
def quick() -> None:
    with telemetry.tracked():
        time.sleep(random.uniform(0.05, 0.3))


@queues.job(name="demo.async")
async def async_job() -> None:
    with telemetry.tracked():
        await asyncio.sleep(random.uniform(0.05, 0.3))


@queues.job(name="demo.slow")
def slow() -> None:
    with telemetry.tracked():
        time.sleep(3)


@queues.job(name="demo.flaky", tries=3, backoff=[2])
def flaky() -> None:
    with telemetry.tracked():
        if current_job().attempt == 1:
            raise RuntimeError("flaky job fails on its first attempt")


@queues.job(name="demo.failing", tries=2, backoff=[1])
def failing() -> None:
    with telemetry.tracked():
        raise RuntimeError("this job always fails")


@queues.job(name="demo.timeout", tries=2, timeout=TIMEOUT_SECONDS)
def timeout() -> None:
    # Exceeds its timeout: the worker exits 124 and the platform restarts it.
    with telemetry.tracked():
        time.sleep(TIMEOUT_SECONDS + 7)


DISPATCHES = {  # kind: (job, delay seconds, how many)
    "quick": (quick, 0, 1),
    "async": (async_job, 0, 1),
    "slow": (slow, 0, 1),
    "delayed": (quick, DELAY_SECONDS, 1),
    "flaky": (flaky, 0, 1),
    "failing": (failing, 0, 1),
    "timeout": (timeout, 0, 1),
    "burst": (quick, 0, BURST_SIZE),
}


async def dispatch_kind(kind: str) -> list[str]:
    job, delay, count = DISPATCHES[kind]
    uuids = []
    for _ in range(count):
        at = time.time()
        receipt = await job.options(delay=delay).dispatch_async()
        await run_in_threadpool(telemetry.queued, job.name, receipt.uuid, at, delay)
        uuids.append(receipt.uuid)
    return uuids


def require_json(content_type: str | None = Header(None)) -> None:
    # A JSON content type forces a CORS preflight, so other sites cannot trigger dispatches.
    if content_type != "application/json":
        raise HTTPException(415, "expected application/json")


@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(INDEX)


@app.get("/api/stats")
def stats() -> dict[str, object]:
    return telemetry.snapshot()


@app.post("/api/dispatch/{kind}", dependencies=[Depends(require_json)])
async def dispatch(kind: str) -> dict[str, list[str]]:
    if kind not in DISPATCHES:
        raise HTTPException(404, "not found")
    return {"uuids": await dispatch_kind(kind)}


@app.post("/api/check", dependencies=[Depends(require_json)])
async def check() -> dict[str, bool]:
    cases = {kind: await dispatch_kind(kind) for kind in CHECK_KINDS}
    await run_in_threadpool(telemetry.save_check, cases)
    return {"ok": True}


@app.post("/api/reset", dependencies=[Depends(require_json)])
def reset() -> dict[str, bool]:
    telemetry.reset()
    return {"ok": True}
