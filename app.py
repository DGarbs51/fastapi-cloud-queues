"""FastAPI canary for Laravel Cloud's Python web and queue runtimes."""

from __future__ import annotations

import asyncio
import collections
import hashlib
import json
import logging
import os
import random
import re
import socket
import sys
import threading
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Literal

from fastapi import Body, FastAPI, Query, Request, WebSocket
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from laravel_cloud_logging import ALERT, EMERGENCY, NOTICE, asgi_middleware
from laravel_cloud_queues import current_job
from laravel_cloud_queues.fastapi import LaravelCloudQueues
from pydantic import BaseModel, ConfigDict, Field, StrictInt
from starlette.exceptions import HTTPException as StarletteHTTPException

import checks
import logs
import throughput
from telemetry import (
    BURST_SIZE,
    CHECK_DEADLINE,
    CHECK_KINDS,
    DELAY_SECONDS,
    KEY_PREFIX,
    TIMEOUT_SECONDS,
    Telemetry,
)

LOG_ROLE = "worker" if "work" in sys.argv else "web"
log = logs.setup(LOG_ROLE)
INDEX = Path(__file__).with_name("index.html")
MAX_BODY = 64 * 1024
UPLOAD_MAX = 512 * 1024 * 1024
UPLOAD_CHUNK = 1024 * 1024
IO_TIMEOUT = 10
SLOW_MAX = 300
CPU_ROUNDS = 100_000
STREAM_CHUNKS = 6
LAG_INTERVAL = 0.1
SERVER = os.environ.get("WEB_SERVER", "uvicorn")
REDIS_HITS = KEY_PREFIX + "redis-hits"
CHECK_LOCK = KEY_PREFIX + "check-lock"
MARKER = re.compile(r"[A-Za-z0-9-]{1,64}")
LOG_TEST_UNICODE = "日本語 · émoji 🚀"
LOG_TEST_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "notice": NOTICE,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
    "alert": ALERT,
    "emergency": EMERGENCY,
}
LAG: collections.deque[float] = collections.deque(maxlen=600)
TASKS: set[asyncio.Task] = set()
START_LOCK = asyncio.Lock()
STARTED = False


class Boom(Exception):
    """Exercise the server's uncaught-exception logging path."""


class EmptyBody(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ThroughputBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    count: StrictInt = Field(default=1000, ge=1, le=10_000)


class SuiteRow(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Literal["pass", "warn", "fail", "skip"]
    detail: str = Field(max_length=1000)


class ResponseModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class InstanceResponse(ResponseModel):
    deploy: str
    pod: str
    pid: int


class PingResponse(InstanceResponse):
    ok: bool


class SlowResponse(PingResponse):
    slept: int


class CpuResponse(PingResponse):
    rounds: int
    digest: str


class RedisResponse(PingResponse):
    hits: int


class OkResponse(ResponseModel):
    ok: bool


class MarkerResponse(OkResponse):
    marker: str


class UploadResponse(ResponseModel):
    bytes: int


class RunResponse(ResponseModel):
    run: str


class WaitPercentiles(ResponseModel):
    p50: float | None
    p95: float | None


class ThroughputResponse(ResponseModel):
    state: Literal["running", "done", "failed"]
    count: int
    processed: int
    jobs_per_s: float | None
    wait_ms: WaitPercentiles
    lost: int | None
    duplicates: int
    verdict: Literal["pass", "warn", "fail"] | None
    error: str | None = None


class CheckRow(ResponseModel):
    id: str
    label: str
    status: Literal["pass", "warn", "fail", "skip"]
    detail: str
    help: str


class CheckGroup(ResponseModel):
    name: str
    checks: list[CheckRow]


class ChecksResponse(ResponseModel):
    groups: list[CheckGroup]


class JobEvent(ResponseModel):
    event: str
    at: float
    job: str
    uuid: str
    delay: int | None = None
    attempt: int | None = None
    worker: str | None = None
    python: str | None = None
    ms: int | None = None
    error: str | None = None


class QueueResult(ResponseModel):
    case: str
    ok: bool | None
    detail: str


class QueueCheck(ResponseModel):
    status: Literal["running", "pass", "fail"]
    started_at: float
    results: list[QueueResult]


class QueueDepth(ResponseModel):
    ready: int
    delayed: int
    reserved: int


class StatsResponse(ResponseModel):
    framework: str
    python: str
    version: str
    mode: str
    queue: str | None
    depth: QueueDepth | None
    counts: dict[str, int]
    events: list[JobEvent]
    check: QueueCheck | None
    server: str
    loop_lag_ms: dict[str, float] | None


class PythonInventory(ResponseModel):
    version: str
    executable: str
    prefix: str
    platform: str
    libc: str
    free_threaded: bool


class NativeInventory(ResponseModel):
    openssl: str
    sqlite: str | None
    zlib: str
    expat: str


class InventoryResponse(ResponseModel):
    python: PythonInventory
    native: NativeInventory
    packages: dict[str, dict[str, str]]


SuiteBody = Annotated[dict[str, SuiteRow], Field(min_length=1, max_length=2)]
RunId = Annotated[str, Field(pattern=r"^[a-f0-9]{32}$")]


async def _startup() -> None:
    global STARTED
    async with START_LOCK:
        if STARTED:
            return
        await telemetry.aopen()
        await checks.aopen()
        TASKS.add(asyncio.create_task(_probe_lag()))
        STARTED = True
        log.info(
            "startup",
            extra={"server": SERVER, "pid": os.getpid(), "web_concurrency": os.environ.get("WEB_CONCURRENCY")},
        )


@asynccontextmanager
async def lifespan(_: FastAPI):
    await _startup()
    try:
        yield
    finally:
        tasks = [*TASKS, *throughput._tasks]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await registry.aclose_producer()
        await telemetry.aclose()
        await checks.aclose()


app = FastAPI(
    title="FastAPI Laravel Cloud Canary",
    version="1.0.0",
    description=(
        "Public, unauthenticated canary endpoints for an isolated demo environment. "
        "Mutation endpoints require JSON and enforce request limits."
    ),
    lifespan=lifespan,
)
api = app
queues = LaravelCloudQueues(app)
registry = queues.registry
telemetry = Telemetry(registry, "FastAPI")
throughput.install(registry, telemetry, log)


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
    with telemetry.tracked():
        time.sleep(TIMEOUT_SECONDS + 7)


DISPATCHES = {
    "quick": (quick, 0, 1),
    "async": (async_job, 0, 1),
    "slow": (slow, 0, 1),
    "delayed": (quick, DELAY_SECONDS, 1),
    "flaky": (flaky, 0, 1),
    "failing": (failing, 0, 1),
    "timeout": (timeout, 0, 1),
    "burst": (quick, 0, BURST_SIZE),
}


async def adispatch(kind: str) -> list[str]:
    job, delay, count = DISPATCHES[kind]

    async def one() -> str:
        at = time.time()
        receipt = await job.options(delay=delay).dispatch_async()
        await telemetry.aqueued(job.name, receipt.uuid, at, delay)
        return receipt.uuid

    return list(await asyncio.gather(*(one() for _ in range(count))))


async def arun_check() -> bool:
    if not await telemetry.astore.set(CHECK_LOCK, "1", nx=True, ex=CHECK_DEADLINE):
        return False
    uuids = await asyncio.gather(*(adispatch(kind) for kind in CHECK_KINDS))
    await telemetry.asave_check(dict(zip(CHECK_KINDS, uuids)))
    return True


def instance() -> dict[str, object]:
    return {
        "deploy": os.environ.get("DEPLOY_MARKER", ""),
        "pod": socket.gethostname(),
        "pid": os.getpid(),
    }


def loop_lag() -> dict[str, float] | None:
    if not LAG:
        return None
    return {
        "p50": round(throughput.percentile(list(LAG), 50), 2),
        "p99": round(throughput.percentile(list(LAG), 99), 2),
    }


async def _probe_lag() -> None:
    while True:
        started = time.monotonic()
        await asyncio.sleep(LAG_INTERVAL)
        LAG.append((time.monotonic() - started - LAG_INTERVAL) * 1000)


def request_id(request: Request) -> str:
    supplied = request.headers.get("x-request-id", "")
    return supplied if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", supplied) else uuid.uuid4().hex


def error(status: int, message: str) -> JSONResponse:
    return JSONResponse({"error": message}, status_code=status)


@api.middleware("http")
async def request_guard(request: Request, call_next):
    await _startup()  # Daphne does not send ASGI lifespan events.
    rid, started = request_id(request), time.monotonic()
    checks.PEER.set(request.client.host if request.client else "")
    with logs.context(request_id=rid):
        response = None
        try:
            if request.method == "POST" and request.url.path != "/api/upload":
                if request.headers.get("content-type", "").split(";", 1)[0].strip() != "application/json":
                    response = error(415, "expected application/json")
                else:
                    body = bytearray()
                    async for chunk in request.stream():
                        body.extend(chunk)
                        if len(body) > MAX_BODY:
                            response = error(400, "body exceeds 64 KiB")
                            break
                    if response is None:
                        request._body = bytes(body)
            if response is None:
                response = await call_next(request)
        except Boom:
            raise
        except Exception:
            log.exception("Request failed: %s %s", request.method, request.url.path)
            response = error(503, "service unavailable")
        response.headers["X-Request-ID"] = rid
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        level = logging.DEBUG if request.method == "GET" and request.url.path == "/api/stats" else logging.INFO
        if response.status_code >= 400:
            level = logging.ERROR if response.status_code >= 500 else logging.WARNING
        log.log(
            level,
            "access",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "duration_ms": round((time.monotonic() - started) * 1000, 2),
            },
        )
        return response


@api.exception_handler(RequestValidationError)
async def validation_error(_: Request, exc: RequestValidationError):
    first = exc.errors()[0] if exc.errors() else {}
    location = ".".join(str(part) for part in first.get("loc", ())[1:])
    message = first.get("msg", "invalid request")
    return error(400, f"{location}: {message}" if location else message)


@api.exception_handler(StarletteHTTPException)
async def http_error(_: Request, exc: StarletteHTTPException):
    message = "not found" if exc.status_code == 404 else str(exc.detail).lower()
    return error(exc.status_code, message)


PUBLIC = {
    "description": "Public canary endpoint. No authentication. Deploy only in an isolated demo environment.",
    "openapi_extra": {"security": [], "x-public": True},
}


@api.get("/", **PUBLIC)
async def index() -> FileResponse:
    return FileResponse(INDEX)


@api.get("/api/ping", **PUBLIC)
async def ping() -> PingResponse:
    return {"ok": True, **instance()}


@api.get("/api/stats", **PUBLIC)
async def stats() -> StatsResponse:
    async with asyncio.timeout(IO_TIMEOUT):
        snapshot = await telemetry.asnapshot()
    return {**snapshot, "server": SERVER, "loop_lag_ms": loop_lag()}


@api.get("/api/redis", **PUBLIC)
async def redis() -> RedisResponse:
    async with asyncio.timeout(IO_TIMEOUT):
        hits = await telemetry.astore.incr(REDIS_HITS)
    return {"ok": True, "hits": hits, **instance()}


@api.get("/api/checks", **PUBLIC)
async def platform_checks(request: Request) -> ChecksResponse:
    return await checks.arun(dict(request.headers))


@api.get("/api/packages", **PUBLIC)
async def packages() -> InventoryResponse:
    return await asyncio.to_thread(checks.inventory)


@api.get("/api/throughput/{run}", response_model=ThroughputResponse, **PUBLIC)
async def throughput_status(run: RunId):
    async with asyncio.timeout(IO_TIMEOUT):
        status, body = await throughput.astatus(run)
    return JSONResponse(body, status_code=status)


@api.get("/api/slow", **PUBLIC)
async def slow_request(seconds: Annotated[int, Query(ge=1, le=SLOW_MAX)]) -> SlowResponse:
    await asyncio.sleep(seconds)
    return {"ok": True, "slept": seconds, **instance()}


@api.get("/api/cpu", **PUBLIC)
async def cpu() -> CpuResponse:
    digest = b"lcq"
    for _ in range(CPU_ROUNDS):
        digest = hashlib.sha256(digest).digest()
    return {"ok": True, "rounds": CPU_ROUNDS, "digest": digest.hex(), **instance()}


@api.get("/api/stream", **PUBLIC)
async def stream(accel: str | None = Query(default=None)):
    async def chunks() -> AsyncIterator[bytes]:
        for i in range(STREAM_CHUNKS):
            if i:
                await asyncio.sleep(1)
            yield f"data: {json.dumps({'i': i, 'sent': round(time.time(), 3)})}\n\n".encode()

    headers = {"X-Accel-Buffering": "no"} if accel == "no" else {}
    return StreamingResponse(chunks(), media_type="text/event-stream", headers=headers)


@api.get("/api/log-test", **PUBLIC)
async def log_test(marker: Annotated[str, Query(pattern=r"^[A-Za-z0-9-]{1,64}$")]) -> MarkerResponse:
    for name, level in LOG_TEST_LEVELS.items():
        log.log(level, "log-test %s", name, extra={"marker": marker, "case": name})
    log.info("log-test extra", extra={"marker": marker, "case": "extra", "order": {"id": 42, "items": ["a", "b"]}})
    log.info("log-test multi-line\nsecond line\nthird line", extra={"marker": marker, "case": "multiline"})
    log.info("log-test unicode %s", LOG_TEST_UNICODE, extra={"marker": marker, "case": "unicode"})
    try:
        try:
            raise KeyError("inner cause")
        except KeyError as exc:
            raise ValueError("log-test outer") from exc
    except ValueError:
        log.exception("log-test exception", extra={"marker": marker, "case": "exception"})
    return {"ok": True, "marker": marker}


def _boom_inner(marker: str) -> None:
    raise Boom(f"uncaught boom {marker}")


def _boom_thread(marker: str) -> None:
    _boom_inner(marker)


@api.get("/api/boom", **PUBLIC)
async def boom(marker: Annotated[str, Query(pattern=r"^[A-Za-z0-9-]{1,64}$")]):
    _boom_inner(marker)


@api.get("/api/boom-thread", **PUBLIC)
async def boom_thread(marker: Annotated[str, Query(pattern=r"^[A-Za-z0-9-]{1,64}$")]) -> MarkerResponse:
    thread = threading.Thread(target=_boom_thread, args=(marker,), name="boom-thread")
    thread.start()
    await asyncio.to_thread(thread.join)
    return {"ok": True, "marker": marker}


@api.post("/api/check", response_model=OkResponse, **PUBLIC)
async def queue_check(_: EmptyBody = Body(default_factory=EmptyBody)):
    async with asyncio.timeout(IO_TIMEOUT):
        if not await arun_check():
            return error(409, "a queue check is already running")
    return {"ok": True}


@api.post("/api/throughput", response_model=RunResponse, **PUBLIC)
async def throughput_start(body: ThroughputBody):
    async with asyncio.timeout(IO_TIMEOUT):
        status, payload = await throughput.astart(body.model_dump())
    return JSONResponse(payload, status_code=status)


@api.post("/api/suite-results", response_model=OkResponse, **PUBLIC)
async def suite_results(body: SuiteBody):
    data = {key: row.model_dump() for key, row in body.items()}
    async with asyncio.timeout(IO_TIMEOUT):
        status, payload = await checks.asave_suite(data)
    return JSONResponse(payload, status_code=status)


@api.post("/api/reset", **PUBLIC)
async def reset(_: EmptyBody = Body(default_factory=EmptyBody)) -> OkResponse:
    async with asyncio.timeout(IO_TIMEOUT):
        await telemetry.areset()
    return {"ok": True}


@api.post("/api/upload", **PUBLIC)
async def upload(request: Request) -> UploadResponse:
    content_length = request.headers.get("content-length")
    if content_length is not None and (not content_length.isdigit() or int(content_length) < 0):
        return error(400, "invalid Content-Length")
    if content_length is not None and int(content_length) > UPLOAD_MAX:
        return error(413, "body exceeds 512 MiB")
    uploaded = 0
    async for chunk in request.stream():
        uploaded += len(chunk)
        if uploaded > UPLOAD_MAX:
            return error(413, "body exceeds 512 MiB")
    return {"bytes": uploaded}


@api.websocket("/ws/echo")
async def websocket_echo(websocket: WebSocket) -> None:
    await websocket.accept()
    while True:
        try:
            await websocket.send_text(await websocket.receive_text())
        except Exception:
            return


# Preserve FastAPI's app object at ``api`` for managed workers; web servers use the wrapped ASGI app.
app = asgi_middleware(api)


def self_check() -> None:
    """Exercise FastAPI's public contract without Redis, a database, or outbound requests."""
    import httpx

    async def run() -> None:
        global _startup

        startup, _startup = _startup, _noop_startup
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, raise_app_exceptions=True),
                base_url="http://self-check",
            ) as client:
                response = await client.get("/api/ping", headers={"X-Request-ID": "self-check"})
                assert response.status_code == 200 and response.json()["ok"]
                assert response.headers["x-request-id"] == "self-check"
                assert (await client.get("/")).status_code == 200
                assert (await client.get("/missing")).json() == {"error": "not found"}
                bad = await client.post("/api/throughput", json={"count": 1, "unknown": True})
                assert bad.status_code == 400 and set(bad.json()) == {"error"}
                upload = await client.post("/api/upload", content=b"canary")
                assert upload.status_code == 200 and upload.json() == {"bytes": 6}
                operations = [
                    operation
                    for methods in api.openapi()["paths"].values()
                    for method, operation in methods.items()
                    if method in {"get", "post", "put", "patch", "delete"}
                ]
                assert operations and all(operation["security"] == [] and operation["x-public"] for operation in operations)
        finally:
            _startup = startup

    async def _noop_startup() -> None:
        return None

    asyncio.run(run())
    print("FastAPI self-check passed")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-check"]:
        self_check()
        raise SystemExit(0)
    raise SystemExit(__doc__)
