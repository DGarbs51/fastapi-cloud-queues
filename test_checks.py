"""FastAPI route, check registry, and security contracts. No external services."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import date, timedelta
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

import httpx

import app
import checks
import cloud_suite
import telemetry

SECRET = "hunter2-s3cret"
STATUSES = {"pass", "warn", "fail", "skip"}
ROW_KEYS = {"id", "label", "status", "detail", "help"}


def check_shape(result: dict) -> list[dict]:
    assert list(result) == ["groups"]
    assert [group["name"] for group in result["groups"]] == ["Web", "Logging", "Runtime", "Services"]
    rows = [row for group in result["groups"] for row in group["checks"]]
    assert all(set(row) == ROW_KEYS and row["status"] in STATUSES for row in rows), rows
    assert all(isinstance(value, str) for row in rows for value in row.values())
    assert len({row["id"] for row in rows}) == len(rows) == len(checks.CHECKS)
    json.dumps(result)
    return rows


class RouteContracts(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.startup = patch.object(app, "_startup", AsyncMock())
        self.startup.start()
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app.app, raise_app_exceptions=False),
            base_url="http://test",
        )

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        self.startup.stop()

    async def test_public_routes_and_headers(self) -> None:
        response = await self.client.get("/api/ping", headers={"X-Request-ID": "test-1"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["x-request-id"], "test-1")
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(response.headers["x-frame-options"], "DENY")
        self.assertNotIn("access-control-allow-credentials", response.headers)

        operations = [
            operation
            for methods in app.api.openapi()["paths"].values()
            for method, operation in methods.items()
            if method in {"get", "post", "put", "patch", "delete"}
        ]
        self.assertTrue(operations)
        self.assertTrue(all(operation["security"] == [] and operation["x-public"] is True for operation in operations))

    async def test_bounded_strict_inputs(self) -> None:
        headers = {"Content-Type": "application/json"}
        for body in (
            {"count": 0},
            {"count": 10_001},
            {"count": True},
            {"count": "10"},
            {"count": 1, "extra": 1},
        ):
            response = await self.client.post("/api/throughput", headers=headers, json=body)
            self.assertEqual(response.status_code, 400, body)
            self.assertEqual(set(response.json()), {"error"})

        for path in ("/api/check", "/api/reset"):
            response = await self.client.post(path, headers=headers, json={"unexpected": True})
            self.assertEqual(response.status_code, 400)
        response = await self.client.post(
            "/api/suite-results",
            headers=headers,
            json={"logging.cloud_viewer": {"status": "pass", "detail": "ok", "unexpected": True}},
        )
        self.assertEqual(response.status_code, 400)

    async def test_body_and_content_type_limits(self) -> None:
        response = await self.client.post("/api/check", content=b"{}", headers={"Content-Type": "text/plain"})
        self.assertEqual(response.status_code, 415)
        self.assertEqual(response.json(), {"error": "expected application/json"})

        response = await self.client.post(
            "/api/check",
            content=b'{"x":"' + b"x" * app.MAX_BODY + b'"}',
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {"error": "body exceeds 64 KiB"})

        response = await self.client.post(
            "/api/upload",
            content=b"x",
            headers={"Content-Type": "application/octet-stream", "Content-Length": str(app.UPLOAD_MAX + 1)},
        )
        self.assertEqual(response.status_code, 413)

    async def test_invalid_json_and_unknown_routes(self) -> None:
        response = await self.client.post(
            "/api/check",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(set(response.json()), {"error"})
        self.assertEqual((await self.client.get("/nope")).status_code, 404)
        self.assertEqual((await self.client.post("/api/dispatch/quick", json={})).status_code, 404)

    async def test_check_and_suite_routes_use_async_contracts(self) -> None:
        result = {"groups": [{"name": "Web", "checks": []}]}
        with patch.object(checks, "arun", AsyncMock(return_value=result)) as run:
            response = await self.client.get("/api/checks", headers={"X-Test": "yes"})
        self.assertEqual((response.status_code, response.json()), (200, result))
        self.assertEqual(run.await_args.args[0]["x-test"], "yes")

        saved = {"logging.cloud_viewer": {"status": "pass", "detail": "ok"}}
        with patch.object(checks, "asave_suite", AsyncMock(return_value=(200, {"ok": True}))) as save:
            response = await self.client.post("/api/suite-results", json=saved)
        self.assertEqual((response.status_code, response.json()), (200, {"ok": True}))
        save.assert_awaited_once_with(saved)


class CheckContracts(unittest.TestCase):
    def test_check_ids_and_queue_cases(self) -> None:
        expected = [
            "web.server", "web.concurrency", "web.port", "web.proto", "web.forwarded_for",
            "web.request_id", "web.event_loop", "web.websocket", "web.upstream_ipv6",
            "web.streaming", "web.static", "web.upload_limit", "logging.handler",
            "logging.socket", "logging.cloud_viewer", "runtime.python", "runtime.stdlib",
            "runtime.packages", "runtime.https", "runtime.tmp", "runtime.cpu", "runtime.memory",
            "runtime.subprocess", "runtime.threads", "services.redis", "services.database",
            "services.dns",
        ]
        self.assertEqual([entry[1] for entry in checks.CHECKS], expected)
        self.assertEqual(telemetry.CHECK_KINDS, ("quick", "async", "delayed", "flaky", "failing", "timeout", "burst"))
        self.assertEqual(set(app.DISPATCHES), {*telemetry.CHECK_KINDS, "slow"})
        self.assertEqual({entry[1] for entry in checks.CHECKS if entry[5] == "full"}, set(cloud_suite.JOBS))
        self.assertTrue(all(len(entry) == 6 and callable(entry[4]) and entry[5] in ("quick", "full")
                            for entry in checks.CHECKS))
        self.assertTrue(all(callable(entry[3]) for entry in checks.CHECKS if entry[5] == "quick"))
        self.assertTrue(all(entry[4] is checks.everywhere or entry[4].__doc__ for entry in checks.CHECKS),
                        "a skip needs its reason")

    def test_downstream_schema_and_host_allowlist(self) -> None:
        with self.assertRaises(ValueError):
            checks._python_release("running", {"latest": ["3.14.1"]})
        with self.assertRaises(ValueError):
            checks._suite_result(json.dumps({"status": "pass", "detail": "ok"}))
        self.assertEqual(checks._public_host({"host": "demo.laravel.cloud"}), "demo.laravel.cloud")
        for host in ("127.0.0.1", "169.254.169.254", "evil.example", "demo.laravel.cloud.evil.example"):
            with self.assertRaises(ValueError, msg=host):
                checks._public_host({"host": host})

    def test_daphne_concurrency_checks_pass_without_web_concurrency(self) -> None:
        with patch.dict(os.environ, {"LARAVEL_CLOUD": "1"}, clear=True), \
                patch.object(app, "SERVER", "daphne"), \
                patch.object(checks, "_cgroup", {"cpu.max": "200000 100000"}.get), \
                patch("os.cpu_count", return_value=2), \
                patch("os.sysconf", side_effect=lambda name: 4096 if name == "SC_PAGE_SIZE" else 4096 * 256):
            self.assertEqual(checks.web_server({})[0], "pass")
            self.assertEqual(checks.web_concurrency({})[0], "pass")

    def test_cloud_proxy_headers(self) -> None:
        cloud = {"LARAVEL_CLOUD": "1", "WEB_CONCURRENCY": "1"}
        entries = {entry[1]: entry for entry in checks.CHECKS}
        with patch.dict(os.environ, cloud, clear=True):
            bad = {name: checks._one(entries[name], {"x-forwarded-proto": "http"})["status"]
                   for name in ("web.proto", "web.forwarded_for", "web.request_id")}
            good_headers = {
                "X-Forwarded-Proto": "https",
                "x-forwarded-for": "1.2.3.4",
                "Cloud-Request-Id": "request",
            }
            good = {name: checks._one(entries[name], good_headers)["status"] for name in bad}
        self.assertEqual(set(bad.values()), {"fail"})
        self.assertEqual(set(good.values()), {"pass"})

    def test_event_loop_upstream_ipv6_and_concurrency(self) -> None:
        with patch.object(app, "SERVER", "uvicorn"):
            for seconds, expected in ((0.21, "pass"), (3.0, "fail")):
                token = checks.LOOP_SECONDS.set(seconds)
                try:
                    self.assertEqual(checks.web_event_loop({})[0], expected)
                finally:
                    checks.LOOP_SECONDS.reset(token)

        self.assertEqual(checks.web_upstream_ipv6({})[0], "skip")
        with patch.dict(os.environ, {"LARAVEL_CLOUD": "1"}, clear=True):
            for peer, expected in (
                ("::1", "pass"),
                ("127.0.0.1", "warn"),
                ("::ffff:127.0.0.1", "warn"),
                ("10.0.0.9", "warn"),
            ):
                token = checks.PEER.set(peer)
                try:
                    self.assertEqual(checks.web_upstream_ipv6({})[0], expected, peer)
                finally:
                    checks.PEER.reset(token)
            token = checks.PEER.set("2600:1f16::2c")
            try:
                for server, expected in (("uvicorn", "pass"), ("granian-asgi", "warn")):
                    with patch.object(app, "SERVER", server):
                        self.assertEqual(
                            checks.web_upstream_ipv6({"x-real-ip": "2600:1f16::2c"})[0],
                            expected,
                        )
                        self.assertEqual(
                            checks.web_upstream_ipv6({"x-real-ip": "2600:1f16::99"})[0],
                            "warn",
                        )
            finally:
                checks.PEER.reset(token)

        pages_per_mib = 2**20 // 4096
        cases = (
            (2, 4096, "5", "pass"),
            (2, 512, "3", "pass"),
            (1, 256, "1", "pass"),
            (2, 4096, "3", "warn"),
            (2, 4096, "", "warn"),
        )
        for cores, memory, value, expected in cases:
            with patch.dict(os.environ, {"LARAVEL_CLOUD": "1", "WEB_CONCURRENCY": value}, clear=True), \
                    patch.object(app, "SERVER", "uvicorn"), \
                    patch.object(checks, "_cgroup", {"cpu.max": f"{cores}00000 100000"}.get), \
                    patch("os.cpu_count", return_value=cores), \
                    patch("os.sysconf", lambda name: 4096 if name == "SC_PAGE_SIZE" else memory * pages_per_mib):
                self.assertEqual(checks.web_concurrency({})[0], expected)

    def test_database_url_edge_cases(self) -> None:
        with patch.dict(os.environ, {"DATABASE_URL": "sqlite:///x"}, clear=True):
            with self.assertRaisesRegex(ValueError, "scheme 'sqlite'"):
                checks._database_options()
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(checks._database_options())
        with patch.dict(os.environ, {"DATABASE_URL": "mysql://[broken"}, clear=True):
            with self.assertRaises(ValueError):
                checks._database_options()

    def test_listeners_limits_and_stream_scoring(self) -> None:
        head = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
        tables = {
            "tcp6": head + "   0: 00000000000000000000000000000000:1F40 00000000000000000000000000000000:0000 0A 0:0 00:0 0 1000 0 1\n",
            "tcp": head + "   0: 0100007F:1F40 00000000:0000 0A 0:0 00:0 0 1000 0 2\n"
                          "   1: 00000000:1F41 00000000:0000 0A 0:0 00:0 0 1000 0 3\n"
                          "   2: 00000000:1F40 0100007F:9999 01 0:0 00:0 0 1000 0 4\n",
        }
        with patch.object(checks.Path, "read_text", lambda path: tables[path.name]):
            self.assertEqual(checks._listeners(8000), {"::", "0100007F"})
            self.assertEqual(checks._listeners(8001), {"0.0.0.0"})

        with patch.dict(os.environ, {"PORT": "8000"}, clear=True), \
                patch.object(checks.Path, "is_file", return_value=True):
            for listeners, expected in (({"::"}, "pass"), ({"0.0.0.0"}, "fail"), (set(), "warn")):
                with patch.object(checks, "_listeners", return_value=listeners):
                    self.assertEqual(checks.web_port({})[0], expected)
        with patch.dict(os.environ, {"PORT": "nope"}, clear=True):
            self.assertEqual(checks.web_port({})[0], "warn")

        self.assertEqual(
            checks._streamed([0.1, 1.1, 2.1, 3.1, 4.1, 5.1]),
            (True, "first chunk at 0.10 s, largest gap 1.00 s (streamed)"),
        )
        self.assertTrue(checks._streamed([5.1] * 6)[1].endswith("(buffered: all at once)"))
        self.assertTrue(checks._streamed([0.1, 0.1, 0.1, 3.1, 3.1, 5.1])[1].endswith("(in bursts)"))
        self.assertEqual(checks._streamed([0.1, 1.1]), (False, "2 of 6 chunks arrived"))

    def test_python_release_stdlib_and_package_contracts(self) -> None:
        running = f"{sys.version_info.major}.{sys.version_info.minor}"
        recent = str(date.today() - timedelta(days=5))
        stale = str(date.today() - timedelta(days=checks.PATCH_LAG_DAYS + 1))
        cases = (
            (f"{running}.{sys.version_info.micro}", recent, "2999-01-01", "pass"),
            (f"{running}.{sys.version_info.micro + 1}", recent, "2999-01-01", "pass"),
            (f"{running}.{sys.version_info.micro + 2}", stale, "2999-01-01", "warn"),
            (f"{running}.{sys.version_info.micro}", recent, "2000-01-01", "warn"),
        )
        for latest, released, eol, expected in cases:
            cycle = {"latest": latest, "latestReleaseDate": released, "eol": eol}
            self.assertEqual(checks._python_release("running", cycle)[0], expected)

        for missing, expected in (({}, "pass"), ({"_curses": "ModuleNotFoundError"}, "warn"),
                                  ({"_lzma": "ModuleNotFoundError"}, "fail")):
            probe = json.dumps({"missing": missing, "encodings": ["utf-8", "UTF-8"]})
            with patch("subprocess.run", return_value=subprocess.CompletedProcess([], 0, probe, "")):
                status, detail, _ = checks.stdlib_complete({})
            self.assertEqual(status, expected, detail)
            self.assertTrue(all(name in detail for name in missing))
        self.assertIn(checks.stdlib_complete({})[0], ("pass", "warn"))
        self.assertEqual(checks.packages_match({})[0], "pass")

        real = checks.inventory()
        site = next(iter(real["packages"]))
        image = checks.sysconfig.get_paths(
            vars={"base": sys.base_prefix, "platbase": sys.base_prefix}
        )["purelib"]
        cases = (
            ({site: dict(real["packages"][site], redis="0.0.1")}, "fail"),
            ({site: dict(real["packages"][site], websockets="16.1.1")}, "pass"),
            ({**real["packages"], image: {"setuptools": "79.0.1", "pip": "24.0"}}, "pass"),
            ({image: {"setuptools": "79.0.1"}, **real["packages"]}, "pass"),
            ({image: {"certifi": "1999.1.1"}, **real["packages"]}, "warn"),
        )
        read_text = checks.Path.read_text

        def fixture_text(path, *args, **kwargs):
            content = read_text(path, *args, **kwargs)
            if path.name == "uv.lock":
                return content + '\n[[package]]\nname = "websockets"\nversion = "16.1.1"\n'
            return content

        for packages, expected in cases:
            with patch.object(checks, "inventory", return_value={**real, "packages": packages}), \
                    patch.object(checks.Path, "read_text", fixture_text):
                status, detail, _ = checks.packages_match({})
            self.assertEqual(status, expected, detail)

    def test_cgroup_limit_rows(self) -> None:
        cgroup = {"cpu.max": "200000 100000", "memory.max": str(4096 * 2**20)}
        with patch.object(checks, "_cgroup", cgroup.get):
            with patch("os.cpu_count", return_value=16):
                self.assertEqual(checks.cpu_limit({})[0], "warn")
            with patch("os.cpu_count", return_value=2):
                self.assertEqual(checks.cpu_limit({})[0], "pass")
            with patch("os.sysconf", lambda name: 4096 if name == "SC_PAGE_SIZE" else 126511 * 256):
                self.assertEqual(checks.memory_limit({})[0], "warn")
            with patch("os.sysconf", lambda name: 4096 if name == "SC_PAGE_SIZE" else 4096 * 256):
                status, detail, _ = checks.memory_limit({})
                self.assertEqual(status, "pass")
                self.assertIn("sysconf reports 4096 MiB", detail)

    def test_static_file_contract(self) -> None:
        def answers(**override):
            result = {}
            for name in checks.STATIC:
                cache = {"cache-control": checks.static_cache(name)} if checks.static_cache(name) else {}
                result[name] = (200, cache, (checks.PUBLIC / name).read_bytes())
            for name in checks.STATIC_BLOCKED:
                result[name] = (403, {}, b"<html>403 Forbidden</html>")
            result["cloud-check-missing.txt"] = (404, {"x-request-id": "request"}, b'{"error":"not found"}')
            result.update(override)
            return result

        self.assertEqual(checks._static_result(answers())[0], "pass")
        css = "static/cloud-check.css"
        self.assertEqual(checks._static_result(answers(**{
            css: (200, {}, (checks.PUBLIC / css).read_bytes()),
        }))[0], "warn")
        leaked = "cloud-check.sql"
        self.assertEqual(checks._static_result(answers(**{
            leaked: (200, {}, (checks.PUBLIC / leaked).read_bytes()),
        }))[0], "fail")
        self.assertEqual(checks._static_result(answers(**{
            "robots.txt": (404, {"x-request-id": "request"}, b'{"error":"not found"}'),
        }))[0], "fail")
        self.assertEqual(checks._static_result(answers(**{
            "cloud-check-missing.txt": (404, {}, b"<html>nginx 404</html>"),
        }))[0], "fail")


class AsyncCheckContracts(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self) -> None:
        await checks.aclose()
        checks._running.clear()

    async def test_registry_never_raises_and_skip_reasons(self) -> None:
        def response(request: httpx.Request) -> httpx.Response:
            if request.url.host == "endoflife.date":
                return httpx.Response(200, json={
                    "latest": f"{checks.python_minor()}.{sys.version_info.micro}",
                    "latestReleaseDate": str(date.today()),
                    "eol": "2999-01-01",
                })
            return httpx.Response(200, content=b"ok")

        http = httpx.AsyncClient(transport=httpx.MockTransport(response))
        env = {
            "LARAVEL_CLOUD": "",
            "DATABASE_URL": "",
            "REDIS_URL": "",
            "LARAVEL_CLOUD_QUEUES_REDIS_URL": "",
        }
        with patch.dict(os.environ, env, clear=True), patch.object(app, "SERVER", "uvicorn"), \
                patch("checks.httpx.AsyncClient", return_value=http):
            await checks.aopen()
            rows = {row["id"]: row for row in check_shape(await checks.arun({}))}
            self.assertEqual(rows["web.event_loop"]["status"], "pass")
            self.assertEqual(rows["runtime.https"]["status"], "pass")
            self.assertEqual(rows["services.database"]["status"], "skip")
            self.assertEqual(rows["services.redis"]["status"], "skip")
            self.assertEqual(rows["web.proto"]["status"], "skip")
            self.assertIn("Not on Laravel Cloud", rows["web.proto"]["detail"])
            self.assertEqual(rows["logging.cloud_viewer"]["status"], "skip")
            self.assertIn("cloud_suite.py --tier full", rows["logging.cloud_viewer"]["detail"])
            self.assertEqual(rows["runtime.tmp"]["status"], "pass")
            self.assertEqual(rows["runtime.subprocess"]["status"], "pass")
            self.assertEqual(rows["runtime.threads"]["status"], "pass")

            with patch("tempfile.NamedTemporaryFile", side_effect=PermissionError("read-only")):
                broken = {row["id"]: row for row in check_shape(await checks.arun({}))}
            self.assertEqual(broken["runtime.tmp"]["status"], "fail")
            self.assertEqual(broken["runtime.tmp"]["detail"], "PermissionError: read-only")
            self.assertEqual(broken["runtime.threads"]["status"], "pass")
            self.assertIsNone(checks.LOOP_SECONDS.get())
            await checks.aclose()
        self.assertTrue(http.is_closed)
        self.assertIsNone(checks._http)
        self.assertEqual(checks.SUITE_KEY, checks.telemetry.KEY_PREFIX + "suite:")

    async def test_parallel_checks_isolate_failure_and_scrub_credentials(self) -> None:
        arrived: list[bool] = []
        ready = asyncio.Event()

        async def probe(headers):
            self.assertEqual(checks.PEER.get(), "::1")
            self.assertEqual(headers, {"test": "request"})
            arrived.append(True)
            if len(arrived) == 2:
                ready.set()
            await ready.wait()
            return "pass", "overlapped", ""

        entries = [("Web", name, name, None, checks.everywhere, "quick") for name in ("test.a", "test.b")]
        token = checks.PEER.set("::1")
        try:
            with patch.object(checks, "CHECKS", entries), \
                    patch.object(checks, "ASYNC_CHECKS", {"test.a": probe, "test.b": probe}), \
                    patch.object(checks, "DEADLINE", 0.5):
                result = await checks.arun({"test": "request"})
            self.assertEqual([row["status"] for row in result["groups"][0]["checks"]], ["pass", "pass"])
        finally:
            checks.PEER.reset(token)

        async def fail(_):
            raise RuntimeError(f"mysql://app:{SECRET}@db.example/x ({SECRET})")

        with patch.dict(os.environ, {"DATABASE_URL": f"mysql://app:{SECRET}@db.example/x"}, clear=True), \
                patch.object(checks, "CHECKS", entries), \
                patch.object(checks, "ASYNC_CHECKS", {
                    "test.a": fail,
                    "test.b": AsyncMock(return_value=("pass", "ok", "")),
                }):
            result = await checks.arun({})
        self.assertNotIn(SECRET, json.dumps(result))
        self.assertEqual([row["status"] for row in result["groups"][0]["checks"]], ["fail", "pass"])

    async def test_deadline_cancellation_and_gather_cleanup(self) -> None:
        started = asyncio.Event()
        stopped: list[bool] = []

        async def hang(_):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                stopped.append(True)

        entry = ("Web", "test.a", "test", None, checks.everywhere, "quick")
        with patch.object(checks, "CHECKS", [entry]), \
                patch.object(checks, "ASYNC_CHECKS", {"test.a": hang}), \
                patch.object(checks, "DEADLINE", 0.05):
            result = await checks.arun({})
        row = result["groups"][0]["checks"][0]
        self.assertEqual((row["status"], row["detail"]), ("fail", "no answer within 0.05 s"))
        self.assertEqual(stopped, [True])

        started.clear()
        with patch.object(checks, "CHECKS", [entry]), patch.object(checks, "ASYNC_CHECKS", {"test.a": hang}):
            task = asyncio.create_task(checks.arun({}))
            await started.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(stopped, [True, True])

        started.clear()

        async def fail_after_start():
            await started.wait()
            raise OSError("offline")

        with self.assertRaises(OSError):
            await checks._agather(hang({}), fail_after_start())
        self.assertEqual(stopped, [True, True, True])

    async def test_timed_out_thread_probe_stays_guarded(self) -> None:
        release = threading.Event()
        entered = threading.Event()

        def sync_probe(_):
            self.assertEqual(checks.PEER.get(), "::1")
            entered.set()
            release.wait(2)
            return "pass", "finished", ""

        entry = ("Runtime", "test.thread", "thread", sync_probe, checks.everywhere, "quick")
        token = checks.PEER.set("::1")
        try:
            with patch.object(checks, "CHECKS", [entry]), patch.object(checks, "SYNC_PROBES", {sync_probe}), \
                    patch.object(checks, "DEADLINE", 0.05):
                result = await checks.arun({})
                self.assertTrue(entered.is_set())
                self.assertEqual(result["groups"][0]["checks"][0]["status"], "fail")
                self.assertEqual((await checks.arun({}))["groups"][0]["checks"][0]["status"], "warn")
        finally:
            release.set()
            checks.PEER.reset(token)
        async with asyncio.timeout(1):
            while "test.thread" in checks._running:
                await asyncio.sleep(0.01)

    async def test_suite_results_validation_and_round_trip(self) -> None:
        saved: dict[str, str] = {}

        async def get(key):
            return saved.get(key)

        async def set_row(key, value):
            saved[key] = value

        client = SimpleNamespace(get=get, set=set_row)
        with patch.object(checks, "_aredis", return_value=client):
            bad = (
                {},
                {"web.server": {"status": "pass", "detail": ""}},
                {"logging.cloud_viewer": {"status": "ok", "detail": ""}},
                {"logging.cloud_viewer": {"status": "pass"}},
                {"logging.cloud_viewer": "pass"},
            )
            for body in bad:
                self.assertEqual((await checks.asave_suite(body))[0], 400, body)
            self.assertEqual((await checks.asave_suite({
                "logging.cloud_viewer": {"status": "fail", "detail": "noise"},
            }))[0], 200)
            status, detail, _ = await checks.asuite_result("logging.cloud_viewer")
            self.assertEqual(status, "fail")
            self.assertTrue(detail.startswith("noise (cloud_suite.py, "))
        self.assertEqual(set(saved), {checks.SUITE_KEY + "logging.cloud_viewer"})
        with patch.object(checks, "_aredis", return_value=None):
            self.assertEqual((await checks.asave_suite({
                "logging.cloud_viewer": {"status": "pass", "detail": "ok"},
            }))[0], 503)

    async def test_database_pools_and_malformed_configuration(self) -> None:
        cursor = SimpleNamespace(execute=AsyncMock(), fetchone=AsyncMock(return_value=(1,)))

        @asynccontextmanager
        async def cursor_context():
            yield cursor

        conn = SimpleNamespace(cursor=cursor_context)

        @asynccontextmanager
        async def connection_context():
            yield conn

        for verify in ("1", "0"):
            pool = SimpleNamespace(open=AsyncMock(), close=AsyncMock(), connection=connection_context)
            env = {
                "LARAVEL_CLOUD": "1",
                "DATABASE_URL": f"postgresql://u:{SECRET}@db.example/x",
                "DB_SSL_VERIFY": verify,
            }
            with patch.dict(os.environ, env, clear=True), \
                    patch("psycopg_pool.AsyncConnectionPool", return_value=pool) as factory:
                await checks.aopen()
                options = factory.call_args.kwargs
                self.assertEqual((options["min_size"], options["max_size"]), (0, 5))
                self.assertEqual(options["kwargs"]["host"], "db.example")
                self.assertIsNone(options["kwargs"]["prepare_threshold"])
                self.assertEqual(options["kwargs"]["sslmode"], "verify-full" if verify == "1" else "require")
                self.assertIn("statement_timeout=5000", options["kwargs"]["options"])
                if verify == "1":
                    self.assertEqual(options["kwargs"]["sslrootcert"], checks.certifi.where())
                self.assertEqual((await checks.adatabase_query({}))[0], "pass")
                await checks.aclose()
            pool.open.assert_awaited_once()
            pool.close.assert_awaited_once()

            pool = SimpleNamespace(close=Mock(), wait_closed=AsyncMock(), acquire=connection_context)
            with patch.dict(os.environ, {
                "DATABASE_URL": f"mysql://u:{SECRET}@db.example/x",
                "DB_SSL_VERIFY": verify,
            }, clear=True), patch("asyncmy.create_pool", AsyncMock(return_value=pool)) as factory:
                await checks.aopen()
                options = factory.call_args.kwargs
                self.assertEqual((options["minsize"], options["maxsize"]), (0, 5))
                self.assertEqual(
                    options["ssl"].verify_mode,
                    checks.ssl.CERT_REQUIRED if verify == "1" else checks.ssl.CERT_NONE,
                )
                self.assertEqual(options["ssl"].check_hostname, verify == "1")
                self.assertEqual(options["read_timeout"], checks.TIMEOUT)
                self.assertEqual((await checks.adatabase_query({}))[0], "pass")
                await checks.aclose()
            pool.close.assert_called_once()
            pool.wait_closed.assert_awaited_once()
        self.assertEqual(cursor.execute.await_count, 4)
        self.assertTrue(all(call.args == ("SELECT 1",) for call in cursor.execute.await_args_list))

        with patch.dict(os.environ, {"DATABASE_URL": "mysql://[broken"}, clear=True):
            await checks.aopen()
            entry = next(entry for entry in checks.CHECKS if entry[1] == "services.database")
            row = await checks._aone(entry, {})
            self.assertEqual(row["status"], "fail")
            self.assertEqual((await checks._aone(next(
                item for item in checks.CHECKS if item[1] == "runtime.threads"
            ), {}))["status"], "pass")

        with patch.dict(os.environ, {"REDIS_URL": "redis://[broken"}, clear=True), \
                patch.object(checks, "_aredis", side_effect=ValueError("malformed Redis URL")):
            redis_entry = next(entry for entry in checks.CHECKS if entry[1] == "services.redis")
            row = await checks._aone(redis_entry, {})
            self.assertEqual(row["status"], "fail")
            self.assertIn("ValueError", row["detail"])

    async def test_async_websocket_and_chunked_stream_parsing(self) -> None:
        def stream(data: bytes):
            reader = asyncio.StreamReader()
            reader.feed_data(data)
            reader.feed_eof()
            writer = SimpleNamespace(write=Mock(), drain=AsyncMock(), close=Mock(), wait_closed=AsyncMock())
            return reader, writer

        key = base64.b64encode(b"k" * 16).decode()
        accept = base64.b64encode(hashlib.sha1(
            (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()
        ).digest())
        upgraded = b"HTTP/1.1 101 Switching Protocols\r\nSec-WebSocket-Accept: " + accept + b"\r\n\r\n"
        with patch.dict(os.environ, {"LARAVEL_CLOUD": "1"}, clear=True), \
                patch("os.urandom", return_value=b"k" * 16):
            for reply, expected in ((upgraded, "pass"), (b"HTTP/1.1 404 Not Found\r\n\r\n", "fail")):
                reader, writer = stream(reply)
                with patch("asyncio.open_connection", AsyncMock(return_value=(reader, writer))) as connect:
                    self.assertEqual((await checks.aweb_websocket({"Host": "app.laravel.cloud"}))[0], expected)
                self.assertTrue(connect.call_args.kwargs["ssl"].check_hostname)
                writer.close.assert_called_once()

            chunks = [b"da", b"ta: 0\n\n", *(f"data: {i}\n\n".encode() for i in range(1, app.STREAM_CHUNKS))]
            for chunked in (False, True):
                data = (b"".join(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n" for chunk in chunks) + b"0\r\n\r\n"
                        if chunked else b"".join(chunks))
                head = b"HTTP/1.1 200 OK\r\n" + (
                    b"Transfer-Encoding: chunked\r\n" if chunked else b""
                ) + b"\r\n"
                reader, writer = stream(head + data)
                with patch("asyncio.open_connection", AsyncMock(return_value=(reader, writer))):
                    arrivals = await checks._aarrivals("app.laravel.cloud", "/api/stream")
                self.assertEqual(len(arrivals), app.STREAM_CHUNKS)
                self.assertEqual(arrivals, sorted(arrivals))
                writer.close.assert_called_once()

            streamed = [0.1, 1.1, 2.1, 3.1, 4.1, 5.1]
            buffered = [5.1] * 6
            for plain, opted, expected in (
                (streamed, streamed, "pass"),
                (buffered, streamed, "pass"),
                (buffered, buffered, "fail"),
            ):
                with patch.object(checks, "_aarrivals", AsyncMock(side_effect=(plain, opted))):
                    self.assertEqual(
                        (await checks.aweb_streaming({"host": "app.laravel.cloud"}))[0],
                        expected,
                    )


if __name__ == "__main__":
    unittest.main()
