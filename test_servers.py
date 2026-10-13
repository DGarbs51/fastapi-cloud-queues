"""Boot and smoke-test every supported ASGI server through serve.py.

Run: uv run python test_servers.py [uvicorn|granian-asgi|hypercorn-asgi|daphne ...]
"""

from __future__ import annotations

import http.client
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from serve import COMMANDS

ROOT = Path(__file__).resolve().parent


def free_port(family: socket.AddressFamily) -> int:
    host = "::1" if family == socket.AF_INET6 else "127.0.0.1"
    with socket.socket(family) as sock:
        sock.bind((host, 0))
        return sock.getsockname()[1]


def request(host: str, port: int, method: str, path: str, body=b"", headers=None):
    connection = http.client.HTTPConnection(host, port, timeout=10)
    try:
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        return response.status, {key.lower(): value for key, value in response.getheaders()}, response.read()
    finally:
        connection.close()


def wait_ready(process: subprocess.Popen, host: str, port: int, output) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            output.seek(0)
            raise AssertionError(f"server exited {process.returncode}:\n{output.read().decode(errors='replace')[-2000:]}")
        try:
            if request(host, port, "GET", "/api/ping")[0] == 200:
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise AssertionError(f"server did not become ready on {host}:{port}")


def stop(process: subprocess.Popen) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)


def websocket(port: int) -> None:
    with socket.create_connection(("::1", port), timeout=10) as connection:
        connection.sendall(
            b"GET /ws/echo HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            b"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
        )
        response = b""
        while b"\r\n\r\n" not in response:
            chunk = connection.recv(4096)
            if not chunk:
                break
            response += chunk
    assert response.startswith(b"HTTP/1.1 101"), response
    assert b"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" in response, response


def stream(port: int) -> None:
    connection = http.client.HTTPConnection("::1", port, timeout=10)
    try:
        started = time.monotonic()
        connection.request("GET", "/api/stream?accel=no")
        response = connection.getresponse()
        first = response.readline()
        assert response.status == 200 and first.startswith(b"data:"), (response.status, first)
        assert time.monotonic() - started < 1, "first stream event was buffered"
        assert response.getheader("x-accel-buffering") == "no"
    finally:
        connection.close()


def environment(port: int) -> dict[str, str]:
    env = {**os.environ, "PORT": str(port), "WEB_CONCURRENCY": "1", "PYTHONUNBUFFERED": "1"}
    for name in (
        "LARAVEL_CLOUD",
        "LARAVEL_CLOUD_QUEUES_BACKEND",
        "LARAVEL_CLOUD_QUEUES_REDIS_URL",
        "LARAVEL_CLOUD_MANAGED_QUEUES_CONFIG",
        "REDIS_URL",
        "DATABASE_URL",
    ):
        env.pop(name, None)
    return env


def launch(server: str, port: int, default: bool = False) -> None:
    command = [sys.executable, "serve.py", *([] if default else [server])]
    with tempfile.TemporaryFile() as output:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            env=environment(port),
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            wait_ready(process, "::1", port, output)
            status, headers, body = request("::1", port, "GET", "/api/ping")
            payload = json.loads(body)
            assert status == 200 and payload["ok"] is True and payload["pid"], payload
            assert headers["x-content-type-options"] == "nosniff"
            if sys.platform.startswith("linux"):
                assert request("127.0.0.1", port, "GET", "/api/ping")[0] == 200
            assert request("::1", port, "GET", "/")[0] == 200
            assert request("::1", port, "GET", "/missing")[2] == b'{"error":"not found"}'
            assert request(
                "::1", port, "POST", "/api/check", b"{}", {"Content-Type": "text/plain"}
            )[0] == 415
            websocket(port)
            stream(port)
        finally:
            stop(process)
        output.seek(0)
        logs = output.read().decode(errors="replace")
        assert "Traceback (most recent call last)" not in logs, logs[-2000:]


def check(server: str) -> None:
    launch(server, free_port(socket.AF_INET6), default=server == "uvicorn")
    print(f"{server} IPv6 HTTP, WebSocket, and stream smoke passed"
          + ("; IPv4 passed" if sys.platform.startswith("linux") else "; IPv4 requires Linux dual-stack"))


if __name__ == "__main__":
    selected = sys.argv[1:] or list(COMMANDS)
    unknown = set(selected) - set(COMMANDS)
    if unknown:
        raise SystemExit(f"unknown server(s): {', '.join(sorted(unknown))}")
    for name in selected:
        check(name)
    print("test_servers passed")
