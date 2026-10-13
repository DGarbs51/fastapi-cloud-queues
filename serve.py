"""Start one supported ASGI server: ``python serve.py [server]``."""

from __future__ import annotations

import os
import sys
from pathlib import Path

COMMANDS = {
    "uvicorn": (
        "uvicorn app:app --host :: --port {port} --workers {workers} --loop uvloop --http httptools "
        "--timeout-graceful-shutdown {grace} --log-config logging.json"
    ),
    "granian-asgi": (
        "granian --interface asgi --host :: --port {port} --workers {workers} --loop uvloop "
        "--runtime-mode st --backpressure 1024 --workers-kill-timeout {grace} "
        "--log-config logging.json app:app"
    ),
    "hypercorn-asgi": (
        "hypercorn --bind [::]:{port} --workers {workers} --worker-class uvloop "
        "--graceful-timeout {grace} --log-config json:logging.json app:app"
    ),
    "daphne": "daphne -v 0 --bind :: --port {port} app:app",
}
GRACE = 20
ASGI = set(COMMANDS)
SINGLE_PROCESS = {"daphne"}


def command(server: str) -> list[str]:
    port = os.environ.get("PORT", "8002")
    values = dict(port=port, workers=os.environ.get("WEB_CONCURRENCY", "1"), grace=GRACE)
    return COMMANDS[server].format(**values).split()


if __name__ == "__main__":
    server = sys.argv[1] if len(sys.argv) > 1 else Path(__file__).with_name(".web-server").read_text().strip()
    if server not in COMMANDS:
        raise SystemExit(f"unknown server {server!r}; expected one of {', '.join(COMMANDS)}")
    argv = command(server)
    os.environ["WEB_SERVER"] = server
    os.execvp(argv[0], argv)
