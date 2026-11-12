"""Shared test infrastructure.

Tests drive the real fixture HTTP server in-process (threads) and the real
syncworker as a subprocess for crash scenarios. The production code under
test only ever calls /v1/ endpoints; only these test helpers use /admin/.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from fixtures.server import make_server

ROOT = Path(__file__).resolve().parents[1]


class FixtureServer:
    def __init__(self, seed: int = 20261111, size: int = 24, port: int = 0):
        self.server = make_server(port=port, seed=seed, size=size)
        self.thread = None

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.server.server_port}"

    def __enter__(self):
        self.thread = __import__("threading").Thread(
            target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    # -- admin controls (test driver only) ---------------------------------

    def control(self, action: str, source: str = "all", **body):
        payload = {"action": action, "source": source, **body}
        request = urllib.request.Request(
            self.base + "/admin/control",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.build_opener(
            urllib.request.ProxyHandler({})
        ).open(request, timeout=10) as response:
            return json.load(response)

    def expected(self) -> dict:
        with urllib.request.build_opener(
            urllib.request.ProxyHandler({})
        ).open(self.base + "/admin/expected", timeout=10) as response:
            return json.load(response)

    def status(self) -> dict:
        with urllib.request.build_opener(
            urllib.request.ProxyHandler({})
        ).open(self.base + "/admin/status", timeout=10) as response:
            return json.load(response)


def write_config(path: Path, base_url: str, *, page_size: int = 7,
                 max_concurrency: int = 4, source_concurrency: int = 2,
                 poll_interval_ms: int = 100,
                 request_timeout_ms: int = 1500) -> Path:
    config = {
        "poll_interval_ms": poll_interval_ms,
        "request_timeout_ms": request_timeout_ms,
        "page_size": page_size,
        "max_concurrency": max_concurrency,
        "sources": [
            {"name": "alpha",
             "base_url": base_url + "/v1/alpha",
             "max_concurrency": source_concurrency},
            {"name": "beta",
             "base_url": base_url + "/v1/beta",
             "max_concurrency": source_concurrency},
        ],
    }
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


class WorkerProcess:
    """Run the real syncworker entry point as a subprocess."""

    def __init__(self, config_path: Path, db_path: Path,
                 crash_at: str | None = None,
                 crash_source: str | None = None):
        self.config_path = config_path
        self.db_path = db_path
        self.crash_at = crash_at
        self.crash_source = crash_source
        self.process: subprocess.Popen | None = None
        self.log_path = db_path.with_suffix(".log")

    def start(self) -> "WorkerProcess":
        args = [sys.executable, "-u", "-m", "syncworker",
                "--config", str(self.config_path), "--db", str(self.db_path)]
        if self.crash_at:
            args += ["--crash-at", self.crash_at,
                     "--crash-source", self.crash_source or "alpha"]
        log = open(self.log_path, "w", encoding="utf-8")
        self.process = subprocess.Popen(
            args, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self._log_handle = log
        return self

    def stop(self, timeout: float = 10) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=timeout)
        self._log_handle.close()

    def wait_exit(self, timeout: float = 20) -> int:
        assert self.process is not None
        code = self.process.wait(timeout=timeout)
        self._log_handle.close()
        return code

    def logs(self) -> str:
        return self.log_path.read_text(encoding="utf-8")

    def log_lines(self) -> list[dict]:
        lines = []
        for line in self.logs().splitlines():
            try:
                lines.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        return lines


def read_views(db_path: Path) -> dict:
    """Read the four public tables in one consistent read transaction.

    A plain connection is used instead of read-only URI mode: under WAL a
    mode=ro connection cannot create the -shm/-wal files it needs when it is
    the very first opener, which intermittently fails while a test worker is
    still initializing. The connection only issues SELECT statements.
    """
    deadline = time.monotonic() + 10
    while True:
        try:
            conn = sqlite3.connect(str(db_path), timeout=10)
            conn.row_factory = sqlite3.Row
            try:
                conn.execute("BEGIN")
                result = {}
                for table in ("sync_state", "sync_items", "sync_applied",
                              "sync_quarantine"):
                    result[table] = [dict(row) for row in
                                     conn.execute(f'SELECT * FROM "{table}"')]
                return result
            finally:
                conn.close()
        except sqlite3.Error:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.1)


def wait_for_condition(description: str, predicate, timeout: float = 30.0,
                       interval: float = 0.1):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:  # transient read during WAL rotation etc.
            last_error = exc
        time.sleep(interval)
    raise AssertionError(
        f"condition not met within {timeout}s: {description} "
        f"(last_error={last_error})"
    )


def wait_published(views, source: str, expected_head: int,
                   expected_epoch: str, timeout: float = 30.0) -> dict:
    def condition():
        state = views()["sync_state"]
        row = next(r for r in state if r["source"] == source)
        return (row["epoch"] == expected_epoch
                and row["checkpoint"] == expected_head
                and row["generation"] is not None)
    wait_for_condition(f"{source} published at {expected_head}", condition,
                       timeout=timeout)
    return next(r for r in views()["sync_state"] if r["source"] == source)


def expected_live_map(fixture: FixtureServer) -> dict:
    data = fixture.expected()
    result = {}
    for source in ("alpha", "beta"):
        result[source] = {
            row["item_id"]: (row["revision"], row["payload"])
            for row in data[source]["live"]
        }
    return result


def views_live_map(views: dict) -> dict:
    result = {"alpha": {}, "beta": {}}
    for row in views["sync_items"]:
        if row["deleted"] == 1:
            continue
        result[row["source"]][row["item_id"]] = (
            row["revision"], json.loads(row["payload_json"])
        )
    return result


class TempWorkspace:
    def __init__(self):
        base = ROOT / "runtime"
        # os.mkdir keeps default inherited ACLs; tempfile.mkdtemp creates
        # directories that the execution sandbox later denies writes into.
        self.dir = base / f"tw_{os.getpid()}_{id(self):x}"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.counter = 0

    def config(self, base_url: str, **kwargs) -> Path:
        self.counter += 1
        return write_config(
            self.dir / f"config_{self.counter}.json", base_url, **kwargs)

    def db(self, name: str = "state.sqlite") -> Path:
        return self.dir / name
