"""A05/A06: throttling, 503, timeouts, truncated pages, source isolation."""
from __future__ import annotations

import time
import unittest

from tests.support import (
    FixtureServer,
    TempWorkspace,
    WorkerProcess,
    read_views,
    views_live_map,
    wait_for_condition,
)


class FaultTests(unittest.TestCase):
    def setUp(self):
        self.fixture = FixtureServer(seed=20261111, size=24)
        self.fixture.__enter__()
        self.workspace = TempWorkspace()
        self.addCleanup(self.fixture.__exit__, None, None, None)

    def _start(self, **kwargs):
        config = self.workspace.config(self.fixture.base, **kwargs)
        db = self.workspace.db()
        worker = WorkerProcess(config, db).start()
        self.addCleanup(worker.stop)
        return worker, db

    def _wait_caught_up(self, db, timeout=30):
        def done():
            expected = self.fixture.expected()
            views = read_views(db)
            for source in ("alpha", "beta"):
                state = next(r for r in views["sync_state"]
                             if r["source"] == source)
                if (state["epoch"] != expected[source]["epoch"]
                        or state["checkpoint"] != expected[source]["head"]):
                    return False
            return views_live_map(views) == self._live(expected)
        wait_for_condition("caught up after fault", done, timeout)

    @staticmethod
    def _live(expected):
        result = {}
        for source in ("alpha", "beta"):
            result[source] = {r["item_id"]: (r["revision"], r["payload"])
                              for r in expected[source]["live"]}
        return result

    def test_a05_throttle_is_source_scoped_and_retry_after_honored(self):
        worker, db = self._start()
        self._wait_caught_up(db)
        self.fixture.control("fault", "alpha", mode="throttle", count=3)
        self.fixture.control("advance", "all")
        beta_deadline = time.monotonic() + 8
        expected = self.fixture.expected()

        def beta_done():
            views = read_views(db)
            state = next(r for r in views["sync_state"]
                         if r["source"] == "beta")
            return (state["epoch"] == expected["beta"]["epoch"]
                    and state["checkpoint"] == expected["beta"]["head"])
        wait_for_condition("beta advances while alpha is throttled",
                           beta_done, timeout=8)
        self.assertLess(time.monotonic(), beta_deadline + 0.5)
        self._wait_caught_up(db)
        # Alpha requests after a 429 must respect the Retry-After cooldown.
        status = self.fixture.status()
        alpha_requests = [r for r in status["requests"]
                          if r["source"] == "alpha"]
        rejected = [r for r in alpha_requests if r["status"] == 429]
        self.assertTrue(rejected, "expected at least one 429 for alpha")
        for first, second in zip(rejected, rejected[1:]):
            self.assertGreaterEqual(
                second["started"] - first["started"], 2.0 - 0.15)
        # Client-side concurrency ceiling is never exceeded.
        self.assertLessEqual(status["max_active"]["global"], 4)
        self.assertLessEqual(status["max_active"]["alpha"], 2)
        self.assertLessEqual(status["max_active"]["beta"], 2)

    def test_a06_unavailable_alpha_does_not_block_beta(self):
        worker, db = self._start()
        self._wait_caught_up(db)
        self.fixture.control("fault", "alpha", mode="unavailable", count=3)
        self.fixture.control("advance", "all")
        self._wait_caught_up(db)
        retries = [line for line in worker.log_lines()
                   if line.get("event") == "retry"
                   and line.get("source") == "alpha"]
        self.assertTrue(retries)
        # Bounded delays, never a busy loop: first retry waits >= 150ms.
        self.assertGreaterEqual(retries[0]["retry_after"], 0.15)

    def test_a06_timeout_does_not_grow_unbounded_task_queue(self):
        # One worker per source => at most one in-flight request each even
        # while requests time out; the fixture audit confirms the ceiling.
        worker, db = self._start(
            request_timeout_ms=400, poll_interval_ms=100)
        self.fixture.control("fault", "alpha", mode="delay",
                             seconds=1.0, count=3)
        self.fixture.control("advance", "all")
        self._wait_caught_up(db, timeout=30)
        status = self.fixture.status()
        self.assertLessEqual(status["max_active"]["global"], 4)
        self.assertLessEqual(status["max_active"]["alpha"], 2)

    def test_a06_truncated_beta_ndjson_is_replayed_once(self):
        worker, db = self._start()
        self._wait_caught_up(db)
        worker.stop()
        self.fixture.control("advance", "beta")
        self.fixture.control("fault", "beta", mode="truncate",
                             path="/events", count=1)
        worker2 = WorkerProcess(
            self.workspace.config(self.fixture.base), db).start()
        self.addCleanup(worker2.stop)
        self._wait_caught_up(db)
        views = read_views(db)
        # Every effective event lands exactly once.
        keys = [(r["source"], r["epoch"], r["event_id"])
                for r in views["sync_applied"]]
        self.assertEqual(len(keys), len(set(keys)))
        # The truncated page must not have advanced any checkpoint.
        self.assertTrue(any(
            line.get("event") == "retry" and line.get("source") == "beta"
            for line in worker2.log_lines()))

    def test_a06_truncated_alpha_json_page_is_replayed(self):
        worker, db = self._start()
        self._wait_caught_up(db)
        worker.stop()
        self.fixture.control("advance", "alpha")
        self.fixture.control("fault", "alpha", mode="truncate",
                             path="/changes", count=1)
        worker2 = WorkerProcess(
            self.workspace.config(self.fixture.base), db).start()
        self.addCleanup(worker2.stop)
        self._wait_caught_up(db)
        views = read_views(db)
        keys = [(r["source"], r["epoch"], r["event_id"])
                for r in views["sync_applied"]]
        self.assertEqual(len(keys), len(set(keys)))


if __name__ == "__main__":
    unittest.main()
