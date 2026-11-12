"""A11: parameter variation, page_size=1 and the 5000-row performance case."""
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


class ScaleTests(unittest.TestCase):
    def _run_case(self, seed, size, page_size):
        fixture = FixtureServer(seed=seed, size=size)
        fixture.__enter__()
        self.addCleanup(fixture.__exit__, None, None, None)
        workspace = TempWorkspace()
        config = workspace.config(fixture.base, page_size=page_size)
        db = workspace.db()
        worker = WorkerProcess(config, db).start()
        self.addCleanup(worker.stop)

        def live(expected):
            return {source: {r["item_id"]: (r["revision"], r["payload"])
                             for r in expected[source]["live"]}
                    for source in ("alpha", "beta")}

        def published():
            expected = fixture.expected()
            views = read_views(db)
            for source in ("alpha", "beta"):
                state = next(r for r in views["sync_state"]
                             if r["source"] == source)
                if (state["epoch"] != expected[source]["epoch"]
                        or state["checkpoint"] != expected[source]["head"]
                        or state["generation"] is None):
                    return False
            return views_live_map(views) == live(expected)
        wait_for_condition(f"seed={seed} size={size} page_size={page_size}",
                           published, timeout=60)
        # Exercise A02/A03 style dynamics with the varied parameters too.
        fixture.control("mutate-on-scan", "all")
        fixture.control("advance", "all")
        wait_for_condition("converged after dynamics", published, timeout=60)
        worker.stop()

    def test_a11_small_page_and_varied_seed(self):
        self._run_case(seed=42, size=31, page_size=1)

    def test_a11_medium_seed_and_page_size(self):
        self._run_case(seed=9001, size=300, page_size=19)

    def test_a11_large_initial_publish_under_120_seconds(self):
        fixture = FixtureServer(seed=777, size=5000)
        fixture.__enter__()
        self.addCleanup(fixture.__exit__, None, None, None)
        workspace = TempWorkspace()
        config = workspace.config(fixture.base, page_size=100)
        db = workspace.db()
        worker = WorkerProcess(config, db).start()
        self.addCleanup(worker.stop)
        started = time.monotonic()

        def both_published():
            views = read_views(db)
            expected = fixture.expected()
            for source in ("alpha", "beta"):
                state = next(r for r in views["sync_state"]
                             if r["source"] == source)
                if (state["generation"] is None
                        or state["epoch"] != expected[source]["epoch"]
                        or state["checkpoint"] != expected[source]["head"]):
                    return False
            return True
        wait_for_condition("5000-row dual-source first publish",
                           both_published, timeout=120)
        elapsed = time.monotonic() - started
        # Record the actual measured time; correctness does not depend on a
        # hard-coded duration, but the acceptance budget is 120s.
        self.assertLess(elapsed, 120.0)
        status = fixture.status()
        self.assertLessEqual(status["max_active"]["global"], 4)
        self.assertLessEqual(status["max_active"]["alpha"], 2)
        self.assertLessEqual(status["max_active"]["beta"], 2)
        expected = fixture.expected()
        views = read_views(db)
        self.assertEqual(views_live_map(views), {
            source: {r["item_id"]: (r["revision"], r["payload"])
                     for r in expected[source]["live"]}
            for source in ("alpha", "beta")
        })


if __name__ == "__main__":
    unittest.main()
