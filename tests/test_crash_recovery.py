"""A07/A13: crash windows, coupled rollback and restart convergence."""
from __future__ import annotations

import unittest

from tests.support import (
    FixtureServer,
    TempWorkspace,
    WorkerProcess,
    read_views,
    views_live_map,
    wait_for_condition,
)


class CrashRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.fixture = FixtureServer(seed=20261111, size=24)
        self.fixture.__enter__()
        self.workspace = TempWorkspace()
        self.addCleanup(self.fixture.__exit__, None, None, None)

    def _bootstrap_worker(self):
        config = self.workspace.config(self.fixture.base)
        db = self.workspace.db()
        worker = WorkerProcess(config, db).start()
        self._wait_caught_up(db, worker)
        worker.stop()
        return config, db

    def _wait_caught_up(self, db, worker=None, timeout=30,
                        degraded_allowed=False):
        def done():
            expected = self.fixture.expected()
            views = read_views(db)
            for source in ("alpha", "beta"):
                state = next(r for r in views["sync_state"]
                             if r["source"] == source)
                if (state["epoch"] != expected[source]["epoch"]
                        or state["checkpoint"] != expected[source]["head"]):
                    return False
            if views_live_map(views) != self._live(expected):
                return False
            if not degraded_allowed:
                if any(r["degraded"] for r in views["sync_state"]):
                    return False
            return True
        wait_for_condition("convergence after recovery", done, timeout)

    @staticmethod
    def _live(expected):
        out = {}
        for source in ("alpha", "beta"):
            out[source] = {r["item_id"]: (r["revision"], r["payload"])
                           for r in expected[source]["live"]}
        return out

    def _state(self, db, source):
        return next(r for r in read_views(db)["sync_state"]
                    if r["source"] == source)

    def test_a07_three_crash_windows_for_alpha_and_beta_before_commit(self):
        for crash_at in ("after_receive", "before_commit", "after_commit"):
            with self.subTest(crash_at=crash_at):
                self._run_crash_window("alpha", crash_at)
        self._run_crash_window("beta", "before_commit")

    def _run_crash_window(self, source, crash_at):
        config, db = self._bootstrap_worker()
        before = read_views(db)
        self.fixture.control("advance", "all")
        crasher = WorkerProcess(
            config, db, crash_at=crash_at, crash_source=source).start()
        code = crasher.wait_exit(timeout=20)
        self.assertEqual(code, 86)
        self.assertTrue(any(
            line.get("event") == "crash_injected"
            and line.get("crash_at") == crash_at
            for line in crasher.log_lines()))
        after_crash = read_views(db)
        target_state = self._state(db, source)
        if crash_at in ("after_receive", "before_commit"):
            # Nothing from the open page is visible after the process dies.
            self.assertEqual(
                target_state["checkpoint"],
                next(r for r in before["sync_state"]
                     if r["source"] == source)["checkpoint"])
            self.assertEqual(
                len(after_crash["sync_applied"]),
                len(before["sync_applied"]))
            self.assertEqual(
                len(after_crash["sync_quarantine"]),
                len(before["sync_quarantine"]))
        else:
            self.assertGreater(
                target_state["checkpoint"],
                next(r for r in before["sync_state"]
                     if r["source"] == source)["checkpoint"])
        # Restart without the crash flag and converge without duplicates.
        worker = WorkerProcess(config, db).start()
        self.addCleanup(worker.stop)
        self._wait_caught_up(db, worker)
        worker.stop()
        views = read_views(db)
        applied = [(r["source"], r["epoch"], r["event_id"])
                   for r in views["sync_applied"]]
        self.assertEqual(len(applied), len(set(applied)))

    def test_a13_combined_throttle_poison_and_before_commit_rollback(self):
        config, db = self._bootstrap_worker()
        before = read_views(db)
        self.fixture.control("poison", "beta")
        self.fixture.control("advance", "all")
        self.fixture.control("fault", "alpha", mode="throttle", count=3)
        crasher = WorkerProcess(
            config, db, crash_at="before_commit", crash_source="beta").start()
        self.assertEqual(crasher.wait_exit(timeout=25), 86)
        crashed = read_views(db)
        beta = self._state(db, "beta")
        # The whole beta page (quarantine + data + applied + checkpoint)
        # rolls back together.
        self.assertEqual(beta["checkpoint"],
                         next(r for r in before["sync_state"]
                              if r["source"] == "beta")["checkpoint"])
        self.assertEqual(len(crashed["sync_quarantine"]),
                         len(before["sync_quarantine"]))
        self.assertEqual(len(crashed["sync_applied"]),
                         len(before["sync_applied"]))
        worker = WorkerProcess(config, db).start()
        self.addCleanup(worker.stop)

        def beta_degraded_and_caught_up():
            expected = self.fixture.expected()
            views = read_views(db)
            state = next(r for r in views["sync_state"]
                         if r["source"] == "beta")
            if state["epoch"] != expected["beta"]["epoch"]:
                return False
            if state["checkpoint"] != expected["beta"]["head"]:
                return False
            return state["degraded"] == 1
        wait_for_condition("beta caught up and degraded",
                           beta_degraded_and_caught_up, timeout=8)
        views = read_views(db)
        quarantine = [q for q in views["sync_quarantine"]
                      if q["source"] == "beta"]
        self.assertEqual(len(quarantine), 1)
        self.assertEqual(quarantine[0]["resolved"], 0)
        self.assertTrue(quarantine[0]["raw_json"])
        self.assertTrue(quarantine[0]["reason"])
        self.assertEqual(quarantine[0]["item_id"], "P00000004")
        # Alpha eventually honors its own backoff and also converges.
        self._wait_caught_up(db, worker, degraded_allowed=True)
        applied = [(r["source"], r["epoch"], r["event_id"])
                   for r in read_views(db)["sync_applied"]]
        self.assertEqual(len(applied), len(set(applied)))
        # The quarantine is not duplicated on another restart.
        worker.stop()
        worker2 = WorkerProcess(config, db).start()
        self.addCleanup(worker2.stop)
        self._wait_caught_up(db, worker2, degraded_allowed=True)
        worker2.stop()
        self.assertEqual(
            len([q for q in read_views(db)["sync_quarantine"]
                 if q["source"] == "beta"]), 1)


if __name__ == "__main__":
    unittest.main()
