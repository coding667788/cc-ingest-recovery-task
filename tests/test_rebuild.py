"""A08/A09/A13: history-gap rebuild, epoch rotation, atomic publication."""
from __future__ import annotations

import sqlite3
import threading
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


def _snapshot_public_data(db):
    """One read transaction: (state rows, item rows) as comparable tuples."""
    conn = sqlite3.connect(str(db), timeout=10)
    try:
        conn.execute("BEGIN")
        states = tuple(sorted(
            (r[0], r[1], r[2], r[3]) for r in conn.execute(
                "SELECT source, epoch, generation, checkpoint FROM sync_state")
        ))
        items = tuple(sorted(
            (r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in conn.execute(
                "SELECT source, epoch, generation, item_id, revision, "
                "deleted, payload_json FROM sync_items")
        ))
        return states, items
    finally:
        conn.close()


class RebuildTests(unittest.TestCase):
    def setUp(self):
        self.fixture = FixtureServer(seed=20261111, size=24)
        self.fixture.__enter__()
        self.workspace = TempWorkspace()
        self.addCleanup(self.fixture.__exit__, None, None, None)

    def _start(self):
        config = self.workspace.config(self.fixture.base)
        db = self.workspace.db()
        worker = WorkerProcess(config, db).start()
        self.addCleanup(worker.stop)
        return config, db, worker

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
        wait_for_condition("caught up", done, timeout)

    @staticmethod
    def _live(expected):
        out = {}
        for source in ("alpha", "beta"):
            out[source] = {r["item_id"]: (r["revision"], r["payload"])
                           for r in expected[source]["live"]}
        return out

    def test_a08_history_gap_rebuild_keeps_old_generation_until_publish(self):
        config, db, worker = self._start()
        self._wait_caught_up(db)
        worker.stop()
        old_states, old_items = _snapshot_public_data(db)
        self.fixture.control("advance", "beta")
        self.fixture.control("trim", "beta")
        self.fixture.control("advance", "beta")
        # A 3s scan delay gives the reader thread a real window during rebuild.
        self.fixture.control("fault", "beta", mode="delay",
                             path="/scan", seconds=3, count=1)

        observed = []
        stop = threading.Event()

        def reader():
            while not stop.is_set():
                try:
                    observed.append(_snapshot_public_data(db))
                except sqlite3.Error:
                    pass
                time.sleep(0.02)
        thread = threading.Thread(target=reader, daemon=True)
        thread.start()
        # Let the reader observe the old generation before the rebuild starts.
        time.sleep(0.2)
        worker2 = WorkerProcess(config, db).start()
        self.addCleanup(worker2.stop)
        try:
            self._wait_caught_up(db)
            # Keep reading after convergence so the new generation is sampled
            # under the same continuous-reader guarantee.
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                observed.append(_snapshot_public_data(db))
                time.sleep(0.02)
        finally:
            stop.set()
            thread.join(timeout=5)
        new_states, new_items = _snapshot_public_data(db)
        old_beta_state = next(s for s in old_states if s[0] == "beta")
        new_beta_state = next(s for s in new_states if s[0] == "beta")
        old_beta_items = tuple(i for i in old_items if i[0] == "beta")
        new_beta_items = tuple(i for i in new_items if i[0] == "beta")
        # Every observation is either the complete old generation or the
        # complete new one; never an empty or mixed intermediate state.
        for states, items in observed:
            beta_state = next(s for s in states if s[0] == "beta")
            beta_items = tuple(i for i in items if i[0] == "beta")
            if beta_state == old_beta_state:
                self.assertEqual(beta_items, old_beta_items)
            elif beta_state == new_beta_state:
                self.assertEqual(beta_items, new_beta_items)
            else:
                self.fail(f"observed non-atomic beta state: {beta_state}")
        self.assertTrue(any(
            next(s for s in states if s[0] == "beta") == old_beta_state
            for states, _ in observed))
        self.assertTrue(any(
            next(s for s in states if s[0] == "beta") == new_beta_state
            for states, _ in observed),
            msg=("old=" + str(old_beta_state) + " new=" + str(new_beta_state)
                 + " seen=" + str(sorted({next(s for s in states if s[0] == "beta")
                                          for states, _ in observed}))))

    def test_a08_other_source_keeps_advancing_during_rebuild(self):
        config, db, worker = self._start()
        self._wait_caught_up(db)
        worker.stop()
        self.fixture.control("advance", "beta")
        self.fixture.control("trim", "beta")
        self.fixture.control("advance", "beta")
        self.fixture.control("fault", "beta", mode="delay",
                             path="/scan", seconds=2, count=1)
        worker2 = WorkerProcess(config, db).start()
        self.addCleanup(worker2.stop)
        self.fixture.control("advance", "alpha")
        expected = self.fixture.expected()

        def alpha_advanced():
            state = next(r for r in read_views(db)["sync_state"]
                         if r["source"] == "alpha")
            return (state["epoch"] == expected["alpha"]["epoch"]
                    and state["checkpoint"] == expected["alpha"]["head"])
        wait_for_condition("alpha advances during beta rebuild",
                           alpha_advanced, timeout=15)
        self._wait_caught_up(db)

    def test_a09_epoch_rotation_publishes_new_generation_without_ghosts(self):
        config, db, worker = self._start()
        self._wait_caught_up(db)
        worker.stop()
        self.fixture.control("advance", "all")
        self._restart_and_catch_up(config, db)
        before = read_views(db)
        old_alpha_epoch = next(r for r in before["sync_state"]
                               if r["source"] == "alpha")["epoch"]
        self.fixture.control("rotate", "alpha")
        self._restart_and_catch_up(config, db)
        views = read_views(db)
        alpha_state = next(r for r in views["sync_state"]
                           if r["source"] == "alpha")
        self.assertNotEqual(alpha_state["epoch"], old_alpha_epoch)
        expected = self.fixture.expected()
        self.assertEqual(alpha_state["epoch"], expected["alpha"]["epoch"])
        self.assertEqual(alpha_state["checkpoint"],
                         expected["alpha"]["head"])
        alpha_rows = [r for r in views["sync_items"]
                      if r["source"] == "alpha"]
        self.assertTrue(all(r["epoch"] == alpha_state["epoch"]
                            for r in alpha_rows))
        ids = {r["item_id"] for r in alpha_rows if r["deleted"] == 0}
        self.assertNotIn("P00000001", ids)
        self.assertNotIn("A00000001", ids)
        # Version 1 in the new epoch is accepted despite higher old revisions.
        p2 = next(r for r in alpha_rows if r["item_id"] == "P00000002")
        self.assertEqual(p2["revision"], 1)
        # Beta remains untouched by alpha rotation.
        beta_state = next(r for r in views["sync_state"]
                          if r["source"] == "beta")
        self.assertEqual(beta_state["epoch"],
                         expected["beta"]["epoch"])

    def test_a09_second_rotation_while_unpublished_discards_stale_generation(self):
        config, db, worker = self._start()
        self._wait_caught_up(db)
        worker.stop()
        self.fixture.control("rotate", "alpha")
        # rotate clears source faults, so the delay must be installed after
        # it. The rebuild row is persisted before the export; the long export
        # keeps the build unpublished while the second rotation is prepared.
        self.fixture.control("fault", "alpha", mode="delay",
                             path="/exports", seconds=8, count=2)
        rotating = WorkerProcess(config, db).start()
        self.addCleanup(rotating.stop)
        # Wait until an unpublished alpha rebuild row exists, then rotate again.
        def rebuild_seen():
            conn = sqlite3.connect(str(db), timeout=10)
            try:
                row = conn.execute(
                    "SELECT epoch FROM sync_rebuild WHERE source='alpha'"
                ).fetchone()
            except sqlite3.Error:
                return False
            finally:
                conn.close()
            return row is not None
        wait_for_condition("unpublished alpha rebuild exists", rebuild_seen, 20)
        self.fixture.control("rotate", "alpha")
        # The in-flight export still belongs to the now-stale epoch; when it
        # returns the worker must detect the mismatch and start another build.
        self._wait_caught_up(db, timeout=40)
        views = read_views(db)
        state = next(r for r in views["sync_state"]
                     if r["source"] == "alpha")
        expected = self.fixture.expected()
        self.assertEqual(state["epoch"], expected["alpha"]["epoch"])
        self.assertEqual(state["checkpoint"], expected["alpha"]["head"])
        self.assertEqual(views_live_map(views)["alpha"],
                         self._live(expected)["alpha"])

    def test_a13_continuous_writes_and_rotation_before_publish_barrier(self):
        config, db, worker = self._start()
        self._wait_caught_up(db)
        worker.stop()
        old = read_views(db)
        old_beta = next(r for r in old["sync_state"]
                        if r["source"] == "beta")
        # Open a gap, keep writing, and rotate before any publish completes.
        self.fixture.control("advance", "beta")
        self.fixture.control("trim", "beta")
        self.fixture.control("fault", "beta", mode="delay",
                             path="/scan", seconds=2, count=2)
        rotating = WorkerProcess(config, db).start()
        self.addCleanup(rotating.stop)

        def rebuild_seen():
            conn = sqlite3.connect(str(db), timeout=10)
            try:
                row = conn.execute(
                    "SELECT epoch FROM sync_rebuild WHERE source='beta'"
                ).fetchone()
            except sqlite3.Error:
                return False
            finally:
                conn.close()
            return row is not None
        wait_for_condition("beta rebuild row visible", rebuild_seen, 10)
        self.fixture.control("advance", "beta")
        self.fixture.control("rotate", "beta")
        # rotate clears faults; keep the rebuild observable with a fresh delay.
        self.fixture.control("fault", "beta", mode="delay",
                             path="/scan", seconds=1, count=1)
        self._wait_caught_up(db, timeout=30)
        views = read_views(db)
        state = next(r for r in views["sync_state"]
                     if r["source"] == "beta")
        expected = self.fixture.expected()
        self.assertEqual(state["epoch"], expected["beta"]["epoch"])
        self.assertEqual(state["checkpoint"], expected["beta"]["head"])
        self.assertEqual(views_live_map(views)["beta"],
                         self._live(expected)["beta"])
        # The old published generation stayed readable until replaced.
        self.assertNotEqual(state["generation"], old_beta["generation"])

    def test_a08_force_kill_during_rebuild_then_restart_converges(self):
        config, db, worker = self._start()
        self._wait_caught_up(db)
        worker.stop()
        old_states, old_items = _snapshot_public_data(db)
        self.fixture.control("advance", "beta")
        self.fixture.control("trim", "beta")
        self.fixture.control("advance", "beta")
        # Long enough to kill the process while the rebuild is in flight.
        self.fixture.control("fault", "beta", mode="delay",
                             path="/scan", seconds=5, count=3)
        rebuilding = WorkerProcess(config, db).start()

        def rebuild_row():
            conn = sqlite3.connect(str(db), timeout=10)
            try:
                return conn.execute(
                    "SELECT 1 FROM sync_rebuild WHERE source='beta'"
                ).fetchone() is not None
            except sqlite3.Error:
                return False
            finally:
                conn.close()
        wait_for_condition("rebuild in progress", rebuild_row, 10)
        rebuilding.process.kill()
        rebuilding.wait_exit(timeout=10)
        # Public data must still be the intact old generation.
        states, items = _snapshot_public_data(db)
        self.assertEqual(
            next(s for s in states if s[0] == "beta"),
            next(s for s in old_states if s[0] == "beta"))
        self.assertEqual(
            tuple(i for i in items if i[0] == "beta"),
            tuple(i for i in old_items if i[0] == "beta"))
        # Clear the delay so restart can converge; same DB, same fixture.
        self.fixture.control("clear-faults", "beta")
        worker2 = WorkerProcess(config, db).start()
        self.addCleanup(worker2.stop)
        self._wait_caught_up(db)
        views = read_views(db)
        expected = self.fixture.expected()
        state = next(r for r in views["sync_state"]
                     if r["source"] == "beta")
        self.assertEqual(state["epoch"], expected["beta"]["epoch"])
        self.assertEqual(state["checkpoint"], expected["beta"]["head"])

    def _restart_and_catch_up(self, config, db):
        worker = WorkerProcess(config, db).start()
        self.addCleanup(worker.stop)
        self._wait_caught_up(db)
        worker.stop()


if __name__ == "__main__":
    unittest.main()
