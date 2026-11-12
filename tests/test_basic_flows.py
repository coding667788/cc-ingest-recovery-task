"""A01-A03: bootstrap, mutation during full scan, ordering/dedup semantics."""
from __future__ import annotations

import json
import unittest

from tests.support import (
    FixtureServer,
    TempWorkspace,
    WorkerProcess,
    expected_live_map,
    read_views,
    views_live_map,
    wait_for_condition,
    wait_published,
)


class BasicFlowTests(unittest.TestCase):
    def setUp(self):
        self.fixture = FixtureServer(seed=20261111, size=24)
        self.fixture.__enter__()
        self.workspace = TempWorkspace()
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        self.fixture.__exit__(None, None, None)

    def _start_worker(self, **config_kwargs):
        config = self.workspace.config(self.fixture.base, **config_kwargs)
        db = self.workspace.db()
        worker = WorkerProcess(config, db).start()
        self.addCleanup(lambda: worker.stop())
        return worker, db

    def _wait_caught_up(self, db: str):
        def done():
            expected = self.fixture.expected()
            views = read_views(db)
            for source in ("alpha", "beta"):
                state = next(r for r in views["sync_state"]
                             if r["source"] == source)
                if state["epoch"] != expected[source]["epoch"]:
                    return False
                if state["checkpoint"] != expected[source]["head"]:
                    return False
            return views_live_map(views) == expected_live_map(self.fixture)
        try:
            wait_for_condition("both sources caught up with expected", done, 30)
        except AssertionError:
            views = read_views(db)
            expected = self.fixture.expected()
            states = {r["source"]: {k: r[k] for k in
                                     ("epoch", "checkpoint", "status",
                                      "generation", "degraded")}
                      for r in views["sync_state"]}
            heads = {s: {"epoch": expected[s]["epoch"],
                         "head": expected[s]["head"]}
                     for s in ("alpha", "beta")}
            got = views_live_map(views)
            want = expected_live_map(self.fixture)
            details = {}
            for source in ("alpha", "beta"):
                details[source] = {
                    "only_expected": sorted(set(want[source]) - set(got[source])),
                    "only_got": sorted(set(got[source]) - set(want[source])),
                    "mismatched": [
                        key for key in sorted(set(want[source]) & set(got[source]))
                        if want[source][key] != got[source][key]
                    ],
                }
            raise AssertionError(json.dumps(
                {"states": states, "heads": heads, "diff": details},
                ensure_ascii=False))

    def test_a01_fresh_bootstrap_and_source_identity(self):
        worker, db = self._start_worker()
        expected = self.fixture.expected()
        for source in ("alpha", "beta"):
            wait_published(
                lambda db=db: read_views(db), source,
                expected[source]["head"], expected[source]["epoch"],
            )
        self._wait_caught_up(db)
        views = read_views(db)
        # Shared item ids remain source-isolated (different titles per source).
        alpha = {r["item_id"]: r for r in views["sync_items"]
                 if r["source"] == "alpha"}
        beta = {r["item_id"]: r for r in views["sync_items"]
                if r["source"] == "beta"}
        self.assertIn("P00000001", alpha)
        self.assertIn("P00000001", beta)
        self.assertNotEqual(alpha["P00000001"]["payload_json"],
                            beta["P00000001"]["payload_json"])
        events = [line for line in worker.log_lines()
                  if line["event"] in ("bootstrap", "publish")]
        self.assertEqual(
            sorted({line["source"] for line in events}), ["alpha", "beta"])

    def test_a02_mutation_during_full_scan(self):
        self.fixture.control("mutate-on-scan", "all")
        _, db = self._start_worker(page_size=7)
        self._wait_caught_up(db)
        views = read_views(db)
        live = views_live_map(views)
        expected = expected_live_map(self.fixture)
        self.assertEqual(live, expected)
        # Specific assertions from the acceptance scenario.
        for source in ("alpha", "beta"):
            ids = set(live[source])
            self.assertNotIn("P00000003", ids)
            self.assertIn("P00000002", ids)
            self.assertIn("A00000001", ids)
            self.assertIn("P00000001", ids)

    def test_a03_ordering_duplicates_recreate_shared_timestamp_and_restart(self):
        worker, db = self._start_worker()
        self._wait_caught_up(db)
        worker.stop()
        self.fixture.control("advance", "all")
        worker2 = WorkerProcess(
            self.workspace.config(self.fixture.base), db).start()
        self.addCleanup(lambda: worker2.stop())
        self._wait_caught_up(db)
        views = read_views(db)
        live = views_live_map(views)
        self.assertEqual(live, expected_live_map(self.fixture))
        for source in ("alpha", "beta"):
            ids = set(live[source])
            self.assertIn("P00000001", ids)
            self.assertIn("P00000002", ids)
            self.assertNotIn("P00000003", ids)
            self.assertIn("A00000001", ids)
            self.assertIn("Z00000001", ids)
        applied = views["sync_applied"]
        # Seven unique logical events per source in this batch, no duplicates.
        for source in ("alpha", "beta"):
            rows = [r for r in applied if r["source"] == source]
            event_ids = [r["event_id"] for r in rows]
            self.assertEqual(len(event_ids), len(set(event_ids)))
        # Restart again: convergence must not add duplicate applied rows.
        worker2.stop()
        before = len(read_views(db)["sync_applied"])
        worker3 = WorkerProcess(
            self.workspace.config(self.fixture.base), db).start()
        self.addCleanup(lambda: worker3.stop())
        self._wait_caught_up(db)
        worker3.stop()
        self.assertEqual(len(read_views(db)["sync_applied"]), before)


if __name__ == "__main__":
    unittest.main()
