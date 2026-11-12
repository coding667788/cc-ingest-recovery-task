"""A04/A10: cursor expiry vs history gap, quarantine and bad envelopes."""
from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from tests.support import (
    FixtureServer,
    TempWorkspace,
    WorkerProcess,
    read_views,
    views_live_map,
    wait_for_condition,
)


class CursorQuarantineTests(unittest.TestCase):
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

    def _wait_caught_up(self, db, timeout=30, degraded=False):
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
            if not degraded and any(r["degraded"]
                                    for r in views["sync_state"]):
                return False
            return True
        wait_for_condition("caught up", done, timeout)

    @staticmethod
    def _live(expected):
        out = {}
        for source in ("alpha", "beta"):
            out[source] = {r["item_id"]: (r["revision"], r["payload"])
                           for r in expected[source]["live"]}
        return out

    def test_a04_changes_cursor_expiry_replays_from_numeric_checkpoint(self):
        config, db, worker = self._start()
        self._wait_caught_up(db)
        worker.stop()
        self.fixture.control("advance", "alpha")
        self.fixture.control("expire-on-page", "alpha", path="changes")
        worker2 = WorkerProcess(config, db).start()
        self.addCleanup(worker2.stop)
        self._wait_caught_up(db)
        lines = worker2.log_lines()
        self.assertTrue(any(line.get("event") == "cursor_expired"
                            for line in lines))
        self.assertFalse(any(line.get("event") == "history_gap"
                             for line in lines))
        views = read_views(db)
        applied = [(r["source"], r["event_id"]) for r in views["sync_applied"]]
        self.assertEqual(len(applied), len(set(applied)))

    def test_a04_snapshot_expiry_discards_unpublished_snapshot(self):
        self.fixture.control("expire-on-page", "alpha", path="snapshot")
        config, db, worker = self._start()
        self._wait_caught_up(db)
        expected = self.fixture.expected()
        views = read_views(db)
        state = next(r for r in views["sync_state"]
                     if r["source"] == "alpha")
        self.assertEqual(state["epoch"], expected["alpha"]["epoch"])
        self.assertEqual(state["checkpoint"], expected["alpha"]["head"])
        self.assertEqual(views_live_map(views)["alpha"],
                         self._live(expected)["alpha"])

    def test_a10_poison_payload_quarantined_then_repair_resolves(self):
        config, db, worker = self._start()
        self._wait_caught_up(db)
        worker.stop()
        self.fixture.control("poison", "beta")
        self.fixture.control("advance", "beta")
        worker2 = WorkerProcess(config, db).start()
        self.addCleanup(worker2.stop)
        self._wait_caught_up(db, degraded=True)
        views = read_views(db)
        state = next(r for r in views["sync_state"]
                     if r["source"] == "beta")
        self.assertEqual(state["degraded"], 1)
        quarantine = [q for q in views["sync_quarantine"]
                      if q["source"] == "beta"]
        self.assertEqual(len(quarantine), 1)
        row = quarantine[0]
        self.assertEqual(row["item_id"], "P00000004")
        self.assertTrue(row["reason"])
        raw = json.loads(row["raw_json"])
        self.assertEqual(raw["payload"]["stock"], "broken")
        # The previous valid version remains readable.
        item = next(r for r in views["sync_items"]
                    if r["source"] == "beta"
                    and r["item_id"] == "P00000004")
        self.assertEqual(item["deleted"], 0)
        # Restart does not duplicate the quarantine.
        worker2.stop()
        worker3 = WorkerProcess(config, db).start()
        self.addCleanup(worker3.stop)
        self._wait_caught_up(db, degraded=True)
        worker3.stop()
        self.assertEqual(
            len([q for q in read_views(db)["sync_quarantine"]
                 if q["source"] == "beta"]), 1)
        # Repair publishes a higher valid revision and resolves the record.
        self.fixture.control("repair", "beta")
        worker4 = WorkerProcess(config, db).start()
        self.addCleanup(worker4.stop)
        self._wait_caught_up(db)
        views = read_views(db)
        qrow = next(q for q in views["sync_quarantine"]
                    if q["source"] == "beta")
        self.assertEqual(qrow["resolved"], 1)
        item = next(r for r in views["sync_items"]
                    if r["source"] == "beta"
                    and r["item_id"] == "P00000004")
        self.assertEqual(json.loads(item["payload_json"])["stock"], 444)


class _BadEnvelopeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    counters = {"events": 0, "changes": 0}
    lock = threading.Lock()

    def log_message(self, *_):
        pass

    def _send(self, status, body: bytes, mime="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", mime + "; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def do_GET(self):
        parts = urlsplit(self.path).path.strip("/").split("/")
        query = parse_qs(urlsplit(self.path).query)
        if parts[-1] == "head":
            self._send(200, b'{"epoch":"e1","head":10,"floor":0}')
            return
        if parts[-1] == "exports":
            self._send(200, b'{"epoch":"e1","export_id":"x","fence":0}')
            return
        if parts[-1] == "snapshot":
            self._send(200, b'{"epoch":"e1","fence":0,"items":[],'
                             b'"next_cursor":null}')
            return
        if parts[-1] in ("bootstrap",):
            self._send(200, b'{"epoch":"e1","ticket":"t","fence":0}')
            return
        if parts[-1] == "scan":
            self._send(200, b'{"epoch":"e1","rows":[],"next_key":null}')
            return
        if parts[-1] == "events":
            with _BadEnvelopeHandler.lock:
                _BadEnvelopeHandler.counters["events"] += 1
                count = _BadEnvelopeHandler.counters["events"]
            if count == 1:
                # A change frame missing the locating key field; the window
                # frame is present but the page cannot be committed.
                body = (
                    '{"kind":"window","epoch":"e1","until":10,'
                    '"through":8,"more":false}\n'
                    '{"kind":"change","offset":5,"change":{'
                    '"event_key":"x","rev":1,"op":"upsert",'
                    '"body":{"title":"t","stock":1,"currency":"CNY"},'
                    '"updated_at":"2026"}}\n'
                ).encode()
                self._send(200, body, "application/x-ndjson")
            else:
                body = (
                    '{"kind":"window","epoch":"e1","until":10,'
                    '"through":10,"more":false}\n'
                ).encode()
                self._send(200, body, "application/x-ndjson")
            return
        if parts[-1] == "changes":
            with _BadEnvelopeHandler.lock:
                _BadEnvelopeHandler.counters["changes"] += 1
                changes_count = _BadEnvelopeHandler.counters["changes"]
            if changes_count == 1:
                body = (
                    '{"epoch":"e1","until":10,"through":8,"events":['
                    '{"seq":5,"event_id":"x","revision":1,'
                    '"deleted":false,"updated_at":"2026",'
                    '"payload":{"title":"t","stock":1,'
                    '"currency":"CNY"}}],"next_cursor":null}'
                ).encode()
            else:
                body = (
                    '{"epoch":"e1","until":10,"through":10,'
                    '"events":[],"next_cursor":null}'
                ).encode()
            self._send(200, body)
            return
        self._send(404, b'{"error":"not_found"}')

    def do_POST(self):
        self.do_GET()


class BadEnvelopeTests(unittest.TestCase):
    def test_missing_locator_field_blocks_checkpoint_until_valid_page(self):
        _BadEnvelopeHandler.counters = {"events": 0, "changes": 0}
        server = ThreadingHTTPServer(("127.0.0.1", 0), _BadEnvelopeHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        def cleanup():
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
        self.addCleanup(cleanup)
        port = server.server_port
        workspace = TempWorkspace()
        config = workspace.config(f"http://127.0.0.1:{port}")
        # Point both source names at the same stub for this focused test.
        raw = json.loads(config.read_text(encoding="utf-8"))
        for source in raw["sources"]:
            source["base_url"] = (f"http://127.0.0.1:{port}/v1/"
                                   + source["name"])
        config.write_text(json.dumps(raw), encoding="utf-8")
        db = workspace.db()
        worker = WorkerProcess(config, db).start()
        self.addCleanup(worker.stop)

        def checkpoint_advances():
            views = read_views(db)
            return all(r["checkpoint"] >= 10 for r in views["sync_state"])
        # The malformed first page is retried; the stub's second response is a
        # valid empty window, after which the checkpoint may advance to 10.
        wait_for_condition("checkpoint advances only after a valid page",
                           checkpoint_advances, timeout=20)
        self.assertGreaterEqual(
            _BadEnvelopeHandler.counters["events"]
            + _BadEnvelopeHandler.counters["changes"], 2)


if __name__ == "__main__":
    unittest.main()
