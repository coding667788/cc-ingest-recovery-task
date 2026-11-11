"""Tests for supplied inputs only; these do not solve or grade syncworker."""
import json
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import ProxyHandler, Request, build_opener

from fixtures.server import APIError, Lab, make_server


class ProtocolTests(unittest.TestCase):
    def setUp(self):
        self.lab = Lab(seed=17, size=24)

    def call(self, path, query=None, body=None):
        method = "GET" if body is None else "POST"
        return self.lab.dispatch(method, path, query or {}, body or {})[0]

    def control(self, action, source="all", **kwargs):
        return self.call("/admin/control", body=dict(action=action, source=source, **kwargs))

    def test_alpha_export_stays_frozen_during_mutation(self):
        before = self.lab.sources["alpha"].rows()
        export = self.call("/v1/alpha/exports", body={})
        self.control("mutate-on-scan", "alpha")
        rows, cursor = [], None
        while True:
            query = {"export_id": export["export_id"], "limit": "5"}
            if cursor:
                query["cursor"] = cursor
            page = self.call("/v1/alpha/snapshot", query)
            rows.extend(page["items"])
            self.assertEqual(page["fence"], 24)
            cursor = page["next_cursor"]
            if not cursor:
                break
        self.assertEqual(rows, before)
        self.assertEqual(self.lab.sources["alpha"].head, 34)

    def test_beta_mutation_behind_scan_is_present_in_log(self):
        start = self.call("/v1/beta/bootstrap", body={})
        self.control("mutate-on-scan", "beta")
        first = self.call("/v1/beta/scan", {"ticket": start["ticket"], "limit": "7"})
        second = self.call("/v1/beta/scan", {"ticket": start["ticket"], "after_key": first["next_key"]})
        self.assertNotIn("A00000001", [r["item_id"] for r in first["rows"] + second["rows"]])
        source = self.lab.sources["beta"]
        self.assertIn("A00000001", [e["item_id"] for e in source.events if e["seq"] > start["fence"]])
        self.assertEqual(start["fence"], 24)

    def test_duplicate_out_of_order_delete_recreate_and_shared_keys(self):
        self.control("advance")
        alpha, beta = self.lab.sources.values()
        events = alpha.events[24:]
        self.assertEqual(len(events), 10)
        self.assertEqual(events[1]["event_id"], events[-1]["event_id"])
        self.assertGreater(events[1]["revision"], events[2]["revision"])
        self.assertTrue(events[3]["deleted"])
        self.assertFalse(alpha.current["P00000002"]["deleted"])
        self.assertTrue(alpha.current["P00000003"]["deleted"])
        self.assertEqual(alpha.current["P00000001"]["revision"], 3)
        self.assertNotEqual(alpha.current["P00000001"]["payload"], beta.current["P00000001"]["payload"])
        self.assertEqual(len({e["updated_at"] for e in events}), 1)

    def test_alpha_page_cursor_captures_until(self):
        src = self.lab.sources["alpha"]
        self.control("advance", "alpha")
        first = self.call("/v1/alpha/changes", {"epoch": src.epoch, "after": "24", "until": "34", "limit": "7"})
        self.assertEqual(first["through"], 31)
        self.control("advance", "alpha")
        second = self.call("/v1/alpha/changes", {"cursor": first["next_cursor"], "limit": "7"})
        self.assertEqual(second["through"], 34)
        self.assertEqual(second["until"], 34)
        self.assertIsNone(second["next_cursor"])
        self.assertEqual([e["seq"] for e in second["events"]], [32, 33, 34])

    def test_cursor_expiry_is_distinct_from_history_gap(self):
        src = self.lab.sources["alpha"]
        self.control("advance", "alpha")
        self.control("expire-on-page", "alpha", path="changes")
        page = self.call("/v1/alpha/changes", {"epoch": src.epoch, "after": "24", "until": "34"})
        with self.assertRaises(APIError) as ctx:
            self.call("/v1/alpha/changes", {"cursor": page["next_cursor"]})
        self.assertEqual(ctx.exception.code, "cursor_expired")
        replay = self.call("/v1/alpha/changes", {"epoch": src.epoch, "after": "24", "until": "34"})
        self.assertEqual(replay["through"], 31)
        self.control("trim", "alpha")
        with self.assertRaises(APIError) as ctx:
            self.call("/v1/alpha/changes", {"epoch": src.epoch, "after": "31", "until": "34"})
        self.assertEqual(ctx.exception.code, "history_gap")
        empty = self.call("/v1/alpha/changes", {"epoch": src.epoch, "after": "34", "until": "34"})
        self.assertEqual(empty["events"], [])
        self.assertEqual(empty["through"], 34)

    def test_rotate_removes_old_rows_and_resets_versions(self):
        source = self.lab.sources["beta"]
        old_epoch = source.epoch
        self.control("advance", "beta")
        self.control("rotate", "beta")
        self.assertNotEqual(source.epoch, old_epoch)
        self.assertNotIn("P00000001", source.current)
        self.assertEqual(source.current["P00000002"]["revision"], 1)
        with self.assertRaises(APIError) as ctx:
            source.window(old_epoch, 0, 1, 7)
        self.assertEqual(ctx.exception.status, 409)

    def test_poison_envelope_retained_but_current_inventory_not_corrupted(self):
        source = self.lab.sources["beta"]
        before = dict(source.current["P00000004"])
        self.control("poison", "beta")
        self.assertEqual(source.events[-1]["payload"]["stock"], "broken")
        self.assertEqual(source.current["P00000004"], before)
        self.control("repair", "beta")
        self.assertEqual(source.current["P00000004"]["revision"], 3)
        self.assertEqual(source.current["P00000004"]["payload"]["stock"], 444)

    def test_beta_ndjson_schema_and_empty_window(self):
        src = self.lab.sources["beta"]
        self.control("advance", "beta")
        lines = self.call("/v1/beta/events", {"epoch": src.epoch, "after": "24", "until": "34", "limit": "1"})
        self.assertEqual(lines[0]["through"], 25)
        self.assertTrue(lines[0]["more"])
        self.assertEqual(lines[1]["change"]["op"], "upsert")
        self.assertEqual(lines[1]["offset"], 25)
        empty = self.call("/v1/beta/events", {"epoch": src.epoch, "after": "34", "until": "34"})
        self.assertEqual(len(empty), 1)
        self.assertFalse(empty[0]["more"])

    def test_seed_and_size_vary_inputs_without_changing_contract(self):
        other = Lab(seed=19, size=31)
        self.assertEqual(len(other.sources["alpha"].rows()), 31)
        self.assertNotEqual(other.sources["alpha"].rows()[0]["payload"]["stock"],
                            self.lab.sources["alpha"].rows()[0]["payload"]["stock"])

    def test_invalid_controls_do_not_partially_mutate_sources(self):
        with self.assertRaises(APIError):
            self.control("expire-on-page", "all")
        self.assertIsNone(self.lab.sources["alpha"].expire_on_page)
        with self.assertRaises(APIError):
            self.control("fault", "all", mode="delay", seconds=1000)
        self.assertIsNone(self.lab.sources["alpha"].fault)


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.server = make_server(port=0, seed=17, size=24)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"
        self.opener = build_opener(ProxyHandler({}))

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)

    def get(self, path, body=None):
        request = Request(self.base + path,
                          data=None if body is None else json.dumps(body).encode(),
                          headers={"Content-Type": "application/json"})
        with self.opener.open(request, timeout=5) as response:
            return response.headers, response.read()

    def control(self, action, source="all", **kwargs):
        return self.get("/admin/control", dict(action=action, source=source, **kwargs))

    def test_http_full_flow_json_and_ndjson_are_valid(self):
        _, data = self.get("/v1/alpha/exports", {})
        export = json.loads(data)
        _, data = self.get("/v1/alpha/snapshot?" + urlencode({"export_id": export["export_id"], "limit": 100}))
        self.assertEqual(len(json.loads(data)["items"]), 24)
        self.control("advance")
        epoch = self.server.lab.sources["beta"].epoch
        headers, data = self.get("/v1/beta/events?" + urlencode({"epoch": epoch, "after": 24, "until": 34, "limit": 100}))
        self.assertIn("application/x-ndjson", headers["Content-Type"])
        self.assertTrue(data.endswith(b"\n"))
        self.assertEqual(len([json.loads(line) for line in data.splitlines()]), 11)

    def test_throttle_is_source_scoped_and_headers_present(self):
        self.control("fault", "alpha", mode="throttle")
        with self.assertRaises(HTTPError) as ctx:
            self.get("/v1/alpha/head")
        self.assertEqual(ctx.exception.code, 429)
        self.assertEqual(ctx.exception.headers["Retry-After"], "2")
        ctx.exception.close()
        _, data = self.get("/v1/beta/head")
        self.assertEqual(json.loads(data)["head"], 24)
        self.control("clear-faults", "alpha")
        _, data = self.get("/v1/alpha/head")
        self.assertEqual(json.loads(data)["head"], 24)

    def test_truncation_then_replay_and_http_503(self):
        self.control("fault", "beta", mode="truncate", path="/events")
        epoch = self.server.lab.sources["beta"].epoch
        path = "/v1/beta/events?" + urlencode({"epoch": epoch, "after": 0, "until": 24, "limit": 7})
        _, broken = self.get(path)
        self.assertFalse(broken.endswith(b"\n"))
        with self.assertRaises(json.JSONDecodeError):
            for line in broken.splitlines():
                json.loads(line)
        _, complete = self.get(path)
        self.assertEqual(len(complete.splitlines()), 8)
        self.control("fault", "alpha", mode="unavailable")
        with self.assertRaises(HTTPError) as ctx:
            self.get("/v1/alpha/head")
        self.assertEqual(ctx.exception.code, 503)
        ctx.exception.close()

    def test_delay_does_not_hold_global_dispatch_lock(self):
        self.control("fault", "alpha", mode="delay", seconds=0.4)
        with ThreadPoolExecutor(max_workers=2) as pool:
            alpha = pool.submit(self.get, "/v1/alpha/head")
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                with self.server.lab.lock:
                    if self.server.lab.active["alpha"]:
                        break
                time.sleep(0.005)
            _, data = self.get("/v1/beta/head")
            self.assertEqual(json.loads(data)["head"], 24)
            self.assertFalse(alpha.done())
            alpha.result(timeout=2)

    def test_bad_inputs_have_explicit_errors(self):
        for path, status in [("/missing", 404), ("/v1/beta/scan?limit=0", 400),
                             ("/v1/alpha/changes?after=bad", 400)]:
            with self.assertRaises(HTTPError) as ctx:
                self.get(path)
            self.assertEqual(ctx.exception.code, status)
            ctx.exception.close()


if __name__ == "__main__":
    unittest.main()
