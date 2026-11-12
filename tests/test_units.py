"""Focused unit tests for backoff, envelope validation and HTTP framing."""
from __future__ import annotations

import threading
import unittest

from syncworker.backoff import Backoff
from syncworker.errors import BadEnvelope, IncompletePage
from syncworker.httpclient import parse_json, parse_ndjson
from syncworker.validation import validate_envelope, validate_payload


class BackoffTests(unittest.TestCase):
    def test_rate_limit_uses_server_retry_after_and_is_interruptible(self):
        shutdown = threading.Event()
        backoff = Backoff(shutdown)
        delay = backoff.rate_limited(2)
        self.assertEqual(delay, 2)
        self.assertGreaterEqual(backoff.backoff_until, 0)
        shutdown.set()
        self.assertFalse(backoff.wait(delay))

    def test_transient_failures_are_bounded_and_reset(self):
        backoff = Backoff(threading.Event())
        delays = [backoff.failure() for _ in range(12)]
        self.assertTrue(all(d <= Backoff.MAX_DELAY for d in delays))
        self.assertLess(delays[0], delays[1])
        backoff.reset()
        self.assertIsNone(backoff.backoff_until)
        self.assertLessEqual(backoff.failure(), Backoff.MAX_DELAY)

    def test_permanent_error_uses_slow_retry_delay(self):
        backoff = Backoff(threading.Event())
        self.assertEqual(backoff.permanent(), Backoff.PERMANENT_DELAY)


class ValidationTests(unittest.TestCase):
    def test_envelope_requires_locator_fields(self):
        valid = {"seq": 1, "event_id": "e", "item_id": "k", "revision": 1,
                 "deleted": False,
                 "payload": {"title": "t", "stock": 1, "currency": "CNY"},
                 "updated_at": "2026"}
        validate_envelope(valid, require_seq=True)
        snapshot = dict(valid)
        del snapshot["seq"]
        validate_envelope(snapshot, require_seq=False)
        for field in ("event_id", "item_id", "revision", "deleted",
                      "payload", "updated_at"):
            broken = dict(valid)
            del broken[field]
            with self.assertRaises(BadEnvelope):
                validate_envelope(broken, require_seq=True)
        boolean_stock = dict(valid, payload={
            "title": "t", "stock": True, "currency": "CNY"})
        validate_envelope(boolean_stock, require_seq=True)
        self.assertIn("stock", validate_payload(boolean_stock))

    def test_payload_rules(self):
        upsert = {"deleted": False,
                  "payload": {"title": "t", "stock": 0, "currency": "CNY"}}
        self.assertIsNone(validate_payload(upsert))
        delete = {"deleted": True, "payload": None}
        self.assertIsNone(validate_payload(delete))
        self.assertTrue(validate_payload({"deleted": True, "payload": {}}))
        self.assertTrue(validate_payload(
            {"deleted": False, "payload": None}))
        self.assertTrue(validate_payload(
            {"deleted": False,
             "payload": {"title": "", "stock": 1, "currency": "CNY"}}))
        self.assertTrue(validate_payload(
            {"deleted": False,
             "payload": {"title": "t", "stock": -1,
                          "currency": "CNY"}}))
        self.assertTrue(validate_payload(
            {"deleted": False,
             "payload": {"title": "t", "stock": 1, "currency": "USD"}}))


class FramingTests(unittest.TestCase):
    def test_parse_json_rejects_truncated_object(self):
        with self.assertRaises(IncompletePage):
            parse_json(b'{"a": 1')

    def test_parse_ndjson_requires_trailing_newline_and_full_lines(self):
        good = b'{"kind":"window"}\n{"kind":"change"}\n'
        self.assertEqual(len(parse_ndjson(good)), 2)
        with self.assertRaises(IncompletePage):
            parse_ndjson(b'{"kind":"window"}\n{"kind":"change"}')
        with self.assertRaises(IncompletePage):
            parse_ndjson(b'{"kind":"window"}\n{broken}\n')


if __name__ == "__main__":
    unittest.main()
