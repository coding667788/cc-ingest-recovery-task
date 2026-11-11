"""Two local upstream APIs and explicit test-only fault controls (stdlib)."""
from __future__ import annotations

import argparse
import copy
import json
import math
import random
import threading
import time
import uuid
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


class APIError(Exception):
    def __init__(self, status, code, headers=None):
        self.status, self.code, self.headers = status, code, headers or {}


def valid_payload(event):
    if event["deleted"]:
        return event["payload"] is None
    body = event["payload"]
    return (isinstance(body, dict) and isinstance(body.get("title"), str)
            and bool(body["title"]) and type(body.get("stock")) is int
            and body["stock"] >= 0 and body.get("currency") == "CNY")


class Source:
    def __init__(self, name, seed, size, run_id):
        self.name, self.seed, self.size, self.run_id = name, seed, size, run_id
        self.generation = 0
        self.reset()

    def reset(self):
        self.generation += 1
        self.epoch = f"{self.name}-{self.run_id}-{self.generation}"
        self.head = self.floor = self.round = 0
        self.events, self.current, self.revisions = [], {}, {}
        self.exports, self.tickets, self.tokens = {}, {}, {}
        self.fault = None
        self.blocked_until = 0.0
        self.mutate_on_scan = False
        self.expire_on_page = None
        rng = random.Random(self.seed + self.generation)
        # A new epoch deliberately removes P00000001 from its baseline.
        first = 1 if self.generation == 1 else 2
        for i in range(first, self.size + 1):
            self.emit(f"P{i:08d}", 1, stock=rng.randrange(1, 1000))

    def emit(self, item_id, revision, *, deleted=False, stock=10):
        event = {
            "event_id": f"{self.epoch}:{item_id}:{revision}",
            "item_id": item_id, "revision": revision, "deleted": deleted,
            "payload": None if deleted else {
                "title": f"{self.name}/{item_id}/r{revision}/e{self.generation}",
                "stock": stock, "currency": "CNY"},
            "updated_at": "2026-01-01T00:00:00Z",
        }
        self.append(event)
        return copy.deepcopy(event)

    def append(self, event):
        self.head += 1
        record = copy.deepcopy(event)
        record["seq"] = self.head
        self.events.append(record)
        key, rev = record["item_id"], record["revision"]
        self.revisions[key] = max(rev, self.revisions.get(key, 0))
        if valid_payload(record) and rev > self.current.get(key, {}).get("revision", 0):
            self.current[key] = {k: v for k, v in record.items() if k != "seq"}

    def next_rev(self, key):
        return self.revisions.get(key, 0) + 1

    def advance(self):
        self.round += 1
        key = "P00000001"
        old = self.emit(key, self.next_rev(key), stock=100 + self.round)
        newer = self.emit(key, self.next_rev(key), stock=200 + self.round)
        self.append(old)  # Old revision delivered after new revision.
        key = "P00000002"
        tombstone = self.emit(key, self.next_rev(key), deleted=True)
        self.emit(key, self.next_rev(key), stock=300 + self.round)
        self.append(tombstone)  # Must not delete the recreated record.
        self.emit("P00000003", self.next_rev("P00000003"), deleted=True)
        self.emit(f"A{self.round:08d}", 1, stock=401)
        self.emit(f"Z{self.round:08d}", 1, stock=501)
        self.append(newer)  # Exact same event_id, different transport seq.

    def rows(self):
        return [copy.deepcopy(self.current[k]) for k in sorted(self.current)
                if not self.current[k]["deleted"]]

    def token(self, value):
        token = uuid.uuid4().hex
        self.tokens[token] = value
        return token

    def expire(self):
        self.tokens.clear()
        self.exports.clear()

    def check_epoch(self, epoch):
        if epoch != self.epoch:
            raise APIError(409, "epoch_changed")

    def window(self, epoch, after, until, limit):
        self.check_epoch(epoch)
        if after < self.floor:
            raise APIError(410, "history_gap")
        if not self.floor <= after <= until <= self.head:
            raise APIError(400, "bad_window")
        rows = [e for e in self.events if after < e["seq"] <= until]
        page = copy.deepcopy(rows[:limit])
        more = len(rows) > limit
        through = page[-1]["seq"] if more else until
        return page, through, more


class Lab:
    def __init__(self, seed=20261111, size=24):
        if not 8 <= size <= 20000:
            raise ValueError("size must be between 8 and 20000")
        self.lock = threading.RLock()
        run_id = uuid.uuid4().hex[:12]
        self.sources = {name: Source(name, seed + n, size, run_id)
                        for n, name in enumerate(("alpha", "beta"))}
        self.active = {"alpha": 0, "beta": 0}
        self.maximum = {"alpha": 0, "beta": 0, "global": 0}
        self.requests = deque(maxlen=4000)
        self.request_count = 0

    def begin(self, name, path):
        with self.lock:
            self.active[name] += 1
            self.maximum[name] = max(self.maximum[name], self.active[name])
            self.maximum["global"] = max(self.maximum["global"], sum(self.active.values()))
            self.request_count += 1
            return {"id": self.request_count, "source": name, "path": path,
                    "started": time.monotonic()}

    def finish(self, row, status):
        with self.lock:
            self.active[row["source"]] -= 1
            self.requests.append(dict(row, status=status, finished=time.monotonic()))

    def fault_for(self, name, path):
        with self.lock:
            src = self.sources[name]
            remaining = src.blocked_until - time.monotonic()
            if remaining > 0:
                return {"mode": "throttle", "retry": max(1, math.ceil(remaining))}
            fault = src.fault
            if fault and fault["count"] > 0 and fault["path"] in path:
                fault["count"] -= 1
                result = dict(fault)
                if fault["mode"] == "throttle":
                    src.blocked_until = time.monotonic() + 2
                    result["retry"] = 2
                return result
            return None

    @staticmethod
    def limit(query):
        limit = int(query.get("limit", "7"))
        if not 1 <= limit <= 100:
            raise APIError(400, "bad_limit")
        return limit

    def dispatch(self, method, path, query, body):
        with self.lock:
            if path.startswith("/admin/"):
                return self.admin(method, path, body), "application/json"
            parts = path.strip("/").split("/")
            if len(parts) != 3 or parts[0] != "v1" or parts[1] not in self.sources:
                raise APIError(404, "not_found")
            name, action = parts[1:]
            src = self.sources[name]
            if method == "GET" and action == "head":
                return {"epoch": src.epoch, "head": src.head, "floor": src.floor}, "application/json"
            if method == "POST" and name == "alpha" and action == "exports":
                export_id = uuid.uuid4().hex
                src.exports[export_id] = (src.epoch, src.head, src.rows())
                return {"epoch": src.epoch, "export_id": export_id, "fence": src.head}, "application/json"
            if method == "POST" and name == "beta" and action == "bootstrap":
                ticket = uuid.uuid4().hex
                src.tickets[ticket] = src.epoch
                return {"epoch": src.epoch, "ticket": ticket, "fence": src.head}, "application/json"
            if method != "GET":
                raise APIError(405, "method_not_allowed")
            limit = self.limit(query)
            if name == "alpha" and action == "snapshot":
                export_id = query.get("export_id")
                if export_id not in src.exports:
                    raise APIError(410, "cursor_expired")
                epoch, fence, rows = src.exports[export_id]
                src.check_epoch(epoch)
                offset = 0
                if "cursor" in query:
                    token = src.tokens.get(query["cursor"])
                    if not token or token[:2] != ("snapshot", export_id):
                        raise APIError(410, "cursor_expired")
                    offset = token[2]
                page = copy.deepcopy(rows[offset:offset + limit])
                next_cursor = src.token(("snapshot", export_id, offset + limit)) if offset + limit < len(rows) else None
                result = {"epoch": epoch, "fence": fence, "items": page, "next_cursor": next_cursor}
                self.after_page(src, action)
                return result, "application/json"
            if name == "beta" and action == "scan":
                epoch = src.tickets.get(query.get("ticket"))
                src.check_epoch(epoch)
                after_key = query.get("after_key", "")
                rows = [r for r in src.rows() if r["item_id"] > after_key]
                page = rows[:limit]
                result = {"epoch": epoch, "rows": page,
                          "next_key": page[-1]["item_id"] if len(rows) > limit else None}
                self.after_page(src, action)
                return result, "application/json"
            if (name, action) in (("alpha", "changes"), ("beta", "events")):
                if name == "alpha" and "cursor" in query:
                    if any(k in query for k in ("epoch", "after", "until")):
                        raise APIError(400, "ambiguous_cursor")
                    token = src.tokens.get(query["cursor"])
                    if not token or token[0] != "changes":
                        raise APIError(410, "cursor_expired")
                    _, epoch, after, until = token
                else:
                    epoch, after, until = query["epoch"], int(query["after"]), int(query["until"])
                page, through, more = src.window(epoch, after, until, limit)
                if name == "alpha":
                    cursor = src.token(("changes", epoch, through, until)) if more else None
                    result = {"epoch": epoch, "until": until, "through": through,
                              "events": page, "next_cursor": cursor}
                    self.after_page(src, action)
                    return result, "application/json"
                lines = [{"kind": "window", "epoch": epoch, "until": until,
                          "through": through, "more": more}]
                for event in page:
                    lines.append({"kind": "change", "offset": event["seq"], "change": {
                        "event_key": event["event_id"], "key": event["item_id"],
                        "rev": event["revision"], "op": "delete" if event["deleted"] else "upsert",
                        "body": event["payload"], "updated_at": event["updated_at"]}})
                return lines, "application/x-ndjson"
            raise APIError(404, "not_found")

    def after_page(self, src, action):
        if action in ("snapshot", "scan") and src.mutate_on_scan:
            src.mutate_on_scan = False
            src.advance()
        if src.expire_on_page == action:
            src.expire_on_page = None
            src.expire()

    def admin(self, method, path, body):
        if method == "GET" and path == "/admin/status":
            return {"sources": {n: {"epoch": s.epoch, "head": s.head, "floor": s.floor}
                                for n, s in self.sources.items()},
                    "active": dict(self.active), "max_active": dict(self.maximum),
                    "request_count": self.request_count, "requests": list(self.requests)}
        if method == "GET" and path == "/admin/expected":
            return {n: {"epoch": s.epoch, "head": s.head, "live": s.rows()}
                    for n, s in self.sources.items()}
        if method != "POST" or path != "/admin/control":
            raise APIError(404, "not_found")
        target, action = body.get("source", "all"), body["action"]
        if target not in ("all", "alpha", "beta"):
            raise APIError(400, "bad_source")
        names = list(self.sources) if target == "all" else [target]
        allowed = {"advance", "poison", "repair", "trim", "rotate", "expire",
                   "mutate-on-scan", "expire-on-page", "fault", "clear-faults"}
        if action not in allowed:
            raise APIError(400, "bad_action")
        if action in ("expire", "expire-on-page") and names != ["alpha"]:
            raise APIError(400, "alpha_only")
        if action == "expire-on-page" and body.get("path", "changes") not in ("changes", "snapshot"):
            raise APIError(400, "bad_expiry_path")
        if action == "fault":
            if body.get("mode") not in ("throttle", "unavailable", "truncate", "delay"):
                raise APIError(400, "bad_fault")
            if not 1 <= int(body.get("count", 1)) <= 100 or not 0 <= float(body.get("seconds", 3)) <= 10:
                raise APIError(400, "bad_fault_bounds")
        for name in names:
            src = self.sources[name]
            if action == "advance":
                src.advance()
            elif action == "poison":
                src.emit("P00000004", src.next_rev("P00000004"), stock="broken")
            elif action == "repair":
                src.emit("P00000004", src.next_rev("P00000004"), stock=444)
            elif action == "trim":
                src.floor = src.head
                src.events.clear()
            elif action == "rotate":
                src.reset()
            elif action == "expire":
                src.expire()
            elif action == "mutate-on-scan":
                src.mutate_on_scan = True
            elif action == "expire-on-page":
                src.expire_on_page = body.get("path", "changes")
            elif action == "fault":
                src.fault = {"mode": body["mode"], "count": int(body.get("count", 1)),
                             "seconds": float(body.get("seconds", 3)), "path": body.get("path", "")}
            elif action == "clear-faults":
                src.fault, src.blocked_until = None, 0.0
                src.mutate_on_scan, src.expire_on_page = False, None
        return {"ok": True, "action": action, "sources": names}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass  # Request audit is available on the test control plane.

    def do_GET(self):
        self.handle_request()

    def do_POST(self):
        self.handle_request()

    def handle_request(self):
        row, status = None, 500
        headers = {}
        lab = self.server.lab
        try:
            url = urlsplit(self.path)
            query = {k: v[0] for k, v in parse_qs(url.query, keep_blank_values=True).items()}
            parts = url.path.strip("/").split("/")
            fault = None
            if len(parts) >= 2 and parts[0] == "v1" and parts[1] in lab.sources:
                row = lab.begin(parts[1], url.path)
                fault = lab.fault_for(parts[1], url.path)
            if fault:
                if fault["mode"] == "throttle":
                    raise APIError(429, "rate_limited", {"Retry-After": str(fault["retry"])})
                if fault["mode"] == "unavailable":
                    raise APIError(503, "temporary_unavailable")
                if fault["mode"] == "delay":
                    time.sleep(fault["seconds"])
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 <= length <= 65536:
                raise APIError(413, "body_too_large")
            body = json.loads(self.rfile.read(length)) if length else {}
            result, mime = lab.dispatch(self.command, url.path, query, body)
            if mime == "application/x-ndjson":
                data = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in result).encode("utf-8")
            else:
                data = json.dumps(result, ensure_ascii=False).encode("utf-8")
            if fault and fault["mode"] == "truncate":
                # Deliberately incomplete syntax even with a matching Content-Length.
                data = data[:-max(1, min(13, len(data) // 3))]
            status = 200
        except APIError as exc:
            status, headers = exc.status, exc.headers
            data, mime = json.dumps({"error": exc.code}).encode(), "application/json"
        except (ValueError, KeyError, TypeError):
            status, data, mime = 400, b'{"error":"bad_request"}', "application/json"
        except Exception:
            status, data, mime = 500, b'{"error":"fixture_error"}', "application/json"
        try:
            self.send_response(status)
            self.send_header("Content-Type", mime + "; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            # Consume only one request per connection, including rejected POSTs.
            self.send_header("Connection", "close")
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(data)
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        finally:
            self.close_connection = True
            if row:
                lab.finish(row, status)


def make_server(port=8765, seed=20261111, size=24):
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    server.lab = Lab(seed, size)
    return server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--seed", type=int, default=20261111)
    parser.add_argument("--size", type=int, default=24)
    args = parser.parse_args()
    server = make_server(args.port, args.seed, args.size)
    print(json.dumps({"event": "fixture_ready", "url": f"http://127.0.0.1:{server.server_port}",
                      "seed": args.seed, "size": args.size}), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
