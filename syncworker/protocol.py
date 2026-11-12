"""Protocol adapters for the alpha and beta HTTP interfaces.

Both adapters return plain dictionaries normalized to the alpha event shape:
{seq, event_id, item_id, revision, deleted, payload, updated_at}.
All network calls go through /v1/ only; this layer never touches /admin/.
"""
from __future__ import annotations

from urllib.parse import urlencode

from .errors import BadEnvelope, IncompletePage
from .httpclient import parse_json, parse_ndjson
from .validation import validate_envelope


def _normalized_change(change: dict, seq: int) -> dict:
    if not isinstance(change, dict):
        raise BadEnvelope("beta change is not an object")
    op = change.get("op")
    if op not in ("upsert", "delete"):
        raise BadEnvelope(f"unsupported beta op: {op!r}")
    return {
        "seq": seq,
        "event_id": change.get("event_key"),
        "item_id": change.get("key"),
        "revision": change.get("rev"),
        "deleted": op == "delete",
        "payload": change.get("body"),
        "updated_at": change.get("updated_at"),
    }


class AlphaClient:
    def __init__(self, http, base_url: str, page_size: int):
        self._http = http
        self._base = base_url
        self._page_size = page_size

    def head(self) -> dict:
        _, _, data = self._http.request("alpha", self._base + "/head")
        return parse_json(data)

    def create_export(self) -> dict:
        _, _, data = self._http.request(
            "alpha", self._base + "/exports", post=True
        )
        return parse_json(data)

    def snapshot_page(self, export_id: str, cursor: str | None) -> dict:
        query = {"export_id": export_id, "limit": self._page_size}
        if cursor is not None:
            query["cursor"] = cursor
        _, _, data = self._http.request(
            "alpha", self._base + "/snapshot?" + urlencode(query)
        )
        page = parse_json(data)
        for item in page.get("items", []):
            validate_envelope(item, require_seq=False)
        return page

    def changes_window(self, epoch: str, after: int, until: int) -> dict:
        query = urlencode({
            "epoch": epoch, "after": after, "until": until,
            "limit": self._page_size,
        })
        _, _, data = self._http.request(
            "alpha", self._base + "/changes?" + query
        )
        page = parse_json(data)
        for event in page.get("events", []):
            validate_envelope(event, require_seq=True)
        return page

    def changes_cursor(self, cursor: str) -> dict:
        query = urlencode({"cursor": cursor, "limit": self._page_size})
        _, _, data = self._http.request(
            "alpha", self._base + "/changes?" + query
        )
        page = parse_json(data)
        for event in page.get("events", []):
            validate_envelope(event, require_seq=True)
        return page


class BetaClient:
    def __init__(self, http, base_url: str, page_size: int):
        self._http = http
        self._base = base_url
        self._page_size = page_size

    def head(self) -> dict:
        _, _, data = self._http.request("beta", self._base + "/head")
        return parse_json(data)

    def bootstrap(self) -> dict:
        _, _, data = self._http.request(
            "beta", self._base + "/bootstrap", post=True
        )
        return parse_json(data)

    def scan_page(self, ticket: str, after_key: str) -> dict:
        query = {"ticket": ticket, "limit": self._page_size}
        if after_key:
            query["after_key"] = after_key
        _, _, data = self._http.request(
            "beta", self._base + "/scan?" + urlencode(query)
        )
        page = parse_json(data)
        for row in page.get("rows", []):
            validate_envelope(row, require_seq=False)
        return page

    def events_window(self, epoch: str, after: int, until: int) -> dict:
        query = urlencode({
            "epoch": epoch, "after": after, "until": until,
            "limit": self._page_size,
        })
        _, _, data = self._http.request(
            "beta", self._base + "/events?" + query
        )
        lines = parse_ndjson(data)
        window = None
        events = []
        for line in lines:
            if not isinstance(line, dict) or "kind" not in line:
                raise IncompletePage("NDJSON line is not a framed object")
            if line["kind"] == "window":
                if window is not None:
                    raise IncompletePage("NDJSON page contains multiple windows")
                window = line
            elif line["kind"] == "change":
                offset = line.get("offset")
                if type(offset) is not int or offset <= 0:
                    raise BadEnvelope("beta offset must be a positive integer")
                change = _normalized_change(line.get("change"), offset)
                validate_envelope(change, require_seq=True)
                events.append(change)
            else:
                raise BadEnvelope(f"unknown NDJSON frame kind: {line['kind']!r}")
        if window is None:
            raise IncompletePage("NDJSON page has no window frame")
        for field in ("epoch", "until", "through", "more"):
            if field not in window:
                raise IncompletePage(f"window frame missing field: {field}")
        if type(window["until"]) is not int or type(window["through"]) is not int:
            raise IncompletePage("window through/until must be integers")
        if not isinstance(window["more"], bool):
            raise IncompletePage("window more must be a boolean")
        if events and any(event["seq"] > window["through"] for event in events):
            raise IncompletePage("change offset exceeds window through")
        return {
            "epoch": window["epoch"],
            "until": window["until"],
            "through": window["through"],
            "more": window["more"],
            "events": events,
        }
