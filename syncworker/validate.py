"""Event envelope and business payload validation."""
from __future__ import annotations

from .errors import EnvelopeError

_MISSING = object()


def _is_int(value):
    return type(value) is int


def _nonempty_str(value):
    return isinstance(value, str) and bool(value)


def envelope_error(event, require_seq):
    if require_seq and not _is_int(event["seq"]):
        return "missing_or_bad_seq"
    if not _nonempty_str(event["event_id"]):
        return "missing_or_bad_event_id"
    if not _nonempty_str(event["item_id"]):
        return "missing_or_bad_item_id"
    if not _is_int(event["revision"]) or event["revision"] < 1:
        return "missing_or_bad_revision"
    if not isinstance(event["deleted"], bool):
        return "missing_or_bad_deleted"
    if event["payload"] is _MISSING:
        return "missing_payload"
    if not isinstance(event["updated_at"], str):
        return "missing_or_bad_updated_at"
    return None


def payload_error(event):
    """Return a non-empty reason string for a bad business payload, else None."""
    payload = event["payload"]
    if event["deleted"]:
        return None if payload is None else "delete_event_with_payload"
    if not isinstance(payload, dict):
        return "payload_not_object"
    if not _nonempty_str(payload.get("title")):
        return "bad_title"
    if not _is_int(payload.get("stock")) or payload["stock"] < 0:
        return "bad_stock"
    if payload.get("currency") != "CNY":
        return "bad_currency"
    return None


def normalize_alpha_event(raw, require_seq):
    if not isinstance(raw, dict):
        raise EnvelopeError("event is not an object")
    event = {
        "seq": raw.get("seq"),
        "event_id": raw.get("event_id"),
        "item_id": raw.get("item_id"),
        "revision": raw.get("revision"),
        "deleted": raw.get("deleted"),
        "payload": raw.get("payload", _MISSING),
        "updated_at": raw.get("updated_at"),
        "raw": raw,
    }
    reason = envelope_error(event, require_seq)
    if reason:
        raise EnvelopeError(reason)
    return event


def normalize_beta_change(line):
    if not isinstance(line, dict) or line.get("kind") != "change":
        raise EnvelopeError("line is not a change record")
    offset = line.get("offset")
    if not _is_int(offset):
        raise EnvelopeError("missing_or_bad_offset")
    change = line.get("change")
    if not isinstance(change, dict):
        raise EnvelopeError("missing_change_object")
    op = change.get("op")
    if op not in ("upsert", "delete"):
        raise EnvelopeError("missing_or_bad_op")
    event = {
        "seq": offset,
        "event_id": change.get("event_key"),
        "item_id": change.get("key"),
        "revision": change.get("rev"),
        "deleted": op == "delete",
        "payload": change.get("body", _MISSING),
        "updated_at": change.get("updated_at"),
        "raw": line,
    }
    reason = envelope_error(event, require_seq=True)
    if reason:
        raise EnvelopeError(reason)
    return event
