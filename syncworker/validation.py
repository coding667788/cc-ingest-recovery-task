"""Envelope and payload validation per PROTOCOL.md section 1.

A malformed envelope cannot be located safely and therefore fails the whole
page (BadEnvelope). A legal envelope with a bad business payload is returned
to the caller so it can be quarantined while the rest of the page proceeds.
"""
from __future__ import annotations

from .errors import BadEnvelope


def validate_envelope(record, *, require_seq: bool) -> None:
    if not isinstance(record, dict):
        raise BadEnvelope("record is not an object")
    required = (["seq"] if require_seq else []) + [
        "event_id", "item_id", "revision", "deleted", "payload", "updated_at",
    ]
    for field in required:
        if field not in record:
            raise BadEnvelope(f"missing envelope field: {field}")
    if require_seq and (type(record["seq"]) is not int or record["seq"] <= 0):
        raise BadEnvelope("seq must be a positive integer")
    if not isinstance(record["event_id"], str) or not record["event_id"]:
        raise BadEnvelope("event_id must be a non-empty string")
    if not isinstance(record["item_id"], str) or not record["item_id"]:
        raise BadEnvelope("item_id must be a non-empty string")
    if type(record["revision"]) is not int or record["revision"] <= 0:
        raise BadEnvelope("revision must be a positive integer")
    if not isinstance(record["deleted"], bool):
        raise BadEnvelope("deleted must be a boolean")


def validate_payload(record) -> str | None:
    """Return None for a valid business payload, otherwise a non-empty reason."""
    deleted = record["deleted"]
    payload = record["payload"]
    if deleted:
        if payload is not None:
            return "delete event payload must be null"
        return None
    if not isinstance(payload, dict):
        return "payload must be an object"
    title = payload.get("title")
    if not isinstance(title, str) or not title:
        return "title must be a non-empty string"
    if type(payload.get("stock")) is not int or payload["stock"] < 0:
        return "stock must be a non-negative integer (booleans are not integers)"
    if payload.get("currency") != "CNY":
        return 'currency must be the fixed string "CNY"'
    return None
