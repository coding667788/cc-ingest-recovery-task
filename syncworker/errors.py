"""Typed upstream errors used by the per-source state machine."""
from __future__ import annotations


class SyncError(Exception):
    """Base class for all collector errors."""


class UpstreamError(SyncError):
    def __init__(self, message: str, *, code: str | None = None,
                 retry_after: float | None = None):
        super().__init__(message)
        self.code = code
        self.retry_after = retry_after


class RateLimited(UpstreamError):
    """HTTP 429; retry_after carries the server instructed wait in seconds."""


class TransientUpstreamError(UpstreamError):
    """503, connection failure, timeout, truncated/unparseable whole page."""


class EpochChanged(UpstreamError):
    """HTTP 409: the upstream epoch no longer matches the requested one."""


class HistoryGap(UpstreamError):
    """HTTP 410 history_gap: retained log no longer covers the checkpoint."""


class CursorExpired(UpstreamError):
    """HTTP 410 cursor_expired: opaque token must be replaced."""


class PermanentUpstreamError(UpstreamError):
    """400/404/405 style configuration or interface errors."""


class BadEnvelope(SyncError):
    """A record envelope is malformed. The whole page is uncommittable and
    must be retried; this is never a payload-level quarantine case."""


class IncompletePage(SyncError):
    """Raised when a page cannot be fully parsed (bad JSON / cut NDJSON)."""
