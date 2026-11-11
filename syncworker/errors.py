"""Exception taxonomy for the sync worker."""
from __future__ import annotations


class Transient(Exception):
    """Retryable failure: network errors, 5xx, 429, truncated/unparseable bodies."""

    def __init__(self, message, retry_after=None):
        super().__init__(message)
        self.retry_after = retry_after


class Permanent(Exception):
    """Non-retryable failure: 4xx interface/config errors."""


class EnvelopeError(Exception):
    """A well-formed response page contains a structurally invalid event envelope.

    Such records must never be silently skipped; the page cannot be committed.
    """


class CursorExpired(Exception):
    """410 cursor_expired on a changes/snapshot continuation."""


class RestartBuild(Exception):
    """The pending generation must be discarded and rebuilt.

    kind is one of: epoch_changed, history_gap, cursor_expired.
    """

    def __init__(self, kind):
        super().__init__(kind)
        self.kind = kind
