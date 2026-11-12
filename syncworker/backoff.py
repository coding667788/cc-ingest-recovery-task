"""Per-source bounded backoff schedule; waits are event-driven, never busy."""
from __future__ import annotations

import random
import threading
import time


class Backoff:
    MIN_DELAY = 0.2
    MAX_DELAY = 30.0
    PERMANENT_DELAY = 15.0

    def __init__(self, shutdown: threading.Event):
        self._shutdown = shutdown
        self._failures = 0
        self._backoff_until: float | None = None

    @property
    def backoff_until(self) -> float | None:
        return self._backoff_until

    def reset(self) -> None:
        self._failures = 0
        self._backoff_until = None

    def rate_limited(self, retry_after: float | None) -> float:
        delay = retry_after if retry_after is not None and retry_after > 0 else 2.0
        self._backoff_until = time.monotonic() + delay
        return delay

    def failure(self) -> float:
        self._failures += 1
        base = min(self.MAX_DELAY,
                   self.MIN_DELAY * (2 ** min(self._failures - 1, 10)))
        delay = min(self.MAX_DELAY, base * (0.75 + random.random() * 0.5))
        self._backoff_until = time.monotonic() + delay
        return delay

    def permanent(self) -> float:
        self._backoff_until = time.monotonic() + self.PERMANENT_DELAY
        return self.PERMANENT_DELAY

    def wait(self, delay: float) -> bool:
        """Return False when shutdown was requested during the wait."""
        return not self._shutdown.wait(timeout=min(delay, self.MAX_DELAY))

    def clear(self) -> None:
        self._backoff_until = None
