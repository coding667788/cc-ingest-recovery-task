"""Deterministic crash injection points (PROTOCOL.md section 6).

Only the first non-empty incremental page of a source that already has a
published generation is eligible. Initialization and rebuild catch-up never
arm the hooks.
"""
from __future__ import annotations

import os

from .jsonlog import JsonLineLogger


class CrashController:
    def __init__(self, crash_at: str | None, crash_source: str | None,
                 logger: JsonLineLogger):
        self.crash_at = crash_at
        self.crash_source = crash_source
        self._logger = logger
        self._armed: dict[str, bool] = {}

    def eligible(self, source: str, *, is_incremental: bool,
                 page_events: int) -> bool:
        if (self.crash_at is None or source != self.crash_source
                or self._armed.get(source, False) or not is_incremental
                or page_events == 0):
            return False
        self._armed[source] = True
        return True

    def after_receive(self, source: str, epoch: str) -> None:
        if (self.crash_at == "after_receive"
                and self._armed.get(source, False)):
            self._crash(source, epoch)

    def before_commit(self, source: str, epoch: str) -> None:
        if (self.crash_at == "before_commit"
                and self._armed.get(source, False)):
            self._crash(source, epoch)

    def after_commit(self, source: str, epoch: str) -> None:
        if (self.crash_at == "after_commit"
                and self._armed.get(source, False)):
            self._crash(source, epoch)

    def _crash(self, source: str, epoch: str) -> None:
        self._logger.log(
            "crash_injected", source=source, epoch=epoch,
            crash_at=self.crash_at,
        )
        # Immediate process death: no atexit handlers, no connection cleanup.
        os._exit(86)
