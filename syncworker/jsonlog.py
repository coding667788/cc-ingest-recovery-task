"""Stdout UTF-8 JSON Lines logger."""
from __future__ import annotations

import datetime
import json
import sys
import threading


class JsonLineLogger:
    def __init__(self, stream=None):
        self._stream = stream or sys.stdout
        self._lock = threading.Lock()
        try:
            self._stream.reconfigure(encoding="utf-8")
        except (AttributeError, ValueError):
            pass

    def log(self, event: str, *, source: str | None = None,
            epoch: str | None = None, checkpoint: int | None = None,
            **extra) -> None:
        record = {
            "ts": datetime.datetime.now(datetime.timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "event": event,
            "source": source,
            "epoch": epoch,
            "checkpoint": checkpoint,
        }
        record.update(extra)
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            self._stream.write(line + "\n")
            self._stream.flush()
