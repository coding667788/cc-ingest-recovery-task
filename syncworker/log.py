"""Structured JSON-lines logging to stdout."""
from __future__ import annotations

import json
import sys
import threading
from datetime import datetime, timezone

_lock = threading.Lock()


def configure_stdout():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass


def log(event, source=None, epoch=None, checkpoint=None, **extra):
    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "event": event,
        "source": source,
        "epoch": epoch,
        "checkpoint": checkpoint,
    }
    record.update(extra)
    line = json.dumps(record, ensure_ascii=False)
    with _lock:
        print(line, flush=True)
