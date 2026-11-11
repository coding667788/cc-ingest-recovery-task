"""Worker configuration loading and validation."""
from __future__ import annotations

import json
from pathlib import Path

KNOWN_SOURCES = ("alpha", "beta")


def load_config(path):
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    page_size = int(raw.get("page_size", 7))
    if not 1 <= page_size <= 100:
        raise ValueError("page_size must be within 1..100")
    poll_interval_ms = int(raw.get("poll_interval_ms", 200))
    request_timeout_ms = int(raw.get("request_timeout_ms", 1500))
    if poll_interval_ms <= 0 or request_timeout_ms <= 0:
        raise ValueError("poll_interval_ms and request_timeout_ms must be positive")
    sources = raw.get("sources")
    if not sources:
        raise ValueError("config must list at least one source")
    normalized = []
    seen = set()
    for entry in sources:
        name = entry["name"]
        if name not in KNOWN_SOURCES:
            raise ValueError(f"unknown source name: {name!r}")
        if name in seen:
            raise ValueError(f"duplicate source: {name!r}")
        seen.add(name)
        base_url = entry["base_url"].rstrip("/")
        if not base_url.startswith("http://") and not base_url.startswith("https://"):
            raise ValueError(f"bad base_url for {name}: {base_url!r}")
        normalized.append({"name": name, "base_url": base_url,
                           "max_concurrency": int(entry.get("max_concurrency", 1))})
    return {
        "page_size": page_size,
        "poll_interval_ms": poll_interval_ms,
        "request_timeout_ms": request_timeout_ms,
        "max_concurrency": int(raw.get("max_concurrency", len(normalized))),
        "sources": normalized,
    }
