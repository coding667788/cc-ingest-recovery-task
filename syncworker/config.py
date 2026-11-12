"""Configuration loading and validation."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SourceConfig:
    name: str
    base_url: str
    max_concurrency: int


@dataclass(frozen=True)
class Config:
    poll_interval_ms: int
    request_timeout_ms: int
    page_size: int
    max_concurrency: int
    sources: tuple[SourceConfig, ...]

    @property
    def poll_interval(self) -> float:
        return self.poll_interval_ms / 1000.0

    @property
    def request_timeout(self) -> float:
        return self.request_timeout_ms / 1000.0


def load_config(path: str | Path) -> Config:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    try:
        poll_interval_ms = int(raw["poll_interval_ms"])
        request_timeout_ms = int(raw["request_timeout_ms"])
        page_size = int(raw["page_size"])
        max_concurrency = int(raw["max_concurrency"])
        sources_raw = raw["sources"]
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"invalid config: {exc}") from exc
    if poll_interval_ms <= 0 or request_timeout_ms <= 0:
        raise ValueError("poll_interval_ms and request_timeout_ms must be positive")
    if not 1 <= page_size <= 100:
        raise ValueError("page_size must be between 1 and 100")
    if max_concurrency < 1 or not isinstance(sources_raw, list) or not sources_raw:
        raise ValueError("max_concurrency must be >= 1 and sources must be non-empty")
    sources = []
    seen = set()
    for item in sources_raw:
        source = SourceConfig(
            name=str(item["name"]),
            base_url=str(item["base_url"]).rstrip("/"),
            max_concurrency=int(item["max_concurrency"]),
        )
        if source.name in seen:
            raise ValueError(f"duplicate source name: {source.name}")
        if source.max_concurrency < 1:
            raise ValueError("source max_concurrency must be >= 1")
        seen.add(source.name)
        sources.append(source)
    return Config(
        poll_interval_ms=poll_interval_ms,
        request_timeout_ms=request_timeout_ms,
        page_size=page_size,
        max_concurrency=max_concurrency,
        sources=tuple(sources),
    )
