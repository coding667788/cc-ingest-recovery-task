"""Bounded HTTP client built on urllib (stdlib only, no proxies).

Concurrency is limited by both a global semaphore and one semaphore per
source. Only one request per source worker is ever submitted, so the
semaphores make the bounds explicit and enforceable.
"""
from __future__ import annotations

import json
import socket
import threading
import urllib.error
import urllib.request

from .errors import (
    CursorExpired,
    EpochChanged,
    HistoryGap,
    IncompletePage,
    PermanentUpstreamError,
    RateLimited,
    TransientUpstreamError,
)


def _retry_after(headers) -> float | None:
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, seconds)


class BoundedHttpClient:
    def __init__(self, config, logger):
        self._timeout = config.request_timeout
        self._global = threading.BoundedSemaphore(config.max_concurrency)
        self._per_source = {
            source.name: threading.BoundedSemaphore(source.max_concurrency)
            for source in config.sources
        }
        self._logger = logger
        self._opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({})
        )

    def _open(self, request: urllib.request.Request):
        try:
            return self._opener.open(request, timeout=self._timeout)
        except urllib.error.HTTPError as exc:
            code = self._error_code(exc)
            retry_after = _retry_after(exc.headers)
            exc.close()
            if exc.code == 429:
                raise RateLimited(
                    f"429 rate_limited, retry_after={retry_after}",
                    code=code, retry_after=retry_after,
                )
            if exc.code == 409:
                raise EpochChanged(f"409 {code}", code=code)
            if exc.code == 410:
                if code == "history_gap":
                    raise HistoryGap("410 history_gap", code=code)
                raise CursorExpired(f"410 {code}", code=code)
            if exc.code in (400, 404, 405, 413):
                raise PermanentUpstreamError(
                    f"permanent upstream error {exc.code} {code}", code=code,
                )
            if 500 <= exc.code < 600:
                raise TransientUpstreamError(
                    f"upstream HTTP {exc.code} {code}", code=code,
                )
            raise PermanentUpstreamError(
                f"unexpected HTTP {exc.code} {code}", code=code,
            )
        except (urllib.error.URLError, socket.timeout, TimeoutError,
                ConnectionError, OSError) as exc:
            raise TransientUpstreamError(f"network failure: {exc}") from exc

    @staticmethod
    def _error_code(exc: urllib.error.HTTPError) -> str | None:
        try:
            body = json.loads(exc.read().decode("utf-8"))
            if isinstance(body, dict) and isinstance(body.get("error"), str):
                return body["error"]
        except (ValueError, UnicodeDecodeError):
            pass
        return None

    def request(self, source: str, url: str, *, post: bool = False):
        """Perform a request holding both concurrency permits."""
        source_gate = self._per_source[source]
        acquired_source = source_gate.acquire()
        try:
            self._global.acquire()
            try:
                request = urllib.request.Request(
                    url,
                    data=b"{}" if post else None,
                    headers={"Content-Type": "application/json"},
                )
                with self._open(request) as response:
                    data = response.read()
                    expected = response.headers.get("Content-Length")
                    if expected is not None and len(data) != int(expected):
                        raise IncompletePage(
                            f"truncated response: got {len(data)} of {expected} bytes"
                        )
                    return response.status, response.headers, data
            finally:
                self._global.release()
        finally:
            source_gate.release()


def parse_json(data: bytes):
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise IncompletePage(f"response is not valid utf-8: {exc}") from exc
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise IncompletePage(f"unparseable JSON page: {exc}") from exc
    if not isinstance(parsed, dict):
        raise IncompletePage("JSON page is not an object")
    return parsed


def parse_ndjson(data: bytes):
    """Parse a complete NDJSON page. Every line must end with a newline and
    parse successfully; otherwise the whole page is uncommittable."""
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise IncompletePage(f"response is not valid utf-8: {exc}") from exc
    if not text.endswith("\n"):
        raise IncompletePage("NDJSON response did not end with a newline")
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    if not lines:
        raise IncompletePage("NDJSON response contained no lines")
    parsed_lines = []
    for line in lines:
        try:
            parsed_lines.append(json.loads(line))
        except json.JSONDecodeError as exc:
            raise IncompletePage(f"unparseable NDJSON line: {exc}") from exc
    return parsed_lines
