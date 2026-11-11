"""Minimal stdlib HTTP client. Never touches /admin/; callers pass /v1/ URLs."""
from __future__ import annotations

import http.client
import json
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener

from .errors import Transient

_opener = build_opener(ProxyHandler({}))


def http_request(method, url, body=None, timeout=1.5):
    """Return (status, headers, raw_body). Raises Transient on transport errors."""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = Request(url, data=data, method=method,
                      headers={"Content-Type": "application/json"})
    try:
        with _opener.open(request, timeout=timeout) as response:
            return response.status, response.headers, response.read()
    except HTTPError as exc:
        try:
            payload = exc.read()
        except Exception:
            payload = b""
        finally:
            exc.close()
        return exc.code, exc.headers, payload
    except (URLError, OSError, http.client.HTTPException) as exc:
        raise Transient(f"network error: {exc}") from exc
