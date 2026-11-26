"""Small JSON HTTP client shared by local processes and verification."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple


class HttpError(RuntimeError):
    def __init__(self, status: int, code: str, message: str, payload: Dict[str, Any]):
        super().__init__(f"HTTP {status} {code}: {message}")
        self.status = status
        self.code = code
        self.payload = payload
        self.details = payload.get("error", {}) if isinstance(payload, dict) else {}


def request_json(
    method: str,
    url: str,
    body: Optional[Dict[str, Any]] = None,
    *,
    timeout: float = 5.0,
) -> Tuple[int, Dict[str, Any], Any]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            headers_obj = resp.headers
            status = resp.status
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            payload = json.loads(raw.decode("utf-8")) if raw else {}
        except json.JSONDecodeError:
            payload = {"error": {"code": "non_json_error", "message": raw.decode("utf-8", "replace")}}
        err = payload.get("error", {}) if isinstance(payload, dict) else {}
        raise HttpError(exc.code, err.get("code", "http_error"), err.get("message", str(exc)), payload) from exc
    except urllib.error.URLError:
        raise
    try:
        payload = json.loads(raw.decode("utf-8")) if raw else {}
    except json.JSONDecodeError:
        payload = {"raw": raw.decode("utf-8", "replace")}
    return status, payload, headers_obj


def post(url: str, body: Optional[Dict[str, Any]] = None, timeout: float = 5.0):
    return request_json("POST", url, body or {}, timeout=timeout)


def get(url: str, timeout: float = 5.0):
    return request_json("GET", url, None, timeout=timeout)
