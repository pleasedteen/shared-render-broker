"""Threaded JSON/HTTP queue service backed by one SQLite database."""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import re
import secrets
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional

from .db import Database
from .meter import WriteMeter, request_meter
from .renderer import canonical_json, fixture_manifest, sha256_bytes


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}


class FaultController:
    """Verification-only deterministic response-loss injection."""

    def __init__(self) -> None:
        self.armed: Dict[str, Dict[str, Any]] = {}

    def arm(self, fault: str, match: Dict[str, Any]) -> None:
        self.armed[fault] = match

    def take(self, fault: str, context: Dict[str, Any]) -> bool:
        match = self.armed.get(fault)
        if not match:
            return False
        for key, value in match.items():
            if context.get(key) != value:
                return False
        return True


def _now() -> float:
    return time.time()


def _parse_json_object(handler: "QueueHandler") -> Dict[str, Any]:
    length = int(handler.headers.get("Content-Length") or 0)
    if length <= 0:
        return {}
    if length > 32 * 1024 * 1024:
        raise ApiError(413, "body_too_large", "request body exceeds 32 MiB")
    raw = handler.rfile.read(length)
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ApiError(400, "invalid_json", f"request body is not valid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ApiError(400, "invalid_body", "JSON body must be an object")
    return value


def _require_string(body: Dict[str, Any], *keys: str, allow_empty: bool = False) -> str:
    for key in keys:
        value = body.get(key)
        if value is not None:
            if not isinstance(value, str) or (not allow_empty and not value.strip()):
                raise ApiError(400, "invalid_field", f"{key} must be a non-empty string")
            return value.strip()
    raise ApiError(400, "missing_field", f"one of {', '.join(keys)} is required")


def _lease_seconds(body: Dict[str, Any], default: float, minimum: float = 0.2, maximum: float = 120.0) -> float:
    raw = body.get("lease_seconds", default)
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ApiError(400, "invalid_lease", "lease_seconds must be a number") from exc
    if value != value or value <= 0:
        raise ApiError(400, "invalid_lease", "lease_seconds must be positive")
    return min(max(value, minimum), maximum)


def round_view(row: Any) -> Dict[str, Any]:
    data = dict(row)
    data.pop("result_body", None)
    data["params"] = json.loads(data.pop("params_json"))
    return data


def subscription_view(row: Any) -> Dict[str, Any]:
    data = dict(row)
    # Credentials are not exposed through ordinary status reads.
    data.pop("delivery_token", None)
    return data


class QueueHandler(BaseHTTPRequestHandler):
    server_version = "RenderQueue/1.0"

    # Quieter logs; verification captures process stdout anyway.
    def log_message(self, fmt: str, *args: Any) -> None:
        if getattr(self.server, "verbose_http", False):
            super().log_message(fmt, *args)

    @property
    def qs(self) -> Dict[str, str]:
        parsed = urllib.parse.urlparse(self.path)
        return {k: v[-1] for k, v in urllib.parse.parse_qs(parsed.query).items()}

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def do_DELETE(self) -> None:
        self._dispatch("DELETE")

    def _dispatch(self, method: str) -> None:
        ctx = request_meter.set(
            {
                "write_statements": 0,
                "records_affected": 0,
                "app_value_bytes": 0,
                "insert_records": 0,
                "update_records": 0,
                "delete_records": 0,
                "insert_value_bytes": 0,
                "update_value_bytes": 0,
                "by_table": {},
            }
        )
        try:
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            if method == "GET" and path == "/health":
                self._ok(self.health())
            elif method == "GET" and path == "/v1/stats":
                self._ok(self.stats())
            elif method == "GET" and path == "/v1/metrics/writes":
                self._ok({"meter": self.server.meter.snapshot()})
            elif method == "GET" and path == "/v1/rounds":
                self._ok(self.list_rounds(self.qs))
            elif method == "GET" and (m := re.fullmatch(r"/v1/rounds/(\d+)", path)):
                self._ok(self.get_round(int(m.group(1))))
            elif method == "GET" and (m := re.fullmatch(r"/v1/subscriptions/(\d+)", path)):
                self._ok(self.get_subscription(int(m.group(1))))
            elif method == "GET" and (
                m := re.fullmatch(r"/v1/pipelines/([^/]+)/requests/([^/]+)", path)
            ):
                self._ok(
                    self.get_named_subscription(
                        urllib.parse.unquote(m.group(1)), urllib.parse.unquote(m.group(2))
                    )
                )
            elif method == "POST" and path == "/v1/subscriptions":
                created = self.create_subscription(_parse_json_object(self))
                self._ok(created, status=200 if created.get("replayed") else 201)
            elif method == "POST" and (
                m := re.fullmatch(r"/v1/pipelines/([^/]+)/requests/([^/]+)/revoke", path)
            ):
                body = _parse_json_object(self)
                self._ok(
                    self.revoke_named(
                        urllib.parse.unquote(m.group(1)),
                        urllib.parse.unquote(m.group(2)),
                        body,
                    )
                )
            elif method == "POST" and path == "/v1/deliveries/claim":
                self._ok(self.claim_delivery(_parse_json_object(self)))
            elif method == "POST" and (m := re.fullmatch(r"/v1/deliveries/(\d+)/renew", path)):
                self._ok(self.renew_delivery(int(m.group(1)), _parse_json_object(self)))
            elif method == "POST" and (m := re.fullmatch(r"/v1/deliveries/(\d+)/ack", path)):
                self._ok(self.ack_delivery(int(m.group(1)), _parse_json_object(self)))
            elif method == "POST" and path == "/v1/computations/claim":
                self._ok(self.claim_computation(_parse_json_object(self)))
            elif method == "POST" and (m := re.fullmatch(r"/v1/computations/(\d+)/renew", path)):
                self._ok(self.renew_computation(int(m.group(1)), _parse_json_object(self)))
            elif method == "POST" and (m := re.fullmatch(r"/v1/computations/(\d+)/result", path)):
                self._ok(self.submit_result(int(m.group(1)), _parse_json_object(self)))
            elif method == "POST" and path == "/v1/admin/faults/arm":
                self._ok(self.arm_fault(_parse_json_object(self)))
            else:
                raise ApiError(404, "not_found", f"no route for {method} {path}")
        except ApiError as exc:
            self._error(exc)
        except Exception as exc:  # noqa: BLE001 - boundary must return JSON
            self._error(ApiError(500, "internal_error", repr(exc)))
        finally:
            request_meter.reset(ctx)

    # ----- response helpers -------------------------------------------------

    def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
        metrics = WriteMeter.request_snapshot()
        if metrics is not None:
            payload["request_write_metrics"] = metrics
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("X-Write-Statements", str(metrics["write_statements"] if metrics else 0))
        self.send_header("X-Write-Records", str(metrics["records_affected"] if metrics else 0))
        self.send_header("X-Write-App-Bytes", str(metrics["app_value_bytes"] if metrics else 0))
        self.end_headers()
        self.wfile.write(raw)

    def _ok(self, payload: Optional[Dict[str, Any]] = None, status: int = 200) -> None:
        self._send_json(status, {"ok": True, **(payload or {})})

    def _error(self, exc: ApiError) -> None:
        payload = {"ok": False, "error": {"code": exc.code, "message": exc.message, **exc.details}}
        self._send_json(exc.status, payload)

    # ----- simple reads -----------------------------------------------------

    def health(self) -> Dict[str, Any]:
        with self.server.db.tx() as tx:
            tx.one("SELECT 1")
        return {
            "status": "healthy",
            "now": _now(),
            "default_open_quota": self.server.default_quota,
            "pipeline_quotas": self.server.pipeline_quotas,
            "input_manifest_hash": self.server.input_manifest_hash,
        }

    def stats(self) -> Dict[str, Any]:
        with self.server.db.tx() as tx:
            rounds = [
                dict(r)
                for r in tx.all(
                    "SELECT state, COUNT(*) AS count, COALESCE(SUM(open_count), 0) AS open_count "
                    "FROM rounds GROUP BY state ORDER BY state"
                )
            ]
            subscriptions = [
                dict(r)
                for r in tx.all(
                    "SELECT pipeline_id, state, COUNT(*) AS count, "
                    "SUM(CASE WHEN delivery_lease_expires_at > ? THEN 1 ELSE 0 END) AS leased "
                    "FROM subscriptions GROUP BY pipeline_id, state "
                    "ORDER BY pipeline_id, state",
                    (_now(),),
                )
            ]
            totals = tx.one(
                "SELECT (SELECT COUNT(*) FROM rounds) AS rounds, "
                "(SELECT COUNT(*) FROM subscriptions) AS subscriptions"
            )
        return {"now": _now(), "rounds": rounds, "subscriptions": subscriptions, "totals": dict(totals)}

    def list_rounds(self, qs: Dict[str, str]) -> Dict[str, Any]:
        where = []
        params: list[Any] = []
        for col in ("asset_id", "asset_version", "params_hash", "state"):
            if qs.get(col):
                where.append(f"{col} = ?")
                params.append(qs[col])
        sql = "SELECT * FROM rounds"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY id DESC LIMIT 100"
        with self.server.db.tx() as tx:
            rows = tx.all(sql, params)
        return {"rounds": [round_view(r) for r in rows]}

    def get_round(self, round_id: int) -> Dict[str, Any]:
        with self.server.db.tx() as tx:
            row = tx.one("SELECT * FROM rounds WHERE id = ?", (round_id,))
        if row is None:
            raise ApiError(404, "round_not_found", f"round {round_id} not found")
        return {"round": round_view(row)}

    def get_subscription(self, sub_id: int) -> Dict[str, Any]:
        with self.server.db.tx() as tx:
            row = tx.one("SELECT * FROM subscriptions WHERE id = ?", (sub_id,))
            if row is None:
                raise ApiError(404, "subscription_not_found", f"subscription {sub_id} not found")
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (row["round_id"],))
        return {"subscription": subscription_view(row), "round": round_view(rnd)}

    def get_named_subscription(self, pipeline: str, request_id: str) -> Dict[str, Any]:
        with self.server.db.tx() as tx:
            row = tx.one(
                "SELECT * FROM subscriptions WHERE pipeline_id = ? AND request_id = ?",
                (pipeline, request_id),
            )
            if row is None:
                raise ApiError(
                    404,
                    "subscription_not_found",
                    f"{pipeline}/{request_id} not found",
                )
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (row["round_id"],))
        return {"subscription": subscription_view(row), "round": round_view(rnd)}

    # ----- subscriptions ----------------------------------------------------

    def create_subscription(self, body: Dict[str, Any]) -> Dict[str, Any]:
        pipeline = _require_string(body, "pipeline_id", "pipeline")
        request_id = _require_string(body, "request_id", "request_number")
        asset_id = body.get("asset_id") or body.get("material_id") or body.get("asset") or "default-material"
        if not isinstance(asset_id, str) or not asset_id.strip():
            raise ApiError(400, "invalid_field", "asset_id must be a non-empty string")
        asset_id = asset_id.strip()
        version = _require_string(body, "asset_version", "material_version", "version")
        params = body.get("params", body.get("parameters", {}))
        if not isinstance(params, dict):
            raise ApiError(400, "invalid_params", "params must be an object")

        params_json = canonical_json(params)
        params_hash = sha256_bytes(params_json.encode("utf-8"))
        input_hash = self.server.input_manifest_hash
        now = _now()
        limit = self.server.quota_for(pipeline)

        with self.server.db.tx() as tx:
            existing = tx.one(
                "SELECT * FROM subscriptions WHERE pipeline_id = ? AND request_id = ?",
                (pipeline, request_id),
            )
            if existing is not None:
                rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (existing["round_id"],))
                return {
                    "replayed": True,
                    "subscription": subscription_view(existing),
                    "round": round_view(rnd),
                }

            open_count = tx.one(
                "SELECT COUNT(*) AS c FROM subscriptions WHERE pipeline_id = ? AND state = 'open'",
                (pipeline,),
            )["c"]
            if open_count >= limit:
                raise ApiError(
                    429,
                    "pipeline_quota_full",
                    f"pipeline {pipeline!r} has {open_count} open subscriptions; limit is {limit}",
                    {
                        "pipeline_id": pipeline,
                        "open_count": open_count,
                        "limit": limit,
                        "retry_hint": "ack or revoke a subscription, then retry this request number",
                    },
                )

            rnd = tx.one(
                """
                SELECT * FROM rounds
                WHERE asset_id = ? AND asset_version = ? AND params_hash = ?
                  AND input_manifest_hash = ? AND state IN ('queued', 'saved')
                """,
                (asset_id, version, params_hash, input_hash),
            )

            if rnd is None:
                round_id = tx.insert(
                    "rounds",
                    {
                        "asset_id": asset_id,
                        "asset_version": version,
                        "params_hash": params_hash,
                        "params_json": params_json,
                        "input_manifest_hash": input_hash,
                        "state": "queued",
                        "open_count": 1,
                        "compute_fence": 0,
                        "compute_attempts": 0,
                        "created_at": now,
                        "updated_at": now,
                    },
                )
            else:
                round_id = int(rnd["id"])
                tx.update(
                    "rounds",
                    {"open_count": int(rnd["open_count"]) + 1, "updated_at": now},
                    "id = ?",
                    (round_id,),
                )

            sub_id = tx.insert(
                "subscriptions",
                {
                    "pipeline_id": pipeline,
                    "request_id": request_id,
                    "round_id": round_id,
                    "state": "open",
                    "delivery_fence": 0,
                    "delivery_attempts": 0,
                    "created_at": now,
                    "updated_at": now,
                },
            )
            sub = tx.one("SELECT * FROM subscriptions WHERE id = ?", (sub_id,))
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (round_id,))

        return {
            "replayed": False,
            "subscription": subscription_view(sub),
            "round": round_view(rnd),
            "quota": {"limit": limit},
        }

    def revoke_named(self, pipeline: str, request_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
        with self.server.db.tx() as tx:
            sub = tx.one(
                "SELECT * FROM subscriptions WHERE pipeline_id = ? AND request_id = ?",
                (pipeline, request_id),
            )
            if sub is None:
                raise ApiError(404, "subscription_not_found", f"{pipeline}/{request_id} not found")
            if sub["state"] == "revoked":
                rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (sub["round_id"],))
                return {"replayed": True, "subscription": subscription_view(sub), "round": round_view(rnd)}
            if sub["state"] == "acked":
                rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (sub["round_id"],))
                raise ApiError(
                    409,
                    "subscription_already_acked",
                    "acknowledged subscription cannot be revoked",
                    {"subscription": subscription_view(sub), "round": round_view(rnd)},
                )

            now = _now()
            tx.update(
                "subscriptions",
                {"state": "revoked", "revoked_at": now, "updated_at": now},
                "id = ?",
                (sub["id"],),
            )
            self._close_round_if_last(tx, int(sub["round_id"]), now)
            sub = tx.one("SELECT * FROM subscriptions WHERE id = ?", (sub["id"],))
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (sub["round_id"],))
        return {"subscription": subscription_view(sub), "round": round_view(rnd)}

    @staticmethod
    def _close_round_if_last(tx: Any, round_id: int, now: float) -> None:
        rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (round_id,))
        next_count = max(0, int(rnd["open_count"]) - 1)
        values: Dict[str, Any] = {"open_count": next_count, "updated_at": now}
        if next_count == 0:
            values["state"] = "closed"
        tx.update("rounds", values, "id = ?", (round_id,))

    # ----- delivery leases --------------------------------------------------

    def claim_delivery(self, body: Dict[str, Any]) -> Dict[str, Any]:
        pipeline = _require_string(body, "pipeline_id", "pipeline")
        receiver = _require_string(body, "receiver_id", "owner")
        lease_seconds = _lease_seconds(body, self.server.delivery_lease)
        now = _now()
        expires = now + lease_seconds
        token = secrets.token_urlsafe(24)

        with self.server.db.tx() as tx:
            row = tx.one(
                """
                SELECT s.* FROM subscriptions s
                JOIN rounds r ON r.id = s.round_id
                WHERE s.pipeline_id = ?
                  AND s.state = 'open'
                  AND r.state = 'saved'
                  AND r.result_body IS NOT NULL
                  AND (s.delivery_lease_expires_at IS NULL
                       OR s.delivery_lease_expires_at <= ?)
                ORDER BY s.id
                LIMIT 1
                """,
                (pipeline, now),
            )
            if row is None:
                return {"delivery": None}

            updated = tx.update(
                "subscriptions",
                {
                    "delivery_owner": receiver,
                    "delivery_token": token,
                    "delivery_fence": int(row["delivery_fence"]) + 1,
                    "delivery_attempts": int(row["delivery_attempts"]) + 1,
                    "delivery_lease_expires_at": expires,
                    "updated_at": now,
                },
                "id = ? AND state = 'open' AND "
                "(delivery_lease_expires_at IS NULL OR delivery_lease_expires_at <= ?)",
                (row["id"], now),
            )
            if updated != 1:
                return {"delivery": None}

            sub = tx.one("SELECT * FROM subscriptions WHERE id = ?", (row["id"],))
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (sub["round_id"],))
            result_body = bytes(rnd["result_body"])

        return {
            "delivery": {
                "subscription_id": sub["id"],
                "pipeline_id": sub["pipeline_id"],
                "request_id": sub["request_id"],
                "round_id": rnd["id"],
                "lease": {
                    "token": token,
                    "fence": sub["delivery_fence"],
                    "owner": receiver,
                    "expires_at": expires,
                    "lease_seconds": lease_seconds,
                },
                "result": {
                    "sha256": rnd["result_sha256"],
                    "bytes": rnd["result_bytes"],
                    "content_type": rnd["result_content_type"],
                    "body_b64": base64.b64encode(result_body).decode("ascii"),
                },
            }
        }

    def renew_delivery(self, sub_id: int, body: Dict[str, Any]) -> Dict[str, Any]:
        token = _require_string(body, "token")
        lease_seconds = _lease_seconds(body, self.server.delivery_lease)
        now = _now()
        with self.server.db.tx() as tx:
            sub = tx.one("SELECT * FROM subscriptions WHERE id = ?", (sub_id,))
            if sub is None:
                raise ApiError(404, "subscription_not_found", f"delivery {sub_id} not found")
            if sub["delivery_token"] != token:
                raise ApiError(401, "invalid_credential", "delivery token does not match")
            if sub["delivery_lease_expires_at"] is None or sub["delivery_lease_expires_at"] <= now:
                raise ApiError(
                    410,
                    "lease_expired",
                    "delivery lease has expired; reclaim delivery",
                    {"current_state": sub["state"]},
                )
            if sub["state"] != "open":
                raise ApiError(409, "subscription_terminal", f"subscription is {sub['state']}")
            expires = now + lease_seconds
            tx.update(
                "subscriptions",
                {"delivery_lease_expires_at": expires, "updated_at": now},
                "id = ?",
                (sub_id,),
            )
        return {"lease": {"subscription_id": sub_id, "token": token, "expires_at": expires}}

    def ack_delivery(self, sub_id: int, body: Dict[str, Any]) -> Dict[str, Any]:
        token = _require_string(body, "token")
        now = _now()
        with self.server.db.tx() as tx:
            sub = tx.one("SELECT * FROM subscriptions WHERE id = ?", (sub_id,))
            if sub is None:
                raise ApiError(404, "subscription_not_found", f"delivery {sub_id} not found")

            # Credential checks precede terminal replay semantics.  An expired
            # credential can never perform a new confirmation.
            if sub["delivery_token"] != token:
                raise ApiError(
                    401,
                    "invalid_credential",
                    "delivery token does not match",
                    {"current_state": sub["state"]},
                )
            if sub["delivery_lease_expires_at"] is None or sub["delivery_lease_expires_at"] <= now:
                raise ApiError(
                    410,
                    "lease_expired",
                    "delivery credential is expired",
                    {"current_state": sub["state"]},
                )

            if sub["state"] == "acked":
                rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (sub["round_id"],))
                return {"replayed": True, "subscription": subscription_view(sub), "round": round_view(rnd)}
            if sub["state"] == "revoked":
                raise ApiError(
                    409,
                    "subscription_revoked",
                    "subscription was revoked and cannot be acked",
                    {"current_state": "revoked"},
                )

            tx.update(
                "subscriptions",
                {"state": "acked", "acked_at": now, "updated_at": now},
                "id = ?",
                (sub_id,),
            )
            self._close_round_if_last(tx, int(sub["round_id"]), now)
            sub = tx.one("SELECT * FROM subscriptions WHERE id = ?", (sub_id,))
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (sub["round_id"],))
        return {"subscription": subscription_view(sub), "round": round_view(rnd)}

    # ----- computation leases ----------------------------------------------

    def claim_computation(self, body: Dict[str, Any]) -> Dict[str, Any]:
        worker = _require_string(body, "worker_id", "owner")
        lease_seconds = _lease_seconds(body, self.server.compute_lease)
        now = _now()
        expires = now + lease_seconds
        token = secrets.token_urlsafe(24)

        with self.server.db.tx() as tx:
            row = tx.one(
                """
                SELECT * FROM rounds
                WHERE state = 'queued'
                  AND open_count > 0
                  AND (compute_lease_expires_at IS NULL OR compute_lease_expires_at <= ?)
                ORDER BY id
                LIMIT 1
                """,
                (now,),
            )
            if row is None:
                return {"job": None}

            updated = tx.update(
                "rounds",
                {
                    "compute_owner": worker,
                    "compute_token": token,
                    "compute_fence": int(row["compute_fence"]) + 1,
                    "compute_attempts": int(row["compute_attempts"]) + 1,
                    "compute_lease_expires_at": expires,
                    "updated_at": now,
                },
                "id = ? AND state = 'queued' AND "
                "(compute_lease_expires_at IS NULL OR compute_lease_expires_at <= ?)",
                (row["id"], now),
            )
            if updated != 1:
                return {"job": None}
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (row["id"],))

        return {
            "job": {
                "round_id": rnd["id"],
                "asset_id": rnd["asset_id"],
                "asset_version": rnd["asset_version"],
                "params": json.loads(rnd["params_json"]),
                "input_manifest_hash": rnd["input_manifest_hash"],
                "lease": {
                    "token": token,
                    "fence": rnd["compute_fence"],
                    "attempt": rnd["compute_attempts"],
                    "owner": worker,
                    "expires_at": expires,
                    "lease_seconds": lease_seconds,
                },
            }
        }

    def renew_computation(self, round_id: int, body: Dict[str, Any]) -> Dict[str, Any]:
        token = _require_string(body, "token")
        lease_seconds = _lease_seconds(body, self.server.compute_lease)
        now = _now()
        with self.server.db.tx() as tx:
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (round_id,))
            if rnd is None:
                raise ApiError(404, "round_not_found", f"round {round_id} not found")
            if rnd["compute_token"] != token:
                raise ApiError(401, "invalid_credential", "compute token does not match")
            if rnd["compute_lease_expires_at"] is None or rnd["compute_lease_expires_at"] <= now:
                raise ApiError(
                    410,
                    "lease_expired",
                    "compute lease has expired; another worker may take over",
                    {"round_state": rnd["state"], "attempts": rnd["compute_attempts"]},
                )
            if rnd["state"] != "queued":
                raise ApiError(409, "round_not_renderable", f"round is {rnd['state']}")
            expires = now + lease_seconds
            tx.update(
                "rounds",
                {"compute_lease_expires_at": expires, "updated_at": now},
                "id = ?",
                (round_id,),
            )
        return {"lease": {"round_id": round_id, "token": token, "expires_at": expires}}

    def submit_result(self, round_id: int, body: Dict[str, Any]) -> Dict[str, Any]:
        token = _require_string(body, "token")
        body_b64 = body.get("result_body_b64")
        if not isinstance(body_b64, str):
            raise ApiError(400, "missing_result", "result_body_b64 is required")
        try:
            result_body = base64.b64decode(body_b64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ApiError(400, "invalid_result", "result_body_b64 is invalid base64") from exc
        if not result_body:
            raise ApiError(400, "invalid_result", "result body must not be empty")
        if len(result_body) > 16 * 1024 * 1024:
            raise ApiError(413, "result_too_large", "result body exceeds 16 MiB")

        digest = sha256_bytes(result_body)
        claimed_sha = body.get("result_sha256")
        if claimed_sha is not None and claimed_sha != digest:
            raise ApiError(400, "sha_mismatch", "provided result_sha256 does not match body")
        content_type = body.get("content_type", "application/octet-stream")
        if not isinstance(content_type, str):
            content_type = "application/octet-stream"
        now = _now()

        fault_now = False
        with self.server.db.tx() as tx:
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (round_id,))
            if rnd is None:
                raise ApiError(404, "round_not_found", f"round {round_id} not found")

            if rnd["state"] == "saved":
                if rnd["compute_token"] != token:
                    raise ApiError(
                        409,
                        "result_already_saved",
                        "a different compute attempt already saved this round",
                        {"round": round_view(rnd)},
                    )
                if rnd["compute_lease_expires_at"] is None or rnd["compute_lease_expires_at"] <= now:
                    raise ApiError(
                        410,
                        "lease_expired",
                        "credential expired; saved result was retained",
                        {"round": round_view(rnd)},
                    )
                return {"replayed": True, "round": round_view(rnd)}

            if rnd["state"] == "closed":
                raise ApiError(409, "round_closed", "all subscriptions ended before result submission")

            if rnd["compute_token"] != token:
                raise ApiError(401, "invalid_credential", "compute token does not match current attempt")
            if rnd["compute_lease_expires_at"] is None or rnd["compute_lease_expires_at"] <= now:
                raise ApiError(
                    410,
                    "lease_expired",
                    "compute lease has expired; another worker may take over",
                    {"attempts": rnd["compute_attempts"]},
                )

            # Exactly one application row/body is updated per round.  There is
            # deliberately no loop over subscriptions here.
            tx.update(
                "rounds",
                {
                    "state": "saved",
                    "result_body": result_body,
                    "result_sha256": digest,
                    "result_bytes": len(result_body),
                    "result_content_type": content_type,
                    "submitted_at": now,
                    "updated_at": now,
                },
                "id = ?",
                (round_id,),
            )
            fault_now = self.server.faults.take(
                "crash_before_submit_response", {"round_id": round_id}
            )

        if fault_now:
            print(
                f"FAULT: committed result for round {round_id}; exiting before HTTP response",
                flush=True,
            )
            # Commit is already durable.  Simulates response loss / hard restart.
            import os

            os._exit(87)

        with self.server.db.tx() as tx:
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (round_id,))
        return {"replayed": False, "round": round_view(rnd)}

    # ----- faults -----------------------------------------------------------

    def arm_fault(self, body: Dict[str, Any]) -> Dict[str, Any]:
        if not self.server.enable_faults:
            raise ApiError(403, "faults_disabled", "start server with --enable-faults")
        fault = _require_string(body, "fault")
        match = body.get("match", {})
        if not isinstance(match, dict):
            raise ApiError(400, "invalid_match", "match must be an object")
        self.server.faults.arm(fault, match)
        return {"armed": fault, "match": match}


class QueueServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        db: Database,
        meter: WriteMeter,
        *,
        default_quota: int,
        pipeline_quotas: Dict[str, int],
        compute_lease: float,
        delivery_lease: float,
        input_manifest_hash: str,
        enable_faults: bool,
    ) -> None:
        super().__init__(address, QueueHandler)
        self.db = db
        self.meter = meter
        self.default_quota = default_quota
        self.pipeline_quotas = pipeline_quotas
        self.compute_lease = compute_lease
        self.delivery_lease = delivery_lease
        self.input_manifest_hash = input_manifest_hash
        self.enable_faults = enable_faults
        self.faults = FaultController()
        self.verbose_http = False

    def quota_for(self, pipeline: str) -> int:
        return int(self.pipeline_quotas.get(pipeline, self.default_quota))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Render sharing queue service")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--db", default="data/queue.db")
    p.add_argument("--fixtures-dir", default="fixtures")
    p.add_argument("--default-open-quota", type=int, default=8)
    p.add_argument("--pipeline-quota", action="append", default=[], help="pipeline=N (repeatable)")
    p.add_argument("--compute-lease", type=float, default=30.0)
    p.add_argument("--delivery-lease", type=float, default=60.0)
    p.add_argument("--enable-faults", action="store_true")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    quotas: Dict[str, int] = {}
    for item in args.pipeline_quota:
        if "=" not in item:
            raise SystemExit("--pipeline-quota expects pipeline=N")
        name, raw = item.split("=", 1)
        quotas[name] = int(raw)

    fixtures = Path(args.fixtures_dir)
    if fixtures.exists():
        input_hash = fixture_manifest(fixtures)["manifest_sha256"]
    else:
        input_hash = "missing-fixtures"

    meter = WriteMeter()
    db = Database(args.db, meter)
    server = QueueServer(
        (args.host, args.port),
        db,
        meter,
        default_quota=args.default_open_quota,
        pipeline_quotas=quotas,
        compute_lease=args.compute_lease,
        delivery_lease=args.delivery_lease,
        input_manifest_hash=input_hash,
        enable_faults=args.enable_faults,
    )
    print(f"queue-server listening on http://{args.host}:{args.port} db={args.db}", flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
