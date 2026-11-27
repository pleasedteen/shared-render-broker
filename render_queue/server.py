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


def safe_round_view(row: Any) -> Dict[str, Any]:
    """Round view with lease credentials removed for read APIs."""
    data = round_view(row)
    data.pop("compute_token", None)
    return data


def subscription_view(row: Any) -> Dict[str, Any]:
    data = dict(row)
    # Credentials are not exposed through ordinary status reads.
    data.pop("delivery_token", None)
    return data


def _event_detail(detail: Optional[Dict[str, Any]]) -> str:
    return canonical_json(detail or {})


def _event_view(row: Any, *, redact: bool = True) -> Dict[str, Any]:
    data = dict(row)
    raw = data.pop("detail_json", "") or ""
    try:
        detail = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        detail = {}
    if redact:
        # Lease credentials must never leave the investigation API.
        detail.pop("token", None)
    data["detail"] = detail
    return data


def record_round_event(
    tx: Any,
    *,
    round_id: int,
    event_type: str,
    now: float,
    owner: Optional[str] = None,
    fence: Optional[int] = None,
    attempt: Optional[int] = None,
    lease_expires_at: Optional[float] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> None:
    seq_row = tx.one("SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq FROM round_events WHERE round_id = ?", (round_id,))
    tx.insert(
        "round_events",
        {
            "round_id": round_id,
            "seq": int(seq_row["next_seq"]),
            "event_type": event_type,
            "owner": owner,
            "fence": fence,
            "attempt": attempt,
            "lease_expires_at": lease_expires_at,
            "detail_json": _event_detail(detail),
            "created_at": now,
        },
    )


def record_delivery_event(
    tx: Any,
    *,
    subscription_id: int,
    pipeline_id: str,
    request_id: str,
    event_type: str,
    now: float,
    receiver_id: Optional[str] = None,
    fence: Optional[int] = None,
    attempt: Optional[int] = None,
    lease_expires_at: Optional[float] = None,
    reported_by: Optional[str] = None,
    source: str = "live",
    detail: Optional[Dict[str, Any]] = None,
) -> None:
    seq_row = tx.one(
        "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq FROM delivery_events "
        "WHERE subscription_id = ?",
        (subscription_id,),
    )
    tx.insert(
        "delivery_events",
        {
            "subscription_id": subscription_id,
            "pipeline_id": pipeline_id,
            "request_id": request_id,
            "seq": int(seq_row["next_seq"]),
            "event_type": event_type,
            "receiver_id": receiver_id,
            "fence": fence,
            "attempt": attempt,
            "lease_expires_at": lease_expires_at,
            "reported_by": reported_by,
            "source": source,
            "detail_json": _event_detail(detail),
            "created_at": now,
        },
    )


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
            elif method == "GET" and (
                m := re.fullmatch(r"/v1/pipelines/([^/]+)/requests/([^/]+)/trace", path)
            ):
                self._ok(
                    self.investigation_trace(
                        urllib.parse.unquote(m.group(1)), urllib.parse.unquote(m.group(2))
                    )
                )
            elif method == "GET" and (m := re.fullmatch(r"/v1/rounds/(\d+)/trace", path)):
                self._ok(self.round_trace(int(m.group(1))))
            elif method == "POST" and (m := re.fullmatch(r"/v1/deliveries/(\d+)/persisted", path)):
                self._ok(self.report_persisted(int(m.group(1)), _parse_json_object(self)))
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
        return {"rounds": [safe_round_view(r) for r in rows]}

    def get_round(self, round_id: int) -> Dict[str, Any]:
        with self.server.db.tx() as tx:
            row = tx.one("SELECT * FROM rounds WHERE id = ?", (round_id,))
        if row is None:
            raise ApiError(404, "round_not_found", f"round {round_id} not found")
        return {"round": safe_round_view(row)}

    def get_subscription(self, sub_id: int) -> Dict[str, Any]:
        with self.server.db.tx() as tx:
            row = tx.one("SELECT * FROM subscriptions WHERE id = ?", (sub_id,))
            if row is None:
                raise ApiError(404, "subscription_not_found", f"subscription {sub_id} not found")
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (row["round_id"],))
        return {"subscription": subscription_view(row), "round": safe_round_view(rnd)}

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
        return {"subscription": subscription_view(row), "round": safe_round_view(rnd)}

    # ----- stuck-ticket investigation --------------------------------------

    @staticmethod
    def _round_events_with_saved_fallback(tx: Any, round_id: int) -> list[Dict[str, Any]]:
        """Return durable round events, synthesising a lost save event if needed."""
        rows = tx.all(
            "SELECT * FROM round_events WHERE round_id = ? ORDER BY id",
            (round_id,),
        )
        events = [_event_view(r) for r in rows]
        has_save = any(e["event_type"] == "result_saved" for e in events)
        if not has_save:
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (round_id,))
            if rnd is not None and rnd["state"] in ("saved", "closed") and rnd["submitted_at"] is not None:
                # Defensive fallback: the save transaction is durable even when
                # its separate event transaction was interrupted by a crash.
                events.append(
                    {
                        "id": None,
                        "round_id": round_id,
                        "seq": len(events) + 1,
                        "event_type": "result_saved",
                        "owner": rnd["compute_owner"],
                        "fence": rnd["compute_fence"],
                        "attempt": rnd["compute_attempts"],
                        "lease_expires_at": None,
                        "created_at": rnd["submitted_at"],
                        "detail": {
                            "result_sha256": rnd["result_sha256"],
                            "result_bytes": rnd["result_bytes"],
                            "content_type": rnd["result_content_type"],
                            "synthesized": True,
                        },
                    }
                )
        return events

    def investigation_trace(self, pipeline: str, request_id: str) -> Dict[str, Any]:
        with self.server.db.tx() as tx:
            sub = tx.one(
                "SELECT * FROM subscriptions WHERE pipeline_id = ? AND request_id = ?",
                (pipeline, request_id),
            )
            if sub is None:
                raise ApiError(
                    404,
                    "subscription_not_found",
                    f"{pipeline}/{request_id} not found",
                )
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (sub["round_id"],))
            round_events = self._round_events_with_saved_fallback(tx, int(sub["round_id"]))
            delivery_rows = tx.all(
                "SELECT * FROM delivery_events WHERE subscription_id = ? ORDER BY id",
                (sub["id"],),
            )
            delivery_events = [_event_view(r) for r in delivery_rows]

        persisted = [e for e in delivery_events if e["event_type"] == "receiver_persisted"]
        acked = [e for e in delivery_events if e["event_type"] == "delivery_acked"]
        claims = [
            e
            for e in delivery_events
            if e["event_type"] in ("delivery_claim", "delivery_handoff")
        ]
        handovers = [e for e in claims if e["event_type"] == "delivery_handoff"]
        compute_handovers = [e for e in round_events if e["event_type"] == "compute_handoff"]

        # Server-side confirmation is the subscription row's durable state;
        # receiver-side persistence is a separate, explicitly reported fact.
        server_confirmed = sub["state"] == "acked"
        receiver_persisted = len(persisted) > 0
        if server_confirmed and receiver_persisted:
            stuck_status = "complete"
        elif receiver_persisted:
            # The artifact is safely on the receiver's disk while the server
            # still shows no confirmation: this is exactly the re-report /
            # pending-ack window an investigator needs to see.
            stuck_status = "persisted_pending_ack"
        elif server_confirmed:
            stuck_status = "server_acked_without_persist_report"
        else:
            stuck_status = "in_flight"

        return {
            "query": {"pipeline_id": pipeline, "request_id": request_id},
            "subscription": subscription_view(sub),
            "round": safe_round_view(rnd),
            "round_events": round_events,
            "delivery_events": delivery_events,
            "summary": {
                "stuck_status": stuck_status,
                "server_confirmed": server_confirmed,
                "receiver_persisted": receiver_persisted,
                "delivery_claim_count": len(claims),
                "delivery_handoff_count": len(handovers),
                "compute_handoff_count": len(compute_handovers),
                "persisted_claim_fences": sorted(int(e["fence"]) for e in persisted),
                "ack_event_count": len(acked),
                "persisted_event_count": len(persisted),
            },
        }

    def round_trace(self, round_id: int) -> Dict[str, Any]:
        with self.server.db.tx() as tx:
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (round_id,))
            if rnd is None:
                raise ApiError(404, "round_not_found", f"round {round_id} not found")
            round_events = self._round_events_with_saved_fallback(tx, round_id)
            subs = tx.all(
                "SELECT * FROM subscriptions WHERE round_id = ? ORDER BY id",
                (round_id,),
            )
            sub_traces: list[Dict[str, Any]] = []
            for sub in subs:
                events = [
                    _event_view(r)
                    for r in tx.all(
                        "SELECT * FROM delivery_events WHERE subscription_id = ? ORDER BY id",
                        (sub["id"],),
                    )
                ]
                sub_traces.append(
                    {
                        "subscription": subscription_view(sub),
                        "delivery_events": events,
                    }
                )
        return {
            "query": {"round_id": round_id},
            "round": safe_round_view(rnd),
            "round_events": round_events,
            "subscriptions": sub_traces,
        }

    def report_persisted(self, sub_id: int, body: Dict[str, Any]) -> Dict[str, Any]:
        """Receiver re-report: the original claim's artifact hit local disk.

        This endpoint is a pure append to delivery_events.  It never touches
        leases, delivery ownership, or the subscription's ack/revoke state, and
        the unique index guarantees one persisted row per original claim fence.
        """
        token = _require_string(body, "token")
        fence = body.get("fence")
        if not isinstance(fence, int) or fence <= 0:
            raise ApiError(400, "invalid_field", "fence must be a positive integer")
        receiver_id = _require_string(body, "receiver_id", "reported_by")
        persisted_at = body.get("persisted_at")
        if persisted_at is not None and not isinstance(persisted_at, (int, float)):
            raise ApiError(400, "invalid_field", "persisted_at must be a number")
        sha = body.get("sha256")
        if sha is not None and not isinstance(sha, str):
            raise ApiError(400, "invalid_field", "sha256 must be a string")
        now = _now()
        detail: Dict[str, Any] = {}
        if sha:
            detail["sha256"] = sha
        if persisted_at is not None:
            detail["persisted_at"] = float(persisted_at)

        with self.server.db.tx() as tx:
            sub = tx.one("SELECT * FROM subscriptions WHERE id = ?", (sub_id,))
            if sub is None:
                raise ApiError(
                    404, "subscription_not_found", f"delivery {sub_id} not found"
                )
            # The re-report authenticates against the original claim's recorded
            # credential, not the current lease: lease expiry/handoff must not
            # block an offline receiver from reporting its disk write.
            claim_event = tx.one(
                "SELECT * FROM delivery_events "
                "WHERE subscription_id = ? AND fence = ? "
                "AND event_type IN ('delivery_claim', 'delivery_handoff') "
                "ORDER BY id DESC LIMIT 1",
                (sub_id, fence),
            )
            if claim_event is None:
                raise ApiError(
                    404,
                    "claim_not_found",
                    f"no delivery claim with fence {fence} for subscription {sub_id}",
                )
            try:
                claim_detail = json.loads(claim_event["detail_json"] or "{}")
            except json.JSONDecodeError:
                claim_detail = {}
            if claim_detail.get("token") != token:
                raise ApiError(
                    401,
                    "invalid_credential",
                    "persisted report token does not match the original delivery claim",
                )

            duplicate = tx.one(
                "SELECT * FROM delivery_events "
                "WHERE subscription_id = ? AND fence = ? AND event_type = 'receiver_persisted'",
                (sub_id, fence),
            )
            if duplicate is not None:
                # Same original claim reported again (e.g. receiver restarted
                # twice): return the existing row without writing anything.
                return {
                    "replayed": True,
                    "event": _event_view(duplicate),
                    "subscription_state": sub["state"],
                }

            record_delivery_event(
                tx,
                subscription_id=int(sub["id"]),
                pipeline_id=sub["pipeline_id"],
                request_id=sub["request_id"],
                event_type="receiver_persisted",
                now=now,
                receiver_id=claim_event["receiver_id"],
                fence=fence,
                attempt=claim_event["attempt"],
                lease_expires_at=claim_event["lease_expires_at"],
                reported_by=receiver_id,
                source="rereport",
                detail=detail,
            )
            event = tx.one(
                "SELECT * FROM delivery_events WHERE id = last_insert_rowid()"
            )
            sub_after = tx.one(
                "SELECT * FROM subscriptions WHERE id = ?", (sub_id,)
            )
        return {
            "replayed": False,
            "event": _event_view(event),
            "subscription_state": sub_after["state"],
        }

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
                    "round": safe_round_view(rnd),
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
            "round": safe_round_view(rnd),
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
                return {"replayed": True, "subscription": subscription_view(sub), "round": safe_round_view(rnd)}
            if sub["state"] == "acked":
                rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (sub["round_id"],))
                raise ApiError(
                    409,
                    "subscription_already_acked",
                    "acknowledged subscription cannot be revoked",
                    {"subscription": subscription_view(sub), "round": safe_round_view(rnd)},
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
        return {"subscription": subscription_view(sub), "round": safe_round_view(rnd)}

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
            event_type = (
                "delivery_claim" if int(sub["delivery_fence"]) == 1 else "delivery_handoff"
            )
            record_delivery_event(
                tx,
                subscription_id=int(sub["id"]),
                pipeline_id=sub["pipeline_id"],
                request_id=sub["request_id"],
                event_type=event_type,
                now=now,
                receiver_id=receiver,
                fence=int(sub["delivery_fence"]),
                attempt=int(sub["delivery_attempts"]),
                lease_expires_at=expires,
                detail={"token": token, "lease_seconds": lease_seconds},
            )
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
                return {"replayed": True, "subscription": subscription_view(sub), "round": safe_round_view(rnd)}
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
            record_delivery_event(
                tx,
                subscription_id=int(sub["id"]),
                pipeline_id=sub["pipeline_id"],
                request_id=sub["request_id"],
                event_type="delivery_acked",
                now=now,
                receiver_id=sub["delivery_owner"],
                fence=int(sub["delivery_fence"]),
                attempt=int(sub["delivery_attempts"]),
                detail={"ack_origin": "live"},
            )
            fault_now = self.server.faults.take(
                "crash_before_ack_response", {"subscription_id": sub_id}
            )
            sub = tx.one("SELECT * FROM subscriptions WHERE id = ?", (sub_id,))
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (sub["round_id"],))

        if fault_now:
            print(
                f"FAULT: committed ack for subscription {sub_id}; exiting before HTTP response",
                flush=True,
            )
            # The ack (and its event row) are already durable; this simulates a
            # lost confirmation response followed by a hard restart.
            import os

            os._exit(88)
        return {"subscription": subscription_view(sub), "round": safe_round_view(rnd)}

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
            record_round_event(
                tx,
                round_id=int(rnd["id"]),
                event_type=(
                    "compute_claim" if int(rnd["compute_fence"]) == 1 else "compute_handoff"
                ),
                now=now,
                owner=worker,
                fence=int(rnd["compute_fence"]),
                attempt=int(rnd["compute_attempts"]),
                lease_expires_at=expires,
                detail={"token": token, "lease_seconds": lease_seconds},
            )
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
                        {"round": safe_round_view(rnd)},
                    )
                if rnd["compute_lease_expires_at"] is None or rnd["compute_lease_expires_at"] <= now:
                    raise ApiError(
                        410,
                        "lease_expired",
                        "credential expired; saved result was retained",
                        {"round": safe_round_view(rnd)},
                    )
                return {"replayed": True, "round": safe_round_view(rnd)}

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

        # The investigation event is appended in its own transaction so the
        # completion write above stays exactly one rounds row regardless of
        # subscription count and of event bookkeeping.
        with self.server.db.tx() as tx:
            saved_row = tx.one("SELECT * FROM rounds WHERE id = ?", (round_id,))
            already = tx.one(
                "SELECT 1 FROM round_events WHERE round_id = ? AND event_type = 'result_saved'",
                (round_id,),
            )
            if already is None:
                record_round_event(
                    tx,
                    round_id=round_id,
                    event_type="result_saved",
                    now=float(saved_row["submitted_at"] or now),
                    owner=saved_row["compute_owner"],
                    fence=int(saved_row["compute_fence"]),
                    attempt=int(saved_row["compute_attempts"]),
                    detail={
                        "result_sha256": digest,
                        "result_bytes": len(result_body),
                        "content_type": content_type,
                    },
                )

        if fault_now:
            print(
                f"FAULT: committed result for round {round_id}; exiting before HTTP response",
                flush=True,
            )
            # Commit (plus its investigation event) is already durable.
            # Simulates response loss / hard restart.
            import os

            os._exit(87)

        with self.server.db.tx() as tx:
            rnd = tx.one("SELECT * FROM rounds WHERE id = ?", (round_id,))
        return {"replayed": False, "round": safe_round_view(rnd)}

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
