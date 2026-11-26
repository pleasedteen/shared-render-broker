"""Command-line pipeline/client operations."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, Optional

from .httpclient import HttpError, get, post


def parse_param(values: list[str]) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    for raw in values or []:
        if "=" not in raw:
            raise SystemExit(f"--param expects key=value, got {raw!r}")
        key, value = raw.split("=", 1)
        try:
            params[key] = json.loads(value)
        except json.JSONDecodeError:
            params[key] = value
    return params


def print_payload(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False))


def request_or_exit(fn, *args, **kwargs):
    try:
        _, payload, headers = fn(*args, **kwargs)
        if hasattr(headers, "items"):
            payload["response_headers"] = {
                "x-write-records": headers.get("X-Write-Records"),
                "x-write-app-bytes": headers.get("X-Write-App-Bytes"),
            }
        print_payload(payload)
        return payload
    except HttpError as exc:
        print_payload(exc.payload)
        sys.exit(2)
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"ok": False, "error": {"message": repr(exc)}}, indent=2))
        sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Render queue pipeline client")
    p.add_argument("--server", default="http://127.0.0.1:8765")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("submit", help="submit or retry a pipeline request")
    s.add_argument("--pipeline", required=True)
    s.add_argument("--request-id", required=True)
    s.add_argument("--asset", default="harbor-title")
    s.add_argument("--version", default="1.0.0")
    s.add_argument("--param", action="append", default=[])

    g = sub.add_parser("get", help="get subscription by pipeline/request number")
    g.add_argument("--pipeline", required=True)
    g.add_argument("--request-id", required=True)

    r = sub.add_parser("revoke", help="revoke a subscription")
    r.add_argument("--pipeline", required=True)
    r.add_argument("--request-id", required=True)

    c = sub.add_parser("claim-delivery", help="claim one delivery as a receiver")
    c.add_argument("--pipeline", required=True)
    c.add_argument("--receiver-id", default="cli-receiver")
    c.add_argument("--lease-seconds", type=float, default=30.0)

    a = sub.add_parser("ack", help="acknowledge a delivery")
    a.add_argument("--subscription-id", type=int, required=True)
    a.add_argument("--token", required=True)

    rn = sub.add_parser("renew-delivery", help="renew a delivery lease")
    rn.add_argument("--subscription-id", type=int, required=True)
    rn.add_argument("--token", required=True)
    rn.add_argument("--lease-seconds", type=float, default=30.0)

    m = sub.add_parser("write-metrics", help="show cumulative write meter")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    base = args.server.rstrip("/")

    if args.command == "submit":
        body = {
            "pipeline_id": args.pipeline,
            "request_id": args.request_id,
            "asset_id": args.asset,
            "asset_version": args.version,
            "params": parse_param(args.param),
        }
        request_or_exit(post, f"{base}/v1/subscriptions", body)
    elif args.command == "get":
        from urllib.parse import quote

        request_or_exit(
            get,
            f"{base}/v1/pipelines/{quote(args.pipeline, safe='')}/requests/{quote(args.request_id, safe='')}",
        )
    elif args.command == "revoke":
        from urllib.parse import quote

        request_or_exit(
            post,
            f"{base}/v1/pipelines/{quote(args.pipeline, safe='')}/requests/{quote(args.request_id, safe='')}/revoke",
            {},
        )
    elif args.command == "claim-delivery":
        request_or_exit(
            post,
            f"{base}/v1/deliveries/claim",
            {
                "pipeline_id": args.pipeline,
                "receiver_id": args.receiver_id,
                "lease_seconds": args.lease_seconds,
            },
        )
    elif args.command == "ack":
        request_or_exit(
            post,
            f"{base}/v1/deliveries/{args.subscription_id}/ack",
            {"token": args.token},
        )
    elif args.command == "renew-delivery":
        request_or_exit(
            post,
            f"{base}/v1/deliveries/{args.subscription_id}/renew",
            {"token": args.token, "lease_seconds": args.lease_seconds},
        )
    elif args.command == "write-metrics":
        request_or_exit(get, f"{base}/v1/metrics/writes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
