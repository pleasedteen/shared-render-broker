"""Pipeline receiver process: claims, persists locally, and acknowledges deliveries."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import signal
import threading
import time
import urllib.error
from pathlib import Path
from typing import Any, Dict, Optional

from .httpclient import HttpError, post


class DeliveryRenewer(threading.Thread):
    def __init__(self, url: str, token: str, lease_seconds: float, interval: float) -> None:
        super().__init__(daemon=True)
        self.url = url
        self.body = {"token": token, "lease_seconds": lease_seconds}
        self.interval = max(0.2, interval)
        self.stop_event = threading.Event()
        self.lost_event = threading.Event()

    def run(self) -> None:
        while not self.stop_event.wait(self.interval):
            try:
                post(self.url, self.body, timeout=3)
            except HttpError as exc:
                if exc.status in (401, 409, 410):
                    self.lost_event.set()
                    return
            except (urllib.error.URLError, OSError, TimeoutError):
                pass

    def stop(self) -> None:
        self.stop_event.set()


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)[:120]


class PipelineReceiver:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.base = args.server.rstrip("/")
        self.stop_event = threading.Event()
        self.received = 0

    def request_stop(self, *_: Any) -> None:
        self.stop_event.set()

    def log(self, event: str, **fields: Any) -> None:
        parts = [
            f"receiver={self.args.receiver_id}",
            f"pipeline={self.args.pipeline_id}",
            f"event={event}",
        ]
        for key, value in fields.items():
            parts.append(f"{key}={value}")
        print(" ".join(parts), flush=True)

    def run(self) -> int:
        signal.signal(signal.SIGTERM, self.request_stop)
        signal.signal(signal.SIGINT, self.request_stop)
        out_dir = Path(self.args.output_dir) / safe_name(self.args.pipeline_id)
        out_dir.mkdir(parents=True, exist_ok=True)
        self.log("starting", output_dir=out_dir)

        while not self.stop_event.is_set():
            try:
                _, payload, _ = post(
                    f"{self.base}/v1/deliveries/claim",
                    {
                        "pipeline_id": self.args.pipeline_id,
                        "receiver_id": self.args.receiver_id,
                        "lease_seconds": self.args.lease_seconds,
                    },
                    timeout=6,
                )
            except (urllib.error.URLError, OSError, TimeoutError) as exc:
                self.log("server_unavailable", error=repr(exc))
                self.stop_event.wait(0.5)
                continue

            delivery = payload.get("delivery")
            if not delivery:
                if self.args.once:
                    self.log("no_delivery")
                    return 0
                self.stop_event.wait(self.args.poll_interval)
                continue

            self.handle(delivery, out_dir)
            self.received += 1
            if self.args.max_messages and self.received >= self.args.max_messages:
                self.log("max_messages_reached", count=self.received)
                return 0
            if self.args.once:
                return 0
        self.log("stopped", received=self.received)
        return 0

    def handle(self, delivery: Dict[str, Any], out_dir: Path) -> None:
        sub_id = delivery["subscription_id"]
        request_id = delivery["request_id"]
        round_id = delivery["round_id"]
        lease = delivery["lease"]
        result = delivery["result"]
        token = lease["token"]

        body = base64.b64decode(result["body_b64"], validate=True)
        digest = hashlib.sha256(body).hexdigest()
        if digest != result["sha256"]:
            raise RuntimeError(f"result checksum mismatch for delivery {sub_id}")

        artifact = out_dir / f"{safe_name(request_id)}.rnd"
        artifact.write_bytes(body)
        receipt = out_dir / f"{safe_name(request_id)}.receipt.json"
        receipt.write_text(
            json.dumps(
                {
                    "pipeline_id": self.args.pipeline_id,
                    "request_id": request_id,
                    "subscription_id": sub_id,
                    "round_id": round_id,
                    "sha256": digest,
                    "bytes": len(body),
                    "saved_to": str(artifact),
                    "received_by": self.args.receiver_id,
                    "received_at": time.time(),
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        self.log(
            "received",
            sub_id=sub_id,
            request_id=request_id,
            round_id=round_id,
            bytes=len(body),
            sha256=digest[:16],
        )

        renewer: Optional[DeliveryRenewer] = None
        if self.args.processing_delay > 0:
            interval = max(0.2, min(1.0, self.args.lease_seconds / 3.0))
            # For short delays below the renewal interval there is no need to
            # start a thread.  For long backlog processing it keeps the lease.
            if self.args.processing_delay > interval:
                renewer = DeliveryRenewer(
                    f"{self.base}/v1/deliveries/{sub_id}/renew",
                    token,
                    self.args.lease_seconds,
                    interval,
                )
                renewer.start()
            if self.stop_event.wait(self.args.processing_delay):
                if renewer is not None:
                    renewer.stop()
                    renewer.join(timeout=1.0)
                return

        ack_body = {"token": token}
        try:
            for attempt in range(5):
                try:
                    _, ack_payload, _ = post(
                        f"{self.base}/v1/deliveries/{sub_id}/ack", ack_body, timeout=5
                    )
                    self.log(
                        "acked",
                        sub_id=sub_id,
                        request_id=request_id,
                        replayed=ack_payload.get("replayed", False),
                    )
                    return
                except HttpError as exc:
                    if exc.code == "lease_expired":
                        self.log("ack_lease_expired_reclaimable", sub_id=sub_id, request_id=request_id)
                        return
                    if exc.code in ("invalid_credential", "subscription_revoked"):
                        self.log(
                            "ack_fenced",
                            sub_id=sub_id,
                            code=exc.code,
                            current=(exc.details or {}).get("current_state"),
                        )
                        return
                    if attempt == 4 or exc.status not in (408, 429, 500, 502, 503, 504):
                        self.log("ack_failed", sub_id=sub_id, code=exc.code, status=exc.status)
                        return
                except (urllib.error.URLError, OSError, TimeoutError):
                    self.log("ack_answer_lost_retrying", sub_id=sub_id, attempt=attempt + 1)
                time.sleep(0.2 * (attempt + 1))
        finally:
            if renewer is not None:
                renewer.stop()
                renewer.join(timeout=1.0)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Pipeline receiver")
    p.add_argument("--server", default="http://127.0.0.1:8765")
    p.add_argument("--pipeline-id", required=True)
    p.add_argument("--receiver-id", default=f"receiver-{int(time.time())}")
    p.add_argument("--output-dir", default="received")
    p.add_argument("--poll-interval", type=float, default=0.5)
    p.add_argument("--lease-seconds", type=float, default=30.0)
    p.add_argument("--processing-delay", type=float, default=0.0)
    p.add_argument("--max-messages", type=int, default=0)
    p.add_argument("--once", action="store_true")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.max_messages < 0:
        args.max_messages = 0
    return PipelineReceiver(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
