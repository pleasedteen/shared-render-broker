"""Fixture-backed render worker with expiring, renewable compute leases."""

from __future__ import annotations

import argparse
import json
import signal
import threading
import time
import urllib.error
from pathlib import Path
from typing import Any, Dict, Optional

from .httpclient import HttpError, get, post
from .renderer import render_asset


class LeaseRenewer(threading.Thread):
    def __init__(
        self,
        base_url: str,
        round_id: int,
        token: str,
        lease_seconds: float,
        interval: float,
    ) -> None:
        super().__init__(name=f"lease-{round_id}", daemon=True)
        self.url = f"{base_url}/v1/computations/{round_id}/renew"
        self.body = {"token": token, "lease_seconds": lease_seconds}
        self.interval = max(0.1, interval)
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
                # Transient 5xx: keep trying until the lease deadline.
            except (urllib.error.URLError, TimeoutError, OSError):
                # Server may be restarting; submission will fence us if the
                # lease expired while requests were unavailable.
                pass

    def stop(self) -> None:
        self.stop_event.set()


class RenderWorker:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.base = args.server.rstrip("/")
        self.stop_event = threading.Event()
        self.stall_rules = self._parse_rules(args.stall_when)

    @staticmethod
    def _parse_rules(rules: list[str]) -> Dict[str, str]:
        result: Dict[str, str] = {}
        for rule in rules or []:
            if "=" in rule:
                k, v = rule.split("=", 1)
                result[k] = v
        return result

    def request_stop(self, *_: Any) -> None:
        self.stop_event.set()

    def log(self, event: str, **fields: Any) -> None:
        parts = [f"worker={self.args.worker_id}", f"event={event}"]
        for key, value in fields.items():
            if isinstance(value, (dict, list)):
                value = json.dumps(value, sort_keys=True, separators=(",", ":"))
            parts.append(f"{key}={value}")
        print(" ".join(parts), flush=True)

    def run(self) -> int:
        signal.signal(signal.SIGTERM, self.request_stop)
        signal.signal(signal.SIGINT, self.request_stop)
        self.log("starting", server=self.base, fixtures=self.args.fixtures)
        while not self.stop_event.is_set():
            try:
                _, payload, _ = post(
                    f"{self.base}/v1/computations/claim",
                    {
                        "worker_id": self.args.worker_id,
                        "lease_seconds": self.args.lease_seconds,
                    },
                    timeout=5,
                )
            except (urllib.error.URLError, OSError, TimeoutError) as exc:
                self.log("server_unavailable", error=repr(exc))
                self.stop_event.wait(min(1.0, self.args.poll_interval * 3))
                continue

            job = payload.get("job")
            if not job:
                self.stop_event.wait(self.args.poll_interval)
                continue

            try:
                self.process_job(job)
            except Exception as exc:  # Keep the worker alive across bad jobs.
                self.log("job_error", round_id=job.get("round_id"), error=repr(exc))
                self.stop_event.wait(0.2)
            if self.args.once:
                return 0
        self.log("stopped")
        return 0

    def _write_credential_file(self, job: Dict[str, Any]) -> None:
        if not self.args.credential_file:
            return
        path = Path(self.args.credential_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"round_id": job["round_id"], **job["lease"]}, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def _should_stall(self, job: Dict[str, Any]) -> bool:
        params = job.get("params") or {}
        return all(str(params.get(k)) == v for k, v in self.stall_rules.items()) if self.stall_rules else False

    def process_job(self, job: Dict[str, Any]) -> None:
        round_id = int(job["round_id"])
        lease = job["lease"]
        token = lease["token"]
        self._write_credential_file(job)
        self.log(
            "claimed",
            round_id=round_id,
            attempt=lease.get("attempt"),
            fence=lease.get("fence"),
            expires_at=lease.get("expires_at"),
        )

        stalled = self._should_stall(job)
        renewer: Optional[LeaseRenewer] = None
        if stalled:
            # Fault mode: a zombie/stopped worker cannot renew.  It wakes after
            # lease expiry and discovers its credential is fenced.
            self.log("stall_without_heartbeat", round_id=round_id, seconds=self.args.stall_seconds)
            if self.stop_event.wait(self.args.stall_seconds):
                return
        else:
            interval = self.args.heartbeat_interval or max(0.2, self.args.lease_seconds / 3.0)
            renewer = LeaseRenewer(
                self.base, round_id, token, self.args.lease_seconds, interval
            )
            renewer.start()

        try:
            body, digest, content_type = render_asset(
                self.args.fixtures,
                job["asset_id"],
                job["asset_version"],
                job.get("params") or {},
                delay_seconds=0 if stalled else self.args.render_delay,
            )
        finally:
            if renewer is not None:
                renewer.stop()
                renewer.join(timeout=1.0)

        self.log(
            "rendered",
            round_id=round_id,
            sha256=digest[:16],
            bytes=len(body),
            lease_lost=bool(renewer and renewer.lost_event.is_set()),
        )

        import base64

        submit_body = {
            "worker_id": self.args.worker_id,
            "token": token,
            "result_body_b64": base64.b64encode(body).decode("ascii"),
            "result_sha256": digest,
            "content_type": content_type,
        }
        deadline = time.time() + self.args.submit_timeout
        while not self.stop_event.is_set() and time.time() < deadline:
            try:
                _, payload, _ = post(
                    f"{self.base}/v1/computations/{round_id}/result",
                    submit_body,
                    timeout=8,
                )
                metrics = payload.get("request_write_metrics", {})
                self.log(
                    "submit_ack",
                    round_id=round_id,
                    replayed=payload.get("replayed", False),
                    write_records=metrics.get("records_affected"),
                    write_bytes=metrics.get("app_value_bytes"),
                )
                return
            except HttpError as exc:
                if exc.code == "lease_expired":
                    self.log("stale_credential_rejected", round_id=round_id, status=exc.status)
                    self._log_saved_state_if_any(round_id)
                    return
                if exc.code in ("invalid_credential", "result_already_saved", "round_closed"):
                    self.log(
                        "submit_fenced",
                        round_id=round_id,
                        code=exc.code,
                        current_state=(exc.details.get("round") or {}).get("state"),
                    )
                    return
                if exc.status not in (408, 429, 500, 502, 503, 504):
                    self.log("submit_failed", round_id=round_id, code=exc.code, status=exc.status)
                    return
                self.log("submit_retryable_error", round_id=round_id, code=exc.code)
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                # This is the "worker never received submit answer" path.  The
                # commit may already be durable; retry the same credential.
                self.log("submit_answer_lost_retrying", round_id=round_id, error=repr(exc))
            self.stop_event.wait(0.25)

        self.log("submit_gave_up", round_id=round_id)

    def _log_saved_state_if_any(self, round_id: int) -> None:
        try:
            _, payload, _ = get(f"{self.base}/v1/rounds/{round_id}", timeout=3)
            rnd = payload.get("round", {})
            self.log("round_state_after_fence", round_id=round_id, state=rnd.get("state"), sha256=(rnd.get("result_sha256") or "")[:16])
        except Exception as exc:  # noqa: BLE001
            self.log("round_state_lookup_failed", round_id=round_id, error=repr(exc))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Render queue worker")
    p.add_argument("--server", default="http://127.0.0.1:8765")
    p.add_argument("--worker-id", default=f"worker-{int(time.time())}")
    p.add_argument("--fixtures", default="fixtures")
    p.add_argument("--poll-interval", type=float, default=1.0)
    p.add_argument("--lease-seconds", type=float, default=30.0)
    p.add_argument("--heartbeat-interval", type=float, default=10.0)
    p.add_argument("--render-delay", type=float, default=0.2)
    p.add_argument("--stall-when", action="append", default=[], help="key=value; repeatable")
    p.add_argument("--stall-seconds", type=float, default=4.0)
    p.add_argument("--credential-file")
    p.add_argument("--submit-timeout", type=float, default=30.0)
    p.add_argument("--once", action="store_true")
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return RenderWorker(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
