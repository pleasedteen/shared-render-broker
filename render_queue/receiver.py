"""Pipeline receiver process: claims, persists locally, and acknowledges deliveries.

Every successful local write is also recorded in a durable journal and reported
to the queue service as a ``receiver_persisted`` event.  A receiver that is
offline (or hard-exits after persisting, see ``--offline-persist``) replays the
journal on its next start using the original claim's credential; re-reports are
idempotent per original claim fence and never alter leases or ownership.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import signal
import threading
import time
import urllib.error
from pathlib import Path
from typing import Any, Dict, List, Optional

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


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    """Durable JSON write: temp file, fsync, rename, directory fsync."""
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    dir_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


class PersistJournal:
    """Durable record of every local artifact write and its re-report state."""

    def __init__(self, out_dir: Path) -> None:
        self.dir = out_dir / ".journal"
        self.dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _path(self, key: str) -> Path:
        return self.dir / f"{key}.json"

    def write_persisted(
        self,
        *,
        subscription_id: int,
        fence: int,
        token: str,
        receiver_id: str,
        pipeline_id: str,
        request_id: str,
        round_id: int,
        sha256: str,
        byte_count: int,
        artifact: Path,
        persisted_at: float,
    ) -> str:
        key = f"sub-{subscription_id}-fence-{fence}"
        payload = {
            "key": key,
            "subscription_id": subscription_id,
            "fence": fence,
            "token": token,
            "receiver_id": receiver_id,
            "pipeline_id": pipeline_id,
            "request_id": request_id,
            "round_id": round_id,
            "sha256": sha256,
            "bytes": byte_count,
            "artifact": str(artifact),
            "persisted_at": persisted_at,
            "reported": False,
            "report_attempts": 0,
        }
        with self._lock:
            path = self._path(key)
            if path.exists():
                # Same original claim processed twice within one process life.
                existing = json.loads(path.read_text(encoding="utf-8"))
                return existing["key"]
            _atomic_write_json(path, payload)
        return key

    def load_pending(self) -> List[Dict[str, Any]]:
        entries: List[Dict[str, Any]] = []
        for path in sorted(self.dir.glob("sub-*.json")):
            try:
                entry = json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                continue
            if not entry.get("reported"):
                entries.append(entry)
        return entries

    def mark_reported(self, entry: Dict[str, Any]) -> None:
        with self._lock:
            path = self._path(entry["key"])
            entry = dict(entry)
            entry["reported"] = True
            entry["reported_at"] = time.time()
            _atomic_write_json(path, entry)

    def record_attempt(self, entry: Dict[str, Any]) -> None:
        with self._lock:
            path = self._path(entry["key"])
            entry = dict(entry)
            entry["report_attempts"] = int(entry.get("report_attempts", 0)) + 1
            _atomic_write_json(path, entry)


class PipelineReceiver:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.base = args.server.rstrip("/")
        self.stop_event = threading.Event()
        self.received = 0
        self.journal: Optional[PersistJournal] = None

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

    def report_persisted(self, entry: Dict[str, Any], *, timeout: float = 6.0) -> str:
        """Report one disk write against its original claim.

        Returns "reported", "duplicate", "retry", or "terminal".  A success
        (including the server's idempotent duplicate response) means the
        durable trail now contains exactly one persisted row for this claim.
        """
        body = {
            "token": entry["token"],
            "fence": int(entry["fence"]),
            "receiver_id": self.args.receiver_id,
            "persisted_at": entry.get("persisted_at"),
            "sha256": entry.get("sha256"),
        }
        try:
            _, payload, _ = post(
                f"{self.base}/v1/deliveries/{int(entry['subscription_id'])}/persisted",
                body,
                timeout=timeout,
            )
        except HttpError as exc:
            if exc.status in (401, 404, 409, 410):
                # The original claim is unknown or no longer matches; retrying
                # forever cannot help.  Keep the journal for forensics.
                self.log(
                    "persist_report_terminal",
                    sub_id=entry["subscription_id"],
                    fence=entry["fence"],
                    code=exc.code,
                )
                return "terminal"
            self.journal.record_attempt(entry) if self.journal else None
            return "retry"
        except (urllib.error.URLError, OSError, TimeoutError):
            if self.journal:
                self.journal.record_attempt(entry)
            return "retry"

        assert self.journal is not None
        self.journal.mark_reported(entry)
        self.log(
            "persist_reported" if not payload.get("replayed") else "persist_report_duplicate",
            sub_id=entry["subscription_id"],
            fence=entry["fence"],
            replayed=payload.get("replayed", False),
            subscription_state=payload.get("subscription_state"),
        )
        return "duplicate" if payload.get("replayed") else "reported"

    def replay_journal(self) -> int:
        """Re-report every durable disk write whose report was never confirmed."""
        assert self.journal is not None
        pending = self.journal.load_pending()
        if not pending:
            return 0
        self.log("journal_replay_start", pending=len(pending))
        flushed = 0
        for entry in pending:
            outcome = self.report_persisted(entry)
            if outcome in ("reported", "duplicate", "terminal"):
                flushed += 1
            else:
                # Server still unavailable; the background replayer and the
                # next process start both retry from the durable journal.
                self.log(
                    "journal_replay_deferred",
                    sub_id=entry["subscription_id"],
                    fence=entry["fence"],
                )
        self.log("journal_replay_done", flushed=flushed, remaining=len(pending) - flushed)
        return flushed

    def _replayer_loop(self) -> None:
        while not self.stop_event.wait(max(0.3, self.args.replay_interval)):
            assert self.journal is not None
            pending = self.journal.load_pending()
            for entry in pending:
                if self.stop_event.is_set():
                    return
                self.report_persisted(entry)

    def run(self) -> int:
        # A fresh run (including in-process restarts) starts with a clear stop
        # signal even if a previous lifetime installed the handlers.
        self.stop_event.clear()
        signal.signal(signal.SIGTERM, self.request_stop)
        signal.signal(signal.SIGINT, self.request_stop)
        out_dir = Path(self.args.output_dir) / safe_name(self.args.pipeline_id)
        out_dir.mkdir(parents=True, exist_ok=True)
        self.journal = PersistJournal(out_dir)
        self.log("starting", output_dir=out_dir)

        # Reconnect path: an offline receiver's previously persisted artifacts
        # are re-reported against their original claims before new work starts.
        self.replay_journal()
        replayer = threading.Thread(target=self._replayer_loop, name="journal-replayer", daemon=True)
        replayer.start()

        try:
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

                persisted = self.handle(delivery, out_dir)
                self.received += 1
                if self.args.offline_persist:
                    # Fault mode: the receiver hard-exits after the artifact,
                    # receipt and journal are durable but before acking,
                    # simulating an offline receiver.  Restart replays the
                    # journal against the original claim.
                    self.log(
                        "offline_persist_exit",
                        sub_id=delivery["subscription_id"],
                        fence=delivery["lease"]["fence"],
                    )
                    self.request_stop()
                    return 0
                self.ack_delivery(delivery, persisted)
                if self.args.max_messages and self.received >= self.args.max_messages:
                    self.log("max_messages_reached", count=self.received)
                    return 0
                if self.args.once:
                    return 0
            self.log("stopped", received=self.received)
            return 0
        finally:
            # Best-effort flush on graceful shutdown.  The offline-persist
            # fault path deliberately keeps its journal pending so the next
            # (restarted) process demonstrates the reconnect re-report.
            if self.journal is not None and not self.args.offline_persist:
                self.replay_journal()

    def handle(self, delivery: Dict[str, Any], out_dir: Path) -> Optional[Dict[str, Any]]:
        """Persist phase: validate, write artifact/receipt/journal, re-report.

        Returns the journal entry, or None when shutdown interrupts processing.
        Acknowledgement happens separately in :meth:`ack_delivery`.
        """
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
        # Durable artifact + receipt first; the journal entry is the
        # restart-safe proof that this exact claim reached local disk.
        artifact.write_bytes(body)
        persisted_at = time.time()
        receipt = out_dir / f"{safe_name(request_id)}.receipt.json"
        receipt.write_text(
            json.dumps(
                {
                    "pipeline_id": self.args.pipeline_id,
                    "request_id": request_id,
                    "subscription_id": sub_id,
                    "round_id": round_id,
                    "fence": lease.get("fence"),
                    "sha256": digest,
                    "bytes": len(body),
                    "saved_to": str(artifact),
                    "received_by": self.args.receiver_id,
                    "received_at": persisted_at,
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        assert self.journal is not None
        journal_key = self.journal.write_persisted(
            subscription_id=sub_id,
            fence=int(lease["fence"]),
            token=token,
            receiver_id=self.args.receiver_id,
            pipeline_id=self.args.pipeline_id,
            request_id=request_id,
            round_id=round_id,
            sha256=digest,
            byte_count=len(body),
            artifact=artifact,
            persisted_at=persisted_at,
        )
        self.log(
            "received",
            sub_id=sub_id,
            request_id=request_id,
            round_id=round_id,
            fence=lease.get("fence"),
            bytes=len(body),
            sha256=digest[:16],
        )

        # Live re-report.  In offline-persist fault mode the process is
        # simulating a receiver that loses connectivity right after the disk
        # write, so the report is deliberately skipped: the journal entry
        # stays pending and is replayed on the next start.  Otherwise the
        # journal replayer (and the next process start) retry with the same
        # original claim credential; the server stores one row per claim fence.
        entry: Optional[Dict[str, Any]] = None
        if not self.args.offline_persist:
            entry = next(
                (item for item in self.journal.load_pending() if item["key"] == journal_key),
                None,
            )
            if entry is not None:
                self.report_persisted(entry)

        if self.args.processing_delay > 0:
            interval = max(0.2, min(1.0, self.args.lease_seconds / 3.0))
            # For short delays below the renewal interval there is no need to
            # start a thread.  For long backlog processing it keeps the lease.
            renewer: Optional[DeliveryRenewer] = None
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
                return None
            if renewer is not None:
                renewer.stop()
                renewer.join(timeout=1.0)
        return entry

    def ack_delivery(self, delivery: Dict[str, Any], persisted: Optional[Dict[str, Any]]) -> None:
        """Confirmation phase: runs only after the persist phase completes."""
        sub_id = delivery["subscription_id"]
        request_id = delivery["request_id"]
        token = delivery["lease"]["token"]
        ack_body = {"token": token}
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
    p.add_argument(
        "--offline-persist",
        action="store_true",
        help="hard-exit after the first artifact+journal write (before ack), "
        "simulating an offline receiver; restart replays the journal",
    )
    p.add_argument(
        "--replay-interval",
        type=float,
        default=1.0,
        help="seconds between background journal re-report sweeps",
    )
    return p


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.max_messages < 0:
        args.max_messages = 0
    return PipelineReceiver(args).run()


if __name__ == "__main__":
    raise SystemExit(main())
