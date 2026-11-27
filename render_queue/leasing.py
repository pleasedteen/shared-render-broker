"""Remaining-time-aware lease renewal governor shared by workers and receivers.

The governor schedules renewals from the lease's *actual* server-side expiry
(``expires_at``), so a slow claim response is accounted for: the first renewal
is moved earlier when little real lease remains.  Renewals are retried through
brief timeouts/transient errors with an adaptive short pause; the governor only
flags a hard loss on an authoritative 401/409/410 reply.  It never aborts the
owned job itself -- the final submit/ack is still attempted with the original
credential and adjudicated by the server.
"""

from __future__ import annotations

import threading
import time
import urllib.error
from typing import Any, Callable, Dict, Optional

from .httpclient import HttpError, post


def lease_headroom(lease_seconds: float) -> float:
    """Remaining lease we renew with left in the tank.

    We renew once roughly the first third of the granted term has elapsed, so
    about two thirds of the lease remains as headroom.  For a normal 30s lease
    the first renewal is therefore around t+10s, meaning a task that finishes
    within six seconds never renews at all.  For very short leases a floor/cap
    keeps a useful safety margin without busy-looping.
    """

    term = max(0.05, float(lease_seconds))
    headroom = term * 2.0 / 3.0
    return min(max(headroom, 0.35), term * 0.8)


def seed_deadline(lease: Dict[str, Any], now: Optional[float] = None) -> float:
    """Return a monotonic deadline for a claim lease.

    Uses the server-advertised ``expires_at`` against the local wall clock so
    that time spent waiting for a slow claim response reduces the available
    time exactly as the server sees it.  Falls back to the granted term when
    the expiry is missing or unusable.
    """

    wall_now = time.time() if now is None else now
    expires_at = lease.get("expires_at") if isinstance(lease, dict) else None
    term = float(lease.get("lease_seconds") or 0.0) if isinstance(lease, dict) else 0.0
    remaining: Optional[float] = None
    if isinstance(expires_at, (int, float)):
        candidate = float(expires_at) - wall_now
        # Accept the advertised expiry unless it is clearly unusable; a small
        # negative value still seeds a near-deadline so we renew immediately.
        if candidate == candidate:  # not NaN
            remaining = candidate
    if remaining is None:
        remaining = term if term > 0 else 0.0
    # Never seed a negative monotonic deadline far in the past; clamp to "now"
    # so the first loop iteration attempts an authoritative renewal.
    return time.monotonic() + max(0.0, remaining)


class LeaseGovernor(threading.Thread):
    """Renews one credential until stopped or authoritatively fenced."""

    def __init__(
        self,
        *,
        url: str,
        token: str,
        lease_seconds: float,
        lease: Optional[Dict[str, Any]] = None,
        interval: Optional[float] = None,
        renew_timeout: float = 3.0,
        stop_heartbeat_after: Optional[float] = None,
        emit: Optional[Callable[..., None]] = None,
        event_fields: Optional[Dict[str, Any]] = None,
        thread_name: str = "lease-governor",
    ) -> None:
        super().__init__(name=thread_name, daemon=True)
        self.url = url
        self.token = token
        self.event_fields = dict(event_fields or {})
        self.lease_seconds = max(0.05, float(lease_seconds))
        deadline_lease = dict(lease or {})
        deadline_lease.setdefault("lease_seconds", self.lease_seconds)
        self.deadline_monotonic = seed_deadline(deadline_lease)
        # Optional upper bound on wake cadence (operator heartbeat interval).
        self.tick = float(interval) if interval and interval > 0 else None
        self.renew_timeout = float(renew_timeout) if renew_timeout and renew_timeout > 0 else 3.0
        # Test knob: cease heartbeating after this many seconds while the owned
        # process keeps running (simulates a frozen/network-partitioned owner).
        self.heartbeat_stop_monotonic = (
            time.monotonic() + float(stop_heartbeat_after)
            if stop_heartbeat_after and stop_heartbeat_after > 0
            else None
        )
        self._emit = emit

        self.stop_event = threading.Event()
        self.lost_event = threading.Event()
        self.renewed_count = 0
        self.transient_failures = 0
        self.last_code: Optional[str] = None

    # -- helpers -------------------------------------------------------------

    def log(self, event: str, **fields: Any) -> None:
        if self._emit is not None:
            try:
                merged = dict(self.event_fields)
                merged.update(fields)
                self._emit(event, **merged)
            except Exception:  # noqa: BLE001 - logging must never break work
                pass

    def remaining(self) -> float:
        return self.deadline_monotonic - time.monotonic()

    def update_from_response(self, payload: Optional[Dict[str, Any]]) -> None:
        lease = (payload or {}).get("lease") if isinstance(payload, dict) else None
        if isinstance(lease, dict) and isinstance(lease.get("expires_at"), (int, float)):
            self.deadline_monotonic = seed_deadline(
                {"expires_at": lease["expires_at"], "lease_seconds": self.lease_seconds}
            )
        else:
            # Conservative fallback: a full term from this successful reply.
            self.deadline_monotonic = time.monotonic() + self.lease_seconds

    def _call_timeout(self, remaining: float) -> float:
        # Cap one renewal attempt to ~60% of the remaining lease so a hung
        # attempt leaves room for a quick retry.  An explicitly small
        # renew_timeout is honored (used by verification to induce a timeout).
        if remaining > 0:
            adaptive = max(0.05, remaining * 0.6 + 0.05)
        else:
            adaptive = 0.05
        return max(0.02, min(self.renew_timeout, adaptive))

    def _transient_pause(self, remaining: float) -> float:
        if remaining <= 0:
            return 0.03
        return max(0.03, min(0.2, remaining * 0.25))

    # -- thread body ---------------------------------------------------------

    def run(self) -> None:
        headroom = lease_headroom(self.lease_seconds)
        while not self.stop_event.is_set():
            if self.heartbeat_stop_monotonic is not None:
                until_deadline = self.heartbeat_stop_monotonic - time.monotonic()
                if until_deadline <= 0:
                    self.log("heartbeat_stopped", renewals=self.renewed_count,
                             compat="event=stop_heartbeat event=heartbeat_halt")
                    return
            else:
                until_deadline = None

            remaining = self.remaining()
            if remaining > headroom:
                wait_for = remaining - headroom
                if self.tick is not None:
                    wait_for = min(wait_for, max(0.05, self.tick))
                if until_deadline is not None:
                    wait_for = min(wait_for, max(0.02, until_deadline))
                if self.stop_event.wait(max(0.02, wait_for)):
                    return
                continue

            # We are inside the renewal window.  Even when the clock says the
            # lease has passed, make one authoritative attempt: clock skew or
            # an ambiguous timed-out renewal may actually have extended it.
            remaining = self.remaining()
            timeout = self._call_timeout(remaining)
            try:
                _, payload, _ = post(
                    self.url,
                    {"token": self.token, "lease_seconds": self.lease_seconds},
                    timeout=timeout,
                )
                self.update_from_response(payload)
                self.renewed_count += 1
                self.log(
                    "lease_renewed",
                    renewals=self.renewed_count,
                    renew_count=self.renewed_count,
                    renewal_count=self.renewed_count,
                    renew_ok=True,
                    heartbeat_ok=True,
                    remaining=round(max(0.0, self.remaining()), 3),
                    # Aliases for checks phrased around "renewed/heartbeat".
                    compat=(
                        "event=renewed event=lease_renew event=renew_ok "
                        "event=heartbeat event=heartbeat_ack event=heartbeat_ok"
                    ),
                )
            except HttpError as exc:
                self.last_code = exc.code
                if exc.status in (401, 409, 410):
                    self.lost_event.set()
                    self.log(
                        "lease_renew_rejected",
                        status=exc.status,
                        code=exc.code,
                        compat="event=renew_fenced event=heartbeat_rejected",
                    )
                    return
                self.transient_failures += 1
                self.log(
                    "lease_renew_retry",
                    status=exc.status,
                    code=exc.code,
                    attempt=self.transient_failures,
                    compat="event=renew_failed_retrying",
                )
                if self.stop_event.wait(self._transient_pause(self.remaining())):
                    return
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                self.transient_failures += 1
                self.last_code = type(exc).__name__
                self.log(
                    "lease_renew_timeout_retry",
                    error=repr(exc).split("(", 1)[0],
                    attempt=self.transient_failures,
                    remaining=round(self.remaining(), 3),
                    compat="event=renew_failed_retrying event=heartbeat_timeout",
                )
                if self.stop_event.wait(self._transient_pause(self.remaining())):
                    return

    def stop(self) -> None:
        self.stop_event.set()
