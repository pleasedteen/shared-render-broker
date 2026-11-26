#!/usr/bin/env python3
"""End-to-end verification for the shared render queue.

Starts the real SQLite queue service, render workers and pipeline receiver
processes using the checked-in fixtures.  Designed to finish comfortably below
80 seconds on a normal development machine.
"""

from __future__ import annotations

import base64
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
from pathlib import Path
from typing import Any, Callable, Dict, Optional

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from render_queue.db import Database  # noqa: E402
from render_queue.httpclient import HttpError, request_json  # noqa: E402
from render_queue.meter import WriteMeter  # noqa: E402
from render_queue.renderer import render_asset  # noqa: E402

CHECKS = 0


def ok(message: str, detail: str = "") -> None:
    global CHECKS
    CHECKS += 1
    suffix = f" — {detail}" if detail else ""
    print(f"  ✅ {message}{suffix}", flush=True)


def section(title: str) -> None:
    print(f"\n=== {title} ===", flush=True)


def check(condition: bool, message: str, detail: Any = "") -> None:
    if not condition:
        raise AssertionError(f"{message}: {detail}")
    ok(message, str(detail) if detail else "")


def wait_until(
    predicate: Callable[[], Any],
    timeout: float,
    description: str,
    interval: float = 0.12,
) -> Any:
    deadline = time.time() + timeout
    last_error: Optional[BaseException] = None
    while time.time() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except Exception as exc:  # transient HTTP/DB state
            last_error = exc
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {description}; last_error={last_error!r}")


def free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


class Api:
    def __init__(self, port: int) -> None:
        self.base = f"http://127.0.0.1:{port}"

    def call(self, method: str, path: str, body: Optional[Dict[str, Any]] = None, timeout: float = 10.0):
        return request_json(method, self.base + path, body, timeout=timeout)

    def get(self, path: str, timeout: float = 10.0):
        return self.call("GET", path, None, timeout)

    def post(self, path: str, body: Optional[Dict[str, Any]] = None, timeout: float = 10.0):
        return self.call("POST", path, body or {}, timeout)

    def expect_error(
        self, method: str, path: str, body: Optional[Dict[str, Any]] = None
    ) -> HttpError:
        try:
            self.call(method, path, body or {})
        except HttpError as exc:
            return exc
        raise AssertionError(f"expected HTTP error for {method} {path}")


class ProcessManager:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.log_dir = run_dir / "logs"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.handles: Dict[str, Dict[str, Any]] = {}

    def start(self, name: str, module: str, args: list[str]) -> subprocess.Popen:
        if name in self.handles and self.poll(name) is None:
            raise RuntimeError(f"process {name} already running")
        log_path = self.log_dir / f"{name}.log"
        log_file = open(log_path, "w", encoding="utf-8", buffering=1)
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        env["PYTHONUNBUFFERED"] = "1"
        proc = subprocess.Popen(
            [sys.executable, "-u", "-m", module, *args],
            cwd=ROOT,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.handles[name] = {"proc": proc, "log": log_file, "path": log_path}
        print(f"  ▶ started {name} (pid {proc.pid}, log {log_path})", flush=True)
        return proc

    def poll(self, name: str) -> Optional[int]:
        handle = self.handles.get(name)
        if not handle:
            return None
        return handle["proc"].poll()

    def log_text(self, name: str) -> str:
        path = self.handles[name]["path"]
        try:
            return path.read_text(encoding="utf-8", errors="replace")
        except FileNotFoundError:
            return ""

    def wait_log(self, name: str, needle: str, timeout: float) -> str:
        def pred() -> str:
            text = self.log_text(name)
            if needle in text:
                return text
            return ""

        return wait_until(pred, timeout, f"log {name} contains {needle!r}")

    def stop(self, name: str, timeout: float = 6.0) -> None:
        handle = self.handles.get(name)
        if not handle:
            return
        proc: subprocess.Popen = handle["proc"]
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
        handle["log"].close()

    def stop_all(self) -> None:
        for name in list(self.handles):
            try:
                self.stop(name, timeout=4)
            except Exception as exc:  # noqa: BLE001
                print(f"  ! cleanup error for {name}: {exc!r}", flush=True)

    def tails(self, lines: int = 25) -> str:
        chunks = []
        for name, handle in self.handles.items():
            try:
                text = handle["path"].read_text(encoding="utf-8", errors="replace")
            except FileNotFoundError:
                text = ""
            tail = "\n".join(text.splitlines()[-lines:])
            chunks.append(f"--- {name} ({handle['path']}) ---\n{tail}")
        return "\n\n".join(chunks)


def server_args(port: int, db_path: Path) -> list[str]:
    return [
        "render_queue.server",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--db",
        str(db_path),
        "--fixtures-dir",
        str(ROOT / "fixtures"),
        "--default-open-quota",
        "64",
        "--pipeline-quota",
        "quota-demo=2",
        "--compute-lease",
        "1.6",
        "--delivery-lease",
        "3.0",
        "--enable-faults",
    ]


def main() -> int:
    started_at = time.time()
    run_dir = Path(tempfile.mkdtemp(prefix="rq-verify-"))
    procs = ProcessManager(run_dir)
    port = free_port()
    api = Api(port)
    db_path = run_dir / "queue.db"

    try:
        section("1. Write meter counts rewrites, growth and delete effects")
        meter = WriteMeter()
        mem_db = Database(":memory:", meter)
        mem_db.conn.execute("CREATE TABLE scratch (id INTEGER PRIMARY KEY, payload TEXT)")
        with mem_db.tx() as tx:
            tx.insert("scratch", {"id": 1, "payload": "x"})
            tx.update("scratch", {"payload": "abc"}, "id = ?", (1,))
            tx.update("scratch", {"payload": "z" * 50}, "id = ?", (1,))
            tx.insert("scratch", {"id": 2, "payload": "delete-me"})
            tx.delete("scratch", "id = ?", (2,))
        snap = meter.snapshot()
        check(snap["write_statements"] == 5, "five CUD statements accumulated", snap["write_statements"])
        check(snap["records_affected"] == 5, "five affected record events accumulated", snap["records_affected"])
        check(snap["delete_records"] == 1, "delete affected one record")
        check(snap["app_value_bytes"] == 79, "rewrites and growth count every value write", snap["app_value_bytes"])
        check(snap["deleted_record_bytes"] > 0, "deleted row image is available for audit", snap["deleted_record_bytes"])
        mem_db.close()

        section("2. Start real SQLite queue service and wait for health")
        server_argv = server_args(port, db_path)
        procs.start("server", server_argv[0], server_argv[1:])
        wait_until(lambda: api.get("/health", timeout=1.5)[1].get("ok"), 15, "server health")
        ok("queue service is healthy", api.base)

        section("3. Completion write volume is independent of subscription count")
        fixed_result, fixed_sha, content_type = render_asset(
            ROOT / "fixtures", "bench-asset", "bench-version", {"fixed": "input"}
        )
        completion_bytes: Dict[int, int] = {}
        completion_records: Dict[int, int] = {}
        benchmark_round_id: Optional[int] = None
        benchmark_params = {"benchmark": "write-independence", "fixed_inputs": True}
        for n in (1, 6, 20):
            request_ids = [f"n{n}-{i:02d}" for i in range(n)]
            for request_id in request_ids:
                _, payload, _ = api.post(
                    "/v1/subscriptions",
                    {
                        "pipeline_id": "write-bench",
                        "request_id": request_id,
                        "asset_id": "bench-asset",
                        "asset_version": "bench-version",
                        "params": benchmark_params,
                    },
                )
                check(payload["ok"] and not payload["replayed"], f"N={n}: created request {request_id}")

            _, claim, _ = api.post(
                "/v1/computations/claim",
                {"worker_id": f"meter-worker-{n}", "lease_seconds": 20},
            )
            job = claim["job"]
            check(job is not None and job["params"] == benchmark_params, f"N={n}: claimed fixed-input round")
            benchmark_round_id = int(job["round_id"])
            _, saved, _ = api.post(
                f"/v1/computations/{benchmark_round_id}/result",
                {
                    "token": job["lease"]["token"],
                    "result_body_b64": base64.b64encode(fixed_result).decode("ascii"),
                    "result_sha256": fixed_sha,
                    "content_type": content_type,
                },
            )
            wm = saved["request_write_metrics"]
            completion_bytes[n] = int(wm["app_value_bytes"])
            completion_records[n] = int(wm["records_affected"])
            check(wm["records_affected"] == 1, f"N={n}: completion affects one record", wm["by_table"])
            check(
                set(wm["by_table"].keys()) == {"rounds"},
                f"N={n}: no subscription fanout writes",
                sorted(wm["by_table"].keys()),
            )

            if n == 20:
                _, late_join, _ = api.post(
                    "/v1/subscriptions",
                    {
                        "pipeline_id": "write-bench-late",
                        "request_id": "late-reuse-saved",
                        "asset_id": "bench-asset",
                        "asset_version": "bench-version",
                        "params": benchmark_params,
                    },
                )
                check(
                    late_join["subscription"]["round_id"] == benchmark_round_id,
                    "late request joins the same still-open round",
                )
                check(late_join["round"]["state"] == "saved", "late request reuses the already saved result")
                check(int(late_join["round"]["compute_attempts"]) == 1, "saved-result reuse starts no new computation")
                api.post("/v1/pipelines/write-bench-late/requests/late-reuse-saved/revoke", {})

            # End this generation so the next fixed-key iteration creates a
            # fresh round; these cleanup writes are outside the measured
            # completion request.
            for request_id in request_ids:
                api.post(f"/v1/pipelines/write-bench/requests/{request_id}/revoke", {})

        check(
            len(set(completion_records.values())) == 1,
            "completion record count stays constant as open subscriptions increase",
            completion_records,
        )
        check(
            len(set(completion_bytes.values())) == 1,
            "completion application-value bytes stay constant as open subscriptions increase",
            completion_bytes,
        )
        ok("fixed artifact/inputs: one rounds-row write, independent of subscription count", f"bytes={completion_bytes[1]}")

        section("4. Shared computation and receiver backlog")
        procs.start(
            "worker-main",
            "render_queue.worker",
            [
                "--server",
                api.base,
                "--worker-id",
                "worker-main",
                "--fixtures",
                str(ROOT / "fixtures"),
                "--poll-interval",
                "0.05",
                "--lease-seconds",
                "4",
                "--heartbeat-interval",
                "1",
                "--render-delay",
                "0.25",
            ],
        )

        shared_params = {"scene": "shared-title", "palette": "signal-blue"}
        shared_round_ids = set()
        for i in range(6):
            _, payload, _ = api.post(
                "/v1/subscriptions",
                {
                    "pipeline_id": "backlog",
                    "request_id": f"shared-{i}",
                    "asset_id": "harbor-title",
                    "asset_version": "3.1.0",
                    "params": shared_params,
                },
            )
            shared_round_ids.add(payload["subscription"]["round_id"])
        for i in range(2):
            _, payload, _ = api.post(
                "/v1/subscriptions",
                {
                    "pipeline_id": "fanout",
                    "request_id": f"shared-{i}",
                    "asset_id": "harbor-title",
                    "asset_version": "3.1.0",
                    "params": shared_params,
                },
            )
            shared_round_ids.add(payload["subscription"]["round_id"])
        check(len(shared_round_ids) == 1, "all eight requests joined one round", shared_round_ids)
        shared_round_id = shared_round_ids.pop()

        round_row = wait_until(
            lambda: (
                lambda p: p["round"] if p["round"]["state"] == "saved" else None
            )(api.get(f"/v1/rounds/{shared_round_id}")[1]),
            10,
            "shared round saved",
        )
        check(round_row["compute_attempts"] == 1, "only one compute attempt/render occurred", round_row["compute_attempts"])
        check(round_row["result_sha256"], "shared result checksum saved", round_row["result_sha256"][:16])
        worker_log = procs.wait_log("worker-main", f"event=submit_ack round_id={shared_round_id}", 8)
        rendered = [
            line
            for line in worker_log.splitlines()
            if "event=rendered" in line and f"round_id={shared_round_id}" in line
        ]
        check(len(rendered) == 1, "worker rendered the fixture once for all subscriptions", len(rendered))

        procs.start(
            "receiver-backlog",
            "render_queue.receiver",
            [
                "--server",
                api.base,
                "--pipeline-id",
                "backlog",
                "--receiver-id",
                "slow-backlog-1",
                "--output-dir",
                str(run_dir / "received"),
                "--poll-interval",
                "0.05",
                "--lease-seconds",
                "8",
                "--processing-delay",
                "0.8",
            ],
        )
        procs.wait_log("receiver-backlog", "event=received", 5)
        time.sleep(0.15)
        _, stats, _ = api.get("/v1/stats")
        backlog_open = [
            row
            for row in stats["subscriptions"]
            if row["pipeline_id"] == "backlog" and row["state"] == "open"
        ]
        check(len(backlog_open) == 1 and int(backlog_open[0]["count"]) >= 5, "multiple deliveries remain open for slow receiver", backlog_open)
        check(int(backlog_open[0]["leased"] or 0) >= 1, "one backlog delivery is lease/in-flight", backlog_open)
        unclaimed = int(backlog_open[0]["count"]) - int(backlog_open[0]["leased"] or 0)
        check(unclaimed >= 4, "a multi-message receiver backlog is visible", f"unclaimed={unclaimed}")
        ok("remaining deliveries stay queued while the slow receiver works", f"open={backlog_open[0]['count']}, unclaimed={unclaimed}")

        procs.start(
            "receiver-fanout",
            "render_queue.receiver",
            [
                "--server",
                api.base,
                "--pipeline-id",
                "fanout",
                "--receiver-id",
                "fanout-1",
                "--output-dir",
                str(run_dir / "received"),
                "--poll-interval",
                "0.05",
            ],
        )

        def pipeline_state(pipeline: str, state: str) -> int:
            _, stats_payload, _ = api.get("/v1/stats")
            for row in stats_payload["subscriptions"]:
                if row["pipeline_id"] == pipeline and row["state"] == state:
                    return int(row["count"])
            return 0

        wait_until(lambda: pipeline_state("backlog", "acked") == 6, 12, "backlog drained and acked")
        wait_until(lambda: pipeline_state("fanout", "acked") == 2, 8, "cross-pipeline fanout acked")
        ok("backlog and cross-pipeline subscribers received the same saved artifact")

        section("5. Receiver outage does not block worker from other computations")
        offline_ids = []
        for i in range(3):
            _, payload, _ = api.post(
                "/v1/subscriptions",
                {
                    "pipeline_id": "offline",
                    "request_id": f"offline-{i}",
                    "asset_id": "harbor-title",
                    "asset_version": "3.1.0",
                    "params": {"case": "receiver-down"},
                },
            )
            offline_ids.append(payload["subscription"]["round_id"])
        check(len(set(offline_ids)) == 1, "offline pipeline requests share one round")
        offline_round = offline_ids[0]
        wait_until(
            lambda: api.get(f"/v1/rounds/{offline_round}")[1]["round"]["state"] == "saved",
            8,
            "offline round saved without a receiver",
        )

        _, cont, _ = api.post(
            "/v1/subscriptions",
            {
                "pipeline_id": "offline",
                "request_id": "continuation",
                "asset_id": "harbor-title",
                "asset_version": "3.1.0",
                "params": {"case": "worker-continues-after-handoff"},
            },
        )
        continuation_round = cont["subscription"]["round_id"]
        wait_until(
            lambda: api.get(f"/v1/rounds/{continuation_round}")[1]["round"]["state"] == "saved",
            8,
            "worker continues to next computation",
        )
        procs.wait_log("worker-main", f"event=submit_ack round_id={offline_round}", 5)
        procs.wait_log("worker-main", f"event=submit_ack round_id={continuation_round}", 5)
        check(pipeline_state("offline", "acked") == 0, "deliveries remain pending while receiver is down")

        procs.start(
            "receiver-offline",
            "render_queue.receiver",
            [
                "--server",
                api.base,
                "--pipeline-id",
                "offline",
                "--receiver-id",
                "offline-back",
                "--output-dir",
                str(run_dir / "received"),
                "--poll-interval",
                "0.05",
            ],
        )
        wait_until(lambda: pipeline_state("offline", "acked") == 4, 10, "restarted receiver drains outage backlog")
        ok("worker moved on after handoff; restarted receiver drained four deliveries")

        section("6. Delivery leases renew, expire, reject old credentials, and reclaim")
        _, payload, _ = api.post(
            "/v1/subscriptions",
            {
                "pipeline_id": "deliverylease",
                "request_id": "lease-1",
                "asset_id": "harbor-title",
                "asset_version": "3.1.0",
                "params": {"case": "delivery-lease"},
            },
        )
        lease_sub = payload["subscription"]["id"]
        lease_round = payload["subscription"]["round_id"]
        wait_until(
            lambda: api.get(f"/v1/rounds/{lease_round}")[1]["round"]["state"] == "saved",
            8,
            "lease round saved",
        )
        _, claim, _ = api.post(
            "/v1/deliveries/claim",
            {"pipeline_id": "deliverylease", "receiver_id": "old-lease", "lease_seconds": 0.8},
        )
        delivery = claim["delivery"]
        check(delivery is not None and delivery["subscription_id"] == lease_sub, "old receiver claimed delivery")
        token = delivery["lease"]["token"]
        _, renewed, _ = api.post(
            f"/v1/deliveries/{lease_sub}/renew",
            {"token": token, "lease_seconds": 0.8},
        )
        check(renewed["ok"], "valid delivery credential renewed")
        wait_seconds = max(0.1, float(renewed["lease"]["expires_at"]) - time.time() + 0.25)
        time.sleep(wait_seconds)
        renew_err = api.expect_error(
            "POST", f"/v1/deliveries/{lease_sub}/renew", {"token": token, "lease_seconds": 5}
        )
        check(renew_err.status == 410 and renew_err.code == "lease_expired", "expired delivery renewal rejected", renew_err.status)
        ack_err = api.expect_error("POST", f"/v1/deliveries/{lease_sub}/ack", {"token": token})
        check(ack_err.status == 410 and ack_err.code == "lease_expired", "expired delivery ack rejected", ack_err.status)

        _, other_claim, _ = api.post(
            "/v1/deliveries/claim",
            {"pipeline_id": "a-different-pipeline", "receiver_id": "cross-pipeline"},
        )
        check(other_claim["delivery"] is None, "another pipeline cannot reclaim this delivery")

        procs.start(
            "receiver-lease",
            "render_queue.receiver",
            [
                "--server",
                api.base,
                "--pipeline-id",
                "deliverylease",
                "--receiver-id",
                "reclaimed",
                "--output-dir",
                str(run_dir / "received"),
                "--poll-interval",
                "0.05",
                "--lease-seconds",
                "5",
            ],
        )

        def named_state(pipeline: str, request_id: str) -> str:
            _, p, _ = api.get(f"/v1/pipelines/{pipeline}/requests/{request_id}")
            return p["subscription"]["state"]

        wait_until(lambda: named_state("deliverylease", "lease-1") == "acked", 8, "same pipeline reclaimed and acked")
        receipt = json.loads((run_dir / "received" / "deliverylease" / "lease-1.receipt.json").read_text())
        _, round_payload, _ = api.get(f"/v1/rounds/{lease_round}")
        check(receipt["sha256"] == round_payload["round"]["result_sha256"], "reclaimed delivery returns the same saved artifact")

        section("7. Computation takeover after lease expiry")
        procs.stop("worker-main")
        takeover_params = {"case": "takeover-slow", "lane": "fence-test"}
        _, payload, _ = api.post(
            "/v1/subscriptions",
            {
                "pipeline_id": "takeover",
                "request_id": "takeover-1",
                "asset_id": "harbor-title",
                "asset_version": "3.1.0",
                "params": takeover_params,
            },
        )
        takeover_round = payload["subscription"]["round_id"]
        credential_file = run_dir / "zombie-credential.json"
        procs.start(
            "worker-zombie",
            "render_queue.worker",
            [
                "--server",
                api.base,
                "--worker-id",
                "zombie-1",
                "--fixtures",
                str(ROOT / "fixtures"),
                "--poll-interval",
                "0.05",
                "--lease-seconds",
                "0.8",
                "--heartbeat-interval",
                "0.2",
                "--stall-when",
                "case=takeover-slow",
                "--stall-seconds",
                "2.4",
                "--credential-file",
                str(credential_file),
            ],
        )
        procs.wait_log("worker-zombie", "event=stall_without_heartbeat", 5)
        credential = json.loads(credential_file.read_text())
        check(credential["round_id"] == takeover_round, "zombie worker obtained round credential")
        _, compute_renew_ok, _ = api.post(
            f"/v1/computations/{takeover_round}/renew",
            {"token": credential["token"], "lease_seconds": 0.8},
        )
        check(compute_renew_ok["ok"], "valid compute credential can renew")
        renewed_expiry = float(compute_renew_ok["lease"]["expires_at"])
        check(renewed_expiry > float(credential["expires_at"]), "compute lease expiry was extended")
        time.sleep(max(0.1, renewed_expiry - time.time() + 0.25))

        compute_renew_err = api.expect_error(
            "POST",
            f"/v1/computations/{takeover_round}/renew",
            {"token": credential["token"], "lease_seconds": 5},
        )
        check(compute_renew_err.status == 410, "expired compute credential cannot renew", compute_renew_err.code)
        stale_body = base64.b64encode(b"stale-attempt-body").decode()
        compute_submit_err = api.expect_error(
            "POST",
            f"/v1/computations/{takeover_round}/result",
            {"token": credential["token"], "result_body_b64": stale_body},
        )
        check(compute_submit_err.status == 410, "expired compute credential cannot submit", compute_submit_err.code)

        procs.start(
            "worker-takeover-2",
            "render_queue.worker",
            [
                "--server",
                api.base,
                "--worker-id",
                "healthy-takeover-2",
                "--fixtures",
                str(ROOT / "fixtures"),
                "--poll-interval",
                "0.05",
                "--lease-seconds",
                "5",
                "--render-delay",
                "0.2",
            ],
        )

        def takeover_saved() -> Optional[Dict[str, Any]]:
            _, p, _ = api.get(f"/v1/rounds/{takeover_round}")
            rnd = p["round"]
            return rnd if rnd["state"] == "saved" and int(rnd["compute_attempts"]) >= 2 else None

        takeover_round_row = wait_until(takeover_saved, 10, "healthy worker takes over and saves")
        check(takeover_round_row["compute_owner"] == "healthy-takeover-2", "new owner recorded", takeover_round_row["compute_owner"])
        check(int(takeover_round_row["compute_attempts"]) == 2, "second compute attempt won", takeover_round_row["compute_attempts"])
        zombie_log = procs.wait_log("worker-zombie", "event=submit_fenced", 6)
        check(
            "code=result_already_saved" in zombie_log or "event=stale_credential_rejected" in zombie_log,
            "woken zombie's late submit is fenced by the new compute credential",
        )
        procs.stop("worker-zombie")
        procs.stop("worker-takeover-2")

        section("8. Round generations, idempotent requests, terminal states and quota")
        # After the last ack, the next same-condition request is a new round.
        gen_body = {
            "pipeline_id": "gen",
            "asset_id": "gen-asset",
            "asset_version": "g1",
            "params": {"case": "ack-closes-generation"},
        }
        _, gen1, _ = api.post("/v1/subscriptions", {**gen_body, "request_id": "ack-1"})
        gen1_round = gen1["subscription"]["round_id"]
        _, gen_claim, _ = api.post("/v1/computations/claim", {"worker_id": "gen-worker", "lease_seconds": 20})
        check(gen_claim["job"]["round_id"] == gen1_round, "claimed generation test round")
        gen_result = b"generation-ack-result"
        api.post(
            f"/v1/computations/{gen1_round}/result",
            {
                "token": gen_claim["job"]["lease"]["token"],
                "result_body_b64": base64.b64encode(gen_result).decode(),
                "result_sha256": __import__("hashlib").sha256(gen_result).hexdigest(),
            },
        )
        _, gen_delivery, _ = api.post("/v1/deliveries/claim", {"pipeline_id": "gen", "receiver_id": "gen", "lease_seconds": 20})
        api.post(
            f"/v1/deliveries/{gen_delivery['delivery']['subscription_id']}/ack",
            {"token": gen_delivery["delivery"]["lease"]["token"]},
        )
        _, gen2, _ = api.post("/v1/subscriptions", {**gen_body, "request_id": "ack-2"})
        check(gen2["subscription"]["round_id"] != gen1_round, "last ack closes round; next request starts a new round")
        check(gen2["round"]["state"] == "queued" and int(gen2["round"]["open_count"]) == 1, "new generation has one open subscription")
        api.post("/v1/pipelines/gen/requests/ack-2/revoke", {})

        # The same generation rule applies when the last subscription revokes.
        rev_gen_body = {**gen_body, "params": {"case": "revoke-closes-generation"}}
        _, rgen1, _ = api.post("/v1/subscriptions", {**rev_gen_body, "request_id": "rev-1"})
        rgen1_round = rgen1["subscription"]["round_id"]
        api.post("/v1/pipelines/gen/requests/rev-1/revoke", {})
        _, rgen2, _ = api.post("/v1/subscriptions", {**rev_gen_body, "request_id": "rev-2"})
        check(rgen2["subscription"]["round_id"] != rgen1_round, "last revoke allows a new same-condition round")
        check(rgen2["round"]["state"] == "queued", "post-revoke generation is queued")
        api.post("/v1/pipelines/gen/requests/rev-2/revoke", {})

        _, first, _ = api.post(
            "/v1/subscriptions",
            {
                "pipeline_id": "idem",
                "request_id": "idem-open",
                "asset_id": "a",
                "asset_version": "1",
                "params": {"case": "retry"},
            },
        )
        _, second, _ = api.post(
            "/v1/subscriptions",
            {
                "pipeline_id": "idem",
                "request_id": "idem-open",
                "asset_id": "a",
                "asset_version": "1",
                "params": {"case": "retry"},
            },
        )
        check(first["subscription"]["id"] == second["subscription"]["id"], "original request number returns same subscription")
        check(second["replayed"] is True and second["subscription"]["state"] == "open", "retry reports current open state")
        api.post("/v1/pipelines/idem/requests/idem-open/revoke", {})

        _, ack_req, _ = api.post(
            "/v1/subscriptions",
            {
                "pipeline_id": "idem",
                "request_id": "idem-ack",
                "asset_id": "a",
                "asset_version": "1",
                "params": {"case": "ack-terminal"},
            },
        )
        ack_round = ack_req["subscription"]["round_id"]
        _, compute_claim, _ = api.post("/v1/computations/claim", {"worker_id": "verify-terminal", "lease_seconds": 20})
        check(compute_claim["job"]["round_id"] == ack_round, "claimed deterministic terminal-test round")
        term_result, term_sha, term_ct = render_asset(ROOT / "fixtures", "a", "1", {"case": "ack-terminal"})
        api.post(
            f"/v1/computations/{ack_round}/result",
            {
                "token": compute_claim["job"]["lease"]["token"],
                "result_body_b64": base64.b64encode(term_result).decode(),
                "result_sha256": term_sha,
                "content_type": term_ct,
            },
        )
        _, delivery_claim, _ = api.post(
            "/v1/deliveries/claim", {"pipeline_id": "idem", "receiver_id": "term", "lease_seconds": 20}
        )
        check(delivery_claim["delivery"]["request_id"] == "idem-ack", "claimed ack-terminal delivery")
        ack_token = delivery_claim["delivery"]["lease"]["token"]
        ack_sub_id = delivery_claim["delivery"]["subscription_id"]
        _, ack_ok, _ = api.post(f"/v1/deliveries/{ack_sub_id}/ack", {"token": ack_token})
        check(ack_ok["subscription"]["state"] == "acked", "delivery acknowledged")
        _, ack_replay, _ = api.post(f"/v1/deliveries/{ack_sub_id}/ack", {"token": ack_token})
        check(ack_replay.get("replayed") is True, "same valid ack credential can retry idempotently")
        _, retry_after_ack, _ = api.post(
            "/v1/subscriptions",
            {"pipeline_id": "idem", "request_id": "idem-ack", "asset_id": "a", "asset_version": "1", "params": {"case": "ack-terminal"}},
        )
        check(retry_after_ack["subscription"]["state"] == "acked", "original request retry returns acked state")
        revoke_acked_err = api.expect_error(
            "POST", "/v1/pipelines/idem/requests/idem-ack/revoke", {}
        )
        check(revoke_acked_err.code == "subscription_already_acked", "acked subscription cannot be revoked")
        check(revoke_acked_err.details.get("subscription", {}).get("state") == "acked", "conflict preserves acked state")

        api.post(
            "/v1/subscriptions",
            {"pipeline_id": "idem", "request_id": "idem-rev", "asset_id": "a", "asset_version": "1", "params": {"case": "revoke-terminal"}},
        )
        _, revoked, _ = api.post("/v1/pipelines/idem/requests/idem-rev/revoke", {})
        check(revoked["subscription"]["state"] == "revoked", "subscription revoked")
        _, revoked_replay, _ = api.post("/v1/pipelines/idem/requests/idem-rev/revoke", {})
        check(revoked_replay.get("replayed") is True, "revoke is idempotent")
        _, retry_after_revoke, _ = api.post(
            "/v1/subscriptions",
            {"pipeline_id": "idem", "request_id": "idem-rev", "asset_id": "a", "asset_version": "1", "params": {"case": "revoke-terminal"}},
        )
        check(retry_after_revoke["subscription"]["state"] == "revoked", "original request retry returns revoked state")

        _, cred_req, _ = api.post(
            "/v1/subscriptions",
            {"pipeline_id": "idem", "request_id": "cred-rev", "asset_id": "a", "asset_version": "1", "params": {"case": "cred-rev"}},
        )
        cred_round = cred_req["subscription"]["round_id"]
        _, cc, _ = api.post("/v1/computations/claim", {"worker_id": "verify-terminal-2", "lease_seconds": 20})
        check(cc["job"]["round_id"] == cred_round, "claimed credential-revoke round")
        api.post(
            f"/v1/computations/{cred_round}/result",
            {
                "token": cc["job"]["lease"]["token"],
                "result_body_b64": base64.b64encode(b"cred-revoke-result").decode(),
                "result_sha256": __import__("hashlib").sha256(b"cred-revoke-result").hexdigest(),
            },
        )
        _, cd, _ = api.post("/v1/deliveries/claim", {"pipeline_id": "idem", "receiver_id": "cred", "lease_seconds": 20})
        check(cd["delivery"]["request_id"] == "cred-rev", "claimed credential-revoke delivery")
        cd_token = cd["delivery"]["lease"]["token"]
        cd_id = cd["delivery"]["subscription_id"]
        api.post("/v1/pipelines/idem/requests/cred-rev/revoke", {})
        ack_revoked_err = api.expect_error("POST", f"/v1/deliveries/{cd_id}/ack", {"token": cd_token})
        check(ack_revoked_err.code == "subscription_revoked", "valid credential cannot resurrect revoked subscription")
        check(named_state("idem", "cred-rev") == "revoked", "revoked terminal state remains")

        q_body = {
            "asset_id": "quota-asset",
            "asset_version": "1",
            "params": {"case": "quota"},
        }
        _, q1, _ = api.post("/v1/subscriptions", {**q_body, "pipeline_id": "quota-demo", "request_id": "q1"})
        _, q2, _ = api.post("/v1/subscriptions", {**q_body, "pipeline_id": "quota-demo", "request_id": "q2"})
        check(q1["subscription"]["id"] != q2["subscription"]["id"], "quota pipeline accepted first two open requests")
        _, q1_retry, _ = api.post("/v1/subscriptions", {**q_body, "pipeline_id": "quota-demo", "request_id": "q1"})
        check(q1_retry["replayed"] is True, "full quota still returns original accepted request")
        quota_err = api.expect_error(
            "POST", "/v1/subscriptions", {**q_body, "pipeline_id": "quota-demo", "request_id": "q3"}
        )
        check(quota_err.status == 429 and quota_err.code == "pipeline_quota_full", "third new subscription pauses only quota-demo", quota_err.details)
        _, other_quota, _ = api.post(
            "/v1/subscriptions", {**q_body, "pipeline_id": "quota-other", "request_id": "other-1"}
        )
        check(other_quota["ok"], "another pipeline is unaffected by quota-demo limit")
        api.post("/v1/pipelines/quota-demo/requests/q1/revoke", {})
        _, q3, _ = api.post("/v1/subscriptions", {**q_body, "pipeline_id": "quota-demo", "request_id": "q3"})
        check(q3["ok"] and not q3["replayed"], "after capacity frees, same new request number is accepted")
        _, q3_retry, _ = api.post("/v1/subscriptions", {**q_body, "pipeline_id": "quota-demo", "request_id": "q3"})
        check(q3_retry["replayed"] is True and q3_retry["subscription"]["id"] == q3["subscription"]["id"], "q3 retry returns its original subscription")
        for pipeline, req in (("quota-demo", "q2"), ("quota-demo", "q3"), ("quota-other", "other-1")):
            api.post(f"/v1/pipelines/{pipeline}/requests/{req}/revoke", {})

        _, before_restart_meter, _ = api.get("/v1/metrics/writes")

        section("9. Saved result survives hard restart even if submit response is lost")
        _, recovery_req, _ = api.post(
            "/v1/subscriptions",
            {
                "pipeline_id": "recovery",
                "request_id": "recover-1",
                "asset_id": "harbor-title",
                "asset_version": "4.0.0",
                "params": {"case": "crash-after-commit"},
            },
        )
        recovery_round = recovery_req["subscription"]["round_id"]
        api.post(
            "/v1/admin/faults/arm",
            {"fault": "crash_before_submit_response", "match": {"round_id": recovery_round}},
        )
        procs.start(
            "worker-recovery",
            "render_queue.worker",
            [
                "--server",
                api.base,
                "--worker-id",
                "recovery-worker",
                "--fixtures",
                str(ROOT / "fixtures"),
                "--poll-interval",
                "0.05",
                "--lease-seconds",
                "10",
                "--render-delay",
                "0.2",
                "--submit-timeout",
                "30",
            ],
        )

        exited = wait_until(lambda: procs.poll("server") is not None, 8, "fault-injected server exits after durable commit")
        check(exited is not None, "server hard-exited before sending submit response", exited)
        procs.handles["server"]["log"].close()
        time.sleep(0.2)

        server_argv = server_args(port, db_path)
        procs.start("server-restarted", server_argv[0], server_argv[1:])
        wait_until(lambda: api.get("/health", timeout=1.5)[1].get("ok"), 15, "restarted server health")
        _, recovered_round_payload, _ = api.get(f"/v1/rounds/{recovery_round}")
        recovered_round = recovered_round_payload["round"]
        check(recovered_round["state"] == "saved", "already-committed result is saved after restart")
        check(bool(recovered_round["result_sha256"]) and int(recovered_round["result_bytes"]) > 0, "one result body survived", recovered_round["result_sha256"][:16])

        # Terminal states from before the crash/restart remain unchanged.
        check(named_state("idem", "idem-ack") == "acked", "acked subscription remains acked after restart")
        check(named_state("idem", "idem-rev") == "revoked", "revoked subscription remains revoked after restart")
        check(named_state("idem", "cred-rev") == "revoked", "credential-revoked state remains revoked after restart")
        wait_until(lambda: pipeline_state("backlog", "acked") == 6, 5, "acked backlog state persists")
        wait_until(lambda: pipeline_state("offline", "acked") == 4, 5, "acked offline state persists")

        recovery_worker_log = procs.log_text("worker-recovery")
        check(
            "event=submit_answer_lost_retrying" in recovery_worker_log,
            "worker observed lost submit answer and retried",
        )
        # It may receive the idempotent reply after restart or fence itself if
        # the short verification schedule crossed the lease; either way the
        # saved round and delivery below are the durable source of truth.
        wait_until(
            lambda: (
                "event=submit_ack" in procs.log_text("worker-recovery")
                or "event=stale_credential_rejected" in procs.log_text("worker-recovery")
            ),
            8,
            "recovery worker resolves its submit outcome",
        )

        procs.start(
            "receiver-recovery",
            "render_queue.receiver",
            [
                "--server",
                api.base,
                "--pipeline-id",
                "recovery",
                "--receiver-id",
                "recovery-recv-1",
                "--output-dir",
                str(run_dir / "received"),
                "--poll-interval",
                "0.05",
                "--lease-seconds",
                "10",
            ],
        )
        wait_until(lambda: named_state("recovery", "recover-1") == "acked", 8, "receiver gets recovered result and acks")
        recovery_receipt = json.loads((run_dir / "received" / "recovery" / "recover-1.receipt.json").read_text())
        check(recovery_receipt["sha256"] == recovered_round["result_sha256"], "post-restart delivery has the same saved result")

        section("10. Cumulative write meter")
        _, after_restart_meter, _ = api.get("/v1/metrics/writes")
        before_total = before_restart_meter["meter"]["total"] if "total" in before_restart_meter.get("meter", {}) else before_restart_meter["meter"]
        after_total = after_restart_meter["meter"]
        # The restarted process has its own in-memory cumulative meter; combine
        # the two process lifetimes for the verification summary.
        combined_records = int(before_total["records_affected"]) + int(after_total["records_affected"])
        combined_bytes = int(before_total["app_value_bytes"]) + int(after_total["app_value_bytes"])
        check(combined_records > 20, "cumulative CUD record effects were metered", combined_records)
        check(combined_bytes > 1000, "cumulative application value bytes were metered", combined_bytes)
        print(f"  ℹ️  pre-restart writes: records={before_total['records_affected']} bytes={before_total['app_value_bytes']}")
        print(f"  ℹ️  post-restart writes: records={after_total['records_affected']} bytes={after_total['app_value_bytes']}")
        print(f"  ℹ️  combined verification process lifetimes: records={combined_records} bytes={combined_bytes}")

        elapsed = time.time() - started_at
        section(f"PASS: {CHECKS} checks in {elapsed:.1f}s (limit 80s)")
        check(elapsed < 80, "verification completed within 80 seconds", f"{elapsed:.1f}s")
        print(f"\nRun artifacts and logs: {run_dir}", flush=True)
        return 0

    except Exception as exc:
        print(f"\n❌ VERIFICATION FAILED: {type(exc).__name__}: {exc}", flush=True)
        print("\n--- process log tails ---", flush=True)
        print(procs.tails(), flush=True)
        print(f"\nRun artifacts and logs: {run_dir}", flush=True)
        return 1
    finally:
        procs.stop_all()


if __name__ == "__main__":
    raise SystemExit(main())
