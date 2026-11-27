"""SQLite schema and metered write helpers."""

from __future__ import annotations

import contextlib
import sqlite3
import threading
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

from .meter import WriteMeter

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS rounds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id TEXT NOT NULL,
    asset_version TEXT NOT NULL,
    params_hash TEXT NOT NULL,
    params_json TEXT NOT NULL,
    input_manifest_hash TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL CHECK (state IN ('queued', 'saved', 'closed')),
    open_count INTEGER NOT NULL DEFAULT 0,

    result_body BLOB,
    result_sha256 TEXT,
    result_bytes INTEGER,
    result_content_type TEXT,

    compute_owner TEXT,
    compute_token TEXT,
    compute_fence INTEGER NOT NULL DEFAULT 0,
    compute_attempts INTEGER NOT NULL DEFAULT 0,
    compute_lease_expires_at REAL,
    submitted_at REAL,

    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

-- At most one non-terminal round for a fixed material/version/params/fixtures.
CREATE UNIQUE INDEX IF NOT EXISTS ux_rounds_active_key
    ON rounds(asset_id, asset_version, params_hash, input_manifest_hash)
    WHERE state IN ('queued', 'saved');

CREATE INDEX IF NOT EXISTS ix_rounds_claim
    ON rounds(state, compute_lease_expires_at, id);

CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    round_id INTEGER NOT NULL REFERENCES rounds(id),
    state TEXT NOT NULL CHECK (state IN ('open', 'acked', 'revoked')),

    delivery_owner TEXT,
    delivery_token TEXT,
    delivery_fence INTEGER NOT NULL DEFAULT 0,
    delivery_attempts INTEGER NOT NULL DEFAULT 0,
    delivery_lease_expires_at REAL,

    acked_at REAL,
    revoked_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,

    UNIQUE(pipeline_id, request_id)
);

CREATE INDEX IF NOT EXISTS ix_subscriptions_pipeline_open
    ON subscriptions(pipeline_id, state, delivery_lease_expires_at, id);
CREATE INDEX IF NOT EXISTS ix_subscriptions_round
    ON subscriptions(round_id);

-- Durable investigation trail for the round (compute) side.  Every claim,
-- lease handover and result save is recorded here and survives restarts.
CREATE TABLE IF NOT EXISTS round_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    round_id INTEGER NOT NULL REFERENCES rounds(id),
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'compute_claim', 'compute_handoff', 'result_saved'
    )),
    owner TEXT,
    fence INTEGER,
    attempt INTEGER,
    lease_expires_at REAL,
    detail_json TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_round_events_round
    ON round_events(round_id, id);

-- Durable investigation trail for one subscription's delivery lifecycle:
-- delivery claims/handovers, server-side acks and receiver-side disk persists
-- (including offline re-reports).  Rows are append-only.
CREATE TABLE IF NOT EXISTS delivery_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription_id INTEGER NOT NULL REFERENCES subscriptions(id),
    pipeline_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'delivery_claim', 'delivery_handoff',
        'receiver_persisted', 'delivery_acked'
    )),
    receiver_id TEXT,
    fence INTEGER,
    attempt INTEGER,
    lease_expires_at REAL,
    reported_by TEXT,
    source TEXT NOT NULL DEFAULT 'live',
    detail_json TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_delivery_events_sub
    ON delivery_events(subscription_id, id);
CREATE INDEX IF NOT EXISTS ix_delivery_events_pipeline
    ON delivery_events(pipeline_id, request_id);

-- At most one receiver-persisted record per original delivery claim (fence),
-- even if the re-report is retried after a restart.
CREATE UNIQUE INDEX IF NOT EXISTS ux_delivery_persisted_once
    ON delivery_events(subscription_id, fence)
    WHERE event_type = 'receiver_persisted';
"""


class Transaction:
    def __init__(self, db: "Database", conn: sqlite3.Connection) -> None:
        self.db = db
        self.conn = conn

    def one(self, sql: str, params: Sequence[Any] = ()) -> Optional[sqlite3.Row]:
        return self.conn.execute(sql, tuple(params)).fetchone()

    def all(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        return self.conn.execute(sql, tuple(params)).fetchall()

    def write(
        self,
        *,
        op: str,
        table: str,
        sql: str,
        params: Any = (),
        values: Any = None,
        old_rows: Optional[Iterable[Mapping[str, Any]]] = None,
        multiply_values_by_rows: bool = False,
    ) -> sqlite3.Cursor:
        cur = self.conn.execute(sql, params)
        self.db.meter.record(
            op=op,
            table=table,
            rows=cur.rowcount,
            values=values,
            old_rows=old_rows,
            multiply_values_by_rows=multiply_values_by_rows,
            sql=sql,
        )
        return cur

    def insert(self, table: str, values: Mapping[str, Any]) -> int:
        cols = list(values.keys())
        col_sql = ", ".join(cols)
        mark_sql = ", ".join(f":{c}" for c in cols)
        sql = f"INSERT INTO {table} ({col_sql}) VALUES ({mark_sql})"
        cur = self.write(
            op="insert",
            table=table,
            sql=sql,
            params=values,
            values=values,
        )
        return int(cur.lastrowid)

    def update(
        self,
        table: str,
        values: Mapping[str, Any],
        where: str,
        where_params: Sequence[Any] = (),
    ) -> int:
        q_assignments = ", ".join(f"{col} = ?" for col in values.keys())
        sql = f"UPDATE {table} SET {q_assignments} WHERE {where}"
        cur = self.write(
            op="update",
            table=table,
            sql=sql,
            params=[*values.values(), *where_params],
            values=values,
            multiply_values_by_rows=True,
        )
        return int(cur.rowcount)

    def delete(self, table: str, where: str, where_params: Sequence[Any] = ()) -> int:
        old = self.all(f"SELECT * FROM {table} WHERE {where}", where_params)
        sql = f"DELETE FROM {table} WHERE {where}"
        cur = self.write(
            op="delete",
            table=table,
            sql=sql,
            params=where_params,
            old_rows=[dict(r) for r in old],
        )
        return int(cur.rowcount)


class Database:
    def __init__(self, path: str | Path, meter: WriteMeter) -> None:
        self.path = Path(path)
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.meter = meter
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(
            str(self.path),
            check_same_thread=False,
            isolation_level=None,
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)

    @contextlib.contextmanager
    def tx(self) -> contextlib.AbstractContextManager[Transaction]:
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            transaction = Transaction(self, self.conn)
            try:
                yield transaction
                self.conn.execute("COMMIT")
            except Exception:
                if self.conn.in_transaction:
                    self.conn.execute("ROLLBACK")
                raise

    def close(self) -> None:
        with self.lock:
            self.conn.close()
