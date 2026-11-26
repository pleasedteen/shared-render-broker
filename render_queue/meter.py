"""Application-layer persistence write metering.

The meter counts DML record effects and application value bytes.  SQLite page,
WAL and checkpoint traffic is intentionally outside this metric.
"""

from __future__ import annotations

import json
import threading
from contextvars import ContextVar
from collections import deque
from typing import Any, Deque, Dict, Iterable, Mapping, Optional, Sequence

request_meter: ContextVar[Optional[Dict[str, Any]]] = ContextVar(
    "request_meter", default=None
)


def _scalar_value_bytes(value: Any) -> int:
    """Return the application-value byte volume for one bound value."""
    if value is None:
        return 0
    if isinstance(value, bool):
        # Booleans are application integers; use a stable one-byte application
        # representation rather than SQLite's zero-payload 0/1 optimisation.
        return 1
    if isinstance(value, int):
        # Fixed-width signed 64-bit application scalar.  This keeps write
        # volume independent of row-id magnitude and SQLite serial formats.
        return 8
    if isinstance(value, float):
        return 8
    if isinstance(value, (bytes, bytearray, memoryview)):
        return len(value)
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    if isinstance(value, (list, tuple, dict)):
        return len(
            json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
                "utf-8"
            )
        )
    return len(str(value).encode("utf-8"))


def values_byte_len(values: Any) -> int:
    if values is None:
        return 0
    if isinstance(values, Mapping):
        return sum(_scalar_value_bytes(v) for v in values.values())
    if isinstance(values, Sequence) and not isinstance(values, (str, bytes, bytearray)):
        return sum(_scalar_value_bytes(v) for v in values)
    return _scalar_value_bytes(values)


def new_stats() -> Dict[str, Any]:
    return {
        "write_statements": 0,
        "records_affected": 0,
        "insert_records": 0,
        "update_records": 0,
        "delete_records": 0,
        "app_value_bytes": 0,
        "insert_value_bytes": 0,
        "update_value_bytes": 0,
        "deleted_record_bytes": 0,
        "by_table": {},
    }


class WriteMeter:
    """Process-cumulative meter plus optional request-scoped accounting."""

    def __init__(self, event_limit: int = 200) -> None:
        self._lock = threading.Lock()
        self.total = new_stats()
        self.events: Deque[Dict[str, Any]] = deque(maxlen=event_limit)

    def record(
        self,
        *,
        op: str,
        table: str,
        rows: int,
        values: Any = None,
        old_rows: Optional[Iterable[Mapping[str, Any]]] = None,
        multiply_values_by_rows: bool = False,
        sql: Optional[str] = None,
    ) -> Dict[str, Any]:
        op = op.lower()
        rows = max(0, int(rows))

        one_value_bytes = values_byte_len(values)
        if op == "update" and multiply_values_by_rows:
            byte_count = one_value_bytes * rows
        elif op == "insert" and multiply_values_by_rows:
            byte_count = one_value_bytes * rows
        else:
            # The normal calls in this project insert one row or provide the
            # exact one-row value map.  For zero matched rows nothing was
            # actually written.
            byte_count = one_value_bytes if rows > 0 else 0

        deleted_bytes = 0
        old_row_count = 0
        if old_rows is not None:
            for row in old_rows:
                old_row_count += 1
                deleted_bytes += values_byte_len(dict(row))

        event = {
            "op": op,
            "table": table,
            "rows": rows,
            "app_value_bytes": byte_count,
            "deleted_record_bytes": deleted_bytes,
            "sql": " ".join(sql.split()) if sql else None,
        }

        with self._lock:
            t = self.total
            t["write_statements"] += 1
            t["records_affected"] += rows
            t["app_value_bytes"] += byte_count
            t["deleted_record_bytes"] += deleted_bytes
            t[f"{op}_records"] = t.get(f"{op}_records", 0) + rows
            table_stats = t["by_table"].setdefault(
                table,
                {
                    "write_statements": 0,
                    "records_affected": 0,
                    "app_value_bytes": 0,
                    "delete_records": 0,
                    "deleted_record_bytes": 0,
                },
            )
            table_stats["write_statements"] += 1
            table_stats["records_affected"] += rows
            table_stats["app_value_bytes"] += byte_count
            if op == "delete":
                table_stats["delete_records"] += rows
                table_stats["deleted_record_bytes"] += deleted_bytes
            if op in ("insert", "update"):
                key = f"{op}_value_bytes"
                t[key] = t.get(key, 0) + byte_count
            self.events.append(event)

        ctx = request_meter.get()
        if ctx is not None:
            ctx["write_statements"] += 1
            ctx["records_affected"] += rows
            ctx["app_value_bytes"] += byte_count
            ctx[f"{op}_records"] = ctx.get(f"{op}_records", 0) + rows
            ctx[f"{op}_value_bytes"] = ctx.get(f"{op}_value_bytes", 0) + byte_count
            bt = ctx.setdefault("by_table", {})
            ts = bt.setdefault(
                table,
                {
                    "write_statements": 0,
                    "records_affected": 0,
                    "app_value_bytes": 0,
                },
            )
            ts["write_statements"] += 1
            ts["records_affected"] += rows
            ts["app_value_bytes"] += byte_count

        return event

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            data = json.loads(json.dumps(self.total))
            data["basis"] = (
                "application values: TEXT=UTF-8 bytes, BLOB=raw bytes, "
                "scalar numbers=8 bytes, NULL=0; repeated CUDs accumulate; "
                "SQLite pages/WAL/checkpoints excluded"
            )
            data["recent_events"] = list(self.events)[-20:]
            return data

    @staticmethod
    def request_snapshot() -> Optional[Dict[str, Any]]:
        ctx = request_meter.get()
        if ctx is None:
            return None
        return json.loads(json.dumps(ctx))
