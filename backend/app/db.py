"""SQLite persistence primitives for transfer attempts and compile receipts.

The application originally kept every module row in a process-local list.  A
process restart therefore lost retry counters, transfer continuation
credentials and compile receipts.  This module provides the small, dependency
free persistence layer used by ``app.transfer_ledger``:

* a connection shared by the process (serialised with a re-entrant lock),
* the schema for module snapshots, attempts, continuations, receipts,
  confirmation requests, conflicts and migration runs,
* transaction and helper methods.

Only ``data_transfer`` and ``results_compile`` are snapshotted through this
layer; the other demo modules intentionally stay in memory.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "var" / "app.sqlite3"

SCHEMA = """
CREATE TABLE IF NOT EXISTS module_rows (
    module      TEXT NOT NULL,
    id          INTEGER NOT NULL,
    payload     TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (module, id)
);

CREATE TABLE IF NOT EXISTS idempotent_keys (
    key         TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS continuations (
    continuation_id     TEXT PRIMARY KEY,
    credential          TEXT NOT NULL UNIQUE,
    transfer_id         INTEGER NOT NULL UNIQUE,
    compile_id          INTEGER,
    data_content        TEXT NOT NULL DEFAULT '',
    status              TEXT NOT NULL,
    current_attempt_id  INTEGER,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transfer_attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_key     TEXT NOT NULL UNIQUE,
    transfer_id     INTEGER NOT NULL,
    attempt_no      INTEGER NOT NULL,
    credential      TEXT NOT NULL,
    status          TEXT NOT NULL,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    input_snapshot  TEXT NOT NULL DEFAULT '{}',
    missing_items   TEXT NOT NULL DEFAULT '[]',
    migration_state TEXT NOT NULL DEFAULT 'native',
    started_at      TEXT,
    finished_at     TEXT,
    updated_at      TEXT NOT NULL,
    UNIQUE (transfer_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS receipt_requests (
    request_id          TEXT PRIMARY KEY,
    compile_id          INTEGER NOT NULL,
    continuation_id     TEXT NOT NULL,
    credential          TEXT NOT NULL,
    transfer_id         INTEGER NOT NULL,
    expected_items      TEXT NOT NULL DEFAULT '[]',
    status              TEXT NOT NULL DEFAULT '待确认',
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    UNIQUE (compile_id, continuation_id)
);

CREATE TABLE IF NOT EXISTS receipts (
    receipt_id          TEXT PRIMARY KEY,
    confirmation_id     TEXT NOT NULL UNIQUE,
    continuation_id     TEXT NOT NULL,
    credential          TEXT NOT NULL,
    compile_id          INTEGER NOT NULL,
    transfer_id         INTEGER NOT NULL,
    attempt_id          INTEGER,
    status              TEXT NOT NULL,
    outcome             TEXT NOT NULL,
    received_items      TEXT NOT NULL DEFAULT '[]',
    missing_items       TEXT NOT NULL DEFAULT '[]',
    source              TEXT NOT NULL DEFAULT 'new',
    superseded_by       TEXT,
    created_at          TEXT NOT NULL,
    acked_at            TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_receipts_valid_continuation
    ON receipts(continuation_id) WHERE status = '有效';

CREATE TABLE IF NOT EXISTS confirmation_conflicts (
    conflict_id         TEXT PRIMARY KEY,
    confirmation_id     TEXT NOT NULL,
    continuation_id     TEXT NOT NULL,
    transfer_id         INTEGER NOT NULL,
    compile_id          INTEGER,
    attempt_id          INTEGER,
    reason              TEXT NOT NULL,
    incoming_payload    TEXT NOT NULL DEFAULT '{}',
    status              TEXT NOT NULL DEFAULT '待研判',
    decision            TEXT,
    decided_at          TEXT,
    created_at          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_conflicts_continuation
    ON confirmation_conflicts(continuation_id, status);

CREATE TABLE IF NOT EXISTS migration_runs (
    run_id          TEXT PRIMARY KEY,
    status          TEXT NOT NULL,
    detail          TEXT NOT NULL DEFAULT '{}',
    started_at      TEXT NOT NULL,
    finished_at     TEXT
);
"""

RUNTIME_TABLES = (
    "receipts",
    "receipt_requests",
    "confirmation_conflicts",
    "transfer_attempts",
    "continuations",
    "migration_runs",
    "module_rows",
    "idempotent_keys",
)


def _now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


class Database:
    """A tiny serialised SQLite wrapper.

    ``:memory:`` is supported and keeps one connection for the lifetime of the
    object.  File backed databases are opened with WAL so a new process can
    pick up committed data immediately after a restart.
    """

    def __init__(self, path: str | Path | None = None) -> None:
        configured = path or os.environ.get("APP_DB_PATH") or DEFAULT_DB_PATH
        self.path = str(configured)
        if self.path != ":memory:":
            Path(self.path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
            self.path = str(Path(self.path).expanduser().resolve())
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(
            self.path,
            check_same_thread=False,
            timeout=30.0,
            isolation_level=None,
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        if self.path != ":memory:":
            self.conn.execute("PRAGMA journal_mode = WAL")
            self.conn.execute("PRAGMA synchronous = FULL")
        self._initialize()

    def _initialize(self) -> None:
        with self._lock:
            self.conn.executescript(SCHEMA)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Run a block in one IMMEDIATE transaction.

        Nested ``with db.transaction()`` blocks reuse the outer transaction so
        service code can compose ledger operations without accidental partial
        commits.
        """

        with self._lock:
            if getattr(self, "_in_transaction", False):
                yield self.conn
                return
            self._in_transaction = True
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
            else:
                self.conn.execute("COMMIT")
            finally:
                self._in_transaction = False

    # ------------------------------------------------------------------
    # Module snapshots (data_transfer / results_compile)
    # ------------------------------------------------------------------
    def load_module_rows(self, module: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT id, payload FROM module_rows WHERE module = ? ORDER BY id",
                (module,),
            ).fetchall()
        return [json.loads(row["payload"]) for row in rows]

    def replace_module_rows(
        self,
        module: str,
        rows: Sequence[dict[str, Any]],
        conn: sqlite3.Connection | None = None,
    ) -> None:
        now = _now()
        with self._lock:
            target = conn or self.conn
            target.execute("DELETE FROM module_rows WHERE module = ?", (module,))
            for row in rows:
                target.execute(
                    "INSERT INTO module_rows(module, id, payload, updated_at) "
                    "VALUES (?, ?, ?, ?)",
                    (module, int(row.get("id", 0)), json.dumps(row, ensure_ascii=False), now),
                )

    def upsert_module_row(
        self,
        module: str,
        row: dict[str, Any],
        conn: sqlite3.Connection | None = None,
    ) -> None:
        with self._lock:
            target = conn or self.conn
            target.execute(
                "INSERT INTO module_rows(module, id, payload, updated_at) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(module, id) DO UPDATE SET payload = excluded.payload, "
                "updated_at = excluded.updated_at",
                (module, int(row.get("id", 0)), json.dumps(row, ensure_ascii=False), _now()),
            )

    # ------------------------------------------------------------------
    # Idempotency keys survive a restart for persisted modules
    # ------------------------------------------------------------------
    def load_idempotent_keys(self) -> list[str]:
        with self._lock:
            rows = self.conn.execute("SELECT key FROM idempotent_keys").fetchall()
        return [row["key"] for row in rows]

    def save_idempotent_key(self, key: str) -> None:
        with self._lock:
            self.conn.execute(
                "INSERT OR IGNORE INTO idempotent_keys(key, created_at) VALUES (?, ?)",
                (key, _now()),
            )

    def delete_idempotent_key(self, key: str) -> None:
        with self._lock:
            self.conn.execute("DELETE FROM idempotent_keys WHERE key = ?", (key,))

    def clear_runtime(self) -> None:
        """Reset all persisted runtime state (used by the test helper)."""

        with self._lock:
            with self.transaction() as conn:
                for table in RUNTIME_TABLES:
                    conn.execute(f"DELETE FROM {table}")

    def close(self) -> None:
        with self._lock:
            self.conn.close()
