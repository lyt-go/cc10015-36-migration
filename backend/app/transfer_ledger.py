"""Durable transfer ledger: continuations, attempts, compile receipts, conflicts.

The ledger is the single source of truth for the runtime state that must
survive a process restart:

* one *continuation* (资料接续过程) per transport record, carrying the
  *continuation credential* (传输接续凭证) the duty officer needs after a
  restart,
* every *attempt* (传输尝试) with its original retry counter and input
  snapshot, so switching storage does not recalculate them,
* compile *receipt requests* (汇编收件确认请求) tied to a credential,
* *receipts* (接收确认).  A continuation may have at most one valid receipt at
  any point in time, enforced by a partial unique index,
* *conflicts* raised when a confirmation arrives while an attempt is still
  being migrated / already completed, with an explicit duty-officer decision,
* *migration runs* which can be interrupted and retried without registering
  the same attempt twice.

All writes that touch a receipt and an attempt happen in one SQLite
transaction, so the two sides can never disagree after recovery.
"""
from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

from app.db import Database

TRANSFER_MODULE = "data_transfer"
COMPILE_MODULE = "results_compile"
PERSISTED_MODULES = (TRANSFER_MODULE, COMPILE_MODULE)

INPUT_FIELDS = [
    "数据内容",
    "数据量",
    "传输方式",
    "目标系统",
    "操作人",
    "传输时间",
    "备注说明",
]

ATTEMPT_STATUSES = {
    "待传输",
    "传输中",
    "已完成",
    "已失败",
    "准备失败",
    "恢复待确认",
    "已作废",
}
TERMINAL_ATTEMPT_STATUSES = {"已完成", "已失败", "准备失败", "已作废"}

CONFLICT_STATES = {"migrating", "interrupted", "conflict_pending"}

DECISION_KEEP = "继续办结"
DECISION_RECOVER = "恢复待确认"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _loads(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return default


def _unique(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        text = str(item).strip()
        if text and text not in seen:
            seen.add(text)
            ordered.append(text)
    return ordered


def _row_dict(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


def _attempt_dict(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    data = _row_dict(row)
    data["input_snapshot"] = _loads(data.get("input_snapshot"), {})
    data["missing_items"] = _loads(data.get("missing_items"), [])
    return data


def _receipt_dict(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    data = _row_dict(row)
    data["received_items"] = _loads(data.get("received_items"), [])
    data["missing_items"] = _loads(data.get("missing_items"), [])
    data["acknowledged"] = data.get("status") == "有效"
    return data


def _conflict_dict(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    data = _row_dict(row)
    data["incoming_payload"] = _loads(data.get("incoming_payload"), {})
    return data


def _request_dict(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    data = _row_dict(row)
    data["expected_items"] = _loads(data.get("expected_items"), [])
    return data


def _status_for_row(row: dict[str, Any]) -> str:
    """Map a transport row to an attempt status without losing 准备失败."""

    explicit = str(row.get("尝试状态") or row.get("attempt_status") or "").strip()
    if explicit in ATTEMPT_STATUSES:
        return explicit
    stage = str(row.get("失败原因") or row.get("失败阶段") or row.get("传输状态") or "")
    if "准备" in stage and "失败" in stage:
        return "准备失败"
    status = str(row.get("status") or "").strip()
    return status if status in ATTEMPT_STATUSES else "待传输"


def _input_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    return {field: row.get(field, "") for field in INPUT_FIELDS}


class TransferLedger:
    """Persistence and state machine for transport continuations."""

    def __init__(self, database: Database) -> None:
        self.db = database
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Row helpers
    # ------------------------------------------------------------------
    def _persist_rows(
        self,
        conn: Any,
        module: str,
        rows: Sequence[dict[str, Any]],
    ) -> None:
        self.db.replace_module_rows(module, rows, conn=conn)

    def _find_transfer(self, rows: Sequence[dict[str, Any]], transfer_id: int) -> dict[str, Any] | None:
        for row in rows:
            if int(row.get("id", 0)) == int(transfer_id):
                return row
        return None

    def _find_compile(self, rows: Sequence[dict[str, Any]], compile_id: int) -> dict[str, Any] | None:
        for row in rows:
            if int(row.get("id", 0)) == int(compile_id):
                return row
        return None

    def decorate(self, row: dict[str, Any]) -> dict[str, Any]:
        """Attach credential / attempt metadata to an API-facing row."""

        transfer_id = int(row.get("id", 0))
        with self._lock:
            continuation = self._get_continuation_by_transfer(transfer_id)
            attempts = self._list_attempts(transfer_id)
        if continuation:
            row["接续凭证"] = continuation["credential"]
            row["credential"] = continuation["credential"]
            row["continuation_id"] = continuation["continuation_id"]
            row["current_attempt_id"] = continuation.get("current_attempt_id")
        # 原尝试次数 = 首次 + 已发生的重试次数；即使尚未登记尝试也这样展示，
        # 避免“待传输/传输中”的记录看起来像 0 次尝试。
        attempt_count = len(attempts) if attempts else int(row.get("retry_count", 0) or 0) + 1
        row["尝试次数"] = attempt_count
        row["attempt_count"] = attempt_count
        latest = attempts[-1] if attempts else None
        if latest:
            row["尝试状态"] = latest["status"]
            if latest.get("missing_items"):
                row["缺项"] = "、".join(latest["missing_items"])
        elif not row.get("尝试状态"):
            row["尝试状态"] = _status_for_row(row)
        return row

    # ------------------------------------------------------------------
    # Continuations
    # ------------------------------------------------------------------
    def _get_continuation_by_transfer(self, transfer_id: int) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM continuations WHERE transfer_id = ?", (int(transfer_id),)
        ).fetchone()
        return _row_dict(row) if row else None

    def _get_continuation(self, continuation_id: str) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM continuations WHERE continuation_id = ?", (continuation_id,)
        ).fetchone()
        return _row_dict(row) if row else None

    def _new_credential(self, transfer_id: int) -> str:
        return f"TC-{int(transfer_id):04d}-{uuid.uuid4().hex[:10].upper()}"

    def ensure_continuation(
        self,
        transfer: dict[str, Any],
        *,
        conn: Any = None,
        compile_id: int | None = None,
    ) -> dict[str, Any]:
        transfer_id = int(transfer.get("id", 0))
        data_content = str(transfer.get("数据内容", ""))
        with self._lock:
            existing = self._get_continuation_by_transfer(transfer_id)
            if existing is not None:
                updates: dict[str, Any] = {}
                if compile_id is not None and existing.get("compile_id") != compile_id:
                    updates["compile_id"] = compile_id
                if data_content and existing.get("data_content") != data_content:
                    updates["data_content"] = data_content
                    updates["updated_at"] = _now()
                if updates:
                    sets = ", ".join(f"{key} = ?" for key in updates)
                    (conn or self.db.conn).execute(
                        f"UPDATE continuations SET {sets} WHERE transfer_id = ?",
                        (*updates.values(), transfer_id),
                    )
                    existing.update(updates)
                return existing

            continuation_id = uuid.uuid4().hex
            credential = self._new_credential(transfer_id)
            now = _now()
            (conn or self.db.conn).execute(
                "INSERT INTO continuations(continuation_id, credential, transfer_id, "
                "compile_id, data_content, status, current_attempt_id, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)",
                (continuation_id, credential, transfer_id, compile_id, data_content,
                 str(transfer.get("status") or "待传输"), now, now),
            )
            created = self._get_continuation_by_transfer(transfer_id)
            assert created is not None
            return created

    def get_continuation(self, transfer_id: int) -> dict[str, Any] | None:
        continuation = self._get_continuation_by_transfer(int(transfer_id))
        if continuation is None:
            return None
        continuation["attempts"] = self._list_attempts(int(transfer_id))
        continuation["receipts"] = self._list_receipts(continuation["continuation_id"])
        continuation["requests"] = self._list_requests(continuation_id=continuation["continuation_id"])
        continuation["conflicts"] = self.list_conflicts(transfer_id=int(transfer_id))
        return continuation

    def find_by_credential(self, credential: str) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM continuations WHERE credential = ?", (credential,)
        ).fetchone()
        return _row_dict(row) if row else None

    # ------------------------------------------------------------------
    # Attempts
    # ------------------------------------------------------------------
    def _list_attempts(self, transfer_id: int) -> list[dict[str, Any]]:
        rows = self.db.conn.execute(
            "SELECT * FROM transfer_attempts WHERE transfer_id = ? ORDER BY attempt_no, id",
            (int(transfer_id),),
        ).fetchall()
        return [item for item in (_attempt_dict(r) for r in rows) if item]

    def _get_attempt(self, attempt_id: int) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM transfer_attempts WHERE id = ?", (int(attempt_id),)
        ).fetchone()
        return _attempt_dict(row)

    def _get_attempt_by_no(self, transfer_id: int, attempt_no: int) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM transfer_attempts WHERE transfer_id = ? AND attempt_no = ?",
            (int(transfer_id), int(attempt_no)),
        ).fetchone()
        return _attempt_dict(row)

    def current_attempt(self, transfer_id: int) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM transfer_attempts WHERE transfer_id = ? ORDER BY attempt_no DESC, id DESC LIMIT 1",
            (int(transfer_id),),
        ).fetchone()
        return _attempt_dict(row)

    def list_attempts(self, transfer_id: int) -> list[dict[str, Any]]:
        return self._list_attempts(int(transfer_id))

    def _insert_attempt(
        self,
        *,
        conn: Any,
        transfer_id: int,
        attempt_no: int,
        credential: str,
        status: str,
        retry_count: int,
        input_snapshot: dict[str, Any],
        migration_state: str,
        missing_items: Sequence[str] | None = None,
        started_at: str | None = None,
        finished_at: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Insert an attempt once.  Returns (attempt, created)."""

        attempt_key = f"attempt:{int(transfer_id)}:{int(attempt_no)}"
        existing = self._get_attempt_by_no(transfer_id, attempt_no)
        if existing is not None:
            return existing, False
        now = _now()
        conn.execute(
            "INSERT OR IGNORE INTO transfer_attempts("
            "attempt_key, transfer_id, attempt_no, credential, status, retry_count, "
            "input_snapshot, missing_items, migration_state, started_at, finished_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                attempt_key,
                int(transfer_id),
                int(attempt_no),
                credential,
                status,
                int(retry_count),
                _json(input_snapshot),
                _json(list(missing_items or [])),
                migration_state,
                started_at or now,
                finished_at,
                now,
            ),
        )
        created = self._get_attempt_by_no(transfer_id, attempt_no)
        if created is None:  # pragma: no cover - defensive
            raise RuntimeError("attempt insert failed")
        return created, True

    def _update_attempt(self, conn: Any, attempt_id: int, **fields: Any) -> dict[str, Any]:
        fields["updated_at"] = _now()
        sets = ", ".join(f"{key} = ?" for key in fields)
        conn.execute(
            f"UPDATE transfer_attempts SET {sets} WHERE id = ?",
            (*fields.values(), int(attempt_id)),
        )
        attempt = self._get_attempt(attempt_id)
        assert attempt is not None
        return attempt

    def _ensure_attempt_history(
        self,
        conn: Any,
        transfer: dict[str, Any],
        continuation: dict[str, Any],
        *,
        current_state: str = "native",
        resume_interrupted: bool = False,
    ) -> tuple[dict[str, Any], int, int]:
        """Materialise attempts ``1..retry_count+1`` for one transfer row.

        ``retry_count`` describes how many retries already happened, so the
        attempt count is always ``retry_count + 1``.  Missing historical
        attempts are backfilled and existing ones are never duplicated.  The
        current attempt keeps the row's retry counter and input snapshot, so a
        storage switch never recalculates them.
        """

        transfer_id = int(transfer.get("id", 0))
        retry_count = int(transfer.get("retry_count", 0) or 0)
        current_no = retry_count + 1
        snapshot = _input_snapshot(transfer)
        current_status = _status_for_row(transfer)
        created = 0
        skipped = 0
        for attempt_no in range(1, current_no + 1):
            is_current = attempt_no == current_no
            status = current_status if is_current else "已失败"
            attempt, was_created = self._insert_attempt(
                conn=conn,
                transfer_id=transfer_id,
                attempt_no=attempt_no,
                credential=continuation["credential"],
                status=status,
                retry_count=retry_count if is_current else attempt_no - 1,
                input_snapshot=snapshot,
                migration_state=current_state if is_current else "migrated",
            )
            if was_created:
                created += 1
                continue
            skipped += 1
            if not is_current:
                continue
            if attempt.get("migration_state") in CONFLICT_STATES and not resume_interrupted:
                continue
            if attempt.get("migration_state") == "resolved_pending":
                # 恢复待确认是值班员研判后的结论，重启同步不得把它改回普通失败。
                self._update_attempt(
                    conn,
                    attempt["id"],
                    retry_count=retry_count,
                    input_snapshot=_json(snapshot),
                )
                continue
            attempt = self._update_attempt(
                conn,
                attempt["id"],
                status=status,
                retry_count=retry_count,
                input_snapshot=_json(snapshot),
                migration_state=(
                    current_state
                    if attempt.get("migration_state") in CONFLICT_STATES
                    else attempt.get("migration_state", current_state)
                ),
            )
        current = self._get_attempt_by_no(transfer_id, current_no)
        assert current is not None
        return current, created, skipped

    def sync_row_attempt(
        self,
        transfer: dict[str, Any],
        *,
        migration_state: str = "native",
        conn: Any = None,
    ) -> dict[str, Any]:
        """Make sure the row's current attempt exists and mirrors the row."""

        transfer_id = int(transfer.get("id", 0))
        continuation = self.ensure_continuation(transfer, conn=conn)
        with self._lock:
            attempt, _, _ = self._ensure_attempt_history(
                conn or self.db.conn, transfer, continuation,
                current_state=migration_state,
            )
            target = conn or self.db.conn
            target.execute(
                "UPDATE continuations SET status = ?, current_attempt_id = ?, updated_at = ? "
                "WHERE continuation_id = ?",
                (str(transfer.get("status") or attempt["status"]), attempt["id"], _now(),
                 continuation["continuation_id"]),
            )
        transfer["接续凭证"] = continuation["credential"]
        transfer["credential"] = continuation["credential"]
        transfer["continuation_id"] = continuation["continuation_id"]
        transfer["尝试次数"] = len(self._list_attempts(transfer_id))
        transfer["attempt_count"] = transfer["尝试次数"]
        transfer["尝试状态"] = attempt["status"]
        if attempt.get("missing_items"):
            transfer["缺项"] = "、".join(attempt["missing_items"])
        return attempt

    def record_retry(
        self,
        transfer: dict[str, Any],
        *,
        conn: Any = None,
    ) -> dict[str, Any]:
        """Register a brand new attempt for a retried row (idempotent)."""

        continuation = self.ensure_continuation(transfer, conn=conn)
        attempt, _, _ = self._ensure_attempt_history(
            conn or self.db.conn, transfer, continuation, current_state="native"
        )
        return attempt

    def _legacy_receipt(
        self,
        conn: Any,
        *,
        transfer: dict[str, Any],
        continuation: dict[str, Any],
        attempt: dict[str, Any],
    ) -> None:
        if _status_for_row(transfer) != "已完成":
            return
        if self._valid_receipt(continuation["continuation_id"]) is not None:
            return
        confirmation_id = f"legacy:{int(transfer.get('id', 0))}:{attempt['attempt_no']}"
        if self._receipt_by_confirmation(confirmation_id) is not None:
            return
        item = str(transfer.get("数据内容", "")).strip()
        receipt_id = self._insert_receipt(
            conn,
            confirmation_id=confirmation_id,
            continuation_id=continuation["continuation_id"],
            credential=continuation["credential"],
            compile_id=int(continuation.get("compile_id") or 0),
            transfer_id=int(transfer.get("id", 0)),
            attempt_id=attempt["id"],
            status="有效",
            outcome="已收",
            received_items=[item] if item else [],
            missing_items=[],
            source="迁移前历史",
        )
        self._supersede_receipts(conn, continuation["continuation_id"], receipt_id)

    # ------------------------------------------------------------------
    # Migration
    # ------------------------------------------------------------------
    def migrate(
        self,
        transfer_rows: Sequence[dict[str, Any]],
        *,
        simulate_failure_after: int | None = None,
        reset: bool = False,
    ) -> dict[str, Any]:
        """Register one attempt per existing transfer row.

        Re-running is safe: the attempt key is unique, so an interrupted
        migration can be retried without duplicating an attempt.
        """

        run_id = uuid.uuid4().hex
        started = _now()
        with self._lock:
            with self.db.transaction() as conn:
                conn.execute(
                    "INSERT INTO migration_runs(run_id, status, detail, started_at) "
                    "VALUES (?, 'running', '{}', ?)",
                    (run_id, started),
                )
                if reset:
                    conn.execute("DELETE FROM receipts")
                    conn.execute("DELETE FROM receipt_requests")
                    conn.execute("DELETE FROM confirmation_conflicts")
                    conn.execute("DELETE FROM transfer_attempts")
                    conn.execute("DELETE FROM continuations")
                created = 0
                skipped = 0
                interrupted = False
                for index, row in enumerate(transfer_rows):
                    if simulate_failure_after is not None and index >= simulate_failure_after:
                        # 中断：把下一条标记为“迁移中”，模拟写库途中进程被杀。
                        # 重启后重试时会沿用同一条尝试，不会重复登记。
                        interrupted = True
                        if simulate_failure_after < len(transfer_rows):
                            pending_row = transfer_rows[simulate_failure_after]
                            continuation = self.ensure_continuation(pending_row, conn=conn)
                            retry_count = int(pending_row.get("retry_count", 0) or 0)
                            self._insert_attempt(
                                conn=conn,
                                transfer_id=int(pending_row.get("id", 0)),
                                attempt_no=retry_count + 1,
                                credential=continuation["credential"],
                                status=_status_for_row(pending_row),
                                retry_count=retry_count,
                                input_snapshot=_input_snapshot(pending_row),
                                migration_state="migrating",
                            )
                        break
                    continuation = self.ensure_continuation(row, conn=conn)
                    retry_count = int(row.get("retry_count", 0) or 0)
                    _, was_created = self._insert_attempt(
                        conn=conn,
                        transfer_id=int(row.get("id", 0)),
                        attempt_no=retry_count + 1,
                        credential=continuation["credential"],
                        status=_status_for_row(row),
                        retry_count=retry_count,
                        input_snapshot=_input_snapshot(row),
                        migration_state="migrated",
                    )
                    if was_created:
                        created += 1
                    else:
                        # 上次中断留下的“迁移中”尝试，本次重试完成后转为已完成迁移。
                        existing = self._get_attempt_by_no(
                            int(row.get("id", 0)), retry_count + 1
                        )
                        if existing is not None and existing.get("migration_state") in (
                            "migrating", "interrupted"
                        ):
                            self._update_attempt(
                                conn, existing["id"], migration_state="migrated"
                            )
                        skipped += 1
                status = "interrupted" if interrupted else "completed"
                detail = _json({
                    "created": created,
                    "duplicates_prevented": skipped,
                    "rows": len(transfer_rows),
                })
                conn.execute(
                    "UPDATE migration_runs SET status = ?, detail = ?, finished_at = ? "
                    "WHERE run_id = ?",
                    (status, detail, _now(), run_id),
                )
        return {
            "run_id": run_id,
            "status": status,
            "created": created,
            "duplicates_prevented": skipped,
            "rows": len(transfer_rows),
        }

    def migration_state(self) -> dict[str, Any]:
        runs = [
            _row_dict(row)
            for row in self.db.conn.execute(
                "SELECT * FROM migration_runs ORDER BY started_at"
            ).fetchall()
        ]
        for run in runs:
            run["detail"] = _loads(run.get("detail"), {})
        pending = self.db.conn.execute(
            "SELECT COUNT(*) AS count FROM transfer_attempts WHERE migration_state IN "
            "('migrating', 'interrupted')"
        ).fetchone()["count"]
        return {"runs": runs, "pending_attempts": pending}

    # ------------------------------------------------------------------
    # Receipt requests
    # ------------------------------------------------------------------
    def _list_requests(
        self,
        *,
        compile_id: int | None = None,
        continuation_id: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if compile_id is not None:
            clauses.append("compile_id = ?")
            params.append(int(compile_id))
        if continuation_id is not None:
            clauses.append("continuation_id = ?")
            params.append(continuation_id)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.db.conn.execute(
            f"SELECT * FROM receipt_requests{where} ORDER BY created_at", params
        ).fetchall()
        return [item for item in (_request_dict(r) for r in rows) if item]

    def list_requests(self, compile_id: int) -> list[dict[str, Any]]:
        return self._list_requests(compile_id=int(compile_id))

    def ensure_request(
        self,
        compile_row: dict[str, Any],
        transfer_row: dict[str, Any],
        *,
        expected_items: Sequence[str] | None = None,
        conn: Any = None,
    ) -> dict[str, Any]:
        compile_id = int(compile_row.get("id", 0))
        transfer_id = int(transfer_row.get("id", 0))
        continuation = self.ensure_continuation(transfer_row, conn=conn, compile_id=compile_id)
        existing = self.db.conn.execute(
            "SELECT * FROM receipt_requests WHERE compile_id = ? AND continuation_id = ?",
            (compile_id, continuation["continuation_id"]),
        ).fetchone()
        if existing is not None:
            return _request_dict(existing) or {}
        request_id = uuid.uuid4().hex
        now = _now()
        items = _unique(expected_items or [])
        (conn or self.db.conn).execute(
            "INSERT INTO receipt_requests(request_id, compile_id, continuation_id, credential, "
            "transfer_id, expected_items, status, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, '待确认', ?, ?)",
            (request_id, compile_id, continuation["continuation_id"],
             continuation["credential"], transfer_id, _json(items), now, now),
        )
        return {
            "request_id": request_id,
            "compile_id": compile_id,
            "continuation_id": continuation["continuation_id"],
            "credential": continuation["credential"],
            "transfer_id": transfer_id,
            "expected_items": items,
            "status": "待确认",
            "created_at": now,
            "updated_at": now,
        }

    # ------------------------------------------------------------------
    # Receipts and confirmations
    # ------------------------------------------------------------------
    def _list_receipts(self, continuation_id: str | None = None) -> list[dict[str, Any]]:
        if continuation_id is None:
            rows = self.db.conn.execute(
                "SELECT * FROM receipts ORDER BY created_at"
            ).fetchall()
        else:
            rows = self.db.conn.execute(
                "SELECT * FROM receipts WHERE continuation_id = ? ORDER BY created_at",
                (continuation_id,),
            ).fetchall()
        return [item for item in (_receipt_dict(r) for r in rows) if item]

    def list_receipts(self, *, continuation_id: str | None = None) -> list[dict[str, Any]]:
        return self._list_receipts(continuation_id)

    def _valid_receipt(self, continuation_id: str) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM receipts WHERE continuation_id = ? AND status = '有效'",
            (continuation_id,),
        ).fetchone()
        return _receipt_dict(row)

    def _receipt_by_confirmation(self, confirmation_id: str) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM receipts WHERE confirmation_id = ?", (confirmation_id,)
        ).fetchone()
        return _receipt_dict(row)

    def _insert_receipt(
        self,
        conn: Any,
        *,
        confirmation_id: str,
        continuation_id: str,
        credential: str,
        compile_id: int,
        transfer_id: int,
        attempt_id: int | None,
        status: str,
        outcome: str,
        received_items: Sequence[str],
        missing_items: Sequence[str],
        source: str,
    ) -> str:
        receipt_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO receipts(receipt_id, confirmation_id, continuation_id, credential, "
            "compile_id, transfer_id, attempt_id, status, outcome, received_items, "
            "missing_items, source, superseded_by, created_at, acked_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)",
            (receipt_id, confirmation_id, continuation_id, credential, compile_id,
             transfer_id, attempt_id, status, outcome, _json(list(received_items)),
             _json(list(missing_items)), source, _now(), _now()),
        )
        return receipt_id

    def _supersede_receipts(self, conn: Any, continuation_id: str, new_receipt_id: str) -> None:
        conn.execute(
            "UPDATE receipts SET status = '已作废', superseded_by = ? "
            "WHERE continuation_id = ? AND status = '有效' AND receipt_id <> ?",
            (new_receipt_id, continuation_id, new_receipt_id),
        )

    def _resolved_conflict_for_confirmation(self, confirmation_id: str) -> dict[str, Any] | None:
        row = self.db.conn.execute(
            "SELECT * FROM confirmation_conflicts WHERE confirmation_id = ? "
            "AND status = '已研判' ORDER BY created_at DESC LIMIT 1",
            (confirmation_id,),
        ).fetchone()
        return _conflict_dict(row)

    def confirm(
        self,
        *,
        transfer_rows: list[dict[str, Any]],
        compile_rows: list[dict[str, Any]],
        transfer_id: int | None = None,
        compile_id: int | None = None,
        credential: str | None = None,
        confirmation_id: str,
        received_items: Sequence[str] | None = None,
        missing_items: Sequence[str] | None = None,
        outcome: str | None = None,
        expected_items: Sequence[str] | None = None,
        source: str = "确认请求",
        attempt_id: int | None = None,
    ) -> dict[str, Any]:
        """Acknowledge one delivered confirmation exactly once.

        The whole state transition (receipt + attempt terminal state +
        continuation + compile row) is committed in a single transaction.
        """

        confirmation_id = str(confirmation_id or "").strip()
        if not confirmation_id:
            return {"ok": False, "message": "确认编号不能为空"}
        received = _unique(received_items or [])
        missing = _unique(missing_items or [])

        with self._lock, self.db.transaction() as conn:
            # 1. Exact duplicate confirmation: acknowledge once only.
            existing = self._receipt_by_confirmation(confirmation_id)
            if existing is not None:
                return {
                    "ok": True,
                    "duplicate": True,
                    "receipt": existing,
                    "message": "同一确认已核收，本次未重复登记",
                }

            # 2. A confirmation that a duty officer already resolved must not
            #    be applied again.  「继续办结」keeps the valid receipt;
            #    「恢复待确认」leaves the old attempt for a later 补传 and must
            #    not be silently flipped back to 已完成 by a re-delivery.
            resolved = self._resolved_conflict_for_confirmation(confirmation_id)
            if resolved is not None:
                decision = resolved.get("decision")
                return {
                    "ok": True,
                    "duplicate": True,
                    "ignored": True,
                    "conflict": resolved,
                    "receipt": self._valid_receipt(resolved["continuation_id"]),
                    "next_action": "补传" if decision == DECISION_RECOVER else "无需补传",
                    "message": f"该确认已按「{decision}」研判，本次未重复核收",
                }

            # 3. Locate the continuation and transport row.
            continuation = None
            if credential:
                continuation = self.find_by_credential(credential)
            if continuation is None and transfer_id is not None:
                continuation = self._get_continuation_by_transfer(int(transfer_id))
            if continuation is None:
                return {"ok": False, "message": "找不到传输接续凭证，无法核收"}
            transfer = self._find_transfer(transfer_rows, continuation["transfer_id"])
            if transfer is None:
                return {"ok": False, "message": "接续凭证对应的传输记录不存在"}
            compile_id = int(compile_id if compile_id is not None else (continuation.get("compile_id") or 0))
            compile_row = self._find_compile(compile_rows, compile_id) if compile_id else None

            attempt = self.current_attempt(continuation["transfer_id"])
            if attempt_id is not None:
                selected = self._get_attempt(int(attempt_id))
                if selected is not None:
                    attempt = selected

            expected = _unique(expected_items or [])
            if not expected:
                request = self._request_for(conn, compile_id, continuation["continuation_id"])
                if request is not None:
                    expected = request.get("expected_items", [])
            if not expected:
                expected = _unique([str(transfer.get("数据内容", ""))])

            # 4. Conflict detection: a valid receipt already exists for this
            #    continuation, or the attempt is still being migrated.
            #
            #    A redelivery of the *same* material with the same conclusion
            #    is harmless: treat it as a duplicate instead of bothering the
            #    duty officer.  Only a genuine disagreement (different outcome
            #    or different missing items) is escalated to 待研判.
            valid = self._valid_receipt(continuation["continuation_id"])
            incoming_outcome = outcome if outcome in ("已收", "缺项", "失败") else (
                "缺项" if missing else "已收"
            )
            incoming_missing = _unique(missing or [])
            reason = None
            if (
                valid is not None
                and valid.get("confirmation_id") != confirmation_id
                and (
                    valid.get("outcome") != incoming_outcome
                    or sorted(valid.get("missing_items", [])) != sorted(incoming_missing)
                )
            ):
                reason = "该资料接续过程已有有效接收确认"
            elif valid is not None and valid.get("confirmation_id") != confirmation_id:
                return {
                    "ok": True,
                    "duplicate": True,
                    "receipt": valid,
                    "message": "该资料接续过程已核收同一结论的确认，本次未重复登记",
                }
            elif attempt is not None and attempt.get("migration_state") in CONFLICT_STATES:
                reason = "确认到达时传输尝试仍在迁移，需值班员研判"
            elif attempt is not None and attempt.get("status") == "已完成" and missing:
                reason = "尝试已办结但本次确认报告缺项"
            elif attempt is not None and attempt.get("status") in ("已失败", "准备失败") and not missing:
                reason = "旧尝试已失败，本次确认与迁移处置冲突"

            if reason:
                conflict = self._create_conflict(
                    conn,
                    confirmation_id=confirmation_id,
                    continuation=continuation,
                    transfer_id=continuation["transfer_id"],
                    compile_id=compile_id or None,
                    attempt_id=attempt["id"] if attempt else None,
                    reason=reason,
                    payload={
                        "received_items": received,
                        "missing_items": missing,
                        "outcome": outcome,
                        "credential": continuation["credential"],
                    },
                )
                if attempt is not None:
                    self._update_attempt(conn, attempt["id"], migration_state="conflict_pending")
                return {
                    "ok": True,
                    "status": "待研判",
                    "conflict": conflict,
                    "receipt": None,
                    "message": "确认与正在迁移/已办结的尝试冲突，已转值班员研判",
                }

            # 5. Apply: derive the outcome, write receipt + attempt + compile
            #    in one transaction.
            if outcome not in ("已收", "缺项", "失败"):
                outcome = "缺项" if missing else "已收"
            effective_received = received or ([] if missing else expected)
            if outcome == "已收" and missing:
                outcome = "缺项"
            if outcome == "已收":
                effective_missing: list[str] = []
                effective_received = _unique(list(effective_received) + list(expected))
            else:
                effective_missing = missing or [item for item in expected if item not in effective_received]
                if not effective_missing:
                    effective_missing = expected
                outcome = "失败" if outcome == "失败" else "缺项"

            receipt_id = self._insert_receipt(
                conn,
                confirmation_id=confirmation_id,
                continuation_id=continuation["continuation_id"],
                credential=continuation["credential"],
                compile_id=compile_id,
                transfer_id=continuation["transfer_id"],
                attempt_id=attempt["id"] if attempt else None,
                status="有效",
                outcome=outcome,
                received_items=effective_received,
                missing_items=effective_missing,
                source=source,
            )
            self._supersede_receipts(conn, continuation["continuation_id"], receipt_id)
            receipt = self._receipt_by_confirmation(confirmation_id)

            new_attempt = attempt
            if attempt is None:
                # 确认先于迁移到达：先给当前记录登记尝试，再落到同一个终态，
                # 避免出现“有收件、无尝试终态”的单边状态。
                attempt = self.sync_row_attempt(transfer, conn=conn)
            if attempt is not None:
                terminal = "已完成" if outcome == "已收" else (
                    "准备失败" if attempt.get("status") == "准备失败" and outcome != "已收" else "已失败"
                )
                new_attempt = self._update_attempt(
                    conn,
                    attempt["id"],
                    status=terminal,
                    missing_items=_json(effective_missing),
                    input_snapshot=_json(_input_snapshot(transfer)),
                    migration_state="resolved",
                    finished_at=_now(),
                )
            continuation_status = "已完成" if outcome == "已收" else "已失败"
            conn.execute(
                "UPDATE continuations SET status = ?, updated_at = ? WHERE continuation_id = ?",
                (continuation_status, _now(), continuation["continuation_id"]),
            )

            updated_transfer = dict(transfer)
            updated_transfer["接续凭证"] = continuation["credential"]
            updated_transfer["credential"] = continuation["credential"]
            updated_transfer["continuation_id"] = continuation["continuation_id"]
            updated_transfer["尝试状态"] = new_attempt["status"] if new_attempt else ""
            updated_transfer["缺项"] = "、".join(effective_missing)
            updated_transfer["received_items"] = effective_received
            updated_transfer["missing_items"] = effective_missing
            for item in effective_received:
                if item and item not in str(updated_transfer.get("备注说明", "")):
                    updated_transfer["接收资料"] = _unique(
                        [*(updated_transfer.get("接收资料") or []), item]
                    )
            if outcome == "已收":
                updated_transfer["status"] = "已完成"
                updated_transfer["pending"] = False
                updated_transfer["abnormal"] = False
            else:
                updated_transfer["status"] = "已失败"
                updated_transfer["pending"] = False
                updated_transfer["abnormal"] = True
            updated_transfer["尝试次数"] = len(self._list_attempts(continuation["transfer_id"]))
            updated_transfer["attempt_count"] = updated_transfer["尝试次数"]
            self._persist_rows(conn, TRANSFER_MODULE, transfer_rows)

            updated_compile = dict(compile_row) if compile_row else None
            if updated_compile is not None:
                self._apply_compile_outcome(
                    conn,
                    updated_compile,
                    received=effective_received,
                    missing=effective_missing,
                    outcome=outcome,
                    receipt=receipt,
                    credential=continuation["credential"],
                )
                self._persist_rows(conn, COMPILE_MODULE, compile_rows)
            request = self._request_for(conn, compile_id, continuation["continuation_id"])
            if request is not None:
                status = "已收件" if outcome == "已收" else "需补传"
                conn.execute(
                    "UPDATE receipt_requests SET status = ?, updated_at = ? WHERE request_id = ?",
                    (status, _now(), request["request_id"]),
                )

        # End of transaction: mirror committed values back into memory.
        transfer.clear()
        transfer.update(updated_transfer)
        if updated_compile is not None and compile_row is not None:
            compile_row.clear()
            compile_row.update(updated_compile)
        return {
            "ok": True,
            "duplicate": False,
            "receipt": receipt,
            "attempt": new_attempt,
            "transfer": updated_transfer,
            "compile": updated_compile,
            "message": "收件确认已核收" if outcome == "已收" else "收件确认已登记缺项，已退回传输记录",
        }

    def _request_for(self, conn: Any, compile_id: int, continuation_id: str) -> dict[str, Any] | None:
        if not compile_id:
            return None
        row = conn.execute(
            "SELECT * FROM receipt_requests WHERE compile_id = ? AND continuation_id = ?",
            (int(compile_id), continuation_id),
        ).fetchone()
        return _request_dict(row)

    def _apply_compile_outcome(
        self,
        conn: Any,
        compile_row: dict[str, Any],
        *,
        received: Sequence[str],
        missing: Sequence[str],
        outcome: str,
        receipt: dict[str, Any] | None,
        credential: str,
    ) -> None:
        current = str(compile_row.get("数据完整度", ""))
        for item in received:
            if item and item not in current:
                current = f"{current} | 已接收：{item}" if current else f"已接收：{item}"
        compile_row["数据完整度"] = current
        compile_row["last_transfer_time"] = _now()
        compile_row["接续凭证"] = credential
        compile_row["credential"] = credential
        if receipt:
            history = compile_row.get("receipt_ids") or []
            if isinstance(history, str):
                history = [history]
            history.append(receipt["receipt_id"])
            compile_row["receipt_ids"] = history
        if outcome == "已收":
            compile_row["transfer_complete"] = True
            compile_row["missing_items"] = []
            if compile_row.get("status") == "需补充":
                compile_row["status"] = "汇编中"
                compile_row["pending"] = True
        else:
            compile_row["transfer_complete"] = False
            compile_row["missing_items"] = list(missing)
            compile_row["缺项"] = "、".join(missing)
            compile_row["status"] = "需补充"
            compile_row["pending"] = True
            compile_row["abnormal"] = True
        # Keep the persisted row copy current for callers.
        compile_row["updated_at"] = _now()

    # ------------------------------------------------------------------
    # Conflicts
    # ------------------------------------------------------------------
    def _create_conflict(
        self,
        conn: Any,
        *,
        confirmation_id: str,
        continuation: dict[str, Any],
        transfer_id: int,
        compile_id: int | None,
        attempt_id: int | None,
        reason: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        existing = conn.execute(
            "SELECT * FROM confirmation_conflicts WHERE confirmation_id = ? "
            "AND status = '待研判'",
            (confirmation_id,),
        ).fetchone()
        if existing is not None:
            return _conflict_dict(existing) or {}
        conflict_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO confirmation_conflicts(conflict_id, confirmation_id, continuation_id, "
            "transfer_id, compile_id, attempt_id, reason, incoming_payload, status, decision, "
            "decided_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '待研判', NULL, NULL, ?)",
            (conflict_id, confirmation_id, continuation["continuation_id"], transfer_id,
             compile_id, attempt_id, reason, _json(payload), _now()),
        )
        row = conn.execute(
            "SELECT * FROM confirmation_conflicts WHERE conflict_id = ?", (conflict_id,)
        ).fetchone()
        return _conflict_dict(row) or {}

    def list_conflicts(
        self,
        *,
        transfer_id: int | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if transfer_id is not None:
            clauses.append("transfer_id = ?")
            params.append(int(transfer_id))
        if status is not None:
            clauses.append("status = ?")
            params.append(status)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.db.conn.execute(
            f"SELECT * FROM confirmation_conflicts{where} ORDER BY created_at", params
        ).fetchall()
        return [item for item in (_conflict_dict(r) for r in rows) if item]

    def resolve_conflict(
        self,
        *,
        transfer_rows: list[dict[str, Any]],
        compile_rows: list[dict[str, Any]],
        conflict_id: str,
        decision: str,
    ) -> dict[str, Any]:
        if decision not in (DECISION_KEEP, DECISION_RECOVER):
            return {"ok": False, "message": "研判结论只能是「继续办结」或「恢复待确认」"}
        with self._lock, self.db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM confirmation_conflicts WHERE conflict_id = ?", (conflict_id,)
            ).fetchone()
            conflict = _conflict_dict(row)
            if conflict is None:
                return {"ok": False, "message": "冲突记录不存在"}
            if conflict.get("status") == "已研判":
                return {"ok": True, "duplicate": True, "conflict": conflict,
                        "message": "该冲突已完成研判"}
            continuation = self._get_continuation(conflict["continuation_id"])
            if continuation is None:
                return {"ok": False, "message": "接续过程不存在"}
            transfer = self._find_transfer(transfer_rows, int(conflict["transfer_id"]))
            if transfer is None:
                return {"ok": False, "message": "传输记录不存在"}
            attempt = self.current_attempt(int(conflict["transfer_id"]))
            payload = conflict.get("incoming_payload", {}) or {}
            received = _unique(payload.get("received_items") or [])
            missing = _unique(payload.get("missing_items") or [])
            compile_id = int(conflict.get("compile_id") or continuation.get("compile_id") or 0)
            compile_row = self._find_compile(compile_rows, compile_id) if compile_id else None

            now = _now()
            if decision == DECISION_KEEP:
                # The old attempt continues to completion.  A valid receipt is
                # written from that attempt so receipt history and terminal
                # state agree.
                if attempt is not None:
                    attempt = self._update_attempt(
                        conn, attempt["id"], status="已完成", missing_items="[]",
                        migration_state="resolved_complete", finished_at=now,
                    )
                conn.execute(
                    "UPDATE continuations SET status = '已完成', updated_at = ? "
                    "WHERE continuation_id = ?",
                    (now, continuation["continuation_id"]),
                )
                valid = self._valid_receipt(continuation["continuation_id"])
                if valid is None:
                    receipt_id = self._insert_receipt(
                        conn,
                        confirmation_id=f"legacy:{conflict['conflict_id']}",
                        continuation_id=continuation["continuation_id"],
                        credential=continuation["credential"],
                        compile_id=compile_id,
                        transfer_id=int(conflict["transfer_id"]),
                        attempt_id=attempt["id"] if attempt else None,
                        status="有效",
                        outcome="已收",
                        received_items=received or [str(transfer.get("数据内容", ""))],
                        missing_items=[],
                        source="继续办结",
                    )
                    self._supersede_receipts(conn, continuation["continuation_id"], receipt_id)
                next_action = "无需补传"
            else:
                # The old attempt cannot be trusted; keep it recoverable and
                # hand the original missing items back to the transport record.
                return_missing = missing or [str(transfer.get("数据内容", ""))]
                if attempt is None:
                    attempt = self.sync_row_attempt(transfer, conn=conn)
                if attempt is not None:
                    attempt = self._update_attempt(
                        conn, attempt["id"], status="恢复待确认",
                        missing_items=_json(return_missing),
                        migration_state="resolved_pending",
                    )
                conn.execute(
                    "UPDATE continuations SET status = '恢复待确认', updated_at = ? "
                    "WHERE continuation_id = ?",
                    (now, continuation["continuation_id"]),
                )
                next_action = "补传"

            conn.execute(
                "UPDATE confirmation_conflicts SET status = '已研判', decision = ?, decided_at = ? "
                "WHERE conflict_id = ?",
                (decision, now, conflict_id),
            )
            updated_transfer = dict(transfer)
            updated_transfer["尝试状态"] = attempt["status"] if attempt else ""
            recovered_missing = (attempt.get("missing_items") if attempt else None) or missing
            updated_transfer["缺项"] = "、".join(recovered_missing)
            updated_transfer["last_decision"] = decision
            updated_transfer["next_action"] = next_action
            if decision == DECISION_KEEP:
                updated_transfer["status"] = "已完成"
                updated_transfer["pending"] = False
                updated_transfer["abnormal"] = False
            else:
                updated_transfer["status"] = "已失败"
                updated_transfer["pending"] = False
                updated_transfer["abnormal"] = True
            self._persist_rows(conn, TRANSFER_MODULE, transfer_rows)

            updated_compile = dict(compile_row) if compile_row else None
            if updated_compile is not None:
                if decision == DECISION_KEEP:
                    self._apply_compile_outcome(
                        conn, updated_compile, received=received,
                        missing=[], outcome="已收", receipt=None,
                        credential=continuation["credential"],
                    )
                else:
                    self._apply_compile_outcome(
                        conn, updated_compile, received=received,
                        missing=missing or [str(transfer.get("数据内容", ""))],
                        outcome="缺项", receipt=None,
                        credential=continuation["credential"],
                    )
                self._persist_rows(conn, COMPILE_MODULE, compile_rows)
            request = self._request_for(conn, compile_id, continuation["continuation_id"])
            if request is not None:
                conn.execute(
                    "UPDATE receipt_requests SET status = ?, updated_at = ? WHERE request_id = ?",
                    ("已收件" if decision == DECISION_KEEP else "需补传", now,
                     request["request_id"]),
                )
            resolved = _conflict_dict(conn.execute(
                "SELECT * FROM confirmation_conflicts WHERE conflict_id = ?", (conflict_id,)
            ).fetchone())

        transfer.clear()
        transfer.update(updated_transfer)
        if updated_compile is not None and compile_row is not None:
            compile_row.clear()
            compile_row.update(updated_compile)
        return {
            "ok": True,
            "conflict": resolved,
            "attempt": attempt,
            "next_action": next_action,
            "message": f"冲突已按「{decision}」研判",
        }

    # ------------------------------------------------------------------
    # Recovery report and consistency reconciliation
    # ------------------------------------------------------------------
    def reconcile(
        self,
        transfer_rows: list[dict[str, Any]],
        compile_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Repair one-sided terminal states left by an interrupted restart.

        A valid receipt is authoritative: a received receipt forces its attempt
        to ``已完成``; a missing/failed receipt forces the attempt to a failure
        terminal state and returns the missing items to the compile report.
        """

        fixed: list[dict[str, Any]] = []
        with self._lock, self.db.transaction() as conn:
            receipts = self._list_receipts()
            for receipt in receipts:
                if receipt["status"] != "有效":
                    continue
                continuation = self._get_continuation(receipt["continuation_id"])
                if continuation is None:
                    continue
                transfer = self._find_transfer(transfer_rows, int(receipt["transfer_id"]))
                attempt = self.current_attempt(int(receipt["transfer_id"]))
                if transfer is None or attempt is None:
                    continue
                desired = "已完成" if receipt["outcome"] == "已收" else (
                    "准备失败" if attempt.get("status") == "准备失败" else "已失败"
                )
                changed = False
                if attempt.get("status") != desired:
                    attempt = self._update_attempt(
                        conn, attempt["id"], status=desired,
                        missing_items=_json(receipt.get("missing_items", [])),
                        migration_state="reconciled",
                        finished_at=attempt.get("finished_at") or _now(),
                    )
                    changed = True
                continuation_status = "已完成" if receipt["outcome"] == "已收" else "已失败"
                if continuation.get("status") != continuation_status:
                    conn.execute(
                        "UPDATE continuations SET status = ?, updated_at = ? "
                        "WHERE continuation_id = ?",
                        (continuation_status, _now(), continuation["continuation_id"]),
                    )
                    changed = True
                compile_row = self._find_compile(compile_rows, int(receipt.get("compile_id") or 0))
                if compile_row is not None:
                    if receipt["outcome"] == "已收" and not compile_row.get("transfer_complete"):
                        current = str(compile_row.get("数据完整度", ""))
                        for item in receipt.get("received_items", []):
                            if item not in current:
                                current = f"{current} | 已接收：{item}" if current else f"已接收：{item}"
                        compile_row["数据完整度"] = current
                        compile_row["transfer_complete"] = True
                        compile_row["missing_items"] = []
                        compile_row.pop("缺项", None)
                        changed = True
                    elif receipt["outcome"] != "已收" and not compile_row.get("missing_items"):
                        compile_row["missing_items"] = receipt.get("missing_items", [])
                        compile_row["缺项"] = "、".join(receipt.get("missing_items", []))
                        compile_row["transfer_complete"] = False
                        changed = True
                if changed:
                    fixed.append({
                        "receipt_id": receipt["receipt_id"],
                        "continuation_id": receipt["continuation_id"],
                        "transfer_id": receipt["transfer_id"],
                        "attempt_status": desired,
                        "outcome": receipt["outcome"],
                    })
            if fixed:
                self._persist_rows(conn, TRANSFER_MODULE, transfer_rows)
                self._persist_rows(conn, COMPILE_MODULE, compile_rows)
        return {"ok": True, "fixed": fixed, "count": len(fixed)}

    def report(
        self,
        transfer_rows: list[dict[str, Any]],
        compile_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        transfers: list[dict[str, Any]] = []
        all_received: list[str] = []
        all_missing: list[str] = []
        next_upload: list[dict[str, Any]] = []
        for transfer in transfer_rows:
            transfer_id = int(transfer.get("id", 0))
            continuation = self._get_continuation_by_transfer(transfer_id)
            attempts = self._list_attempts(transfer_id)
            receipts = self._list_receipts(continuation["continuation_id"]) if continuation else []
            conflicts = self.list_conflicts(transfer_id=transfer_id)
            pending_conflicts = [c for c in conflicts if c["status"] == "待研判"]
            valid = next((r for r in receipts if r["status"] == "有效"), None)
            received = _unique([item for r in receipts for item in r.get("received_items", [])])
            missing = _unique([item for r in receipts if r["status"] == "有效"
                               for item in r.get("missing_items", [])])
            latest = attempts[-1] if attempts else None
            # 尚未登记尝试的记录，按“原尝试次数 = retry_count + 1”汇报。
            attempt_count = len(attempts) if attempts else int(transfer.get("retry_count", 0) or 0) + 1
            if pending_conflicts:
                action = "研判冲突"
            elif missing or (latest and latest.get("status") in ("已失败", "准备失败", "恢复待确认")):
                action = "补传"
            elif latest and latest.get("status") in ("待传输", "传输中"):
                action = "等待传输"
            else:
                action = "无需补传"
            item = {
                "transfer_id": transfer_id,
                "数据内容": transfer.get("数据内容", ""),
                "status": transfer.get("status", ""),
                "credential": continuation["credential"] if continuation else transfer.get("接续凭证"),
                "continuation_id": continuation["continuation_id"] if continuation else None,
                "attempt_count": attempt_count,
                "retry_count": int(transfer.get("retry_count", 0) or 0),
                "attempts": attempts,
                "receipts": receipts,
                "valid_receipt_id": valid["receipt_id"] if valid else None,
                "conflicts": conflicts,
                "pending_conflicts": pending_conflicts,
                "received_items": received,
                "missing_items": missing,
                "next_action": action,
            }
            transfers.append(item)
            all_received.extend(received)
            all_missing.extend(missing)
            if action == "补传":
                next_upload.append({
                    "transfer_id": transfer_id,
                    "credential": item["credential"],
                    "missing_items": missing or [transfer.get("数据内容", "")],
                    "reason": "确认缺项" if missing else "上次尝试未办结",
                })
        migration = self.migration_state()
        return {
            "ok": True,
            "generated_at": _now(),
            "transfers": transfers,
            "original_attempt_counts": {t["transfer_id"]: t["attempt_count"] for t in transfers},
            "received_items": _unique(all_received),
            "missing_items": _unique(all_missing),
            "pending_conflicts": [
                c for t in transfers for c in t["pending_conflicts"]
            ],
            "next_upload": next_upload,
            "migration": migration,
            "duplicates_prevented": sum(
                int(run.get("detail", {}).get("duplicates_prevented", 0))
                for run in migration["runs"]
            ),
        }

    # ------------------------------------------------------------------
    # Compile replay after restart
    # ------------------------------------------------------------------
    def compile_recovery(self, compile_id: int) -> dict[str, Any]:
        requests = self.list_requests(compile_id)
        receipts = []
        for request in requests:
            receipts.extend(
                r for r in self._list_receipts(request["continuation_id"])
                if r.get("compile_id") == int(compile_id)
            )
        return {
            "compile_id": int(compile_id),
            "requests": requests,
            "receipts": receipts,
            "received_items": _unique(
                item for r in receipts if r["status"] == "有效"
                for item in r.get("received_items", [])
            ),
            "missing_items": _unique(
                item for r in receipts if r["status"] == "有效"
                for item in r.get("missing_items", [])
            ),
            "pending_conflicts": [
                c for c in self.list_conflicts()
                if c.get("compile_id") == int(compile_id) and c["status"] == "待研判"
            ],
        }
