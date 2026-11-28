"""数据传输领域服务。

传输记录的动作接口保持不变（``发起传输`` / ``确认完成`` / ``重试传输``），
但“传输尝试”和关联的“汇编收件确认”全部落在 SQLite：

* 每条传输记录都有一份稳定的“传输接续凭证”，进程重启后仍可按凭证找回；
* 每次重试登记一个新的尝试，``retry_count`` 和输入快照原样保留，不重算；
* 确认完成时在同一事务里办结尝试并核收成果汇编的收件确认，两边不会一边
  办结、另一边永远缺件。
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.transfer_ledger import (
    COMPILE_MODULE,
    TRANSFER_MODULE,
    _status_for_row,
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class DataTransferService(BaseModuleService):
    MODULE = TRANSFER_MODULE
    ENTITY = "传输记录"
    OPERATION_TERM = "发起传输"
    LIST_FIELDS = ["数据内容", "数据量", "传输方式", "目标系统", "传输状态", "操作人", "传输时间", "备注说明"]
    REQUIRED_FIELDS = ["数据内容", "数据量", "传输方式"]
    STATUS_ORDER = ["待传输", "传输中", "已完成", "已失败"]
    ACTION_RULES = {"发起传输": "传输中", "确认完成": "已完成", "重试传输": "待传输"}
    NEGATIVE_ACTIONS = []

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def decorate(self, entry: dict[str, Any]) -> dict[str, Any]:
        """在对外返回的记录上补充接续凭证和尝试概况。"""

        return store.ledger.decorate(dict(entry))

    def list_entries(self, **kwargs) -> tuple[list[dict[str, Any]], int]:
        rows, total = super().list_entries(**kwargs)
        return [self.decorate(dict(row)) for row in rows], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        return self.decorate(entry)

    # ------------------------------------------------------------------
    # 登记
    # ------------------------------------------------------------------
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing
        requested_status = str(values.get("status") or values.get("传输状态") or "").strip()
        if requested_status in self.STATUS_ORDER:
            entry["status"] = requested_status
            entry["pending"] = requested_status in ("待传输", "传输中")
            entry["尝试状态"] = _status_for_row(entry)
        if str(values.get("传输状态") or "").strip():
            entry["传输状态"] = values["传输状态"]
        try:
            entry["retry_count"] = max(int(values.get("retry_count") or 0), 0)
        except (TypeError, ValueError):
            entry["retry_count"] = 0
        continuation = store.ledger.ensure_continuation(entry)
        entry["接续凭证"] = continuation["credential"]
        entry["credential"] = continuation["credential"]
        store.ledger.sync_row_attempt(entry)
        store.persist_module(self.MODULE)
        return self.decorate(entry), []

    # ------------------------------------------------------------------
    # 动作
    # ------------------------------------------------------------------
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        idempotent_key = f"{self.MODULE}:{entry_id}:{action}"
        if store.is_idempotent(idempotent_key):
            return None, f"动作「{action}」已执行，请勿重复操作"

        if action == "重试传输" and entry.get("status") != "已失败":
            return None, "只能重试已失败的传输"

        tx_id = f"{self.MODULE}:{entry_id}:{action}:tx"
        store.save_snapshot(tx_id, self.MODULE, entry_id)

        old_status = entry.get("status")
        entry["status"] = target
        entry["pending"] = target in ("待传输", "传输中")
        entry["abnormal"] = False
        entry["传输状态"] = target

        with store.db.transaction() as conn:
            if action == "重试传输":
                # 每次重试都是一个新的尝试；retry_count 来自原值递增，
                # 输入字段沿用当前记录，不重新生成。
                entry["retry_count"] = int(entry.get("retry_count", 0) or 0) + 1
                entry["last_retry_time"] = _now()
                attempt = store.ledger.record_retry(entry, conn=conn)
            else:
                attempt = store.ledger.sync_row_attempt(entry, conn=conn)
            store.ledger.decorate(entry)
            entry["尝试状态"] = attempt["status"]
            store.db.replace_module_rows(self.MODULE, store.rows(self.MODULE), conn=conn)

        store.persist_module(self.MODULE)
        store.mark_idempotent(idempotent_key)

        # 传输完成时联动成果汇编的收件确认（同一事务内已落库）
        if action == "确认完成":
            self._update_results_completeness(entry)

        return entry, f"{self.ENTITY}已{action}"

    # ------------------------------------------------------------------
    # 接续凭证 / 尝试 / 确认 / 冲突 / 迁移
    # ------------------------------------------------------------------
    def continuation(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        return store.ledger.get_continuation(entry_id)

    def by_credential(self, credential: str) -> dict[str, Any] | None:
        continuation = store.ledger.find_by_credential(credential)
        if continuation is None:
            return None
        entry = store.find(self.MODULE, int(continuation["transfer_id"]))
        result = {"continuation": continuation}
        if entry is not None:
            result["entry"] = self.decorate(entry)
        result["attempts"] = store.ledger.list_attempts(int(continuation["transfer_id"]))
        result["receipts"] = store.ledger.list_receipts(
            continuation_id=continuation["continuation_id"]
        )
        return result

    def attempts(self, entry_id: int) -> list[dict[str, Any]]:
        return store.ledger.list_attempts(entry_id)

    def confirm(
        self,
        *,
        credential: str | None = None,
        transfer_id: int | None = None,
        compile_id: int | None = None,
        confirmation_id: str | None = None,
        received_items: list[str] | None = None,
        missing_items: list[str] | None = None,
        outcome: str | None = None,
        expected_items: list[str] | None = None,
        attempt_id: int | None = None,
    ) -> dict[str, Any]:
        """核收一次汇编确认请求；同一确认实例只核收一次。"""

        confirmation_id = confirmation_id or f"CF-{uuid.uuid4().hex[:12].upper()}"
        result = store.ledger.confirm(
            transfer_rows=store.rows(self.MODULE),
            compile_rows=store.rows(COMPILE_MODULE),
            transfer_id=transfer_id,
            compile_id=compile_id,
            credential=credential,
            confirmation_id=confirmation_id,
            received_items=received_items,
            missing_items=missing_items,
            outcome=outcome,
            expected_items=expected_items,
            attempt_id=attempt_id,
        )
        store.persist_module(self.MODULE)
        store.persist_module(COMPILE_MODULE)
        result.setdefault("ok", False)
        return result

    def conflicts(self, *, status: str | None = None) -> list[dict[str, Any]]:
        return store.ledger.list_conflicts(status=status)

    def resolve_conflict(
        self,
        *,
        conflict_id: str,
        decision: str,
    ) -> dict[str, Any]:
        result = store.ledger.resolve_conflict(
            transfer_rows=store.rows(self.MODULE),
            compile_rows=store.rows(COMPILE_MODULE),
            conflict_id=conflict_id,
            decision=decision,
        )
        store.persist_module(self.MODULE)
        store.persist_module(COMPILE_MODULE)
        return result

    def migrate(self, *, simulate_failure_after: int | None = None) -> dict[str, Any]:
        """把现有传输行登记为尝试（可中断、可重试、不重复）。"""

        result = store.ledger.migrate(
            store.rows(self.MODULE),
            simulate_failure_after=simulate_failure_after,
        )
        store.persist_module(self.MODULE)
        return result

    def recovery_report(self) -> dict[str, Any]:
        return store.ledger.report(store.rows(self.MODULE), store.rows(COMPILE_MODULE))

    def reconcile(self) -> dict[str, Any]:
        result = store.ledger.reconcile(store.rows(self.MODULE), store.rows(COMPILE_MODULE))
        store.persist_module(self.MODULE)
        store.persist_module(COMPILE_MODULE)
        return result

    # ------------------------------------------------------------------
    # 兼容原有联动：确认完成后更新汇编数据完整度
    # ------------------------------------------------------------------
    def _update_results_completeness(self, entry: dict[str, Any]) -> None:
        """数据传输完成后更新成果汇编中对应的数据完整度。

        若成果汇编已有关联的收件确认请求，则优先走确认核收；否则沿用原有
        “数据完整度追加”的联动行为，保证既有页面与测试仍能工作。
        """

        transfer_id = int(entry.get("id", 0))
        data_content = str(entry.get("数据内容", ""))
        continuation = store.ledger.find_by_credential(
            str(entry.get("接续凭证") or entry.get("credential") or "")
        )
        compile_id = int(continuation.get("compile_id") or 0) if continuation else 0

        if compile_id:
            result = store.ledger.confirm(
                transfer_rows=store.rows(self.MODULE),
                compile_rows=store.rows(COMPILE_MODULE),
                transfer_id=transfer_id,
                compile_id=compile_id,
                confirmation_id=f"complete:{self.MODULE}:{transfer_id}:{entry.get('retry_count', 0)}",
                received_items=[data_content] if data_content else [],
                outcome="已收",
                source="确认完成",
            )
            store.persist_module(COMPILE_MODULE)
            entry["compile_updates"] = 1 if result.get("ok") else 0
            entry.update(result.get("transfer") or {})
            return

        results_rows = store.rows(COMPILE_MODULE)
        updated_count = 0
        for row in results_rows:
            if row.get("status") in ("汇编中", "已提交", "需补充"):
                current = row.get("数据完整度", "")
                if data_content not in str(current):
                    row["数据完整度"] = f"{current} | 已接收：{data_content}"
                    row["pending"] = True
                    row["transfer_complete"] = True
                    row["last_transfer_time"] = entry.get("传输时间", "")
                    updated_count += 1
        entry["compile_updates"] = updated_count
        store.persist_module(COMPILE_MODULE)
