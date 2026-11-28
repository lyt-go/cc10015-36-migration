"""成果汇编领域服务。

汇编报告是“汇编收件”的落点：确认请求在 ``results_compile`` 上创建，但真正
的接收确认、尝试终态和接续凭证由 :mod:`app.transfer_ledger` 持久化。进程
重启后仍可按凭证继续办理收件；失败时把原缺项退回仍负责该资料的传输记录。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.transfer_ledger import COMPILE_MODULE, TRANSFER_MODULE


class ResultsCompileService(BaseModuleService):
    MODULE = COMPILE_MODULE
    ENTITY = "汇编报告"
    OPERATION_TERM = "提交汇编"
    LIST_FIELDS = ["航次编号", "成果类别", "主要发现", "参与人员", "数据完整度", "汇编人", "完成日期", "备注说明"]
    REQUIRED_FIELDS = ["航次编号", "成果类别", "主要发现"]
    STATUS_ORDER = ["汇编中", "已提交", "已审核", "需补充"]
    ACTION_RULES = {"提交汇编": "已提交", "审核通过": "已审核", "退回补充": "需补充"}
    NEGATIVE_ACTIONS = ["退回补充"]

    def decorate(self, entry: dict[str, Any]) -> dict[str, Any]:
        row = dict(entry)
        requests = store.ledger.list_requests(int(entry.get("id", 0)))
        if requests:
            row["收件确认"] = requests
            row["pending_receipts"] = sum(1 for r in requests if r["status"] == "待确认")
        return row

    def list_entries(self, **kwargs) -> tuple[list[dict[str, Any]], int]:
        rows, total = super().list_entries(**kwargs)
        return [self.decorate(dict(row)) for row in rows], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(self.MODULE, entry_id)
        return self.decorate(entry) if entry is not None else None

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        entry, missing = super().create_entry(values)
        if entry is not None:
            store.persist_module(self.MODULE)
        return entry, missing

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

        old_status = entry.get("status")
        entry["status"] = target
        entry["pending"] = target in ("汇编中", "需补充")
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS

        store.mark_idempotent(idempotent_key)
        store.persist_module(self.MODULE)

        return entry, f"{self.ENTITY}已{action}"

    # ------------------------------------------------------------------
    # 汇编收件确认请求
    # ------------------------------------------------------------------
    def request_receipt(
        self,
        entry_id: int,
        *,
        transfer_id: int,
        expected_items: list[str] | None = None,
    ) -> dict[str, Any]:
        """为汇编报告创建（或复用）一条接收确认请求，关联传输接续凭证。"""

        compile_row = store.find(self.MODULE, entry_id)
        if compile_row is None:
            return {"ok": False, "message": f"{self.ENTITY} {entry_id} 不存在或已归档"}
        transfer_row = store.find(TRANSFER_MODULE, transfer_id)
        if transfer_row is None:
            return {"ok": False, "message": f"传输记录 {transfer_id} 不存在或已归档"}

        with store.db.transaction() as conn:
            request = store.ledger.ensure_request(
                compile_row,
                transfer_row,
                expected_items=expected_items,
                conn=conn,
            )
            store.db.replace_module_rows(TRANSFER_MODULE, store.rows(TRANSFER_MODULE), conn=conn)
            store.db.replace_module_rows(self.MODULE, store.rows(self.MODULE), conn=conn)
        store.persist_module(self.MODULE)
        store.persist_module(TRANSFER_MODULE)
        return {"ok": True, "request": request, "message": "收件确认请求已登记"}

    def confirm_receipt(
        self,
        entry_id: int,
        *,
        credential: str | None = None,
        transfer_id: int | None = None,
        confirmation_id: str | None = None,
        received_items: list[str] | None = None,
        missing_items: list[str] | None = None,
        outcome: str | None = None,
        expected_items: list[str] | None = None,
    ) -> dict[str, Any]:
        compile_row = store.find(self.MODULE, entry_id)
        if compile_row is None:
            return {"ok": False, "message": f"{self.ENTITY} {entry_id} 不存在或已归档"}
        result = store.ledger.confirm(
            transfer_rows=store.rows(TRANSFER_MODULE),
            compile_rows=store.rows(self.MODULE),
            compile_id=entry_id,
            transfer_id=transfer_id,
            credential=credential,
            confirmation_id=confirmation_id,
            received_items=received_items,
            missing_items=missing_items,
            outcome=outcome,
            expected_items=expected_items,
            source="汇编确认请求",
        )
        store.persist_module(self.MODULE)
        store.persist_module(TRANSFER_MODULE)
        return result

    def receipt_requests(self, entry_id: int) -> dict[str, Any]:
        compile_row = store.find(self.MODULE, entry_id)
        if compile_row is None:
            return {"ok": False, "message": f"{self.ENTITY} {entry_id} 不存在或已归档"}
        return {
            "ok": True,
            "requests": store.ledger.list_requests(entry_id),
            "recovery": store.ledger.compile_recovery(entry_id),
        }

    def recovery(self, entry_id: int) -> dict[str, Any]:
        compile_row = store.find(self.MODULE, entry_id)
        if compile_row is None:
            return {"ok": False, "message": f"{self.ENTITY} {entry_id} 不存在或已归档"}
        return {"ok": True, **store.ledger.compile_recovery(entry_id)}

    # ------------------------------------------------------------------
    # 兼容原有联动
    # ------------------------------------------------------------------
    def update_completeness(self, entry_id: int, data_source: str, data_desc: str) -> tuple[dict[str, Any] | None, str]:
        """由数据传输模块联动更新数据完整度。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"
        current = entry.get("数据完整度", "")
        entry["数据完整度"] = f"{current} | [{data_source}] {data_desc}"
        entry["pending"] = True
        store.persist_module(self.MODULE)
        return entry, "数据完整度已更新"

    def get_completeness_summary(self) -> dict[str, Any]:
        """获取整体数据完整度摘要。"""
        rows = store.rows(self.MODULE)
        total = len(rows)
        complete = sum(1 for r in rows if r.get("transfer_complete"))
        return {
            "total_reports": total,
            "complete_transfers": complete,
            "completeness_pct": round(complete / total * 100, 1) if total > 0 else 0,
        }
