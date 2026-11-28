"""数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

绝大多数演示模块仍然只依赖进程内存。数据传输与成果汇编的“传输尝试”和
“汇编收件”属于必须跨重启保留的运行状态，统一交给 SQLite：

* ``data_transfer`` / ``results_compile`` 两个模块的行快照落到 ``module_rows``；
* 接续凭证、尝试、收件确认、冲突、迁移记录由 :class:`app.transfer_ledger.TransferLedger`
  维护。

这样进程重启后，值班员仍能凭“传输接续凭证”找回原来的尝试并继续办理收件，
已有的重试次数与输入字段也不会因为切换存储而被重算。
"""
from __future__ import annotations

import threading
import time
from typing import Any

from app.db import Database
from app.seed import SEED_ROWS
from app.transfer_ledger import (
    COMPILE_MODULE,
    PERSISTED_MODULES,
    TRANSFER_MODULE,
    TransferLedger,
)


class Store:
    def __init__(self, db_path: str | None = None) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {}
        self._idempotent_keys: set[str] = set()
        self._locks: dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()
        # 回滚日志：{transaction_id: {module, entry_id, before_snapshot}}
        self._rollback_log: dict[str, dict] = {}
        # 重试配置
        self._retry_max: int = 3
        self._retry_delay: float = 0.01
        self.db = Database(db_path)
        self.ledger = TransferLedger(self.db)
        self._seed(clear_runtime=False)

    # ------------------------------------------------------------------
    # 初始化 / 重置
    # ------------------------------------------------------------------
    def _seed(self, *, clear_runtime: bool) -> None:
        self._tables = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self._rollback_log.clear()
        if clear_runtime:
            self.db.clear_runtime()
            self._idempotent_keys.clear()
        else:
            self._idempotent_keys = set(self.db.load_idempotent_keys())
        self._load_persisted_rows(clear_runtime=clear_runtime)
        self._bootstrap_ledger()

    def _load_persisted_rows(self, *, clear_runtime: bool) -> None:
        """用 SQLite 中的行覆盖种子；库里没有记录时才写入种子。"""

        for module in PERSISTED_MODULES:
            rows = [] if clear_runtime else self.db.load_module_rows(module)
            if rows:
                self._tables[module] = rows
            else:
                self.db.replace_module_rows(module, self._tables.get(module, []))

    def _bootstrap_ledger(self) -> None:
        """补齐接续凭证，并给“已完成”的历史记录补一份有效收件。

        未办结的尝试不在这里预建，等首次动作 / 迁移 / 核收时再登记，这样
        “迁移失败后重试”是真正可观察、可核对是否重复的。
        """

        with self.db.transaction() as conn:
            for row in self.rows(TRANSFER_MODULE):
                continuation = self.ledger.ensure_continuation(row, conn=conn)
                if str(row.get("status")) == "已完成":
                    attempt = self.ledger.sync_row_attempt(row, conn=conn)
                    self.ledger._legacy_receipt(
                        conn, transfer=row, continuation=continuation, attempt=attempt
                    )
            self.db.replace_module_rows(TRANSFER_MODULE, self.rows(TRANSFER_MODULE), conn=conn)
            self.db.replace_module_rows(COMPILE_MODULE, self.rows(COMPILE_MODULE), conn=conn)

    def reset(self) -> None:
        """重置所有数据回到初始种子状态（同时清空 SQLite 运行状态）。"""

        self._seed(clear_runtime=True)
        self._locks.clear()

    def rebind(self, db_path: str | None = None) -> None:
        """切换到另一个 SQLite 数据库并重新装载状态。

        测试里用它模拟“进程重启后指向同一数据库”的情形：先关闭旧连接，
        再用新的 :class:`Database` 装载已提交的行、尝试与收件。
        """

        try:
            self.db.close()
        except Exception:
            pass
        self.db = Database(db_path)
        self.ledger = TransferLedger(self.db)
        self._locks.clear()
        self._seed(clear_runtime=False)

    # ------------------------------------------------------------------
    # 基础访问
    # ------------------------------------------------------------------
    def _get_lock(self, key: str) -> threading.Lock:
        with self._global_lock:
            if key not in self._locks:
                self._locks[key] = threading.Lock()
            return self._locks[key]

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def persist_module(self, module: str) -> None:
        """把内存中的模块快照落库（供服务层在动作完成后调用）。

        传输记录落库前先补齐接续凭证，保证任何写入口都不会留下“没有凭证
        的传输记录”。
        """

        if module not in PERSISTED_MODULES:
            return
        if module == TRANSFER_MODULE:
            with self.db.transaction() as conn:
                for row in self.rows(module):
                    row.setdefault("retry_count", 0)
                    continuation = self.ledger.ensure_continuation(row, conn=conn)
                    row["接续凭证"] = continuation["credential"]
                    row["credential"] = continuation["credential"]
                    row["continuation_id"] = continuation["continuation_id"]
                    # 已完成但没有收件历史（例如外部写入的种子行）补一份，
                    # 保证“已有收件历史可以核对”。
                    if str(row.get("status")) == "已完成":
                        attempt = self.ledger.sync_row_attempt(row, conn=conn)
                        self.ledger._legacy_receipt(
                            conn, transfer=row, continuation=continuation, attempt=attempt
                        )
                self.db.replace_module_rows(module, self.rows(module), conn=conn)
            return
        self.db.replace_module_rows(module, self.rows(module))

    def atomic_update(self, module: str, entry_id: int, updates: dict[str, Any]) -> dict[str, Any] | None:
        """原子更新一条记录，返回更新后的记录或None。"""
        lock = self._get_lock(f"{module}:{entry_id}")
        with lock:
            row = self.find(module, entry_id)
            if row is None:
                return None
            row.update(updates)
            self.persist_module(module)
            return dict(row)

    def atomic_append(self, module: str, entry: dict[str, Any]) -> dict[str, Any]:
        """原子追加一条记录。"""
        lock = self._get_lock(f"{module}:append")
        with lock:
            rows = self.rows(module)
            entry_copy = dict(entry)
            rows.append(entry_copy)
            self.persist_module(module)
            return entry_copy

    def is_idempotent(self, key: str) -> bool:
        """检查幂等键是否已存在（不写入）。"""
        with self._global_lock:
            return key in self._idempotent_keys

    def mark_idempotent(self, key: str) -> None:
        """标记幂等键为已执行；持久化模块的键同时落库以跨重启生效。"""
        with self._global_lock:
            self._idempotent_keys.add(key)
        if any(key.startswith(f"{module}:") for module in PERSISTED_MODULES):
            self.db.save_idempotent_key(key)

    def clear_idempotent_key(self, key: str) -> None:
        """清除幂等键（用于失败回滚）。"""
        with self._global_lock:
            self._idempotent_keys.discard(key)
        self.db.delete_idempotent_key(key)

    def find_by(self, module: str, field: str, value: Any) -> list[dict[str, Any]]:
        """按字段值查找记录。"""
        rows = self.rows(module)
        if value is None:
            return [row for row in rows if row.get(field) is None]
        return [row for row in rows if str(row.get(field, "")) == str(value)]

    def find_one_by(self, module: str, field: str, value: Any) -> dict[str, Any] | None:
        """按字段值查找第一条记录。"""
        results = self.find_by(module, field, value)
        return results[0] if results else None

    def snapshot(self, module: str) -> list[dict[str, Any]]:
        """返回模块的当前快照（浅拷贝）。"""
        return [dict(row) for row in self.rows(module)]

    def save_snapshot(self, transaction_id: str, module: str, entry_id: int) -> str:
        """保存操作前的快照，支持失败后回滚。"""
        row = self.find(module, entry_id)
        key = f"{module}:{entry_id}"
        if row is None:
            return transaction_id
        with self._global_lock:
            self._rollback_log[transaction_id] = {
                "module": module,
                "entry_id": entry_id,
                "before": dict(row),
            }
        return transaction_id

    def rollback(self, transaction_id: str) -> bool:
        """回滚到指定事务前的状态。"""
        with self._global_lock:
            log = self._rollback_log.pop(transaction_id, None)
            if log is None:
                return False
            row = self.find(log["module"], log["entry_id"])
            if row is not None:
                row.clear()
                row.update(log["before"])
            self.persist_module(log["module"])
        return True

    def retry(self, func, *args, max_retries: int | None = None, **kwargs) -> tuple[Any, bool]:
        """带重试机制执行操作。返回(结果, 是否成功)。"""
        retries = max_retries if max_retries is not None else self._retry_max
        last_error = None
        for attempt in range(retries):
            try:
                result = func(*args, **kwargs)
                return result, True
            except Exception as e:
                last_error = e
                if attempt < retries - 1:
                    time.sleep(self._retry_delay * (attempt + 1))
        return None, False

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
