# -*- coding: utf-8 -*-
"""传输接续凭证与汇编收件的 SQLite 持久化测试。

覆盖：
* 切换存储后原有动作接口不变，待传输 / 传输中 / 已失败的记录仍可列出与打开；
* 重试次数与输入字段不因切换被重算；
* 进程重启后凭接续凭证继续办理收件；
* 同一确认重复送达只核收一次，迁移中断重试不重复登记尝试；
* 冲突研判（继续办结 / 恢复待确认）与收件/尝试终态一致；
* 一个接续过程任一时点只有一份有效接收确认。
"""
from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from app import store as store_module
from app.store import Store

BACKEND = Path(__file__).resolve().parents[1]


def make_store(tmp_path: Path) -> Store:
    """把全局 store 重新绑定到独立的 SQLite 文件并清空种子。"""

    store = store_module.store
    store.rebind(str(tmp_path / "ledger.sqlite3"))
    return store


def fresh_store(store: Store, path: Path) -> Store:
    """模拟一次进程重启：重新连接同一个 SQLite 文件。"""

    store.rebind(str(path))
    return store


def seed_three_packages(store: Store) -> None:
    """温盐包（准备失败）、航迹包（已完成）、水深包（在途）。"""

    # 模拟“切换前的进程内存”：清掉 SQLite 运行态，直接写入旧行再落库。
    store.db.clear_runtime()
    store._idempotent_keys.clear()
    rows = store.rows("data_transfer")
    rows.clear()
    rows.extend([
        {"id": 1, "status": "已失败", "pending": False, "abnormal": True,
         "数据内容": "温盐包", "数据量": "2GB", "传输方式": "卫星", "目标系统": "岸端",
         "传输状态": "准备失败", "操作人": "值班员甲", "传输时间": "2026-11-20",
         "备注说明": "温盐包准备失败", "retry_count": 2, "失败阶段": "准备阶段失败",
         "尝试状态": "准备失败"},
        {"id": 2, "status": "已完成", "pending": False, "abnormal": False,
         "数据内容": "航迹包", "数据量": "1GB", "传输方式": "卫星", "目标系统": "岸端",
         "传输状态": "已完成", "操作人": "值班员乙", "传输时间": "2026-11-21",
         "备注说明": "航迹包已完成", "retry_count": 0},
        {"id": 3, "status": "传输中", "pending": True, "abnormal": False,
         "数据内容": "水深包", "数据量": "3GB", "传输方式": "网络", "目标系统": "岸端",
         "传输状态": "传输中", "操作人": "值班员丙", "传输时间": "2026-11-22",
         "备注说明": "水深包在途", "retry_count": 1},
    ])
    store.persist_module("data_transfer")


# ----------------------------------------------------------------------
# 切换存储：常用录入仍能打开，字段不重算
# ----------------------------------------------------------------------
def test_common_rows_open_and_fields_preserved(tmp_path):
    store = make_store(tmp_path)
    seed_three_packages(store)
    data_transfer = store._tables["data_transfer"]

    from app.services.data_transfer import DataTransferService
    svc = DataTransferService()

    items, total = svc.list_entries(page=1, size=10)
    assert total == 3
    by_content = {row["数据内容"]: row for row in items}
    assert by_content["温盐包"]["status"] == "已失败"
    assert by_content["水深包"]["status"] == "传输中"

    # 打开每条记录都不改变重试次数与输入字段
    for row in data_transfer:
        before = dict(row)
        opened = svc.get_entry(row["id"])
        assert opened["retry_count"] == before["retry_count"]
        for field in ("数据内容", "数据量", "传输方式", "目标系统", "操作人", "备注说明"):
            assert opened[field] == before[field]
        # 凭证是稳定的，不随打开动作变化
        assert opened["接续凭证"] == before["接续凭证"]


def test_completed_row_keeps_attempt_and_history(tmp_path):
    store = make_store(tmp_path)
    seed_three_packages(store)
    from app.services.data_transfer import DataTransferService

    svc = DataTransferService()
    entry = svc.get_entry(2)
    assert entry["尝试次数"] == 1
    receipts = store.ledger.list_receipts(continuation_id=entry["continuation_id"])
    assert any(r["outcome"] == "已收" for r in receipts)


# ----------------------------------------------------------------------
# 动作接口不变 + 重试登记新尝试
# ----------------------------------------------------------------------
def test_existing_actions_still_work(tmp_path):
    store = make_store(tmp_path)
    store._tables["data_transfer"].clear()
    store._tables["data_transfer"].extend([
        {"id": 1, "status": "待传输", "pending": True, "abnormal": False,
         "数据内容": "X", "数据量": "1", "传输方式": "网络", "目标系统": "岸端"},
    ])
    store.persist_module("data_transfer")
    from app.services.data_transfer import DataTransferService

    svc = DataTransferService()
    entry, msg = svc.run_action(1, "发起传输")
    assert entry is not None, msg
    assert entry["status"] == "传输中"
    entry, msg = svc.run_action(1, "确认完成")
    assert entry is not None, msg
    assert entry["status"] == "已完成"


def test_retry_increments_count_and_preserves_inputs(tmp_path):
    store = make_store(tmp_path)
    seed_three_packages(store)
    from app.services.data_transfer import DataTransferService

    svc = DataTransferService()
    row = store.find("data_transfer", 1)
    before_inputs = {k: row.get(k) for k in ("数据内容", "数据量", "传输方式", "目标系统")}
    attempt_count_before = len(store.ledger.list_attempts(1))

    entry, msg = svc.run_action(1, "重试传输")
    assert entry is not None, msg
    assert entry["retry_count"] == 3
    assert entry["status"] == "待传输"
    # 原有 retry_count=2 表示已发生过两次重试，本次是第 4 次尝试；
    # 历史尝试会被补齐，但不会重复登记。
    attempts = store.ledger.list_attempts(1)
    assert len(attempts) == 4
    assert [a["attempt_no"] for a in attempts] == [1, 2, 3, 4]
    assert attempts[-1]["retry_count"] == 3
    for key, value in before_inputs.items():
        assert entry[key] == value

    # 重复动作被拒绝，且不会多登记一次尝试
    entry2, msg2 = svc.run_action(1, "重试传输")
    assert entry2 is None
    assert "重复" in msg2


# ----------------------------------------------------------------------
# 重启后凭凭证继续收件
# ----------------------------------------------------------------------
def test_restart_then_continue_receipt_by_credential(tmp_path):
    db_path = str(tmp_path / "restart.sqlite3")

    first = fresh_store(store_module.store, Path(db_path))
    seed_three_packages(first)
    credential = first.find("data_transfer", 3)["接续凭证"]
    # 加一个待确认的收件请求
    from app.services.results_compile import ResultsCompileService
    ResultsCompileService().request_receipt(1, transfer_id=3, expected_items=["水深包"])

    # 模拟新进程：重新打开同一个数据库
    second = fresh_store(store_module.store, Path(db_path))
    from app.services.data_transfer import DataTransferService

    found = DataTransferService().by_credential(credential)
    assert found is not None
    assert found["entry"]["数据内容"] == "水深包"
    assert found["entry"]["retry_count"] == 1

    from app.services.results_compile import ResultsCompileService as RCS
    result = RCS().confirm_receipt(
        1, credential=credential, confirmation_id="CF-RESTART-1",
        received_items=["水深包"], outcome="已收",
    )
    assert result["ok"] is True
    compile_row = second.find("results_compile", 1)
    assert compile_row["transfer_complete"] is True
    attempt = second.ledger.current_attempt(3)
    assert attempt["status"] == "已完成"


# ----------------------------------------------------------------------
# 重复确认只核收一次
# ----------------------------------------------------------------------
def test_duplicate_confirmation_acknowledged_once(tmp_path):
    store = make_store(tmp_path)
    seed_three_packages(store)
    from app.services.results_compile import ResultsCompileService

    svc = ResultsCompileService()
    svc.request_receipt(1, transfer_id=3, expected_items=["水深包"])

    first = svc.confirm_receipt(1, credential=store.find("data_transfer", 3)["接续凭证"],
                                confirmation_id="CF-DUP", received_items=["水深包"], outcome="已收")
    assert first["ok"] and not first["duplicate"]
    second = svc.confirm_receipt(1, credential=store.find("data_transfer", 3)["接续凭证"],
                                 confirmation_id="CF-DUP", received_items=["水深包"], outcome="已收")
    assert second["ok"] and second["duplicate"]
    receipts = store.ledger.list_receipts(
        continuation_id=store.find("data_transfer", 3)["continuation_id"]
    )
    assert len(receipts) == 1


def test_duplicate_confirmation_after_restart(tmp_path):
    db_path = str(tmp_path / "dup.sqlite3")
    first = fresh_store(store_module.store, Path(db_path))
    seed_three_packages(first)
    from app.services.results_compile import ResultsCompileService as RCS

    RCS().request_receipt(1, transfer_id=3, expected_items=["水深包"])
    credential = first.find("data_transfer", 3)["接续凭证"]
    RCS().confirm_receipt(1, credential=credential, confirmation_id="CF-SAME",
                          received_items=["水深包"], outcome="已收")

    second = fresh_store(store_module.store, Path(db_path))
    from app.services.results_compile import ResultsCompileService

    result = ResultsCompileService().confirm_receipt(
        1, credential=credential, confirmation_id="CF-SAME",
        received_items=["水深包"], outcome="已收",
    )
    assert result["ok"] and result["duplicate"]
    assert len(second.ledger.list_receipts(
        continuation_id=second.find("data_transfer", 3)["continuation_id"])) == 1


# ----------------------------------------------------------------------
# 迁移中断后重试不重复登记
# ----------------------------------------------------------------------
def test_migration_interrupted_then_retried_no_duplicates(tmp_path):
    db_path = str(tmp_path / "migrate.sqlite3")
    store = fresh_store(store_module.store, Path(db_path))
    seed_three_packages(store)
    from app.services.data_transfer import DataTransferService

    svc = DataTransferService()
    first = svc.migrate(simulate_failure_after=2)
    assert first["status"] == "interrupted"

    # 复查：已登记两条，第三条停在“迁移中”，重试前未登记成功
    assert len(store.ledger.list_attempts(1)) == 1
    assert len(store.ledger.list_attempts(2)) == 1
    attempts3 = store.ledger.list_attempts(3)
    assert len(attempts3) == 1
    assert attempts3[0]["migration_state"] == "migrating"

    # 重启后再次执行迁移：已登记的尝试不重复
    restarted = fresh_store(store_module.store, Path(db_path))
    from app.services.data_transfer import DataTransferService as DTS

    second = DTS().migrate()
    assert second["status"] == "completed"
    # 三条记录都已在第一次（或中断时的“迁移中”）登记过，重试只做续办，不新增。
    assert second["duplicates_prevented"] == 3
    assert second["created"] == 0
    assert len(restarted.ledger.list_attempts(1)) == 1
    assert len(restarted.ledger.list_attempts(2)) == 1
    assert len(restarted.ledger.list_attempts(3)) == 1
    assert restarted.ledger.list_attempts(3)[0]["retry_count"] == 1


# ----------------------------------------------------------------------
# 冲突研判
# ----------------------------------------------------------------------
def _conflicting_store(tmp_path):
    store = make_store(tmp_path)
    seed_three_packages(store)
    from app.services.results_compile import ResultsCompileService
    svc = ResultsCompileService()
    svc.request_receipt(1, transfer_id=3, expected_items=["水深包"])
    # 让水深包的当前尝试停留在“迁移中”
    attempt = store.ledger.sync_row_attempt(store.find("data_transfer", 3))
    store.db.conn.execute(
        "UPDATE transfer_attempts SET migration_state = 'migrating' WHERE id = ?",
        (attempt["id"],),
    )
    store.db.conn.commit()
    return store, svc


def test_conflict_keep_completion_links_receipt_and_attempt(tmp_path):
    store, svc = _conflicting_store(tmp_path)
    # 旧进程与迁移尝试冲突 -> 进入待研判
    result = svc.confirm_receipt(1, transfer_id=3, confirmation_id="CF-CONFLICT",
                                 received_items=["水深包"], outcome="已收")
    assert result["status"] == "待研判"
    conflict = result["conflict"]
    assert conflict["status"] == "待研判"

    resolved = svc.request_receipt(1, transfer_id=3)  # noqa: F841  (确保请求仍在)
    from app.services.data_transfer import DataTransferService

    decision = DataTransferService().resolve_conflict(
        conflict_id=conflict["conflict_id"], decision="继续办结")
    assert decision["ok"] is True
    assert decision["attempt"]["status"] == "已完成"
    assert store.find("results_compile", 1)["transfer_complete"] is True
    receipts = store.ledger.list_receipts(
        continuation_id=store.find("data_transfer", 3)["continuation_id"])
    valid = [r for r in receipts if r["status"] == "有效"]
    assert len(valid) == 1

    # 同一确认再次送达，按「继续办结」研判后不再重复核收
    replay = svc.confirm_receipt(1, transfer_id=3, confirmation_id="CF-CONFLICT",
                                 received_items=["水深包"], outcome="已收")
    assert replay.get("duplicate") or replay.get("ignored")


def test_conflict_recover_returns_missing_items_to_transfer(tmp_path):
    store, svc = _conflicting_store(tmp_path)
    result = svc.confirm_receipt(1, transfer_id=3, confirmation_id="CF-CONFLICT-2",
                                 missing_items=["水深包-缺"], outcome="缺项")
    assert result["status"] == "待研判"
    conflict = result["conflict"]

    from app.services.data_transfer import DataTransferService

    decision = DataTransferService().resolve_conflict(
        conflict_id=conflict["conflict_id"], decision="恢复待确认")
    assert decision["ok"] is True
    assert decision["attempt"]["status"] == "恢复待确认"
    transfer = store.find("data_transfer", 3)
    assert transfer["缺项"] == "水深包-缺"
    assert decision["next_action"] == "补传"
    # 汇编报告记录缺项，下一次补传据此决定
    assert store.find("results_compile", 1)["missing_items"] == ["水深包-缺"]

    # 同一确认在研判后再送达，仍按「恢复待确认」处理，不会被翻成已完成
    replay = svc.confirm_receipt(1, transfer_id=3, confirmation_id="CF-CONFLICT-2",
                                 missing_items=["水深包-缺"], outcome="缺项")
    assert replay.get("duplicate") and replay.get("ignored")
    assert store.ledger.current_attempt(3)["status"] == "恢复待确认"


# ----------------------------------------------------------------------
# 一个接续过程只有一份有效接收确认
# ----------------------------------------------------------------------
def test_only_one_valid_receipt_per_continuation(tmp_path):
    store = make_store(tmp_path)
    seed_three_packages(store)
    from app.services.results_compile import ResultsCompileService

    svc = ResultsCompileService()
    svc.request_receipt(1, transfer_id=3, expected_items=["水深包"])

    first = svc.confirm_receipt(1, transfer_id=3, confirmation_id="CF-A",
                                received_items=["水深包"], outcome="已收")
    assert first["ok"] and not first["duplicate"]
    continuation_id = store.find("data_transfer", 3)["continuation_id"]

    second = svc.confirm_receipt(1, transfer_id=3, confirmation_id="CF-B",
                                 received_items=["水深包"], outcome="已收")
    # 同一接续过程已有有效确认 -> 不产生第二份有效确认
    receipts = store.ledger.list_receipts(continuation_id=continuation_id)
    valid = [r for r in receipts if r["status"] == "有效"]
    assert len(valid) == 1
    assert second.get("duplicate") or second.get("status") == "待研判"


# ----------------------------------------------------------------------
# 情境演练：三个包，切换与重启之间两次确认水深包
# ----------------------------------------------------------------------
def test_scenario_three_packages_double_confirm_across_restart(tmp_path):
    db_path = str(tmp_path / "scenario.sqlite3")
    first = fresh_store(store_module.store, Path(db_path))
    seed_three_packages(first)
    from app.services.results_compile import ResultsCompileService as RCS

    RCS().request_receipt(1, transfer_id=3, expected_items=["水深包"])
    credential = first.find("data_transfer", 3)["接续凭证"]

    # 第一次确认（切换前）
    res1 = RCS().confirm_receipt(1, credential=credential, confirmation_id="CF-SHUI-1",
                                 received_items=["水深包"], outcome="已收")
    assert res1["ok"] and not res1["duplicate"]

    # 重启后同一确认再次送达
    second = fresh_store(store_module.store, Path(db_path))
    res2 = RCS().confirm_receipt(1, credential=credential, confirmation_id="CF-SHUI-1",
                                 received_items=["水深包"], outcome="已收")
    assert res2["ok"] and res2["duplicate"]

    # 复查原尝试次数与已收资料
    report = second.ledger.report(second.rows("data_transfer"), second.rows("results_compile"))
    by_id = {t["transfer_id"]: t for t in report["transfers"]}
    assert by_id[1]["retry_count"] == 2
    assert by_id[3]["attempt_count"] == 2
    assert "水深包" in report["received_items"]
    assert report["duplicates_prevented"] == 0  # 这里没有迁移，测试迁移在别的用例

    receipts = second.ledger.list_receipts(
        continuation_id=second.find("data_transfer", 3)["continuation_id"])
    assert len(receipts) == 1


# ----------------------------------------------------------------------
# 恢复一致性：不得一边办结、另一边永远缺件
# ----------------------------------------------------------------------
def test_reconcile_repairs_one_sided_state(tmp_path):
    store = make_store(tmp_path)
    seed_three_packages(store)
    from app.services.results_compile import ResultsCompileService

    svc = ResultsCompileService()
    svc.request_receipt(1, transfer_id=3, expected_items=["水深包"])
    credential = store.find("data_transfer", 3)["接续凭证"]
    svc.confirm_receipt(1, credential=credential, confirmation_id="CF-FIX",
                        received_items=["水深包"], outcome="已收")

    # 人为制造“汇编已办结、尝试却仍是传输中”的半边状态
    attempt = store.ledger.current_attempt(3)
    store.db.conn.execute(
        "UPDATE transfer_attempts SET status = '传输中' WHERE id = ?", (attempt["id"],))
    store.db.conn.commit()

    result = store.ledger.reconcile(store.rows("data_transfer"), store.rows("results_compile"))
    assert result["count"] >= 1
    assert store.ledger.current_attempt(3)["status"] == "已完成"


def test_report_reports_attempts_and_received(tmp_path):
    store = make_store(tmp_path)
    seed_three_packages(store)
    from app.services.results_compile import ResultsCompileService

    ResultsCompileService().request_receipt(1, transfer_id=3, expected_items=["水深包"])
    ResultsCompileService().confirm_receipt(
        1, transfer_id=3, confirmation_id="CF-RPT",
        received_items=["水深包"], outcome="已收")
    report = store.ledger.report(store.rows("data_transfer"), store.rows("results_compile"))
    # 温盐包原有 retry_count=2，原尝试次数应为 3（首次 + 两次重试），不重算。
    assert report["original_attempt_counts"][1] == 3
    assert "水深包" in report["received_items"]
    assert any(t["transfer_id"] == 2 for t in report["transfers"])


# ----------------------------------------------------------------------
# 跨进程重启（真实子进程）
# ----------------------------------------------------------------------
def _run(code: str, db_path: Path) -> str:
    script = textwrap.dedent(
        f"""
        import json, sys
        sys.path.insert(0, {str(BACKEND)!r})
        from app.store import Store
        store = Store(db_path={str(db_path)!r})

        """
    ) + textwrap.dedent(code)
    result = subprocess.run(
        [sys.executable, "-I", "-c", script],
        cwd=str(BACKEND), capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def test_real_process_restart_keeps_credential_and_retry(tmp_path):
    db_path = tmp_path / "process.sqlite3"
    _run(
        """
rows = store.rows('data_transfer')
rows.clear()
rows.append({'id': 1, 'status': '已失败', 'pending': False, 'abnormal': True,
             '数据内容': '温盐包', '数据量': '2GB', '传输方式': '卫星', '目标系统': '岸端',
             'retry_count': 2, '失败阶段': '准备阶段失败'})
store.persist_module('data_transfer')
store.mark_idempotent('data_transfer:1:重试传输')
print(store.find('data_transfer', 1)['接续凭证'])
""",
        db_path,
    )

    output = _run(
        """
row = store.find('data_transfer', 1)
print(json.dumps({'credential': row['接续凭证'], 'retry': row['retry_count'],
                  'content': row['数据内容'],
                  'idempotent': store.is_idempotent('data_transfer:1:重试传输')},
                 ensure_ascii=False))
""",
        db_path,
    )
    import json

    payload = json.loads(output)
    assert payload["credential"].startswith("TC-")
    assert payload["retry"] == 2
    assert payload["content"] == "温盐包"
    assert payload["idempotent"] is True
