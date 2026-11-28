"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。
新增跨模块联动快照接口。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for
from app.store import store

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


# ----------------------------------------------------------------------
# 传输接续 / 汇编收件（SQLite 持久化）
# 这些固定路径必须注册在 /{module} 之前。
# ----------------------------------------------------------------------
@router.get("/transfer/recovery-report")
def transfer_recovery_report() -> dict[str, Any]:
    """重启后值班员查看的接续复查报告：原尝试次数、已收资料、待补传。"""

    service = require_service("data_transfer")
    return service.recovery_report()


@router.post("/transfer/recovery-report")
def transfer_recovery_report_post() -> dict[str, Any]:
    return transfer_recovery_report()


@router.get("/transfer/conflicts")
def transfer_conflicts(status: str | None = Query(default=None)) -> dict[str, Any]:
    service = require_service("data_transfer")
    conflicts = service.conflicts(status=status)
    return {"ok": True, "conflicts": conflicts, "total": len(conflicts)}


@router.post("/transfer/conflicts/{conflict_id}/resolve")
def transfer_resolve_conflict(conflict_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    decision = str(payload.get("decision") or "").strip()
    service = require_service("data_transfer")
    return service.resolve_conflict(conflict_id=conflict_id, decision=decision)


@router.post("/transfer/migrate")
def transfer_migrate(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    simulate = payload.get("simulate_failure_after")
    service = require_service("data_transfer")
    return service.migrate(
        simulate_failure_after=int(simulate) if simulate is not None else None,
    )


@router.post("/transfer/reconcile")
def transfer_reconcile() -> dict[str, Any]:
    service = require_service("data_transfer")
    return service.reconcile()


@router.post("/transfer/confirmations")
def transfer_confirm(payload: dict[str, Any]) -> dict[str, Any]:
    """核收一次汇编确认请求（可凭传输接续凭证）。"""

    service = require_service("data_transfer")
    return service.confirm(
        credential=_optional_text(payload.get("credential")),
        transfer_id=_optional_int(payload.get("transfer_id")),
        compile_id=_optional_int(payload.get("compile_id")),
        confirmation_id=_optional_text(payload.get("confirmation_id")),
        received_items=_items(payload.get("received_items")),
        missing_items=_items(payload.get("missing_items")),
        outcome=_optional_text(payload.get("outcome")),
        expected_items=_items(payload.get("expected_items")),
        attempt_id=_optional_int(payload.get("attempt_id")),
    )


@router.get("/transfer/credentials/{credential}")
def transfer_by_credential(credential: str) -> dict[str, Any]:
    service = require_service("data_transfer")
    result = service.by_credential(credential)
    if result is None:
        raise HTTPException(status_code=404, detail=f"传输接续凭证 {credential} 不存在")
    return {"ok": True, **result}


@router.post("/results_compile/{entry_id}/receipts")
def compile_request_receipt(entry_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    service = require_service("results_compile")
    transfer_id = _optional_int(payload.get("transfer_id"))
    if transfer_id is None:
        return {"ok": False, "message": "必须提供关联的 transfer_id"}
    return service.request_receipt(
        entry_id,
        transfer_id=transfer_id,
        expected_items=_items(payload.get("expected_items")),
    )


@router.get("/results_compile/{entry_id}/receipts")
def compile_receipts(entry_id: int) -> dict[str, Any]:
    service = require_service("results_compile")
    return service.receipt_requests(entry_id)


@router.get("/results_compile/{entry_id}/recovery")
def compile_recovery(entry_id: int) -> dict[str, Any]:
    service = require_service("results_compile")
    return service.recovery(entry_id)


@router.post("/results_compile/{entry_id}/confirmations")
def compile_confirm(entry_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    service = require_service("results_compile")
    return service.confirm_receipt(
        entry_id,
        credential=_optional_text(payload.get("credential")),
        transfer_id=_optional_int(payload.get("transfer_id")),
        confirmation_id=_optional_text(payload.get("confirmation_id")),
        received_items=_items(payload.get("received_items")),
        missing_items=_items(payload.get("missing_items")),
        outcome=_optional_text(payload.get("outcome")),
        expected_items=_items(payload.get("expected_items")),
    )


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _items(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, (list, tuple)):
        return [str(part).strip() for part in value if str(part).strip()]
    return None


@router.get("/snapshot/{module}")
def module_snapshot(module: str) -> dict[str, Any]:
    """返回模块跨域快照：本模块数据 + 关联模块状态。"""
    service = require_service(module)
    base = service.domain_snapshot()
    
    # 跨模块关联数据
    cross: dict[str, Any] = {}
    if module == "station_deploy":
        cruise_plan_rows = store.rows("cruise_plan")
        approved = any(r.get("status") == "已批准" for r in cruise_plan_rows)
        weather_rows = store.rows("weather_obs")
        severe = any(r.get("status") == "恶劣海况" for r in weather_rows)
        cross["cruise_plan_approved"] = approved
        cross["severe_weather"] = severe
    elif module == "sampling_record":
        registry_rows = store.rows("sample_registry")
        checked_in = sum(1 for r in registry_rows if r.get("status") == "已入库")
        cross["checked_in_samples"] = checked_in
        cross["total_registry_samples"] = len(registry_rows)
    elif module == "instrument_calib":
        equip_rows = store.rows("equipment_manifest")
        disabled = sum(1 for r in equip_rows if r.get("calib_failed"))
        cross["disabled_equipment"] = disabled
        cross["total_equipment"] = len(equip_rows)
    elif module == "data_transfer":
        compile_rows = store.rows("results_compile")
        complete = sum(1 for r in compile_rows if r.get("transfer_complete"))
        cross["compile_complete_count"] = complete
        cross["compile_total"] = len(compile_rows)
    elif module == "hydro_measure":
        qc_rows = store.rows("data_qc")
        pending_qc = sum(1 for r in qc_rows if r.get("triggered_by") == "hydro_measure" and r.get("status") == "待质控")
        cross["pending_qc_count"] = pending_qc
    elif module == "diving_ops":
        crew_rows = store.rows("crew_schedule")
        unavailable = sum(1 for r in crew_rows if r.get("diving_aborted"))
        cross["unavailable_crew"] = unavailable
    elif module == "weather_obs":
        station_rows = store.rows("station_deploy")
        alerted = sum(1 for r in station_rows if r.get("weather_alert"))
        cross["alerted_stations"] = alerted
    elif module == "analysis_request":
        pending_approval = sum(1 for r in store.rows("analysis_request") if r.get("status") == "已提交")
        cross["pending_approval_count"] = pending_approval
    elif module == "equipment_manifest":
        calib_rows = store.rows("instrument_calib")
        failed_calibs = sum(1 for r in calib_rows if r.get("status") == "不合格")
        cross["failed_calibrations"] = failed_calibs
    elif module == "crew_schedule":
        diving_rows = store.rows("diving_ops")
        aborted_dives = sum(1 for r in diving_rows if r.get("status") == "已中止")
        cross["aborted_dives"] = aborted_dives
    elif module == "bio_trawl":
        bio_samples = sum(1 for r in store.rows("sample_registry") if r.get("synced_from") == "bio_trawl")
        cross["bio_samples"] = bio_samples
        equip_rows = store.rows("equipment_manifest")
        cross["total_equipment"] = len(equip_rows)
    elif module == "sediment_sample":
        sedi_registry = sum(1 for r in store.rows("sample_registry") if r.get("样品类型") == "沉积物")
        cross["sediment_registry"] = sedi_registry
        station_rows = store.rows("station_deploy")
        cross["total_stations"] = len(station_rows)
    elif module == "sample_registry":
        from_sampling = sum(1 for r in store.rows("sample_registry") if r.get("synced_from") == "sampling_record")
        from_bio = sum(1 for r in store.rows("sample_registry") if r.get("synced_from") == "bio_trawl")
        cross["synced_from_sampling"] = from_sampling
        cross["synced_from_bio"] = from_bio
        cross["bio_source"] = from_bio
        cross["sediment_source"] = sum(1 for r in store.rows("sample_registry") if r.get("样品类型") == "沉积物")
    elif module == "results_compile":
        transfer_rows = store.rows("data_transfer")
        completed = sum(1 for r in transfer_rows if r.get("status") == "已完成")
        cross["transfer_complete"] = completed
        cross["transfer_total"] = len(transfer_rows)
        cross["completeness"] = round(completed / len(transfer_rows) * 100, 1) if transfer_rows else 0
    elif module == "track_record":
        station_rows = store.rows("station_deploy")
        cross["station_coords"] = len(station_rows)
    elif module == "data_qc":
        hydro_linked = sum(1 for r in store.rows("data_qc") if r.get("triggered_by") == "hydro_measure")
        cross["triggered_by_hydro"] = hydro_linked
    elif module == "cruise_plan":
        station_rows = store.rows("station_deploy")
        plan_changed = sum(1 for r in station_rows if r.get("plan_changed"))
        cross["plan_changed"] = plan_changed
        results_rows = store.rows("results_compile")
        cross["total_results"] = len(results_rows)
    
    base["cross_reference"] = cross
    return base


@router.get("/cross-links")
def cross_links() -> dict[str, Any]:
    """返回所有模块间的关联关系。"""
    links: list[dict[str, Any]] = []
    module_links = {
        "station_deploy": [{"target": "cruise_plan", "label": "航次计划批准状态", "field": "cruise_plan_approved", "desc": "站位布设需要在航次计划批准后进行"},
                          {"target": "weather_obs", "label": "气象海况", "field": "severe_weather", "desc": "恶劣海况影响站位可作业性"}],
        "sampling_record": [{"target": "sample_registry", "label": "样品登记入库", "field": "checked_in_samples", "desc": "采样入库后自动同步样品登记状态"}],
        "hydro_measure": [{"target": "data_qc", "label": "质控流程", "field": "pending_qc_count", "desc": "上传水文剖面后自动触发质控"}],
        "bio_trawl": [{"target": "sample_registry", "label": "样品登记", "field": "synced_from", "desc": "分拣后自动登记生物样品"}],
        "sediment_sample": [{"target": "sample_registry", "label": "样品信息更新", "field": "save_method", "desc": "分割样品后更新登记信息"}],
        "instrument_calib": [{"target": "equipment_manifest", "label": "设备停用", "field": "disabled_equipment", "desc": "校准不合格自动停用设备"}],
        "data_transfer": [{"target": "results_compile", "label": "成果完整度", "field": "compile_complete_count", "desc": "传输完成更新汇编数据完整度"}],
        "diving_ops": [{"target": "crew_schedule", "label": "队员可用性", "field": "unavailable_crew", "desc": "潜水中止影响人员状态"}],
        "weather_obs": [{"target": "station_deploy", "label": "站位告警", "field": "alerted_stations", "desc": "恶劣海况标记影响站位作业"}],
        "analysis_request": [{"target": "sample_registry", "label": "样品入库校验", "field": "sample_status", "desc": "批准前校验样品是否入库"}],
        "cruise_log": [{"target": "equipment_manifest", "label": "设备安全审核", "field": "safety_review_required", "desc": "异常事件触发设备安全审核"}],
        "crew_schedule": [{"target": "diving_ops", "label": "潜水作业取消", "field": "aborted_dives", "desc": "排班调整影响潜水可执行性"}],
    }
    for source, target_list in module_links.items():
        links.append({"source": source, "links": target_list})
    return {"links": links}


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)