"""融合链路接口：目标校验、来源血缘明细、重算领取、物化复核与采购交付。

这些动作带跨模块前置条件，放在独立路由里，避免资源路由替它做业务判断。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.fusion_engine import fusion_engine

router = APIRouter(prefix="/api/fusion", tags=["融合链路"])


class BuildWideTablePayload(BaseModel):
    wide_table_id: int
    fusion_job_id: int


class RegisterSourcePayload(BaseModel):
    source_code: str
    source_name: str
    source_type: str = "MySQL"
    connection_string: str = ""


class DefineWideTablePayload(BaseModel):
    table_code: str
    table_name: str
    field_list: list[str] = Field(default_factory=list)
    primary_key: str = "id"
    partition_key: str = "dt"
    ttl_days: int = 90


class CreateFusionJobPayload(BaseModel):
    target_wide_table: str
    participant_sources: list[str] = Field(default_factory=list)
    join_key: str = "id"
    fusion_window: str = "1d"
    watermark_field: str = "update_time"


class ConfirmLineagePayload(BaseModel):
    wide_table_id: int
    source_codes: list[str] = Field(default_factory=list)
    operator: str = "值班"


class StartRevisionPayload(BaseModel):
    wide_table_id: int
    source_codes: list[str] = Field(default_factory=list)
    operator: str = "值班"


class InvalidateEdgePayload(BaseModel):
    edge_code: str
    reason: str = "目标错配"


class HistoricalEdgePayload(BaseModel):
    upstream_node: str
    downstream_node: str
    target_wide_table: str = ""
    source_job_code: str = ""
    related_fields: list[str] = Field(default_factory=list)
    invalid_reason: str = "历史版本错配边（上游作业目标是 A）"


class RecomputePayload(BaseModel):
    target_wide_table: str
    source_codes: list[str] = Field(default_factory=list)
    lineage_edge_ids: list[int] = Field(default_factory=list)
    cached_trial_amount: float = 0.0
    accepted_before_fix: bool = False
    revision: int = 0


class ClaimPayload(BaseModel):
    worker: str = "夜班"
    claim_token: str = ""


class ReleasePayload(BaseModel):
    reason: str = "执行超时"


class MaterializePayload(BaseModel):
    claim_token: str = ""


class WriteRequestPayload(BaseModel):
    request_id: str
    claim_token: str = ""


class RecomputeSnapshotPayload(BaseModel):
    partition_key_value: str


class ExportPlanPayload(BaseModel):
    target_wide_table: str
    export_format: str = "parquet"
    partition_filter: str = ""
    field_filter: list[str] = Field(default_factory=list)
    export_path: str = "/exports"
    row_limit: int = 0


class MetricFromLineagePayload(BaseModel):
    lineage_edge_id: int
    target_wide_table: str
    agg_function: str = "SUM"
    group_dimensions: list[str] = Field(default_factory=list)


def _fetch_recompute(recompute_id: int):
    job = fusion_engine.get_recompute(recompute_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"重算作业 {recompute_id} 不存在")
    return job


@router.post("/source")
def register_source(payload: RegisterSourcePayload) -> dict[str, Any]:
    source = fusion_engine.register_source(
        payload.source_code, payload.source_name, payload.source_type,
        payload.connection_string,
    )
    return {"ok": True, "source": {"source_id": source.source_id,
                                   "source_code": source.source_code}}


@router.post("/wide-table")
def define_wide_table(payload: DefineWideTablePayload) -> dict[str, Any]:
    wt = fusion_engine.define_wide_table(
        payload.table_code, payload.table_name, payload.field_list,
        primary_key=payload.primary_key,
        partition_key=payload.partition_key,
        ttl_days=payload.ttl_days,
    )
    return {"ok": True, "wide_table": {"table_id": wt.table_id,
                                       "table_code": wt.table_code,
                                       "table_name": wt.table_name}}


@router.post("/fusion-job")
def create_fusion_job(payload: CreateFusionJobPayload) -> dict[str, Any]:
    job = fusion_engine.create_fusion_job(
        payload.target_wide_table, payload.participant_sources,
        join_key=payload.join_key, fusion_window=payload.fusion_window,
        watermark_field=payload.watermark_field,
    )
    return {"ok": True, "fusion_job": {"job_id": job.job_id, "job_code": job.job_code,
                                       "target_wide_table": job.target_wide_table}}


@router.post("/wide-table/build")
def build_wide_table(payload: BuildWideTablePayload) -> dict[str, Any]:
    return fusion_engine.build_wide_table_from_fusion(
        payload.wide_table_id, payload.fusion_job_id)


@router.get("/wide-table/{wide_table_id}/source-lineage")
def source_lineage(wide_table_id: int, include_historical: bool = False) -> dict[str, Any]:
    return fusion_engine.source_lineage_detail(
        wide_table_id, include_historical=include_historical)


@router.get("/wide-table/{wide_table_id}/historical-lineage")
def historical_lineage(wide_table_id: int) -> dict[str, Any]:
    return fusion_engine.historical_lineage(wide_table_id)


@router.post("/wide-table/confirm-lineage")
def confirm_lineage(payload: ConfirmLineagePayload) -> dict[str, Any]:
    return fusion_engine.confirm_source_lineage(
        payload.wide_table_id, payload.source_codes, payload.operator)


@router.post("/wide-table/start-revision")
def start_revision(payload: StartRevisionPayload) -> dict[str, Any]:
    return fusion_engine.start_source_revision(
        payload.wide_table_id, payload.source_codes, payload.operator)


@router.post("/lineage/invalidate")
def invalidate_lineage(payload: InvalidateEdgePayload) -> dict[str, Any]:
    return fusion_engine.invalidate_lineage_edge(payload.edge_code, payload.reason)


@router.post("/lineage/historical")
def record_historical_lineage(payload: HistoricalEdgePayload) -> dict[str, Any]:
    return fusion_engine.record_historical_lineage(
        payload.upstream_node, payload.downstream_node,
        target_wide_table=payload.target_wide_table,
        source_job_code=payload.source_job_code,
        related_fields=payload.related_fields,
        invalid_reason=payload.invalid_reason,
    )


@router.post("/recompute")
def submit_recompute(payload: RecomputePayload) -> dict[str, Any]:
    job = fusion_engine.submit_recompute(
        payload.target_wide_table, payload.source_codes,
        lineage_edge_ids=payload.lineage_edge_ids,
        cached_trial_amount=payload.cached_trial_amount,
        accepted_before_fix=payload.accepted_before_fix,
        revision=payload.revision,
    )
    return {"ok": True, "job": job.to_summary()}


@router.get("/recompute/{recompute_id}")
def get_recompute(recompute_id: int) -> dict[str, Any]:
    return _fetch_recompute(recompute_id).to_summary()


@router.post("/recompute/{recompute_id}/claim")
def claim_recompute(recompute_id: int, payload: ClaimPayload) -> dict[str, Any]:
    _fetch_recompute(recompute_id)
    return fusion_engine.claim_recompute(
        recompute_id, worker=payload.worker, claim_token=payload.claim_token)


@router.post("/recompute/{recompute_id}/release")
def release_recompute(recompute_id: int, payload: ReleasePayload) -> dict[str, Any]:
    _fetch_recompute(recompute_id)
    return fusion_engine.release_recompute(recompute_id, reason=payload.reason)


@router.post("/recompute/{recompute_id}/materialize")
def materialize_recompute(recompute_id: int, payload: MaterializePayload) -> dict[str, Any]:
    _fetch_recompute(recompute_id)
    return fusion_engine.materialize_recompute(recompute_id, payload.claim_token)


@router.post("/recompute/{recompute_id}/write-requests")
def submit_write_request(recompute_id: int, payload: WriteRequestPayload) -> dict[str, Any]:
    _fetch_recompute(recompute_id)
    return fusion_engine.submit_write_request(
        recompute_id, payload.request_id, payload.claim_token)


@router.get("/recompute/{recompute_id}/replenishment")
def replenishment_detail(recompute_id: int) -> dict[str, Any]:
    _fetch_recompute(recompute_id)
    return fusion_engine.procurement_replenishment_detail(recompute_id)


@router.post("/recompute/{recompute_id}/snapshot")
def create_recompute_snapshot(recompute_id: int,
                              payload: RecomputeSnapshotPayload) -> dict[str, Any]:
    _fetch_recompute(recompute_id)
    return fusion_engine.create_recompute_snapshot(
        recompute_id, payload.partition_key_value)


@router.post("/lineage/metric")
def metric_from_lineage(payload: MetricFromLineagePayload) -> dict[str, Any]:
    return fusion_engine.compute_metric_from_lineage(
        payload.lineage_edge_id, payload.target_wide_table,
        payload.agg_function, payload.group_dimensions)


@router.post("/export-plan")
def create_export_plan(payload: ExportPlanPayload) -> dict[str, Any]:
    plan = fusion_engine.create_export_plan(
        payload.target_wide_table, export_format=payload.export_format,
        partition_filter=payload.partition_filter,
        field_filter=payload.field_filter,
        export_path=payload.export_path,
        row_limit=payload.row_limit,
    )
    return {"ok": True, "plan_id": plan.plan_id, "plan_code": plan.plan_code}


@router.get("/export/{export_id}/snapshot/{snapshot_id}")
def export_snapshot(export_id: int, snapshot_id: int) -> dict[str, Any]:
    return fusion_engine.execute_export_from_snapshot(export_id, snapshot_id)
