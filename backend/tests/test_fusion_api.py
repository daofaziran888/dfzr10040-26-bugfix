"""路由层验收：直接调用融合链路接口函数，核对修复后的目标校验与交付约束。"""
from __future__ import annotations

import pytest

from app import fusion_engine as engine_module
from app.fusion_engine import FusionEngine
from app.routers import fusion as api


@pytest.fixture
def engine(monkeypatch: pytest.MonkeyPatch) -> FusionEngine:
    fresh = FusionEngine()
    monkeypatch.setattr(engine_module, "fusion_engine", fresh)
    monkeypatch.setattr(api, "fusion_engine", fresh)
    return fresh


def _prepare(engine: FusionEngine):
    for code in ("SRC-A", "SRC-S", "SRC-T"):
        api.register_source(api.RegisterSourcePayload(
            source_code=code, source_name=code.lower(),
            connection_string=f"conn://{code}",
        ))
    a = api.define_wide_table(api.DefineWideTablePayload(
        table_code="WIDE-A", table_name="订单宽表A",
        field_list=["id", "amount"],
    ))["wide_table"]["table_id"]
    b = api.define_wide_table(api.DefineWideTablePayload(
        table_code="WIDE-B", table_name="订单宽表B",
        field_list=["id", "amount"],
    ))["wide_table"]["table_id"]
    job_a = api.create_fusion_job(api.CreateFusionJobPayload(
        target_wide_table="订单宽表A", participant_sources=["SRC-A"],
    ))["fusion_job"]["job_id"]
    api.build_wide_table(api.BuildWideTablePayload(
        wide_table_id=a, fusion_job_id=job_a))
    return a, b, job_a


def test_http_surface_target_validation_and_source_count(
        engine: FusionEngine) -> None:
    _, b, job_a = _prepare(engine)

    mismatch = api.build_wide_table(api.BuildWideTablePayload(
        wide_table_id=b, fusion_job_id=job_a))
    assert mismatch["ok"] is False
    assert api.source_lineage(b)["sources"] == []

    job_b = api.create_fusion_job(api.CreateFusionJobPayload(
        target_wide_table="订单宽表B",
        participant_sources=["SRC-S", "SRC-S", "SRC-T"],
    ))["fusion_job"]["job_id"]
    built = api.build_wide_table(api.BuildWideTablePayload(
        wide_table_id=b, fusion_job_id=job_b))
    assert built["source_count"] == 2
    assert [item["source"] for item in api.source_lineage(b)["sources"]] == [
        "SRC-S", "SRC-T"]


def test_http_surface_old_recompute_review_and_procurement(
        engine: FusionEngine) -> None:
    _, b, _ = _prepare(engine)
    legacy = api.record_historical_lineage(api.HistoricalEdgePayload(
        upstream_node="SRC-A", downstream_node="订单宽表B",
        target_wide_table="订单宽表B", source_job_code="FUSI-LEGACY",
    ))["edge"]
    api.start_revision(api.StartRevisionPayload(
        wide_table_id=b, source_codes=["SRC-S", "SRC-T"]))

    old = api.submit_recompute(api.RecomputePayload(
        target_wide_table="订单宽表B", source_codes=["SRC-A"],
        lineage_edge_ids=[legacy["edge_id"]],
        cached_trial_amount=11, accepted_before_fix=True,
    ))["job"]
    claimed = api.claim_recompute(old["recompute_id"], api.ClaimPayload(worker="夜班"))
    assert claimed["status"] == "复核中"
    assert claimed["verification"]["historical_evidence"][0]["edge_code"] == (
        legacy["edge_code"])

    token = claimed["job"]["claim_token"]
    rejected = api.submit_write_request(
        old["recompute_id"],
        api.WriteRequestPayload(request_id="REQ-1", claim_token=token))
    repeated = api.submit_write_request(
        old["recompute_id"],
        api.WriteRequestPayload(request_id="REQ-1", claim_token=token))
    assert rejected["written"] == 0
    assert repeated["duplicate"] is True and repeated["written"] == 0

    replenishment = api.replenishment_detail(old["recompute_id"])
    assert replenishment["rows"] == []
    assert replenishment["trial_amount_excluded"] == 11.0
