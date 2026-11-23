"""目标校验、来源血缘、旧重算复核与采购交付的回归测试。

场景对应真实事故：上游用「目标是 A 的作业」给 B 连来源，导致 B 的来源计数
和血缘归属错位；修复后要求错配边只能查历史，旧重算只能复核，采购只拿有确认依据的聚合行。
"""
from __future__ import annotations

import pytest

from app.fusion_engine import FusionEngine


@pytest.fixture
def engine() -> FusionEngine:
    return FusionEngine()


def _table(engine: FusionEngine, code: str, name: str) -> int:
    return engine.define_wide_table(
        table_code=code, table_name=name,
        field_list=["id", "name", "amount", "update_time"],
    ).table_id


def _job(engine: FusionEngine, target: str, sources: list[str]) -> int:
    return engine.create_fusion_job(
        target_wide_table=target, participant_sources=sources, join_key="id",
    ).job_id


class TestWideTableTargetValidation:
    def test_job_targeting_a_cannot_bind_sources_to_b(self, engine: FusionEngine) -> None:
        """给 B 提交目标为 A 的作业：拒绝构建，B 不增加任何边。"""
        a = _table(engine, "WIDE-A", "订单宽表A")
        b = _table(engine, "WIDE-B", "订单宽表B")
        job_a = _job(engine, "订单宽表A", ["SRC-A"])

        before = engine.source_lineage_detail(b)
        result = engine.build_wide_table_from_fusion(b, job_a)

        assert result["ok"] is False
        assert result["target_check"]["matched"] is False
        assert result["lineage_edges"] == []
        after = engine.source_lineage_detail(b)
        assert after["source_count"] == 0
        assert len(after["sources"]) == len(before["sources"]) == 0
        assert engine.domain_snapshot()["lineages"] == 0

    def test_duplicate_source_in_config_yields_one_support(self, engine: FusionEngine) -> None:
        """B 登记 S/S/T：来源数为 2，重复来源不产生第二个有效支持。"""
        engine.register_source("S", "s", "MySQL", "conn://s")
        engine.register_source("T", "t", "MySQL", "conn://t")
        b = _table(engine, "WIDE-B", "订单宽表B")
        job_b = _job(engine, "订单宽表B", ["S", "S", "T"])

        result = engine.build_wide_table_from_fusion(b, job_b)

        assert result["ok"] is True
        assert result["source_count"] == 2
        assert len(result["lineage_edges"]) == 2
        detail = engine.source_lineage_detail(b)
        assert detail["source_count"] == 2
        assert [item["source"] for item in detail["sources"]] == ["S", "T"]
        support_keys = [item["support_key"] for item in detail["sources"]]
        assert len(support_keys) == len(set(support_keys)) == 2

        # 重复构建同一来源也只刷新原边，不新增支持。
        again = engine.build_wide_table_from_fusion(b, job_b)
        assert again["source_count"] == 2
        assert len(again["lineage_edges"]) == 2

    def test_job_matching_table_code_also_builds(self, engine: FusionEngine) -> None:
        """目标校验同时接受表编号与表名称，避免误杀合法作业。"""
        engine.register_source("SRC-OK", "ok", "MySQL", "conn://ok")
        b = _table(engine, "WIDE-B", "订单宽表B")
        job_b = _job(engine, "WIDE-B", ["SRC-OK"])
        result = engine.build_wide_table_from_fusion(b, job_b)
        assert result["ok"] is True
        assert result["source_count"] == 1

    def test_unregistered_source_is_rejected(self, engine: FusionEngine) -> None:
        b = _table(engine, "WIDE-B", "订单宽表B")
        job_b = _job(engine, "订单宽表B", ["GHOST"])
        result = engine.build_wide_table_from_fusion(b, job_b)
        assert result["ok"] is False
        assert engine.source_lineage_detail(b)["source_count"] == 0


class TestHistoricalMismatchedEdge:
    def test_mismatched_edge_only_queryable_as_history(self, engine: FusionEngine) -> None:
        """错配边：不参与来源计数、不能确认指标，但原证据可查。"""
        engine.register_source("SRC-A", "a", "MySQL", "conn://a")
        engine.register_source("SRC-T", "t", "MySQL", "conn://t")
        b = _table(engine, "WIDE-B", "订单宽表B")
        job_b = _job(engine, "订单宽表B", ["SRC-A", "SRC-T"])
        built = engine.build_wide_table_from_fusion(b, job_b)
        assert built["ok"] is True

        bad_edge = next(
            edge for edge in engine._lineages.values()
            if edge.upstream_node == "SRC-A" and edge.target_table() == "订单宽表B"
        )  # SRC-A -> B，事故遗留的错配边
        engine.invalidate_lineage_edge(bad_edge.edge_code, "上游作业目标是 A")

        detail = engine.source_lineage_detail(b)
        assert detail["source_count"] == 1
        assert [item["source"] for item in detail["sources"]] == ["SRC-T"]

        history = engine.historical_lineage(b)
        assert history["historical_count"] == 1
        assert history["historical"][0]["edge_code"] == bad_edge.edge_code
        assert history["historical"][0]["invalid_reason"] == "上游作业目标是 A"

        metric = engine.compute_metric_from_lineage(
            bad_edge.edge_id, "订单宽表B", "SUM", ["dt"])
        assert metric["ok"] is False
        assert "历史" in metric["message"]


class TestRecomputeClaimAndReview:
    def _scenario(self, engine: FusionEngine):
        """B 的来源正在交接：S/T 已是当前配置但尚未确认，旧作业带 A 血缘和试算 11。"""
        engine.register_source("SRC-A", "a", "MySQL", "conn://a")
        engine.register_source("SRC-S", "s", "MySQL", "conn://s")
        engine.register_source("SRC-T", "t", "MySQL", "conn://t")
        a = _table(engine, "WIDE-A", "订单宽表A")
        b = _table(engine, "WIDE-B", "订单宽表B")
        job_a = _job(engine, "订单宽表A", ["SRC-A"])
        built_a = engine.build_wide_table_from_fusion(a, job_a)
        a_edge_id = next(
            edge.edge_id for edge in engine._lineages.values()
            if edge.upstream_node == "SRC-A"
        )

        job_b = _job(engine, "订单宽表B", ["SRC-S", "SRC-T"])
        engine.build_wide_table_from_fusion(b, job_b)
        engine.start_source_revision(b, ["SRC-S", "SRC-T"])

        old = engine.submit_recompute(
            "订单宽表B", ["SRC-A"], lineage_edge_ids=[a_edge_id],
            cached_trial_amount=11.0, accepted_before_fix=True,
        )
        return a, b, a_edge_id, old, built_a

    def test_claim_conflict_routes_old_job_to_review_only(self, engine: FusionEngine) -> None:
        _, b, _, old, _ = self._scenario(engine)

        claim = engine.claim_recompute(old.recompute_id, worker="夜班")
        assert claim["claimed"] is True
        assert claim["status"] == "复核中"
        assert claim["route"] == "复核"
        assert claim["eligible_for_purchase"] is False
        assert "复核" in claim["evidence"][0]["verdict"] or claim["evidence"]

        # 依据可核查：来源结论 + 当前修订 + 历史错配边仍可查。
        assert old.evidence
        detail = engine.source_lineage_detail(b)
        assert detail["source_count"] == 2
        assert {item["confirmation"] for item in detail["sources"]} == {"待确认"}

        # 重送写入请求也不会写出结果。
        write = engine.submit_write_request(old.recompute_id, "req-old-1")
        assert write["ok"] is False
        assert write["written"] == 0
        again = engine.submit_write_request(old.recompute_id, "req-old-1")
        assert again["ok"] is False
        assert again["duplicate"] is True
        assert again["written"] == 0
        assert old.materialized_rows == []

    def test_old_trial_amount_never_reaches_procurement(self, engine: FusionEngine) -> None:
        _, b, _, old, _ = self._scenario(engine)
        engine.claim_recompute(old.recompute_id)

        detail = engine.procurement_replenishment_detail(old.recompute_id)
        assert detail["rows"] == []
        assert detail["trial_amount_excluded"] == 11.0
        assert detail["eligible"] is False

        snap = engine.create_recompute_snapshot(old.recompute_id, "20261123")
        assert snap["snapshot"]["procurement_eligible"] is False
        plan = engine.create_export_plan("订单宽表B", export_format="csv")
        export = engine.execute_procurement_export_from_snapshot(
            plan.plan_id, snap["partition_id"])
        assert export["ok"] is False
        assert export["rows"] == []

    def test_valid_sources_still_deferred_until_confirmed(self, engine: FusionEngine) -> None:
        """合法来源尚未确认：领取不生效，作业保留待领并留下核验依据。"""
        _, b, _, _, _ = self._scenario(engine)
        fresh = engine.submit_recompute("订单宽表B", ["SRC-S", "SRC-T"])

        claim = engine.claim_recompute(fresh.recompute_id)
        assert claim["claimed"] is False
        assert claim["route"] == "待领"
        assert fresh.status == "待领"
        assert claim["evidence"]
        assert {item["verdict"] for item in claim["evidence"]} == {"待确认"}

    def test_confirmed_sources_materialize_and_export_computed_rows(
            self, engine: FusionEngine) -> None:
        _, b, _, _, _ = self._scenario(engine)
        confirm = engine.confirm_source_lineage(
            b, ["SRC-S", "SRC-T"], operator="值班")
        assert confirm["ok"] is True
        assert confirm["source_count"] == 2

        b_edges = [
            edge.edge_id for edge in engine._lineages.values()
            if edge.target_table() == "订单宽表B" and edge.upstream_node in {"SRC-S", "SRC-T"}
        ]
        fresh = engine.submit_recompute(
            "订单宽表B", ["SRC-S", "SRC-T"], lineage_edge_ids=b_edges)
        claim = engine.claim_recompute(fresh.recompute_id, worker="白班")
        assert claim["ok"] is True
        assert claim["route"] == "物化"

        materialize = engine.materialize_recompute(
            fresh.recompute_id, claim["job"]["claim_token"])
        assert materialize["written"] == 2
        assert {row["source"] for row in materialize["aggregate_rows"]} == {
            "SRC-S", "SRC-T"}

        detail = engine.procurement_replenishment_detail(fresh.recompute_id)
        assert detail["eligible"] is True
        assert len(detail["rows"]) == 2
        assert all(row["confirmation"] == "已确认" for row in detail["rows"])
        assert detail["trial_amount_excluded"] is None

        snap = engine.create_recompute_snapshot(fresh.recompute_id, "20261123")
        assert snap["snapshot"]["procurement_eligible"] is True
        assert snap["snapshot"]["lineage_revision"] == engine._wide_tables[b].lineage_revision
        plan = engine.create_export_plan("订单宽表B", export_format="parquet")
        exported = engine.execute_procurement_export_from_snapshot(
            plan.plan_id, snap["partition_id"])
        assert exported["ok"] is True
        assert exported["exported_rows"] == 2
        assert {row["source"] for row in exported["rows"]} == {"SRC-S", "SRC-T"}
        assert {item["edge_code"] for item in exported["lineage_evidence"]} == {
            row["edge_code"] for row in exported["rows"]}

    def test_declared_historical_edge_cannot_support_new_recompute(
            self, engine: FusionEngine) -> None:
        """新作业若声明历史错配边为来源依据，领取会被拒绝并留下依据。"""
        engine.register_source("SRC-A", "a", "MySQL", "conn://a")
        engine.register_source("SRC-S", "s", "MySQL", "conn://s")
        b = _table(engine, "WIDE-B", "订单宽表B")
        job_b = _job(engine, "订单宽表B", ["SRC-A", "SRC-S"])
        engine.build_wide_table_from_fusion(b, job_b)
        bad_edge = next(
            edge for edge in engine._lineages.values()
            if edge.upstream_node == "SRC-A"
        )
        engine.invalidate_lineage_edge(bad_edge.edge_code, "上游作业目标是 A")
        engine.confirm_source_lineage(b, ["SRC-S"])

        job = engine.submit_recompute(
            "订单宽表B", ["SRC-S"],
            lineage_edge_ids=[bad_edge.edge_id],
        )
        claim = engine.claim_recompute(job.recompute_id)
        assert claim["claimed"] is False
        assert job.status == "待领"
        assert claim["verification"]["invalid_declared_edges"]
        assert claim["verification"]["historical_evidence"][0]["edge_code"] == bad_edge.edge_code

        metric = engine.compute_metric_from_lineage(
            bad_edge.edge_id, "订单宽表B", "SUM", ["dt"])
        assert metric["ok"] is False

    def test_timed_out_old_recompute_reenters_pending_with_trial_amount(
            self, engine: FusionEngine) -> None:
        """旧重算超时回到待领集合，试算金额 11 保留，但领取后仍只能进复核。"""
        _, _, _, old, _ = self._scenario(engine)
        released = engine.release_recompute(old.recompute_id, "执行超时")
        assert released["status"] == "待领"
        assert released["cached_trial_amount"] == 11.0
        assert old.claim_token == ""

        claim = engine.claim_recompute(old.recompute_id, worker="夜班")
        assert claim["status"] == "复核中"
        assert claim["eligible_for_purchase"] is False
        assert claim["job"]["cached_trial_amount"] == 11.0
        assert claim["job"]["materialized_rows"] == []

    def test_replay_queue_claim_is_single_use(self) -> None:
        from app.recovery import ReplayQueue

        rq = ReplayQueue()
        entry = rq.enqueue("FUSI-OLD", "执行超时")
        rq.reenter_pending(entry.entry_id, "旧作业恢复")
        first = rq.claim(entry.entry_id, worker="夜班")
        second = rq.claim(entry.entry_id, worker="夜班")
        assert first["claimed"] is True
        assert second["claimed"] is False
        assert second["duplicate"] is True

    def test_duplicate_claim_is_not_double_effective(self, engine: FusionEngine) -> None:
        _, b, _, _, _ = self._scenario(engine)
        engine.confirm_source_lineage(b, ["SRC-S", "SRC-T"])
        fresh = engine.submit_recompute("订单宽表B", ["SRC-S", "SRC-T"])
        first = engine.claim_recompute(fresh.recompute_id, claim_token="token-1")
        assert first["claimed"] is True
        second = engine.claim_recompute(fresh.recompute_id, claim_token="token-1")
        assert second["claimed"] is False
        assert second["duplicate"] is True

    def test_materialize_requires_matching_claim_token(self, engine: FusionEngine) -> None:
        _, b, _, _, _ = self._scenario(engine)
        engine.confirm_source_lineage(b, ["SRC-S", "SRC-T"])
        fresh = engine.submit_recompute("订单宽表B", ["SRC-S", "SRC-T"])
        engine.claim_recompute(fresh.recompute_id)

        rejected = engine.materialize_recompute(fresh.recompute_id, "wrong-token")
        assert rejected["ok"] is False
        assert rejected["written"] == 0
        assert fresh.materialized_rows == []
