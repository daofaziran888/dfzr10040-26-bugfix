"""熔炉引擎：跨模块数据融合流水线编排器。

负责执行数据源 -> Schema 映射 -> 宽表定义 -> 融合作业的完整链路，
并管理冲突仲裁、质量门禁、分区快照、水位追踪与增量归并。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable

from app.models import (
    BackfillTask,
    ColdStoreStrategy,
    CrossValidatePlan,
    DataDriftRecord,
    ExportPlan,
    FusionJob,
    FusionMonitorMetric,
    LineageEdge,
    MergeConflictRule,
    MetricAggrRule,
    PartitionRule,
    QualityGateRule,
    RecomputeJob,
    SchemaMapping,
    SnapshotPartition,
    SourceDef,
    WatermarkRecord,
    WideTableDef,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _generate_code(prefix: str, index: int) -> str:
    return f"{prefix}-{index:04d}"


class FusionEngine:
    """熔炉引擎：编排跨模块数据融合流水线。"""

    def __init__(self) -> None:
        self._sources: dict[int, SourceDef] = {}
        self._schemas: dict[int, SchemaMapping] = {}
        self._wide_tables: dict[int, WideTableDef] = {}
        self._fusion_jobs: dict[int, FusionJob] = {}
        self._conflict_rules: dict[int, MergeConflictRule] = {}
        self._quality_gates: dict[int, QualityGateRule] = {}
        self._snapshots: dict[int, SnapshotPartition] = {}
        self._backfills: dict[int, BackfillTask] = {}
        self._cold_stores: dict[int, ColdStoreStrategy] = {}
        self._lineages: dict[int, LineageEdge] = {}
        self._metrics: dict[int, MetricAggrRule] = {}
        self._partitions: dict[int, PartitionRule] = {}
        self._watermarks: dict[int, WatermarkRecord] = {}
        self._exports: dict[int, ExportPlan] = {}
        self._cross_validates: dict[int, CrossValidatePlan] = {}
        self._drifts: dict[int, DataDriftRecord] = {}
        self._monitors: dict[int, FusionMonitorMetric] = {}
        self._recomputes: dict[int, RecomputeJob] = {}
        self._aggregate_rows: dict[int, list[dict[str, Any]]] = {}
        self._next_id: int = 1

    def _next_seq(self) -> int:
        sid = self._next_id
        self._next_id += 1
        return sid

    # ---- 1. source_def -> schema_map -> wide_table_def -> fusion_job ----

    def register_source(self, source_code: str, source_name: str, source_type: str,
                        connection_string: str, auth_method: str = "jdbc",
                        charset: str = "UTF-8", timezone: str = "Asia/Shanghai") -> SourceDef:
        sid = self._next_seq()
        source = SourceDef(
            source_id=sid, source_code=source_code, source_name=source_name,
            source_type=source_type, connection_string=connection_string,
            auth_method=auth_method, charset=charset, timezone=timezone,
            access_status="待验证",
        )
        self._sources[sid] = source
        return source

    def create_schema_mapping(self, source_table: str, target_wide_table: str,
                              field_mapping_json: str, type_conversion: str = "",
                              missing_policy: str = "skip") -> SchemaMapping:
        sid = self._next_seq()
        mapping = SchemaMapping(
            mapping_id=sid, plan_code=_generate_code("SCHE", sid),
            source_table=source_table, target_wide_table=target_wide_table,
            field_mapping_json=field_mapping_json,
            type_conversion_rules=type_conversion,
            missing_value_policy=missing_policy,
            mapping_order="ASC", plan_status="草稿",
        )
        self._schemas[sid] = mapping
        return mapping

    def define_wide_table(self, table_code: str, table_name: str,
                          field_list: list[str], primary_key: str = "id",
                          partition_key: str = "dt", ttl_days: int = 90) -> WideTableDef:
        sid = self._next_seq()
        wt = WideTableDef(
            table_id=sid, table_code=table_code, table_name=table_name,
            field_list=field_list, primary_key=primary_key,
            partition_key=partition_key, ttl_days=ttl_days,
            source_count=0, last_updated=_now_iso(),
        )
        self._wide_tables[sid] = wt
        return wt

    def create_fusion_job(self, target_wide_table: str, participant_sources: list[str],
                          join_key: str, fusion_window: str = "1d",
                          watermark_field: str = "update_time") -> FusionJob:
        sid = self._next_seq()
        job = FusionJob(
            job_id=sid, job_code=_generate_code("FUSI", sid),
            target_wide_table=target_wide_table,
            participant_sources=participant_sources,
            join_key=join_key, fusion_window=fusion_window,
            watermark_field=watermark_field,
            output_rows=0, skipped_rows=0,
        )
        self._fusion_jobs[sid] = job
        return job

    # ---- 2. fusion_job -> merge_conflict -> quality_gate -> snapshot_mgr ----

    def create_conflict_rule(self, target_wide_table: str, conflict_fields: list[str],
                             arbitration_strategy: str = "source_priority",
                             priority_source: str = "", time_baseline: str = "") -> MergeConflictRule:
        sid = self._next_seq()
        rule = MergeConflictRule(
            rule_id=sid, rule_code=_generate_code("MERG", sid),
            target_wide_table=target_wide_table,
            conflict_fields=conflict_fields,
            arbitration_strategy=arbitration_strategy,
            priority_source=priority_source,
            time_baseline=time_baseline or _now_iso(),
            conflict_count=0, rule_status="草稿",
        )
        self._conflict_rules[sid] = rule
        return rule

    def create_quality_gate(self, target_wide_table: str, check_type: str,
                            threshold_condition: str, trigger_action: str = "block",
                            block_level: str = "warning") -> QualityGateRule:
        sid = self._next_seq()
        gate = QualityGateRule(
            rule_id=sid, rule_code=_generate_code("QUAL", sid),
            target_wide_table=target_wide_table,
            check_type=check_type,
            threshold_condition=threshold_condition,
            trigger_action=trigger_action,
            block_level=block_level,
            checked_rows=0, last_triggered="",
        )
        self._quality_gates[sid] = gate
        return gate

    def create_snapshot(self, target_wide_table: str, partition_key_value: str,
                        dependency_partitions: list[str] | None = None,
                        schema_digest: str = "") -> SnapshotPartition:
        sid = self._next_seq()
        snap = SnapshotPartition(
            partition_id=sid,
            partition_code=_generate_code("SNAP", sid),
            target_wide_table=target_wide_table,
            partition_key_value=partition_key_value,
            record_count=0,
            schema_digest=schema_digest or f"SHA256-{uuid.uuid4().hex[:8]}",
            status_mark="创建中",
            dependency_partitions=dependency_partitions or [],
            created_time=_now_iso(),
        )
        self._snapshots[sid] = snap
        return snap

    # ---- 3. fusion_job -> watermark_track -> delta_merge ----

    def create_watermark(self, data_source: str, current_watermark: str,
                         data_origin: str = "fusion_job") -> WatermarkRecord:
        sid = self._next_seq()
        wm = WatermarkRecord(
            record_id=sid, record_code=_generate_code("WATE", sid),
            data_source=data_source,
            current_watermark=current_watermark,
            previous_watermark="",
            advance_interval="5m",
            delay_minutes=0,
            data_origin=data_origin,
            last_advance=_now_iso(),
        )
        self._watermarks[sid] = wm
        return wm

    def advance_watermark(self, record_id: int, new_watermark: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(record_id)
        if wm is None:
            return None
        wm.previous_watermark = wm.current_watermark
        wm.current_watermark = new_watermark
        wm.last_advance = _now_iso()
        return wm

    # ---- 4. snapshot_mgr -> backfill_task -> cold_store ----

    def create_backfill_task(self, target_wide_table: str, backfill_range: str,
                             parallel_partitions: int = 4, dependency_check: str = "",
                             checkpoint_position: str = "") -> BackfillTask:
        sid = self._next_seq()
        task = BackfillTask(
            task_id=sid, task_code=_generate_code("BACK", sid),
            target_wide_table=target_wide_table,
            backfill_range=backfill_range,
            parallel_partitions=parallel_partitions,
            dependency_check=dependency_check,
            checkpoint_position=checkpoint_position,
            processed_rows=0, remaining_rows=0,
        )
        self._backfills[sid] = task
        return task

    def create_cold_store_strategy(self, target_wide_table: str, hot_data_days: int,
                                   cold_storage_type: str = "parquet",
                                   compression_method: str = "zstd",
                                   migration_window: str = "02:00-06:00") -> ColdStoreStrategy:
        sid = self._next_seq()
        cs = ColdStoreStrategy(
            strategy_id=sid, strategy_code=_generate_code("COLD", sid),
            target_wide_table=target_wide_table,
            hot_data_days=hot_data_days,
            cold_storage_type=cold_storage_type,
            migration_window=migration_window,
            compression_method=compression_method,
            migrated_rows=0, last_migration="",
        )
        self._cold_stores[sid] = cs
        return cs

    # ---- 5. wide_table_def -> lineage_edge -> metric_aggr ----

    def create_lineage_edge(self, upstream_node: str, downstream_node: str,
                            related_fields: list[str], transform_function: str = "",
                            edge_type: str = "direct", dependency_level: int = 1) -> LineageEdge:
        sid = self._next_seq()
        edge = LineageEdge(
            edge_id=sid, edge_code=_generate_code("LINE", sid),
            upstream_node=upstream_node, downstream_node=downstream_node,
            related_fields=related_fields,
            transform_function=transform_function,
            edge_type=edge_type,
            dependency_level=dependency_level,
            last_traced=_now_iso(),
        )
        self._lineages[sid] = edge
        return edge

    def create_metric_aggr_rule(self, target_wide_table: str, agg_function: str,
                                group_dimensions: list[str], time_granularity: str = "1d",
                                precompute_window: str = "7d",
                                storage_table: str = "") -> MetricAggrRule:
        sid = self._next_seq()
        rule = MetricAggrRule(
            rule_id=sid, rule_code=_generate_code("METR", sid),
            target_wide_table=target_wide_table,
            agg_function=agg_function,
            group_dimensions=group_dimensions,
            time_granularity=time_granularity,
            precompute_window=precompute_window,
            storage_table=storage_table or f"aggr_{target_wide_table}",
            last_computed="",
        )
        self._metrics[sid] = rule
        return rule

    # ---- 6. partition_rule -> cold_store -> archive ----

    def create_partition_rule(self, target_wide_table: str, partition_type: str,
                              partition_field: str, retention_days: int = 30,
                              archive_policy: str = "move_to_cold") -> PartitionRule:
        sid = self._next_seq()
        rule = PartitionRule(
            rule_id=sid, strategy_code=_generate_code("PART", sid),
            target_wide_table=target_wide_table,
            partition_type=partition_type,
            partition_field=partition_field,
            retention_days=retention_days,
            archive_policy=archive_policy,
            partition_count=0,
            last_adjusted=_now_iso(),
        )
        self._partitions[sid] = rule
        return rule

    # ---- 7. snapshot_mgr -> export_snapshot ----

    def create_export_plan(self, target_wide_table: str, export_format: str = "parquet",
                           partition_filter: str = "", field_filter: list[str] | None = None,
                           export_path: str = "/exports", row_limit: int = 0) -> ExportPlan:
        sid = self._next_seq()
        plan = ExportPlan(
            plan_id=sid, plan_code=_generate_code("EXPO", sid),
            target_wide_table=target_wide_table,
            export_format=export_format,
            partition_filter=partition_filter,
            field_filter=field_filter or [],
            export_path=export_path,
            row_limit=row_limit,
            export_status="待导出",
        )
        self._exports[sid] = plan
        return plan

    # ---- 8. cross_validate -> data_drift -> fusion_monitor ----

    def create_cross_validate_plan(self, primary_wide_table: str,
                                   reference_wide_table: str, join_key: str,
                                   validate_fields: list[str],
                                   deviation_threshold: str = "0.05") -> CrossValidatePlan:
        sid = self._next_seq()
        plan = CrossValidatePlan(
            plan_id=sid, plan_code=_generate_code("CROS", sid),
            primary_wide_table=primary_wide_table,
            reference_wide_table=reference_wide_table,
            join_key=join_key,
            validate_fields=validate_fields,
            deviation_threshold=deviation_threshold,
            last_result="",
            validate_status="待校验",
        )
        self._cross_validates[sid] = plan
        return plan

    def create_drift_record(self, wide_table_name: str, drift_type: str,
                            historical_baseline: str, current_distribution: str,
                            kl_divergence: str = "0.0",
                            drift_direction: str = "none") -> DataDriftRecord:
        sid = self._next_seq()
        record = DataDriftRecord(
            record_id=sid, record_code=_generate_code("DATA", sid),
            wide_table_name=wide_table_name,
            drift_type=drift_type,
            historical_baseline=historical_baseline,
            current_distribution=current_distribution,
            kl_divergence=kl_divergence,
            drift_direction=drift_direction,
            detected_at=_now_iso(),
        )
        self._drifts[sid] = record
        return record

    def create_fusion_monitor(self, fusion_job: str, monitor_dimension: str,
                              collection_interval: str = "60s",
                              alert_threshold: str = "",
                              alert_channel: str = "webhook",
                              silence_window: str = "") -> FusionMonitorMetric:
        sid = self._next_seq()
        metric = FusionMonitorMetric(
            metric_id=sid, metric_code=_generate_code("FUSI", sid),
            fusion_job=fusion_job,
            monitor_dimension=monitor_dimension,
            collection_interval=collection_interval,
            alert_threshold=alert_threshold,
            alert_channel=alert_channel,
            silence_window=silence_window,
            last_sample=_now_iso(),
        )
        self._monitors[sid] = metric
        return metric

    # ---- 复合融合操作 ----

    def execute_fusion_pipeline(self, job_id: int) -> dict[str, Any]:
        """执行完整融合流水线：数据源读取 -> 映射转换 -> 冲突仲裁 -> 质量检查 -> 产出快照。

        返回值包含融合记录、冲突日志、质量报告和快照信息。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        raw_records: list[dict[str, Any]] = []
        for source_code in job.participant_sources:
            source = self._find_source_by_code(source_code)
            if source is None:
                continue
            source_records = self._simulate_source_read(source)
            for rec in source_records:
                rec["_source"] = source_code
                rec["_fusion_key"] = f"{source_code}#{rec.get(job.join_key, '')}"
                rec["_timestamp"] = _now_iso()
                rec["_version"] = 1
            raw_records.extend(source_records)

        mapped_records = self._apply_schema_mappings(raw_records, job.target_wide_table)

        job.output_rows = len(mapped_records)
        job.skipped_rows = len(raw_records) - len(mapped_records)
        job.merged_records = mapped_records

        result: dict[str, Any] = {
            "ok": True,
            "job_code": job.job_code,
            "total_sources": len(job.participant_sources),
            "raw_records": len(raw_records),
            "mapped_records": len(mapped_records),
            "skipped_records": job.skipped_rows,
            "records": mapped_records,
        }

        conflict_result = self._apply_conflict_resolution(mapped_records, job.target_wide_table)
        result["conflict_resolution"] = conflict_result

        quality_result = self._apply_quality_gates(
            conflict_result.get("resolved", mapped_records),
            job.target_wide_table,
        )
        result["quality_gate"] = quality_result

        if quality_result.get("passed", True):
            snap = self.create_snapshot(
                job.target_wide_table,
                partition_key_value=datetime.now(timezone.utc).strftime("%Y%m%d"),
            )
            snap.record_count = len(conflict_result.get("resolved", mapped_records))
            result["snapshot"] = {"partition_code": snap.partition_code, "record_count": snap.record_count}

        return result

    def execute_delta_merge(self, job_id: int, delta_records: list[dict[str, Any]],
                            watermark_record_id: int = 0) -> dict[str, Any]:
        """执行增量归并：将增量数据合并到融合作业的输出中。"""
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        existing = {self._make_merge_key(r, job.join_key): r for r in job.merged_records}
        inserted = 0
        updated = 0
        deleted = 0

        for delta in delta_records:
            key = self._make_merge_key(delta, job.join_key)
            op = delta.get("_op", "upsert")
            if op == "delete":
                if key in existing:
                    del existing[key]
                    deleted += 1
            elif key in existing:
                existing[key].update(delta)
                updated += 1
            else:
                existing[key] = dict(delta)
                inserted += 1

        job.merged_records = list(existing.values())

        if watermark_record_id > 0:
            wm = self._watermarks.get(watermark_record_id)
            if wm is not None:
                wm.previous_watermark = wm.current_watermark
                wm.current_watermark = _now_iso()

        return {
            "ok": True,
            "inserted": inserted,
            "updated": updated,
            "deleted": deleted,
            "total_after_merge": len(job.merged_records),
        }

    def execute_backfill_with_checkpoint(self, backfill_id: int,
                                         source_records: list[dict[str, Any]]) -> dict[str, Any]:
        """执行带断点续传的数据回填。

        从 checkpoint_position 处继续处理，失败时保留断点可恢复。
        """
        task = self._backfills.get(backfill_id)
        if task is None:
            return {"ok": False, "message": f"回填任务 {backfill_id} 不存在"}

        start_idx = 0
        if task.checkpoint_position:
            try:
                start_idx = int(task.checkpoint_position)
            except (ValueError, TypeError):
                start_idx = 0

        batch_size = max(task.parallel_partitions * 100, 100)
        end_idx = min(start_idx + batch_size, len(source_records))
        batch = source_records[start_idx:end_idx]

        processed = len(batch)
        task.processed_rows += processed
        task.remaining_rows = max(0, len(source_records) - end_idx)

        if end_idx < len(source_records):
            task.checkpoint_position = str(end_idx)
            complete = False
        else:
            task.checkpoint_position = ""
            complete = True

        return {
            "ok": True,
            "batch_processed": processed,
            "total_processed": task.processed_rows,
            "remaining": task.remaining_rows,
            "checkpoint": task.checkpoint_position,
            "complete": complete,
        }

    def migrate_to_cold_store(self, strategy_id: int,
                              hot_partition_cutoff: str) -> dict[str, Any]:
        """执行冷热分层迁移：将超过热数据保留期的分区迁移到冷存储。"""
        strategy = self._cold_stores.get(strategy_id)
        if strategy is None:
            return {"ok": False, "message": f"冷热分层策略 {strategy_id} 不存在"}

        eligible_snaps = [
            snap for snap in self._snapshots.values()
            if snap.target_wide_table == strategy.target_wide_table
            and snap.partition_key_value < hot_partition_cutoff
        ]
        migrated = 0
        for snap in eligible_snaps:
            migrated += snap.record_count
            snap.status_mark = "已迁移至冷存储"

        strategy.migrated_rows += migrated
        strategy.last_migration = _now_iso()

        return {
            "ok": True,
            "migrated_partitions": len(eligible_snaps),
            "migrated_rows": migrated,
            "strategy_code": strategy.strategy_code,
        }

    def execute_export_from_snapshot(self, export_id: int,
                                     snapshot_id: int) -> dict[str, Any]:
        """从分区快照执行数据导出。"""
        plan = self._exports.get(export_id)
        snap = self._snapshots.get(snapshot_id)
        if plan is None:
            return {"ok": False, "message": f"导出计划 {export_id} 不存在"}
        if snap is None:
            return {"ok": False, "message": f"分区快照 {snapshot_id} 不存在"}

        # 重算产生的快照只读聚合行，且必须携带确认依据才能交付。
        if snap.recompute_id:
            return self.execute_procurement_export_from_snapshot(export_id, snapshot_id)

        fusion_job = self._find_fusion_job_by_table(snap.target_wide_table)
        records = fusion_job.merged_records if fusion_job else []

        if plan.field_filter:
            records = [
                {k: v for k, v in rec.items() if k in plan.field_filter}
                for rec in records
            ]

        if plan.row_limit > 0 and len(records) > plan.row_limit:
            records = records[:plan.row_limit]

        plan.export_status = "已完成"

        return {
            "ok": True,
            "plan_code": plan.plan_code,
            "snapshot_code": snap.partition_code,
            "exported_rows": len(records),
            "format": plan.export_format,
            "path": plan.export_path,
            "records": records,
        }

    def execute_cross_validation(self, plan_id: int) -> dict[str, Any]:
        """执行跨宽表交叉校验，检测数据一致性偏差。"""
        plan = self._cross_validates.get(plan_id)
        if plan is None:
            return {"ok": False, "message": f"交叉校验方案 {plan_id} 不存在"}

        primary_job = self._find_fusion_job_by_table(plan.primary_wide_table)
        ref_job = self._find_fusion_job_by_table(plan.reference_wide_table)

        primary_records = primary_job.merged_records if primary_job else []
        ref_records = ref_job.merged_records if ref_job else []

        primary_by_key = {str(r.get(plan.join_key, "")): r for r in primary_records}
        ref_by_key = {str(r.get(plan.join_key, "")): r for r in ref_records}

        mismatches: list[dict[str, Any]] = []
        drift_detected = False

        for key in set(primary_by_key.keys()) & set(ref_by_key.keys()):
            p = primary_by_key[key]
            r = ref_by_key[key]
            for field in plan.validate_fields:
                pv = str(p.get(field, ""))
                rv = str(r.get(field, ""))
                if pv != rv:
                    mismatches.append({
                        "key": key, "field": field,
                        "primary_value": pv, "reference_value": rv,
                    })
                    drift_detected = True

        plan.validate_status = "偏差告警" if mismatches else "一致"
        plan.last_result = json.dumps({
            "matched_keys": len(set(primary_by_key.keys()) & set(ref_by_key.keys())),
            "mismatch_count": len(mismatches),
            "drift_detected": drift_detected,
        }, ensure_ascii=False)

        result: dict[str, Any] = {
            "ok": True,
            "plan_code": plan.plan_code,
            "validate_status": plan.validate_status,
            "mismatch_count": len(mismatches),
            "drift_detected": drift_detected,
            "mismatches": mismatches[:20],
        }

        if drift_detected:
            drift_record = self.create_drift_record(
                wide_table_name=plan.primary_wide_table,
                drift_type="schema_mismatch",
                historical_baseline="baseline_" + _now_iso(),
                current_distribution="current_" + _now_iso(),
            )
            result["drift_record_code"] = drift_record.record_code

        return result

    def reconcile_partition_archive(self, partition_rule_id: int,
                                    cold_strategy_id: int) -> dict[str, Any]:
        """根据分区策略和冷热分层策略执行归档。

        将超出保留期的分区数据从冷存储转移到归档。
        """
        part_rule = self._partitions.get(partition_rule_id)
        cold_strat = self._cold_stores.get(cold_strategy_id)

        if part_rule is None or cold_strat is None:
            return {"ok": False, "message": "分区策略或冷热分层策略不存在"}

        eligible_snaps = [
            snap for snap in self._snapshots.values()
            if snap.target_wide_table == part_rule.target_wide_table
            and snap.status_mark == "已迁移至冷存储"
        ]

        archived = 0
        for snap in eligible_snaps:
            snap.status_mark = "已归档"
            archived += snap.record_count

        return {
            "ok": True,
            "archived_partitions": len(eligible_snaps),
            "archived_rows": archived,
            "archive_policy": part_rule.archive_policy,
        }

    def reconcile_watermark_with_fusion(self, job_id: int) -> dict[str, Any]:
        """协调水位线与融合作业的推进状态。

        水位延迟时推进受限，融合作业未完成时增量归并暂停。
        """
        job = self._fusion_jobs.get(job_id)
        if job is None:
            return {"ok": False, "message": f"融合作业 {job_id} 不存在"}

        related_watermarks = [
            wm for wm in self._watermarks.values()
            if wm.data_source in job.participant_sources
        ]

        stalled = [wm for wm in related_watermarks if wm.delay_minutes > 30]
        advancing = [wm for wm in related_watermarks if wm.delay_minutes <= 30]

        return {
            "ok": True,
            "job_code": job.job_code,
            "total_watermarks": len(related_watermarks),
            "advancing": len(advancing),
            "stalled": len(stalled),
            "can_proceed": len(stalled) == 0,
        }

    def build_wide_table_from_fusion(self, wide_table_id: int, fusion_job_id: int) -> dict[str, Any]:
        """从融合作业结果构建宽表定义，产生可核查的数据血缘边。

        目标校验是整条链路的前置条件：作业的 ``target_wide_table`` 必须与
        宽表定义一致（表编号或表名称任一匹配）。不匹配时直接拒绝，
        不写任何边、不改来源计数。
        """
        wt = self._wide_tables.get(wide_table_id)
        job = self._fusion_jobs.get(fusion_job_id)

        if wt is None:
            return {"ok": False, "message": f"宽表定义 {wide_table_id} 不存在"}
        if job is None:
            return {"ok": False, "message": f"融合作业 {fusion_job_id} 不存在"}

        target_check = self._check_job_target(wt, job)
        if not target_check["matched"]:
            return {
                "ok": False,
                "message": (
                    f"作业 {job.job_code} 的目标宽表「{job.target_wide_table}」"
                    f"与宽表定义「{wt.table_name}」（{wt.table_code}）不一致，拒绝构建"
                ),
                "target_check": target_check,
                "source_count": self._active_source_count(wt),
                "lineage_edges": [],
            }

        unknown_sources = [
            code for code in job.participant_sources
            if self._find_source_by_code(code) is None
        ]
        if unknown_sources:
            return {
                "ok": False,
                "message": f"作业 {job.job_code} 含未登记来源：{'、'.join(unknown_sources)}",
                "target_check": target_check,
                "source_count": self._active_source_count(wt),
                "lineage_edges": [],
            }

        wt.revision += 1
        wt.lineage_revision = wt.revision
        wt.last_updated = _now_iso()

        # 同一来源在同一修订里只能形成一条有效支持：按来源去重并保序。
        unique_sources = list(dict.fromkeys(job.participant_sources))
        edges: list[LineageEdge] = []
        for source_code in unique_sources:
            support_key = self._source_support_key(wt, job, source_code)
            existing = self._find_active_edge(support_key)
            if existing is not None:
                existing.related_fields = list(wt.field_list)
                existing.last_traced = _now_iso()
                existing.revision = wt.revision
                edges.append(existing)
                continue
            edge = self.create_lineage_edge(
                upstream_node=source_code,
                downstream_node=wt.table_name,
                related_fields=wt.field_list,
                edge_type="fusion_map",
                dependency_level=1,
            )
            edge.target_wide_table = wt.table_name
            edge.source_job_code = job.job_code
            edge.support_key = support_key
            edge.revision = wt.revision
            edge.confirmation = "已追踪"
            self._lineages[edge.edge_id] = edge
            edges.append(edge)

        wt.source_count = len(edges)
        detail = [self._edge_detail(edge) for edge in edges]
        return {
            "ok": True,
            "table_code": wt.table_code,
            "table_name": wt.table_name,
            "target_check": target_check,
            "source_count": wt.source_count,
            "lineage_edges": [edge.edge_code for edge in edges],
            "source_lineage_detail": detail,
            "revision": wt.revision,
            "last_updated": wt.last_updated,
        }

    def confirm_source_lineage(self, wide_table_id: int, source_codes: list[str],
                               operator: str = "值班") -> dict[str, Any]:
        """确认指定来源的血缘；未确认前不得据此发起重算领取。

        只能确认当前修订中的有效边。历史错配边只能查历史，无法被确认。
        """
        wt = self._wide_tables.get(wide_table_id)
        if wt is None:
            return {"ok": False, "message": f"宽表定义 {wide_table_id} 不存在"}

        active = self._active_edges_for(wt)
        by_source = {edge.upstream_node: edge for edge in active}
        missing = [code for code in source_codes if code not in by_source]
        if missing:
            return {
                "ok": False,
                "message": f"来源 {'、'.join(missing)} 没有当前有效血缘，无法确认",
                "source_count": len(active),
            }

        confirmed_at = _now_iso()
        for code in dict.fromkeys(source_codes):
            edge = by_source[code]
            edge.confirmation = "已确认"
            edge.confirmed_by = operator
            edge.confirmed_at = confirmed_at
            edge.last_traced = confirmed_at

        detail = [self.source_lineage_detail(wt.table_id)]
        return {
            "ok": True,
            "table_code": wt.table_code,
            "source_count": self._active_source_count(wt),
            "confirmed_sources": list(dict.fromkeys(source_codes)),
            "source_lineage_detail": detail[0]["sources"],
        }

    def source_lineage_detail(self, wide_table_id: int,
                              include_historical: bool = False) -> dict[str, Any]:
        """生成可核查的来源血缘明细：每条有效边对应一个来源的一次有效支持。"""
        wt = self._wide_tables.get(wide_table_id)
        if wt is None:
            return {"ok": False, "message": f"宽表定义 {wide_table_id} 不存在", "sources": []}

        active: dict[str, LineageEdge] = {}
        for edge in self._active_edges_for(wt):
            # 同一来源只保留最早的一条有效支持，重复配置不会放大来源数。
            active.setdefault(edge.upstream_node, edge)
        historical = self._historical_edges_for(wt)
        sources = [
            {
                "source": edge.upstream_node,
                "support_key": edge.support_key or self._source_support_key(wt, None, edge.upstream_node),
                "edge_code": edge.edge_code,
                "revision": edge.revision,
                "confirmation": edge.confirmation,
                "source_job_code": edge.source_job_code,
                "evidence": edge.to_evidence(),
            }
            for edge in active.values()
        ]
        payload: dict[str, Any] = {
            "ok": True,
            "table_code": wt.table_code,
            "table_name": wt.table_name,
            "revision": wt.lineage_revision,
            "source_count": len(sources),
            "sources": sources,
        }
        if include_historical:
            payload["historical"] = [edge.to_evidence() for edge in historical]
        return payload

    def historical_lineage(self, wide_table_id: int) -> dict[str, Any]:
        """历史版本的原证据仍须可查：错配边只从这里读取。"""
        wt = self._wide_tables.get(wide_table_id)
        if wt is None:
            return {"ok": False, "message": f"宽表定义 {wide_table_id} 不存在", "historical": []}
        historical = [edge.to_evidence() for edge in self._historical_edges_for(wt)]
        return {
            "ok": True,
            "table_code": wt.table_code,
            "historical_count": len(historical),
            "historical": historical,
        }

    def _check_job_target(self, wt: WideTableDef, job: FusionJob) -> dict[str, Any]:
        job_target = str(job.target_wide_table or "").strip()
        matched = job_target in {wt.table_code, wt.table_name}
        return {
            "matched": matched,
            "job_target": job_target,
            "expected_table_code": wt.table_code,
            "expected_table_name": wt.table_name,
        }

    @staticmethod
    def _source_support_key(wt: WideTableDef, job: FusionJob | None, source_code: str) -> str:
        job_code = job.job_code if job is not None else ""
        return f"{wt.table_code}::{source_code}"

    def _find_active_edge(self, support_key: str) -> LineageEdge | None:
        for edge in self._lineages.values():
            if edge.support_key == support_key and edge.is_active():
                return edge
        return None

    @staticmethod
    def _edge_belongs_to(edge: LineageEdge, wt: WideTableDef) -> bool:
        """边归属按宽表表名称或表编号匹配，避免历史数据里的写法差异漏判。"""
        return edge.target_table() in {wt.table_name, wt.table_code}

    def _active_edges_for(self, wt: WideTableDef) -> list[LineageEdge]:
        """当前修订的有效边：必须带构建凭据（support_key），手工边不冒充来源。"""
        edges = [
            edge for edge in self._lineages.values()
            if edge.support_key
            and edge.is_active()
            and self._edge_belongs_to(edge, wt)
        ]
        return sorted(edges, key=lambda edge: edge.edge_id)

    def _active_source_count(self, wt: WideTableDef) -> int:
        return len({edge.upstream_node for edge in self._active_edges_for(wt)})

    def _historical_edges_for(self, wt: WideTableDef) -> list[LineageEdge]:
        edges = [
            edge for edge in self._lineages.values()
            if edge.historical and self._edge_belongs_to(edge, wt)
        ]
        return sorted(edges, key=lambda edge: edge.edge_id)

    @staticmethod
    def _edge_detail(edge: LineageEdge) -> dict[str, Any]:
        return {
            "source": edge.upstream_node,
            "edge_code": edge.edge_code,
            "confirmation": edge.confirmation,
            "revision": edge.revision,
            "evidence": edge.to_evidence(),
        }

    def compute_metric_from_lineage(self, lineage_edge_id: int,
                                    target_wide_table: str,
                                    agg_function: str,
                                    group_dimensions: list[str]) -> dict[str, Any]:
        """基于数据血缘边计算指标聚合规则。"""
        edge = self._lineages.get(lineage_edge_id)
        if edge is None:
            return {"ok": False, "message": f"数据血缘边 {lineage_edge_id} 不存在"}
        if edge.historical:
            return {
                "ok": False,
                "message": f"血缘边 {edge.edge_code} 是历史错配边，只能查询历史，不能用于确认新指标",
                "edge": edge.to_evidence(),
            }

        rule = self.create_metric_aggr_rule(
            target_wide_table=target_wide_table,
            agg_function=agg_function,
            group_dimensions=group_dimensions,
        )
        rule.last_computed = _now_iso()

        return {
            "ok": True,
            "metric_rule_code": rule.rule_code,
            "from_lineage": edge.edge_code,
            "agg_function": agg_function,
            "group_dimensions": group_dimensions,
            "lineage_evidence": edge.to_evidence(),
        }

    # ---- 来源修订、重算领取、物化复核与采购交付 ----

    def start_source_revision(self, wide_table_id: int, source_codes: list[str],
                              operator: str = "值班") -> dict[str, Any]:
        """开启 B 的来源修订交接：把移出配置的旧边留作历史证据，新边待确认。

        修订期间任何重算领取都会先做来源核验，未确认的合法来源不会被当作支持。
        """
        wt = self._wide_tables.get(wide_table_id)
        if wt is None:
            return {"ok": False, "message": f"宽表定义 {wide_table_id} 不存在"}
        wanted = list(dict.fromkeys(source_codes))
        wt.revision += 1
        wt.lineage_revision = wt.revision
        wt.last_updated = _now_iso()

        historical: list[str] = []
        pending: list[str] = []
        for edge in self._active_edges_for(wt):
            if edge.upstream_node not in wanted:
                edge.historical = True
                edge.confirmation = "历史"
                edge.invalid_reason = "来源修订移出当前配置"
                historical.append(edge.edge_code)
            else:
                edge.confirmation = "待确认"
                edge.confirmed_by = ""
                edge.confirmed_at = ""
                edge.revision = wt.revision
                pending.append(edge.edge_code)

        return {
            "ok": True,
            "table_code": wt.table_code,
            "revision": wt.revision,
            "operator": operator,
            "pending_confirmation": pending,
            "moved_to_history": historical,
            "source_count": self._active_source_count(wt),
        }

    def invalidate_lineage_edge(self, edge_code: str, reason: str) -> dict[str, Any]:
        """把错配边标记为历史：保留首次记录的原证据，不再产生任何有效支持。"""
        for edge in self._lineages.values():
            if edge.edge_code == edge_code:
                if edge.historical:
                    return {
                        "ok": True,
                        "already_historical": True,
                        "edge": edge.to_evidence(),
                    }
                edge.historical = True
                edge.confirmation = "历史"
                edge.invalid_reason = reason or "目标错配"
                return {"ok": True, "already_historical": False, "edge": edge.to_evidence()}
        return {"ok": False, "message": f"血缘边 {edge_code} 不存在"}

    def record_historical_lineage(self, upstream_node: str, downstream_node: str,
                                  target_wide_table: str = "",
                                  source_job_code: str = "",
                                  related_fields: list[str] | None = None,
                                  invalid_reason: str = "历史版本错配边") -> dict[str, Any]:
        """导入修复前已存在的错配边：直接以历史证据入库，只可查询、不可使用。"""
        edge = self.create_lineage_edge(
            upstream_node=upstream_node,
            downstream_node=downstream_node,
            related_fields=related_fields or [],
            edge_type="fusion_map",
            dependency_level=1,
        )
        edge.target_wide_table = target_wide_table or downstream_node
        edge.source_job_code = source_job_code
        edge.support_key = f"{edge.target_wide_table}::{upstream_node}"
        edge.confirmation = "历史"
        edge.historical = True
        edge.invalid_reason = invalid_reason
        self._lineages[edge.edge_id] = edge
        return {"ok": True, "edge": edge.to_evidence()}

    def submit_recompute(self, target_wide_table: str, source_codes: list[str],
                         lineage_edge_ids: list[int] | None = None,
                         cached_trial_amount: float = 0.0,
                         accepted_before_fix: bool = False,
                         revision: int = 0) -> RecomputeJob:
        """登记一次重算。修复前受理的作业必须带 ``accepted_before_fix``。"""
        rid = self._next_seq()
        job = RecomputeJob(
            recompute_id=rid,
            job_code=_generate_code("RECO", rid),
            target_wide_table=target_wide_table,
            source_codes=list(dict.fromkeys(source_codes)),
            lineage_edge_ids=list(lineage_edge_ids or []),
            cached_trial_amount=cached_trial_amount,
            status="待领",
            accepted_before_fix=accepted_before_fix,
            revision=revision,
        )
        self._recomputes[rid] = job
        return job

    def release_recompute(self, recompute_id: int, reason: str = "执行超时") -> dict[str, Any]:
        """旧重算超时后重新回到夜班待领集合，试算缓存原样保留。

        回到待领不代表具备写入资格：领取时仍会重新做来源核验。
        """
        job = self._recomputes.get(recompute_id)
        if job is None:
            return {"ok": False, "message": f"重算作业 {recompute_id} 不存在"}
        job.status = "待领"
        job.claim_token = ""
        job.claimed_by = ""
        job.claimed_at = ""
        job.eligible_for_purchase = False
        job.audit_log.append({
            "action": "returned_to_pending",
            "reason": reason,
            "at": _now_iso(),
            "cached_trial_amount": job.cached_trial_amount,
        })
        return {
            "ok": True,
            "status": job.status,
            "cached_trial_amount": job.cached_trial_amount,
            "message": f"重算作业 {job.job_code} 已回到待领集合：{reason}",
            "job": job.to_summary(),
        }

    def get_recompute(self, recompute_id: int) -> RecomputeJob | None:
        return self._recomputes.get(recompute_id)

    def claim_recompute(self, recompute_id: int, worker: str = "夜班",
                        claim_token: str = "") -> dict[str, Any]:
        """领取重算：先核验来源血缘，再决定送复核还是保留待核。

        领取本身可能因来源核验与旧作业恢复发生冲突：
        - 修复前受理的旧作业，即便被领走也只进复核，不具备写入资格；
        - 合法来源尚未确认时，领取不生效，作业保留在待领集合并记录依据；
        - 历史错配边不会成为任何领取的依据。
        """
        job = self._recomputes.get(recompute_id)
        if job is None:
            return {"ok": False, "message": f"重算作业 {recompute_id} 不存在", "claimed": False}

        token = claim_token or f"CLAIM-{job.job_code}-{job.claim_sequence + 1}"
        if job.status not in {"待领", "待重放"}:
            same_claim = bool(job.claim_token) and job.claim_token == token
            return {
                "ok": same_claim,
                "claimed": False,
                "duplicate": same_claim,
                "status": job.status,
                "message": (
                    f"重算作业 {job.job_code} 已被领取（{job.claimed_by}）"
                    if not same_claim else
                    f"重算作业 {job.job_code} 的领取已受理，未重复生效"
                ),
                "job": job.to_summary(),
            }

        verification = self._verify_recompute_sources(job)
        job.evidence = verification["evidence"]
        job.verification = verification
        conflict = self._claim_conflict(job, verification)

        if conflict:
            job.audit_log.append({
                "action": "claim_conflict",
                "worker": worker,
                "at": _now_iso(),
                "conflict": conflict,
                "verification": verification["status"],
            })
            if job.accepted_before_fix:
                # 旧作业恢复优先于来源切换：只送复核，不能写入，也不能作为采购依据。
                job.claim_sequence += 1
                job.claim_token = token
                job.claimed_by = worker
                job.claimed_at = _now_iso()
                job.status = "复核中"
                job.review_reason = conflict
                job.eligible_for_purchase = False
                return {
                    "ok": True,
                    "claimed": True,
                    "status": job.status,
                    "route": "复核",
                    "eligible_for_purchase": False,
                    "message": f"旧作业 {job.job_code} 已进入物化复核，不参与写入与采购",
                    "evidence": job.evidence,
                    "verification": job.verification,
                    "job": job.to_summary(),
                }
            # 合法来源未确认：领取不生效，保留待领等待来源核验完成。
            job.audit_log.append({
                "action": "claim_deferred",
                "worker": worker,
                "at": _now_iso(),
                "reason": conflict,
            })
            return {
                "ok": False,
                "claimed": False,
                "status": job.status,
                "route": "待领",
                "eligible_for_purchase": False,
                "message": f"来源核验未通过，重算作业 {job.job_code} 保留待领",
                "evidence": job.evidence,
                "verification": job.verification,
                "job": job.to_summary(),
            }

        job.claim_sequence += 1
        job.claim_token = token
        job.claimed_by = worker
        job.claimed_at = _now_iso()
        job.status = "已领取"
        job.eligible_for_purchase = True
        return {
            "ok": True,
            "claimed": True,
            "status": job.status,
            "route": "物化",
            "eligible_for_purchase": True,
            "message": f"重算作业 {job.job_code} 来源核验通过，进入物化",
            "evidence": job.evidence,
            "verification": job.verification,
            "job": job.to_summary(),
        }

    def materialize_recompute(self, recompute_id: int, claim_token: str) -> dict[str, Any]:
        """物化阶段：只有复核通过且具备确认依据的领取才能写出聚合行。

        修复前受理的旧重算始终停在复核，即便重复送达写入请求也不会写出结果。
        """
        job = self._recomputes.get(recompute_id)
        if job is None:
            return {"ok": False, "message": f"重算作业 {recompute_id} 不存在", "written": 0}

        if job.status == "复核中":
            return {
                "ok": True,
                "written": 0,
                "review_only": True,
                "message": f"旧作业 {job.job_code} 只允许物化复核，未写出任何聚合行",
                "cached_trial_amount": job.cached_trial_amount,
                "evidence": job.evidence,
                "verification": job.verification,
                "job": job.to_summary(),
            }
        if job.status not in {"已领取", "已物化"}:
            return {
                "ok": False,
                "written": 0,
                "message": f"重算作业 {job.job_code} 当前状态 {job.status} 不允许物化",
                "job": job.to_summary(),
            }
        if not claim_token or claim_token != job.claim_token:
            return {
                "ok": False,
                "written": 0,
                "message": f"重算作业 {job.job_code} 的领取凭据不匹配",
                "job": job.to_summary(),
            }

        rows_after = self.materialize_metric_rows(job)
        job.status = "已物化"
        job.materialized_rows = rows_after
        return {
            "ok": True,
            "written": len(rows_after),
            "review_only": False,
            "message": f"重算作业 {job.job_code} 已按确认来源写出 {len(rows_after)} 行聚合",
            "aggregate_rows": rows_after,
            "evidence": job.evidence,
            "verification": job.verification,
            "job": job.to_summary(),
        }

    def submit_write_request(self, recompute_id: int, request_id: str,
                             claim_token: str = "") -> dict[str, Any]:
        """重送写入请求：按 request_id 幂等，重复请求不会二次写出。"""
        job = self._recomputes.get(recompute_id)
        if job is None:
            return {"ok": False, "message": f"重算作业 {recompute_id} 不存在", "written": 0}
        if not request_id:
            return {"ok": False, "message": "写入请求缺少 request_id", "written": 0}

        prior = next(
            (item for item in job.write_requests if item.get("request_id") == request_id),
            None,
        )
        if prior is not None:
            accepted = prior.get("action") == "written"
            return {
                "ok": accepted,
                "duplicate": True,
                "written": 0,
                "message": (
                    f"写入请求 {request_id} 已写出过，本次未重复写出"
                    if accepted else
                    f"写入请求 {request_id} 已被拒绝，重复送达仍不写出"
                ),
                "evidence": job.evidence,
                "verification": job.verification,
                "job": job.to_summary(),
            }

        if job.accepted_before_fix or job.status == "复核中":
            job.write_requests.append({
                "request_id": request_id,
                "action": "rejected",
                "reason": "修复前受理，仅供物化复核",
                "at": _now_iso(),
            })
            return {
                "ok": False,
                "duplicate": False,
                "written": 0,
                "message": f"旧作业 {job.job_code} 的写入请求被拒绝：只允许复核，不能写出结果",
                "evidence": job.evidence,
                "verification": job.verification,
                "job": job.to_summary(),
            }

        materialize = self.materialize_recompute(recompute_id, claim_token)
        row = {
            "request_id": request_id,
            "action": "written" if materialize.get("written") else "no_write",
            "written": materialize.get("written", 0),
            "at": _now_iso(),
        }
        job.write_requests.append(row)
        return {
            "ok": bool(materialize.get("ok")),
            "duplicate": False,
            "written": materialize.get("written", 0),
            "message": materialize.get("message", ""),
            "aggregate_rows": materialize.get("aggregate_rows", []),
            "evidence": job.evidence,
            "verification": job.verification,
            "job": job.to_summary(),
        }

    def procurement_replenishment_detail(self, recompute_id: int) -> dict[str, Any]:
        """读取补货明细：只返回有确认依据且已物化的聚合行，试算金额永不交付。"""
        job = self._recomputes.get(recompute_id)
        if job is None:
            return {"ok": False, "message": f"重算作业 {recompute_id} 不存在", "rows": []}
        if job.cached_trial_amount:
            trial_excluded = job.cached_trial_amount
        else:
            trial_excluded = None

        blocked = self._procurement_block_reason(job)
        if blocked or not job.materialized_rows:
            return {
                "ok": True,
                "eligible": False,
                "rows": [],
                "trial_amount_excluded": trial_excluded,
                "message": blocked or "没有可交付的聚合行",
                "evidence": job.evidence,
                "verification": job.verification,
            }
        rows = [
            row for row in job.materialized_rows
            if row.get("eligible_for_purchase") is True
        ]
        return {
            "ok": True,
            "eligible": bool(rows),
            "rows": rows,
            "trial_amount_excluded": trial_excluded,
            "message": f"交付 {len(rows)} 行有确认依据的聚合明细",
            "evidence": job.evidence,
            "verification": job.verification,
        }

    def execute_procurement_export_from_snapshot(self, export_id: int,
                                                 snapshot_id: int) -> dict[str, Any]:
        """采购快照导出：只读聚合行，只交付来源已确认且已物化的结果。"""
        plan = self._exports.get(export_id)
        snap = self._snapshots.get(snapshot_id)
        if plan is None:
            return {"ok": False, "message": f"导出计划 {export_id} 不存在", "rows": []}
        if snap is None:
            return {"ok": False, "message": f"分区快照 {snapshot_id} 不存在", "rows": []}
        if plan.target_wide_table != snap.target_wide_table:
            return {"ok": False, "message": "导出计划与快照目标宽表不一致", "rows": []}

        blocked = snap.procurement_block_reason
        if not snap.procurement_eligible or blocked:
            return {
                "ok": False,
                "eligible": False,
                "rows": [],
                "message": blocked or "快照没有确认依据，不能交付采购",
                "snapshot": self._snapshot_evidence(snap),
            }

        rows = [
            row for row in snap.aggregate_rows
            if row.get("eligible_for_purchase") is True
        ]
        if not rows:
            return {
                "ok": False,
                "eligible": False,
                "rows": [],
                "message": "快照内没有具备确认依据的聚合行",
                "snapshot": self._snapshot_evidence(snap),
            }
        plan.export_status = "已完成"
        return {
            "ok": True,
            "eligible": True,
            "plan_code": plan.plan_code,
            "snapshot_code": snap.partition_code,
            "exported_rows": len(rows),
            "rows": rows,
            "lineage_evidence": snap.lineage_evidence,
            "snapshot": self._snapshot_evidence(snap),
        }

    def materialize_metric_rows(self, job: RecomputeJob) -> list[dict[str, Any]]:
        """按来源血缘明细约束真正写入的聚合行。

        只用当前修订中已确认、非历史、且来自本作业来源的边；
        同一来源只计一次支持，试算金额不进入聚合。
        """
        wt = self._find_wide_table(job.target_wide_table)
        if wt is None:
            return []
        confirmed = [
            edge for edge in self._active_edges_for(wt)
            if edge.is_confirmed() and edge.upstream_node in set(job.source_codes)
        ]
        if job.lineage_edge_ids:
            declared = set(job.lineage_edge_ids)
            confirmed = [edge for edge in confirmed if edge.edge_id in declared]
        # 明细按来源去重：一条来源只对应一个有效支持。
        unique: dict[str, LineageEdge] = {}
        for edge in confirmed:
            unique.setdefault(edge.upstream_node, edge)
        if not unique:
            return []

        rows: list[dict[str, Any]] = []
        for source_code, edge in unique.items():
            rows.append({
                "source": source_code,
                "edge_code": edge.edge_code,
                "revision": edge.revision,
                "confirmation": edge.confirmation,
                "target_wide_table": wt.table_name,
                "value": self._source_sample_value(source_code),
                "eligible_for_purchase": True,
                "evidence": edge.to_evidence(),
            })
        return rows

    # ---- 辅助方法 ----

    def _find_wide_table(self, table_ref: str) -> WideTableDef | None:
        for wt in self._wide_tables.values():
            if table_ref in {wt.table_code, wt.table_name}:
                return wt
        return None

    def _verify_recompute_sources(self, job: RecomputeJob) -> dict[str, Any]:
        """核验重算来源：目标必须存在，来源必须落在当前修订的有效边内。"""
        wt = self._find_wide_table(job.target_wide_table)
        if wt is None:
            return {
                "status": "目标宽表不存在",
                "evidence": [],
                "confirmed_sources": [],
                "unconfirmed_sources": list(job.source_codes),
            }
        active = self._active_edges_for(wt)
        by_source = {edge.upstream_node: edge for edge in active}
        confirmed_sources = [
            code for code in job.source_codes
            if code in by_source and by_source[code].is_confirmed()
        ]
        unconfirmed_sources = [
            code for code in job.source_codes
            if code in by_source and not by_source[code].is_confirmed()
        ]
        missing_sources = [code for code in job.source_codes if code not in by_source]
        evidence: list[dict[str, Any]] = []
        for code in job.source_codes:
            edge = by_source.get(code)
            evidence.append({
                "source": code,
                "edge_code": edge.edge_code if edge else "",
                "confirmation": edge.confirmation if edge else "无有效血缘",
                "revision": edge.revision if edge else wt.lineage_revision,
                "historical": edge.historical if edge else False,
                "verdict": (
                    "已确认" if edge and edge.is_confirmed()
                    else "待确认" if edge
                    else "无有效血缘"
                ),
            })
        historical_refs = [
            edge.to_evidence() for edge in self._historical_edges_for(wt)
        ]
        declared_edges: list[dict[str, Any]] = []
        invalid_declared: list[str] = []
        for edge_id in job.lineage_edge_ids:
            edge = self._lineages.get(edge_id)
            if edge is None:
                invalid_declared.append(f"血缘边 {edge_id} 不存在")
                continue
            usable = (
                edge.is_active()
                and self._edge_belongs_to(edge, wt)
                and edge.upstream_node in set(job.source_codes)
            )
            declared_edges.append({
                "edge_id": edge_id,
                "edge_code": edge.edge_code,
                "verdict": "可用" if usable else "不可用",
                "historical": edge.historical,
                "invalid_reason": edge.invalid_reason,
            })
            if not usable:
                invalid_declared.append(
                    f"血缘边 {edge.edge_code} 不能用于本次重算"
                )
        status = "已确认" if len(confirmed_sources) == len(job.source_codes) else "待确认"
        if missing_sources or invalid_declared:
            status = "来源缺失"
        return {
            "status": status,
            "revision": wt.lineage_revision,
            "confirmed_sources": confirmed_sources,
            "unconfirmed_sources": unconfirmed_sources,
            "missing_sources": missing_sources,
            "declared_edges": declared_edges,
            "invalid_declared_edges": invalid_declared,
            "evidence": evidence,
            "historical_evidence": historical_refs,
        }

    def _claim_conflict(self, job: RecomputeJob, verification: dict[str, Any]) -> str:
        if job.accepted_before_fix:
            return "修复前受理的重算：只允许进入物化复核"
        if verification["missing_sources"]:
            return f"来源 {'、'.join(verification['missing_sources'])} 没有当前有效血缘"
        if verification.get("invalid_declared_edges"):
            return "；".join(verification["invalid_declared_edges"])
        if verification["unconfirmed_sources"]:
            return f"来源 {'、'.join(verification['unconfirmed_sources'])} 尚未确认"
        return ""

    def _procurement_block_reason(self, job: RecomputeJob) -> str:
        if job.cached_trial_amount:
            return "仅含旧试算缓存，不能作为采购依据"
        if job.accepted_before_fix:
            return "修复前受理的重算不能作为有资格的数据"
        if job.status != "已物化":
            return "重算尚未物化"
        if not job.eligible_for_purchase:
            return "重算没有采购交付资格"
        return ""

    @staticmethod
    def _source_sample_value(source_code: str) -> float:
        """来源级样本值，用于验证聚合行的真实来源（不读取任何试算缓存）。"""
        return float(sum(ord(ch) for ch in source_code) % 97 + 1)

    @staticmethod
    def _snapshot_evidence(snap: SnapshotPartition) -> dict[str, Any]:
        return {
            "partition_code": snap.partition_code,
            "target_wide_table": snap.target_wide_table,
            "recompute_id": snap.recompute_id,
            "lineage_revision": snap.lineage_revision,
            "source_codes": list(snap.source_codes),
            "procurement_eligible": snap.procurement_eligible,
            "procurement_block_reason": snap.procurement_block_reason,
        }

    def create_recompute_snapshot(self, recompute_id: int,
                                  partition_key_value: str) -> dict[str, Any]:
        """为重算结果创建分区快照，并把聚合行与真实血缘依据一并固化。"""
        job = self._recomputes.get(recompute_id)
        if job is None:
            return {"ok": False, "message": f"重算作业 {recompute_id} 不存在"}
        wt = self._find_wide_table(job.target_wide_table)
        if wt is None:
            return {"ok": False, "message": f"目标宽表 {job.target_wide_table} 不存在"}

        snap = self.create_snapshot(wt.table_name, partition_key_value=partition_key_value)
        snap.recompute_id = job.recompute_id
        snap.lineage_revision = wt.lineage_revision
        snap.source_codes = list(job.source_codes)
        snap.aggregate_rows = list(job.materialized_rows)
        snap.lineage_evidence = [
            edge.to_evidence() for edge in self._active_edges_for(wt)
            if edge.is_confirmed() and edge.upstream_node in set(job.source_codes)
        ]
        block = self._procurement_block_reason(job)
        snap.procurement_eligible = not block and bool(snap.aggregate_rows)
        snap.procurement_block_reason = block
        snap.record_count = len(snap.aggregate_rows)
        return {
            "ok": True,
            "snapshot": self._snapshot_evidence(snap),
            "partition_id": snap.partition_id,
            "partition_code": snap.partition_code,
            "aggregate_rows": snap.aggregate_rows,
            "lineage_evidence": snap.lineage_evidence,
        }

    def _find_source_by_code(self, source_code: str) -> SourceDef | None:
        for source in self._sources.values():
            if source.source_code == source_code:
                return source
        return None

    def _find_fusion_job_by_table(self, wide_table_name: str) -> FusionJob | None:
        for job in self._fusion_jobs.values():
            if job.target_wide_table == wide_table_name:
                return job
        return None

    def _simulate_source_read(self, source: SourceDef) -> list[dict[str, Any]]:
        """模拟从数据源读取样本记录。"""
        return [
            {"id": f"{source.source_code}-rec-1", "name": f"{source.source_name}-rec1",
             "value": 100.0, "update_time": _now_iso()},
            {"id": f"{source.source_code}-rec-2", "name": f"{source.source_name}-rec2",
             "value": 200.0, "update_time": _now_iso()},
            {"id": f"{source.source_code}-rec-3", "name": f"{source.source_name}-rec3",
             "value": 300.0, "update_time": _now_iso()},
        ]

    def _apply_schema_mappings(self, records: list[dict[str, Any]],
                               target_wide_table: str) -> list[dict[str, Any]]:
        """应用 Schema 映射：对关联到目标宽表的映射方案进行字段转换。"""
        relevant = [
            m for m in self._schemas.values()
            if m.target_wide_table == target_wide_table
        ]
        if not relevant:
            return list(records)

        mapped: list[dict[str, Any]] = []
        for rec in records:
            source_table = rec.get("_source", "")
            applicable = [m for m in relevant if m.source_table == source_table]
            if applicable:
                new_rec = dict(rec)
                for mapping in applicable:
                    try:
                        field_map = json.loads(mapping.field_mapping_json)
                        for src_field, tgt_field in field_map.items():
                            if src_field in new_rec:
                                new_rec[tgt_field] = new_rec.pop(src_field)
                    except (json.JSONDecodeError, KeyError):
                        pass
                mapped.append(new_rec)
            else:
                mapped.append(rec)
        return mapped

    def _apply_conflict_resolution(self, records: list[dict[str, Any]],
                                   target_wide_table: str) -> dict[str, Any]:
        """应用冲突仲裁规则到融合记录。"""
        rules = [
            r for r in self._conflict_rules.values()
            if r.target_wide_table == target_wide_table and r.rule_status == "生效中"
        ]
        if not rules:
            return {"applied": False, "reason": "无生效的冲突规则", "resolved": records}

        resolved = list(records)
        all_conflicts: list[dict[str, Any]] = []
        for rule in rules:
            resolved, conflicts = rule.resolve(resolved)
            rule.conflict_count = len(conflicts)
            all_conflicts.extend(conflicts)

        return {
            "applied": True,
            "rules_used": len(rules),
            "conflicts_found": len(all_conflicts),
            "conflicts": all_conflicts[:50],
            "resolved": resolved,
        }

    def _apply_quality_gates(self, records: list[dict[str, Any]],
                             target_wide_table: str) -> dict[str, Any]:
        """应用质量门禁检查。"""
        gates = [
            g for g in self._quality_gates.values()
            if g.target_wide_table == target_wide_table
        ]
        if not gates:
            return {"passed": True, "gates_checked": 0, "violations": []}

        violations: list[dict[str, Any]] = []
        for gate in gates:
            gate.checked_rows = len(records)
            if gate.check_type == "row_count_min" and len(records) < int(gate.threshold_condition):
                violations.append({"rule_code": gate.rule_code, "check": "row_count_min",
                                   "actual": len(records), "threshold": gate.threshold_condition})
            elif gate.check_type == "null_check":
                null_count = sum(
                    1 for rec in records
                    if rec.get(gate.threshold_condition) is None
                )
                if null_count > 0:
                    violations.append({"rule_code": gate.rule_code, "check": "null_check",
                                       "null_count": null_count})

        passed = len(violations) == 0
        return {"passed": passed, "gates_checked": len(gates), "violations": violations}

    @staticmethod
    def _make_merge_key(record: dict[str, Any], join_key: str) -> str:
        return f"{record.get('_source', '_')}#{record.get(join_key, '_')}"

    def domain_snapshot(self) -> dict[str, Any]:
        """输出引擎全局数据质量快照。"""
        return {
            "sources": len(self._sources),
            "schemas": len(self._schemas),
            "wide_tables": len(self._wide_tables),
            "fusion_jobs": len(self._fusion_jobs),
            "conflict_rules": len(self._conflict_rules),
            "quality_gates": len(self._quality_gates),
            "snapshots": len(self._snapshots),
            "backfills": len(self._backfills),
            "cold_stores": len(self._cold_stores),
            "lineages": len(self._lineages),
            "metrics": len(self._metrics),
            "partitions": len(self._partitions),
            "watermarks": len(self._watermarks),
            "exports": len(self._exports),
            "cross_validates": len(self._cross_validates),
            "drifts": len(self._drifts),
            "monitors": len(self._monitors),
            "recomputes": len(self._recomputes),
            "historical_lineages": sum(
                1 for edge in self._lineages.values() if edge.historical),
        }


# 全局单例供测试和跨模块消费
fusion_engine = FusionEngine()