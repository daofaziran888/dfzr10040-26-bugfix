"""恢复与重放：断点续传、中断重放与增量确认机制。

与重放队列联动，融合作业失败后在队列中记录，
按退避策略重试，支持断点恢复。
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ReplayEntry:
    """重放队列条目：记录失败融合片段与重试状态。"""
    entry_id: int
    record_code: str
    fusion_job: str
    failure_reason: str
    retry_count: int = 0
    backoff_strategy: str = "exponential"
    max_retries: int = 5
    last_retry: str = ""
    queue_status: str = "待重放"
    checkpoint_data: dict[str, Any] = field(default_factory=dict)

    def can_retry(self) -> bool:
        return self.retry_count < self.max_retries

    def next_backoff_seconds(self) -> int:
        if self.backoff_strategy == "exponential":
            return min(2 ** (self.retry_count + 1), 300)
        elif self.backoff_strategy == "linear":
            return min((self.retry_count + 1) * 10, 300)
        return 60


class ReplayQueue:
    """重放队列管理器：管理融合失败数据的自动重放。"""

    def __init__(self) -> None:
        self._entries: dict[int, ReplayEntry] = {}
        self._next_id: int = 1

    def enqueue(self, fusion_job_code: str, failure_reason: str,
                checkpoint_data: dict[str, Any] | None = None,
                backoff_strategy: str = "exponential",
                max_retries: int = 5) -> ReplayEntry:
        sid = self._next_id
        self._next_id += 1
        entry = ReplayEntry(
            entry_id=sid,
            record_code=f"REPL-{sid:04d}",
            fusion_job=fusion_job_code,
            failure_reason=failure_reason,
            retry_count=0,
            backoff_strategy=backoff_strategy,
            max_retries=max_retries,
            last_retry="",
            queue_status="待重放",
            checkpoint_data=checkpoint_data or {},
        )
        self._entries[sid] = entry
        return entry

    def dequeue(self, entry_id: int) -> ReplayEntry | None:
        return self._entries.pop(entry_id, None)

    def get_entry(self, entry_id: int) -> ReplayEntry | None:
        return self._entries.get(entry_id)

    def mark_retrying(self, entry_id: int) -> ReplayEntry | None:
        entry = self._entries.get(entry_id)
        if entry is None:
            return None
        entry.queue_status = "重放中"
        entry.last_retry = _now_iso()
        return entry

    def claim(self, entry_id: int, worker: str = "夜班") -> dict[str, Any]:
        """原子领取待领条目：超时回队的旧作业也只能被领取一次。"""
        entry = self._entries.get(entry_id)
        if entry is None:
            return {"ok": False, "claimed": False, "message": "重放记录不存在"}
        if entry.queue_status != "待重放":
            return {
                "ok": False,
                "claimed": False,
                "duplicate": entry.queue_status == "重放中",
                "status": entry.queue_status,
                "message": f"重放记录 {entry.record_code} 当前不可领取",
            }
        entry.queue_status = "重放中"
        entry.last_retry = _now_iso()
        entry.checkpoint_data = {
            **entry.checkpoint_data,
            "claimed_by": worker,
            "claim_token": f"{entry.record_code}-{entry.retry_count + 1}",
            "claimed_at": entry.last_retry,
        }
        return {
            "ok": True,
            "claimed": True,
            "status": entry.queue_status,
            "claim_token": entry.checkpoint_data["claim_token"],
            "entry": entry,
        }

    def reenter_pending(self, entry_id: int, reason: str = "执行超时") -> ReplayEntry | None:
        """超时的旧作业重新进入待领集合，已缓存的试算数据原样保留。"""
        entry = self._entries.get(entry_id)
        if entry is None:
            return None
        entry.queue_status = "待重放"
        entry.failure_reason = reason
        entry.last_retry = _now_iso()
        return entry

    def mark_success(self, entry_id: int) -> ReplayEntry | None:
        entry = self._entries.get(entry_id)
        if entry is None:
            return None
        entry.queue_status = "已恢复"
        return entry

    def mark_permanent_skip(self, entry_id: int) -> ReplayEntry | None:
        entry = self._entries.get(entry_id)
        if entry is None:
            return None
        entry.queue_status = "永久跳过"
        return entry

    def increment_retry(self, entry_id: int,
                        new_failure_reason: str = "") -> ReplayEntry | None:
        entry = self._entries.get(entry_id)
        if entry is None:
            return None
        entry.retry_count += 1
        if new_failure_reason:
            entry.failure_reason = new_failure_reason
        entry.last_retry = _now_iso()
        if not entry.can_retry():
            entry.queue_status = "永久跳过"
        else:
            entry.queue_status = "待重放"
        return entry

    @property
    def pending_entries(self) -> list[ReplayEntry]:
        return [e for e in self._entries.values() if e.queue_status == "待重放"]

    @property
    def all_entries(self) -> list[ReplayEntry]:
        return list(self._entries.values())

    def domain_snapshot(self) -> dict[str, Any]:
        entries = self.all_entries
        by_status = {
            status: sum(1 for e in entries if e.queue_status == status)
            for status in ["待重放", "重放中", "已恢复", "永久跳过"]
        }
        return {
            "module": "replay_queue",
            "entity": "重放记录",
            "count": len(entries),
            "by_status": by_status,
            "pending": by_status.get("待重放", 0),
            "permanently_skipped": by_status.get("永久跳过", 0),
        }


class CheckpointManager:
    """检查点管理器：维护回填任务的断点位置与恢复元数据。"""

    def __init__(self) -> None:
        self._checkpoints: dict[str, dict[str, Any]] = {}
        self._recovery_log: list[dict[str, Any]] = []

    def save_checkpoint(self, task_code: str, position: str,
                        metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        checkpoint = {
            "task_code": task_code,
            "position": position,
            "saved_at": _now_iso(),
            "metadata": metadata or {},
        }
        self._checkpoints[task_code] = checkpoint
        return dict(checkpoint)

    def load_checkpoint(self, task_code: str) -> dict[str, Any] | None:
        checkpoint = self._checkpoints.get(task_code)
        if checkpoint is None:
            return None
        return dict(checkpoint)

    def clear_checkpoint(self, task_code: str) -> bool:
        if task_code in self._checkpoints:
            self._recovery_log.append({
                "task_code": task_code,
                "action": "cleared",
                "at": _now_iso(),
                "former_position": self._checkpoints[task_code].get("position"),
            })
            del self._checkpoints[task_code]
            return True
        return False

    def record_recovery(self, task_code: str, action: str,
                        result: dict[str, Any] | None = None) -> None:
        self._recovery_log.append({
            "task_code": task_code,
            "action": action,
            "at": _now_iso(),
            "result": result or {},
        })

    def get_active_checkpoints(self) -> list[dict[str, Any]]:
        return [
            {"task_code": tc, "position": cp["position"], "saved_at": cp["saved_at"]}
            for tc, cp in self._checkpoints.items()
        ]

    def get_log(self) -> list[dict[str, Any]]:
        return self._recovery_log[-100:]

    def replay_from_checkpoint(self, task_code: str,
                               replay_fn: Any,
                               total_items: int,
                               batch_size: int = 100) -> dict[str, Any]:
        """从检查点重放处理：调用 replay_fn(batch_items, start_index) 逐批恢复。"""
        checkpoint = self.load_checkpoint(task_code)
        start_idx = 0
        if checkpoint:
            try:
                start_idx = int(checkpoint.get("position", 0))
            except (ValueError, TypeError):
                start_idx = 0

        self.record_recovery(task_code, "replay_started",
                             {"start_idx": start_idx, "total": total_items})

        processed = start_idx
        current = start_idx
        errors: list[dict[str, Any]] = []

        while current < total_items:
            end_idx = min(current + batch_size, total_items)
            batch = list(range(current, end_idx))
            try:
                result = replay_fn(batch, current)
                if isinstance(result, dict) and not result.get("ok", True):
                    errors.append({"segment": f"{current}-{end_idx}",
                                   "error": result.get("message", "unknown")})
                    break
                processed = end_idx
                self.save_checkpoint(task_code, str(processed))
                current = end_idx
            except Exception as e:
                errors.append({"segment": f"{current}-{end_idx}", "error": str(e)})
                break

        complete = processed >= total_items
        if complete:
            self.clear_checkpoint(task_code)

        self.record_recovery(task_code,
                             "replay_completed" if complete else "replay_failed",
                             {"processed": processed, "total": total_items, "errors": len(errors)})

        return {
            "ok": len(errors) == 0,
            "task_code": task_code,
            "processed": processed,
            "total": total_items,
            "complete": complete,
            "errors": errors,
        }


# 全局单例
checkpoint_manager = CheckpointManager()
replay_queue = ReplayQueue()