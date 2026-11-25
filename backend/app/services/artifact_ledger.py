"""分区合并产物确认台账：持久化合并产物的实际文件数据、所属运行与确认状态。

表 ``merge_artifact`` 是压缩侧唯一的产物证据来源；运行台账 ``merge_runs`` 与
物理文件仓 ``artifact_files`` 构成合并详情侧的证据。两侧证据必须对得上，
压缩才允许消费，任何半成品或同名旧文件都不会被接管。
"""
from __future__ import annotations

import hashlib
import threading
from datetime import datetime, timezone
from typing import Any

from app.store import store

# 合并与压缩并发访问同一批产物，所有状态迁移都在这把锁内完成。
artifact_lock = threading.RLock()

# 产物确认记录的确认状态（记录维度）。
STATUS_WRITING = "写出中"
STATUS_WRITTEN = "已写出"
STATUS_CONFIRMED = "已确认"
# 产物记录的消费状态。
CONSUME_IDLE = "待消费"
CONSUME_CLAIMED = "消费中"
CONSUME_DONE = "已消费"


def now_ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def checksum_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def list_records() -> list[dict[str, Any]]:
    return list(store.rows("merge_artifact"))


def records_for_run(run_id: str) -> list[dict[str, Any]]:
    return [row for row in list_records() if row.get("run_id") == run_id]


def find_record(record_id: int) -> dict[str, Any] | None:
    return store.find("merge_artifact", record_id)


def get_file(path: str) -> dict[str, Any] | None:
    return store.artifact_files.get(path)


def get_run(run_id: str) -> dict[str, Any] | None:
    return store.merge_runs.get(run_id)


def register_record(
    *,
    run_id: str,
    merge_id: int,
    table: str,
    file_path: str,
    source_partitions: list[str],
    target_partition: str,
    ordinal: int,
) -> dict[str, Any]:
    """登记一条产物确认记录（初始为「写出中/待消费」），并占位物理文件仓。"""
    rows = store.rows("merge_artifact")
    record = {
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "status": STATUS_WRITING,
        "pending": True,
        "abnormal": False,
        "run_id": run_id,
        "merge_id": merge_id,
        "table": table,
        "file_path": file_path,
        "source_partitions": list(source_partitions),
        "target_partition": target_partition,
        "ordinal": ordinal,
        "size_bytes": 0,
        "checksum": "",
        "written_complete": False,
        "receipt_status": "未入账",
        "consume_status": CONSUME_IDLE,
        "consume_count": 0,
        "consumed_by": None,
        "created_at": now_ts(),
        "written_at": None,
        "confirmed_at": None,
        "consumed_at": None,
        # 供通用资源页展示的中文字段。
        "产物文件": file_path,
        "所属运行": run_id,
        "源分区": ",".join(source_partitions),
        "确认状态": STATUS_WRITING,
        "消费状态": CONSUME_IDLE,
        "消费次数": 0,
    }
    rows.append(record)
    store.artifact_files[file_path] = {
        "path": file_path,
        "run_id": run_id,
        "state": STATUS_WRITING,
        "size_bytes": 0,
        "checksum": "",
        "data": b"",
    }
    return record


def mark_written(record_id: int, data: bytes) -> dict[str, Any]:
    """模拟文件完整落盘：写入实际字节并置「已写出」，回执仍未入账。"""
    record = find_record(record_id)
    if record is None:
        raise KeyError(f"产物记录 {record_id} 不存在")
    path = record["file_path"]
    physical = store.artifact_files[path]
    digest = checksum_of(data)
    physical.update({
        "data": data,
        "size_bytes": len(data),
        "checksum": digest,
        "state": STATUS_WRITTEN,
        "written_at": now_ts(),
    })
    record.update({
        "status": STATUS_WRITTEN,
        "size_bytes": len(data),
        "checksum": digest,
        "written_complete": True,
        "written_at": physical["written_at"],
        "确认状态": STATUS_WRITTEN,
    })
    return record


def mark_confirmed(record_id: int) -> dict[str, Any]:
    """回执入账：确认记录置「已确认」，此后才进入压缩可消费范围。"""
    record = find_record(record_id)
    if record is None:
        raise KeyError(f"产物记录 {record_id} 不存在")
    if not record.get("written_complete"):
        raise ValueError(f"产物 {record['file_path']} 尚未完整写出，无法确认")
    record.update({
        "status": STATUS_CONFIRMED,
        "receipt_status": "已入账",
        "confirmed_at": now_ts(),
        "pending": False,
        "确认状态": STATUS_CONFIRMED,
    })
    return record


def claim_for_compaction(run_id: str, consumer: str) -> list[dict[str, Any]]:
    """认领某次运行当前可证实可消费的产物记录。

    可消费的充要条件：属于该运行、文件已完整写出、回执已确认、未被消费，
    且记录校验和与物理文件仓（合并详情证据）完全一致。认领后消费状态置
    「消费中」，并发恢复/重复调用都不会二次认领。
    """
    claimed: list[dict[str, Any]] = []
    with artifact_lock:
        for record in records_for_run(run_id):
            if record["consume_status"] != CONSUME_IDLE:
                continue
            if record["status"] != STATUS_CONFIRMED or not record["written_complete"]:
                continue
            physical = store.artifact_files.get(record["file_path"])
            if physical is None or physical["state"] != STATUS_WRITTEN:
                continue
            if physical["checksum"] != record["checksum"] or not record["checksum"]:
                # 证据冲突：记录与合并详情对不上，拒绝消费并保留现场。
                record["abnormal"] = True
                continue
            record["consume_status"] = CONSUME_CLAIMED
            record["consumed_by"] = consumer
            record["消费状态"] = CONSUME_CLAIMED
            claimed.append(record)
    return claimed


def complete_consumption(record_ids: list[int], consumer: str) -> None:
    """压缩完成后回写消费结果（含消费次数），整个过程只计一次。"""
    with artifact_lock:
        for record_id in record_ids:
            record = find_record(record_id)
            if record is None or record["consume_status"] != CONSUME_CLAIMED:
                continue
            if record["consumed_by"] != consumer:
                continue
            record["consume_status"] = CONSUME_DONE
            record["consume_count"] = int(record.get("consume_count", 0)) + 1
            record["consumed_at"] = now_ts()
            record["消费状态"] = CONSUME_DONE
            record["消费次数"] = record["consume_count"]
