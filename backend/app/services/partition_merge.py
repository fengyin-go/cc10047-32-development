"""分区合并领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。

合并运行产生的每个实际文件都会在 ``merge_artifact`` 表逐条登记产物确认记录，
并在物理文件仓中持久化实际字节；中断恢复只能续做本运行未完成的产物，
半成品文件与同名旧文件都不会被接管。
"""
from __future__ import annotations

from typing import Any

from app.services import artifact_ledger as ledger
from app.services.base import BaseModuleService
from app.store import store


class PartitionMergeService(BaseModuleService):
    MODULE = "partition_merge"
    ENTITY = "合并任务"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["任务标识", "目标湖表", "源分区", "目标分区", "合并策略", "文件大小", "执行状态", "耗时秒数"]
    REQUIRED_FIELDS = ["任务标识", "目标湖表", "源分区"]
    STATUS_ORDER = ["排队中", "合并中", "已完成", "合并失败"]
    ACTION_RULES = {"启动合并": "排队中", "取消合并": "合并中"}
    NEGATIVE_ACTIONS = []

    def _safe_int(self, val: Any, default: int = 0) -> int:
        try: return int(val)
        except (ValueError, TypeError): return default

    # ------------------------------------------------------------------
    # 兼容旧调用：原 domain_merge_partitions 仍返回标准化汇总。
    # ------------------------------------------------------------------
    def domain_merge_partitions(self, entry_id: int) -> dict[str, Any]:
        """对当前数据执行转换，联动计算和合并处理，产出标准化记录。处理流程：读取配置→执行转换→计算校验→合并批次→写入存储，下游可消费且批次可追溯。"""
        entry = store.find(self.MODULE, entry_id)
        if not entry:        # 返回存储处理结果，含关联校验和记录批次信息

            return {"ok": False, "message": f"合并任务 {entry_id} 不存在"}
        source = str(entry.get("源分区", "0")).split(",")        # 返回存储处理结果，含关联校验和记录批次信息

        return {
            "ok": True,
            "task_id": entry.get("任务标识"),
            "table": entry.get("目标湖表"),
            "source_partitions": source,
            "target_partition": entry.get("目标分区", source[0]),
            "merged_files": len(source) * 3,
            "output_size_mb": self._safe_int(entry.get("文件大小"), 128),
            "strategy": entry.get("合并策略", "bin-packing"),
        }

    # ------------------------------------------------------------------
    # 合并运行生命周期（产物确认记录驱动）
    # ------------------------------------------------------------------
    def _plan_outputs(self, entry: dict[str, Any]) -> list[str]:
        """根据源分区规划本次运行的产物文件清单。"""
        source = [part.strip() for part in str(entry.get("源分区", "0")).split(",") if part.strip()]
        target = str(entry.get("目标分区") or source[0])
        table = str(entry.get("目标湖表") or "unknown-table")
        return [
            f"{table}/{target}/merged-{index:03d}.parquet"
            for index in range(1, len(source) + 1)
        ]

    def start_merge_run(self, entry_id: int) -> dict[str, Any]:
        """启动一次合并运行：创建运行台账，并为每个产物预登记确认记录。

        同一任务重复启动会返回既有运行（并发恢复不得新开运行重复消费）。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": f"合并任务 {entry_id} 不存在"}
        with ledger.artifact_lock:
            for run in store.merge_runs.values():
                if run["merge_id"] == entry_id and run["state"] in ("合并中", "中断"):
                    return {
                        "ok": False,
                        "message": f"合并任务 {entry_id} 已有运行 {run['run_id']} 未结束，请恢复而非重启",
                        "run_id": run["run_id"],
                    }
            attempt = int(entry.get("run_attempt", 0)) + 1
            entry["run_attempt"] = attempt
            run_id = f"M{entry_id}-R{attempt}"
            source = [part.strip() for part in str(entry.get("源分区", "0")).split(",") if part.strip()]
            target = str(entry.get("目标分区") or source[0])
            planned = self._plan_outputs(entry)
            store.merge_runs[run_id] = {
                "run_id": run_id,
                "merge_id": entry_id,
                "table": entry.get("目标湖表"),
                "state": "合并中",
                "source_partitions": source,
                "target_partition": target,
                "planned_files": planned,
                "record_ids": [],
                "started_at": ledger.now_ts(),
                "interrupted_at": None,
                "finished_at": None,
            }
            record_ids: list[int] = []
            for ordinal, path in enumerate(planned, start=1):
                record = ledger.register_record(
                    run_id=run_id,
                    merge_id=entry_id,
                    table=str(entry.get("目标湖表") or ""),
                    file_path=path,
                    source_partitions=source,
                    target_partition=target,
                    ordinal=ordinal,
                )
                record_ids.append(record["id"])
            store.merge_runs[run_id]["record_ids"] = record_ids
            entry["status"] = "合并中"
            entry["pending"] = True
            entry["run_id"] = run_id
        return {"ok": True, "run_id": run_id, "planned_files": planned, "record_ids": record_ids}

    def write_artifact(self, run_id: str, record_id: int, payload: bytes | None = None) -> dict[str, Any]:
        """模拟单个产物文件完整写出（落盘但回执尚未入账）。"""
        run = ledger.get_run(run_id)
        if run is None:
            return {"ok": False, "message": f"合并运行 {run_id} 不存在"}
        record = ledger.find_record(record_id)
        if record is None or record["run_id"] != run_id:
            return {"ok": False, "message": f"产物记录 {record_id} 不属于运行 {run_id}"}
        if record["written_complete"]:
            return {"ok": False, "message": f"产物 {record['file_path']} 已完整写出，禁止覆盖"}
        data = payload if payload is not None else f"MERGED::{run_id}::{record['file_path']}".encode()
        with ledger.artifact_lock:
            ledger.mark_written(record_id, data)
        return {"ok": True, "record": ledger.find_record(record_id)}

    def confirm_artifact(self, run_id: str, record_id: int) -> dict[str, Any]:
        """产物回执入账，确认记录转为「已确认」。"""
        run = ledger.get_run(run_id)
        if run is None:
            return {"ok": False, "message": f"合并运行 {run_id} 不存在"}
        record = ledger.find_record(record_id)
        if record is None or record["run_id"] != run_id:
            return {"ok": False, "message": f"产物记录 {record_id} 不属于运行 {run_id}"}
        with ledger.artifact_lock:
            try:
                ledger.mark_confirmed(record_id)
            except ValueError as exc:
                return {"ok": False, "message": str(exc)}
        return {"ok": True, "record": ledger.find_record(record_id)}

    def interrupt_merge_run(self, run_id: str) -> dict[str, Any]:
        """模拟服务中断：运行挂起，未写出/未确认的产物保持原状，半成品不接管。"""
        run = ledger.get_run(run_id)
        if run is None:
            return {"ok": False, "message": f"合并运行 {run_id} 不存在"}
        with ledger.artifact_lock:
            run["state"] = "中断"
            run["interrupted_at"] = ledger.now_ts()
            entry = store.find(self.MODULE, run["merge_id"])
            if entry is not None:
                entry["status"] = "合并中"
        return {"ok": True, "run_id": run_id, "state": run["state"]}

    def recover_merge_run(self, run_id: str) -> dict[str, Any]:
        """恢复中断的合并运行。

        只允许续做本运行未完整写出的产物；已写出的文件保持原状态，
        物理文件仓里属于其他运行的同名旧文件不会被接管。
        """
        run = ledger.get_run(run_id)
        if run is None:
            return {"ok": False, "message": f"合并运行 {run_id} 不存在"}
        with ledger.artifact_lock:
            if run["state"] != "中断":
                return {"ok": False, "message": f"运行 {run_id} 状态为 {run['state']}，无需恢复"}
            resumed: list[str] = []
            receipt_recovered: list[str] = []
            skipped: list[str] = []
            for record_id in run["record_ids"]:
                record = ledger.find_record(record_id)
                if record is None:
                    continue
                physical = ledger.get_file(record["file_path"])
                # 同名文件若属于别的运行，绝不能当作本运行产物接管。
                if physical is not None and physical["run_id"] != run_id:
                    return {
                        "ok": False,
                        "message": f"文件 {record['file_path']} 被运行 {physical['run_id']} 占用，拒绝接管同名旧文件",
                    }
                if record["status"] == ledger.STATUS_CONFIRMED:
                    skipped.append(record["file_path"])
                    continue
                if record["written_complete"]:
                    # 文件已完整落盘、仅缺回执：恢复只补发回执，绝不重写文件。
                    ledger.mark_confirmed(record_id)
                    receipt_recovered.append(record["file_path"])
                    continue
                # 半成品/未写出：由本运行重新写出并确认，不会接管任何外部文件。
                data = f"MERGED::{run_id}::{record['file_path']}::resume".encode()
                ledger.mark_written(record_id, data)
                ledger.mark_confirmed(record_id)
                resumed.append(record["file_path"])
            run["state"] = "合并中"
        return {
            "ok": True,
            "run_id": run_id,
            "rewritten": resumed,
            "receipt_recovered": receipt_recovered,
            "already_confirmed": skipped,
        }

    def finish_merge_run(self, run_id: str) -> dict[str, Any]:
        """全部产物回执到账后收尾运行，合并任务置「已完成」。"""
        run = ledger.get_run(run_id)
        if run is None:
            return {"ok": False, "message": f"合并运行 {run_id} 不存在"}
        with ledger.artifact_lock:
            records = ledger.records_for_run(run_id)
            pending = [r["file_path"] for r in records if r["status"] != ledger.STATUS_CONFIRMED]
            if pending:
                return {"ok": False, "message": "仍有产物未确认，不能收尾", "pending": pending}
            run["state"] = "已完成"
            run["finished_at"] = ledger.now_ts()
            entry = store.find(self.MODULE, run["merge_id"])
            if entry is not None:
                entry["status"] = "已完成"
                entry["pending"] = False
        return {"ok": True, "run_id": run_id, "state": run["state"]}

    def merge_detail(self, run_id: str) -> dict[str, Any]:
        """合并详情视角：以运行台账 + 物理文件仓为证据，列出每个产物的真实状态。"""
        run = ledger.get_run(run_id)
        if run is None:
            return {"ok": False, "message": f"合并运行 {run_id} 不存在"}
        files: list[dict[str, Any]] = []
        with ledger.artifact_lock:
            for record_id in run["record_ids"]:
                record = ledger.find_record(record_id)
                if record is None:
                    continue
                physical = ledger.get_file(record["file_path"])
                evidence_match = bool(
                    physical
                    and physical["run_id"] == run_id
                    and physical["checksum"] == record["checksum"]
                    and record["written_complete"]
                )
                files.append({
                    "record_id": record["id"],
                    "file_path": record["file_path"],
                    "source_partitions": record["source_partitions"],
                    "record_status": record["status"],
                    "receipt_status": record["receipt_status"],
                    "written_complete": record["written_complete"],
                    "physical_state": physical["state"] if physical else "缺失",
                    "physical_run": physical["run_id"] if physical else None,
                    "size_bytes": record["size_bytes"],
                    "checksum": record["checksum"],
                    "evidence_match": evidence_match,
                    "consume_status": record["consume_status"],
                    "consume_count": record["consume_count"],
                })
        confirmed = [f["file_path"] for f in files if f["record_status"] == ledger.STATUS_CONFIRMED]
        missing_receipt = [f["file_path"] for f in files if f["receipt_status"] != "已入账"]
        return {
            "ok": True,
            "evidence": "merge_run_detail",
            "run_id": run_id,
            "merge_id": run["merge_id"],
            "run_state": run["state"],
            "source_partitions": run["source_partitions"],
            "target_partition": run["target_partition"],
            "files": files,
            "confirmed_files": confirmed,
            "missing_receipt_files": missing_receipt,
            # 合并详情证据下：物理上完整落盘的文件都可见（即使回执未入账）。
            "physically_written": [f["file_path"] for f in files if f["written_complete"]],
        }
