"""分区合并产物确认与表压缩协同工作流。

合并侧端点围绕合并运行与产物确认记录；压缩侧端点只从确认记录读取待办、
认领并回写消费结果。合并详情与压缩待办分别代表两种证据视角，
在「文件已落盘、回执未入账」的中断窗口会给出不同的可处置范围。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.services import service_for

router = APIRouter(prefix="/api/merge-workflow", tags=["分区合并产物协同"])


def _require(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(404, f"{module} 服务未注册")
    return service


# ------------------------------------------------------------------
# 合并运行与产物确认
# ------------------------------------------------------------------

@router.post("/runs")
def start_run(merge_id: int) -> dict:
    """启动分区合并运行，预登记全部产物确认记录。"""
    return _require("partition_merge").start_merge_run(merge_id)


@router.post("/runs/{run_id}/artifacts/{record_id}/write")
def write_artifact(run_id: str, record_id: int) -> dict:
    """模拟单个产物文件完整写出（实际字节落盘，回执暂不入账）。"""
    return _require("partition_merge").write_artifact(run_id, record_id)


@router.post("/runs/{run_id}/artifacts/{record_id}/confirm")
def confirm_artifact(run_id: str, record_id: int) -> dict:
    """产物回执入账，确认记录转为已确认。"""
    return _require("partition_merge").confirm_artifact(run_id, record_id)


@router.post("/runs/{run_id}/interrupt")
def interrupt_run(run_id: str) -> dict:
    """模拟服务中断：挂起运行并保留半成品现场。"""
    return _require("partition_merge").interrupt_merge_run(run_id)


@router.post("/runs/{run_id}/recover")
def recover_run(run_id: str) -> dict:
    """恢复中断运行：只续做本运行未写出的产物，不接管同名旧文件。"""
    return _require("partition_merge").recover_merge_run(run_id)


@router.post("/runs/{run_id}/finish")
def finish_run(run_id: str) -> dict:
    """全部产物确认后收尾运行。"""
    return _require("partition_merge").finish_merge_run(run_id)


@router.get("/runs/{run_id}/detail")
def merge_detail(run_id: str) -> dict:
    """合并详情视角：运行台账 + 物理文件仓证据。"""
    return _require("partition_merge").merge_detail(run_id)


# ------------------------------------------------------------------
# 表压缩待办（只认真实产物确认记录）
# ------------------------------------------------------------------

@router.post("/compaction/{compaction_id}/todos/{run_id}/refresh")
def refresh_todo(compaction_id: int, run_id: str) -> dict:
    """刷新压缩待办：从产物确认记录读取本次产物并交叉核验证据。"""
    return _require("compaction_task").refresh_compaction_todo(compaction_id, run_id)


@router.get("/compaction/{compaction_id}/todos/{run_id}")
def get_todo(compaction_id: int, run_id: str) -> dict:
    """读取压缩待办与本次采用的输入、消费次数。"""
    return _require("compaction_task").get_compaction_todo(compaction_id, run_id)


@router.post("/compaction/{compaction_id}/todos/{run_id}/compact")
def compact_from_todo(compaction_id: int, run_id: str) -> dict:
    """按待办执行压缩：认领确认记录、读取实际文件、回写消费结果。"""
    return _require("compaction_task").compact_table_from_todo(compaction_id, run_id)
