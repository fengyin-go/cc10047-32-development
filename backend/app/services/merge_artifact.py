"""分区合并产物确认记录领域服务：逐条持久化合并生成的实际文件数据、
所属运行和确认状态，是表压缩判断可消费范围的唯一产物证据。
"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services import artifact_ledger as ledger


class MergeArtifactService(BaseModuleService):
    MODULE = "merge_artifact"
    ENTITY = "产物确认记录"
    OPERATION_TERM = "登记"
    LIST_FIELDS = ["产物文件", "所属运行", "源分区", "确认状态", "消费状态", "消费次数"]
    REQUIRED_FIELDS = ["产物文件", "所属运行", "源分区"]
    STATUS_ORDER = [ledger.STATUS_WRITING, ledger.STATUS_WRITTEN, ledger.STATUS_CONFIRMED]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS = []

    def domain_snapshot(self) -> dict:
        rows = self.list_entries()[0]
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "分区合并产物确认台账",
            "count": len(rows),
            "by_status": {
                status: sum(1 for row in rows if row.get("status") == status)
                for status in self.STATUS_ORDER
            },
            "abnormal": sum(1 for row in rows if row.get("abnormal")),
        }
