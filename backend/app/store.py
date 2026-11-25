"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。
"""
from __future__ import annotations

from typing import Any

from app.seed import SEED_ROWS


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 分区合并产物确认记录：由合并流程逐条登记、持久化，压缩只经此表认领产物。
        self._tables.setdefault("merge_artifact", [])
        # 合并运行台账：run_id -> 运行状态与产物清单（合并详情的证据来源）。
        self.merge_runs: dict[str, dict[str, Any]] = {}
        # 物理文件仓：文件路径 -> 实际字节与写出状态，模拟湖表目录上的真实文件。
        self.artifact_files: dict[str, dict[str, Any]] = {}

    def reset_artifacts(self) -> None:
        """清空合并运行与产物证据，仅供测试回到干净起点。"""
        self._tables["merge_artifact"] = []
        self.merge_runs.clear()
        self.artifact_files.clear()

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
