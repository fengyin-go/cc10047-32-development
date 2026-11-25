"""分区合并产物确认记录与表压缩协同测试。

覆盖：
- 产物确认记录逐条持久化实际文件、所属运行与确认/消费状态；
- 压缩待办（确认记录证据）与合并详情（运行台账+物理文件证据）给出不同可处置范围；
- M07 场景：f3 已落盘但回执未入账时中断，先压缩再恢复，文件不重复消费；
- 半成品、同名旧文件、校验和冲突均被排除，恢复只续做本运行产物；
- 并发认领同一运行只有一个消费者成功。
"""
from __future__ import annotations

import sys
import threading

import pytest

sys.path.insert(0, '.')

from fastapi.testclient import TestClient

from app.main import app
from app.services import artifact_ledger as ledger
from app.services import service_for
from app.store import store

client = TestClient(app)


@pytest.fixture(autouse=True)
def clean_artifacts():
    store.reset_artifacts()
    yield
    store.reset_artifacts()


def _make_merge(sources: str = "p1,p2,p3", table: str = "lake_m07") -> tuple[int, str, list[int]]:
    pm = service_for("partition_merge")
    entry, missing = pm.create_entry(
        {"任务标识": f"merge-{table}", "目标湖表": table, "源分区": sources, "目标分区": "p0"}
    )
    assert not missing
    run = pm.start_merge_run(entry["id"])
    assert run["ok"]
    return entry["id"], run["run_id"], run["record_ids"]


def _file_name(record_id: int) -> str:
    return ledger.find_record(record_id)["file_path"].split("/")[-1]


class TestArtifactPersistence:
    def test_records_persist_actual_file_data_and_status(self):
        pm = service_for("partition_merge")
        _, run_id, ids = _make_merge()
        pm.write_artifact(run_id, ids[0])
        pm.confirm_artifact(run_id, ids[0])

        record = ledger.find_record(ids[0])
        physical = ledger.get_file(record["file_path"])
        assert record["run_id"] == run_id
        assert record["written_complete"] is True
        assert record["status"] == ledger.STATUS_CONFIRMED
        assert record["receipt_status"] == "已入账"
        # 实际文件字节与校验和都持久化，且两侧证据对得上。
        assert physical["data"]
        assert physical["checksum"] == record["checksum"]
        assert record["consume_status"] == ledger.CONSUME_IDLE
        assert record["consume_count"] == 0

    def test_resource_router_lists_confirmation_records(self):
        pm = service_for("partition_merge")
        _, run_id, ids = _make_merge()
        pm.write_artifact(run_id, ids[0])
        resp = client.get("/api/merge_artifact")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert {row["所属运行"] for row in data["items"]} == {run_id}


class TestM07InterruptionWindow:
    def test_compaction_todo_vs_merge_detail_different_evidence_scope(self):
        """f1/f2 已确认，f3 已落盘但回执未入账时中断，两视角范围不同。"""
        pm = service_for("partition_merge")
        ct = service_for("compaction_task")
        _, run_id, ids = _make_merge()
        for record_id in ids[:2]:
            pm.write_artifact(run_id, record_id)
            pm.confirm_artifact(run_id, record_id)
        pm.write_artifact(run_id, ids[2])  # f3 落盘，不确认
        pm.interrupt_merge_run(run_id)

        todo = ct.refresh_compaction_todo(1, run_id)["todo"]
        detail = pm.merge_detail(run_id)

        # 压缩证据（确认记录）：只能处置 f1/f2，f3 尚缺回执。
        assert sorted(todo["consumable_files"]) == sorted(
            ledger.find_record(i)["file_path"] for i in ids[:2]
        )
        assert todo["awaiting_receipt_files"] == [ledger.find_record(ids[2])["file_path"]]
        assert todo["evidence"] == "merge_artifact_ledger"

        # 合并详情证据：f1/f2/f3 物理上都已落盘，但回执缺口同样可查。
        assert len(detail["physically_written"]) == 3
        assert detail["missing_receipt_files"] == [ledger.find_record(ids[2])["file_path"]]
        assert detail["confirmed_files"] == [
            ledger.find_record(i)["file_path"] for i in ids[:2]
        ]

    def test_compact_first_then_recover_no_double_consumption(self):
        pm = service_for("partition_merge")
        ct = service_for("compaction_task")
        _, run_id, ids = _make_merge()
        f1, f2, f3 = ids
        for record_id in (f1, f2):
            pm.write_artifact(run_id, record_id)
            pm.confirm_artifact(run_id, record_id)
        pm.write_artifact(run_id, f3)
        pm.interrupt_merge_run(run_id)

        # 先启动表压缩：只拿到 f1/f2，f3 因缺回执被挡在可消费范围外。
        first = ct.compact_table_from_todo(1, run_id)
        assert first["ok"] is True
        assert first["result"]["files_compacted"] == 2
        assert sorted(i["record_id"] for i in first["result"]["inputs"]) == sorted([f1, f2])

        # 中断窗口重复压缩不会重复消费。
        again = ct.compact_table_from_todo(1, run_id)
        assert again["ok"] is False
        assert ledger.find_record(f1)["consume_count"] == 1

        # 再恢复合并：f3 已落盘，仅补发回执，文件不重写。
        recovered = pm.recover_merge_run(run_id)
        assert recovered["receipt_recovered"] == [ledger.find_record(f3)["file_path"]]
        assert recovered["rewritten"] == []
        finished = pm.finish_merge_run(run_id)
        assert finished["ok"]

        # 刷新待办后只剩 f3 可消费，压缩只补消费 f3。
        todo = ct.refresh_compaction_todo(1, run_id)["todo"]
        assert todo["consumable_files"] == [ledger.find_record(f3)["file_path"]]
        second = ct.compact_table_from_todo(1, run_id)
        assert second["ok"] is True
        assert second["result"]["files_compacted"] == 1
        assert second["result"]["inputs"][0]["record_id"] == f3

        # 最终：每个文件恰好消费一次，待办回写了本次输入。
        final = ct.get_compaction_todo(1, run_id)["todo"]
        assert {item["file_path"]: item["consume_count"] for item in final["consumed_files"]} == {
            ledger.find_record(i)["file_path"]: 1 for i in ids
        }
        assert final["last_inputs"][0]["record_id"] == f3
        assert ledger.find_record(f3)["status"] == ledger.STATUS_CONFIRMED
        assert ledger.find_record(f3)["consume_status"] == ledger.CONSUME_DONE

    def test_switching_views_via_http_endpoints(self):
        """通过 HTTP 切换压缩待办与合并详情，核对每次读到的文件与 f3 状态。"""
        pm = service_for("partition_merge")
        _, run_id, ids = _make_merge()
        for record_id in ids[:2]:
            client.post(f"/api/merge-workflow/runs/{run_id}/artifacts/{record_id}/write")
            client.post(f"/api/merge-workflow/runs/{run_id}/artifacts/{record_id}/confirm")
        client.post(f"/api/merge-workflow/runs/{run_id}/artifacts/{ids[2]}/write")
        client.post(f"/api/merge-workflow/runs/{run_id}/interrupt")

        todo = client.post(f"/api/merge-workflow/compaction/1/todos/{run_id}/refresh").json()["todo"]
        detail = client.get(f"/api/merge-workflow/runs/{run_id}/detail").json()
        f3_name = _file_name(ids[2])
        assert f3_name not in [p.split("/")[-1] for p in todo["consumable_files"]]
        assert f3_name in [p.split("/")[-1] for p in todo["awaiting_receipt_files"]]
        assert f3_name in [p.split("/")[-1] for p in detail["physically_written"]]

        client.post(f"/api/merge-workflow/runs/{run_id}/recover")
        client.post(f"/api/merge-workflow/runs/{run_id}/finish")
        detail2 = client.get(f"/api/merge-workflow/runs/{run_id}/detail").json()
        assert detail2["run_state"] == "已完成"
        assert detail2["missing_receipt_files"] == []
        f3_view = next(f for f in detail2["files"] if f["file_path"].endswith(f3_name))
        assert f3_view["record_status"] == ledger.STATUS_CONFIRMED
        assert f3_view["consume_count"] == 0


class TestSafetyGuards:
    def test_half_written_artifact_excluded_and_resumed_by_owner(self):
        pm = service_for("partition_merge")
        ct = service_for("compaction_task")
        _, run_id, ids = _make_merge(sources="q1,q2")
        pm.write_artifact(run_id, ids[0])
        pm.confirm_artifact(run_id, ids[0])
        # ids[1] 保持「写出中」半成品后中断。
        pm.interrupt_merge_run(run_id)
        todo = ct.refresh_compaction_todo(1, run_id)["todo"]
        assert todo["consumable_files"] == [ledger.find_record(ids[0])["file_path"]]
        assert any("半成品" in item["reason"] for item in todo["excluded_files"])

        recovered = pm.recover_merge_run(run_id)
        assert recovered["rewritten"] == [ledger.find_record(ids[1])["file_path"]]
        pm.finish_merge_run(run_id)
        todo2 = ct.refresh_compaction_todo(1, run_id)["todo"]
        assert len(todo2["consumable_files"]) == 2
        assert todo2["evidence_conflicts"] == []

    def test_same_name_stale_file_never_taken_over(self):
        pm = service_for("partition_merge")
        _, run_id, ids = _make_merge(sources="q1,q2")
        pm.interrupt_merge_run(run_id)
        stale_path = ledger.find_record(ids[0])["file_path"]
        # 物理目录里存在同名旧文件，但归属另一个运行。
        store.artifact_files[stale_path] = {
            "path": stale_path, "run_id": "STALE-RUN", "state": "已写出",
            "size_bytes": 9, "checksum": "deadbeef", "data": b"stalefile",
        }
        recovered = pm.recover_merge_run(run_id)
        assert recovered["ok"] is False
        assert "同名旧文件" in recovered["message"]
        # 现场保持中断，原运行状态一致。
        assert store.merge_runs[run_id]["state"] == "中断"

    def test_checksum_conflict_isolated_from_consumption(self):
        pm = service_for("partition_merge")
        ct = service_for("compaction_task")
        _, run_id, ids = _make_merge(sources="q1,q2")
        for record_id in ids:
            pm.write_artifact(run_id, record_id)
            pm.confirm_artifact(run_id, record_id)
        target = ledger.find_record(ids[0])
        store.artifact_files[target["file_path"]]["checksum"] = "tampered"

        todo = ct.refresh_compaction_todo(1, run_id)["todo"]
        assert target["file_path"] not in todo["consumable_files"]
        conflict = next(c for c in todo["evidence_conflicts"] if c["file_path"] == target["file_path"])
        assert "校验和不一致" in conflict["reason"]
        result = ct.compact_table_from_todo(1, run_id)
        assert result["ok"] is True
        # 只有未冲突的那个文件被消费。
        assert result["result"]["files_compacted"] == 1

    def test_concurrent_claim_single_consumer(self):
        pm = service_for("partition_merge")
        _, run_id, ids = _make_merge(sources="s1")
        pm.write_artifact(run_id, ids[0])
        pm.confirm_artifact(run_id, ids[0])
        claims: list[tuple[str, list[int]]] = []

        def worker(name: str) -> None:
            claims.append((name, [r["id"] for r in ledger.claim_for_compaction(run_id, name)]))

        threads = [threading.Thread(target=worker, args=(f"worker-{i}",)) for i in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        winners = [name for name, record_ids in claims if record_ids]
        assert winners == ["worker-0"]
        assert ledger.find_record(ids[0])["consume_count"] == 0  # 认领未完成前不计数

    def test_recover_cannot_restart_active_run(self):
        pm = service_for("partition_merge")
        merge_id, run_id, ids = _make_merge(sources="q1")
        duplicate = pm.start_merge_run(merge_id)
        assert duplicate["ok"] is False
        assert duplicate["run_id"] == run_id
        pm.interrupt_merge_run(run_id)
        duplicate_interrupted = pm.start_merge_run(merge_id)
        assert duplicate_interrupted["ok"] is False

    def test_claimed_but_unfinished_file_is_locked_in_todo(self):
        """压缩认领后、回写完成前中断：记录停在「消费中」，待办单列且不可再认领。"""
        pm = service_for("partition_merge")
        ct = service_for("compaction_task")
        _, run_id, ids = _make_merge(sources="s1")
        pm.write_artifact(run_id, ids[0])
        pm.confirm_artifact(run_id, ids[0])
        claimed = ledger.claim_for_compaction(run_id, "compaction-1")
        assert len(claimed) == 1  # 模拟压缩进程在回写前崩溃，未调用 complete_consumption。

        todo = ct.refresh_compaction_todo(1, run_id)["todo"]
        assert todo["consumable_files"] == []
        assert todo["claimed_files"][0]["record_id"] == ids[0]
        assert todo["claimed_files"][0]["claimed_by"] == "compaction-1"
        # 恢复的压缩进程也无法重复认领同一产物。
        assert ledger.claim_for_compaction(run_id, "compaction-recovered") == []

    def test_new_run_does_not_consume_previous_run_files(self):
        pm = service_for("partition_merge")
        ct = service_for("compaction_task")
        merge_id, run_id, ids = _make_merge(sources="a1,a2")
        for record_id in ids:
            pm.write_artifact(run_id, record_id)
            pm.confirm_artifact(run_id, record_id)
        pm.finish_merge_run(run_id)
        ct.compact_table_from_todo(1, run_id)

        second = pm.start_merge_run(merge_id)
        assert second["ok"] is True
        assert second["run_id"] != run_id
        todo = ct.refresh_compaction_todo(1, second["run_id"])["todo"]
        # 新运行的同名产物是全新记录，旧运行的消费历史不会串过来。
        assert todo["consumed_files"] == []
        assert len(ledger.records_for_run(second["run_id"])) == 2
        assert all(r["consume_count"] == 0 for r in ledger.records_for_run(second["run_id"]))
