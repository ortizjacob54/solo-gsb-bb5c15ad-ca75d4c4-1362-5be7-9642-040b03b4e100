"""核心归段逻辑测试：仅依赖标准库，可直接运行：

    python3 -m unittest tests.test_service -v
"""
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.db import init_db, read_only_session, session  # noqa: E402
from app import service  # noqa: E402

SILENCE = 10.0


class ServiceTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        init_db(self.db_path)

    def tearDown(self):
        self.tmp.cleanup()

    def submit(self, source, event_id, occurred_ts, severity="info"):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.ingest_event(
                conn,
                source=source,
                event_id=event_id,
                occurred_ts=occurred_ts,
                severity=severity,
                silence=SILENCE,
            )

    def get(self, segment_id):
        with session(self.db_path) as conn:
            return service.get_segment(conn, segment_id)

    def changes(self, after=0, limit=100):
        with session(self.db_path) as conn:
            return service.list_segment_changes(conn, after=after, limit=limit)

    def retract(self, source, event_id):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.retract_event(
                conn, source=source, event_id=event_id, silence=SILENCE
            )

    def test_first_event_creates_segment(self):
        r = self.submit("s1", "e1", 1000.0, "warning")
        self.assertFalse(r["deduplicated"])
        seg = r["segment"]
        self.assertEqual(seg["event_count"], 1)
        self.assertEqual(seg["revision"], 1)
        self.assertEqual(seg["max_severity"], "warning")
        self.assertEqual(seg["started_at"], seg["ended_at"])

    def test_duplicate_event_id_is_idempotent(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e1", 1000.0)
        self.assertTrue(r2["deduplicated"])
        seg = self.get(r1["segment"]["id"])
        self.assertEqual(seg["event_count"], 1)
        self.assertEqual(seg["revision"], 1)

    def test_same_event_id_different_source_not_duplicate(self):
        self.submit("s1", "e1", 1000.0)
        r = self.submit("s2", "e1", 1000.0)
        self.assertFalse(r["deduplicated"])
        self.assertEqual(r["segment"]["source"], "s2")

    def test_out_of_order_event_extends_start_and_bumps_revision(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e2", 995.0)  # 更早到达的乱序事件
        self.assertEqual(r2["segment"]["id"], r1["segment"]["id"])
        seg = r2["segment"]
        self.assertEqual(seg["event_count"], 2)
        self.assertEqual(seg["revision"], 2)  # 边界变化 → 递增
        self.assertEqual(seg["started_at"], "1970-01-01T00:16:35+00:00")  # 995.0

    def test_inner_event_lower_severity_keeps_revision(self):
        self.submit("s1", "e1", 1000.0, "error")
        r = self.submit("s1", "e2", 1010.0, "info")  # 扩展 end → rev=2
        self.assertEqual(r["segment"]["revision"], 2)
        r = self.submit("s1", "e3", 1005.0, "debug")  # 严格位于段内、级别更低
        self.assertEqual(r["segment"]["event_count"], 3)
        self.assertEqual(r["segment"]["revision"], 2)  # 边界与级别未变
        self.assertEqual(r["segment"]["max_severity"], "error")

    def test_inner_event_higher_severity_bumps_revision(self):
        self.submit("s1", "e1", 1000.0, "info")
        r = self.submit("s1", "e2", 1003.0, "critical")
        self.assertEqual(r["segment"]["revision"], 2)
        self.assertEqual(r["segment"]["max_severity"], "critical")

    def test_event_beyond_silence_creates_new_segment(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e2", 1000.0 + SILENCE + 0.1)
        self.assertNotEqual(r1["segment"]["id"], r2["segment"]["id"])

    def test_event_at_exact_silence_boundary_joins_segment(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e2", 1000.0 + SILENCE)
        self.assertEqual(r1["segment"]["id"], r2["segment"]["id"])

    def test_late_event_bridges_two_segments(self):
        r1 = self.submit("s1", "e1", 0.0)          # 段 A [0, 0]
        r2 = self.submit("s1", "e2", 11.0)         # 11 > 0+10 → 段 B [11, 11]
        self.assertNotEqual(r1["segment"]["id"], r2["segment"]["id"])
        r3 = self.submit("s1", "e3", 5.0)          # 桥接 A 与 B
        seg = r3["segment"]
        self.assertEqual(seg["id"], r1["segment"]["id"])
        self.assertEqual(seg["event_count"], 3)
        self.assertEqual(seg["revision"], 2)
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn, "s1")), 1)

    def test_chain_merge_via_fixpoint(self):
        self.submit("s1", "e1", 0.0)               # 段 A
        self.submit("s1", "e2", 11.0)              # 段 B
        self.submit("s1", "e3", 22.0)              # 22 > 11+10 → 段 C
        self.submit("s1", "e4", 5.0)               # 合并 A+B → [0, 11]
        r = self.submit("s1", "e5", 16.0)          # 桥接 [0,11] 与 [22,22]
        seg = r["segment"]
        self.assertEqual(seg["event_count"], 5)
        with session(self.db_path) as conn:
            segs = service.list_segments(conn, "s1")
        self.assertEqual(len(segs), 1)
        self.assertTrue(segs[0]["ended_at"].startswith("1970-01-01T00:00:22"))

    def test_sources_are_isolated(self):
        self.submit("s1", "e1", 1000.0)
        r = self.submit("s2", "e2", 1001.0)  # 时间接近但 source 不同
        self.assertEqual(r["segment"]["event_count"], 1)
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn)), 2)

    def test_revision_never_decreases_after_merge(self):
        self.submit("s1", "e1", 0.0, "info")
        self.submit("s1", "e2", 1.0, "critical")   # 段 A rev=2
        self.submit("s1", "e3", 21.0, "info")      # 21 > 1+10 → 段 B rev=1
        r = self.submit("s1", "e4", 11.0)          # 桥接 A、B
        self.assertGreaterEqual(r["segment"]["revision"], 3)

    # ---- 变更流（segment_changes）----

    def test_create_emits_upsert_change(self):
        r = self.submit("s1", "e1", 1000.0, "warning")
        page = self.changes()
        self.assertEqual(len(page["changes"]), 1)
        ch = page["changes"][0]
        self.assertEqual(ch["change_id"], 1)
        self.assertEqual(ch["type"], "upsert")
        self.assertEqual(ch["segment_id"], r["segment"]["id"])
        self.assertEqual(ch["revision"], 1)
        self.assertIsNone(ch["merged_into"])
        self.assertEqual(ch["segment"], r["segment"])  # 快照与段状态一致
        self.assertEqual(page["next_after"], 1)

    def test_boundary_or_severity_change_emits_upsert(self):
        self.submit("s1", "e1", 1000.0, "info")
        self.submit("s1", "e2", 1005.0, "error")   # 扩 end + 提级 → rev 2
        page = self.changes()
        self.assertEqual([c["type"] for c in page["changes"]], ["upsert", "upsert"])
        second = page["changes"][1]
        self.assertEqual(second["revision"], 2)
        self.assertEqual(second["segment"]["max_severity"], "error")
        self.assertEqual(second["segment"]["event_count"], 2)
        ids = [c["change_id"] for c in page["changes"]]
        self.assertLess(ids[0], ids[1])            # 单调递增

    def test_count_only_increase_emits_no_change(self):
        self.submit("s1", "e1", 1000.0, "error")
        self.submit("s1", "e2", 1010.0)            # 扩边界 → upsert
        before = self.changes()["changes"]
        self.assertEqual(len(before), 2)
        self.submit("s1", "e3", 1005.0, "debug")   # 仅事件数 +1，revision 不变
        self.assertEqual(self.changes()["changes"], before)  # 无新增变更

    def test_duplicate_event_id_emits_no_change(self):
        self.submit("s1", "e1", 1000.0)
        before = self.changes()["changes"]
        r = self.submit("s1", "e1", 1000.0)        # 幂等重放
        self.assertTrue(r["deduplicated"])
        self.assertEqual(self.changes()["changes"], before)

    def test_merge_emits_tombstone_per_deleted_segment(self):
        r1 = self.submit("s1", "e1", 0.0)          # 段 A
        r2 = self.submit("s1", "e2", 11.0)         # 段 B
        r3 = self.submit("s1", "e3", 22.0)         # 段 C
        self.submit("s1", "e4", 5.0)               # 合并 A+B
        self.submit("s1", "e5", 16.0)              # 桥接 [A+B] 与 C → 三段合并
        page = self.changes()
        types = [c["type"] for c in page["changes"]]
        # 3 次创建 + (B 墓碑 + A upsert) + (C 墓碑 + A upsert)
        self.assertEqual(
            types,
            ["upsert", "upsert", "upsert", "merged", "upsert", "merged", "upsert"],
        )
        tombs = [c for c in page["changes"] if c["type"] == "merged"]
        self.assertEqual(
            {t["segment_id"] for t in tombs},
            {r2["segment"]["id"], r3["segment"]["id"]},
        )
        for t in tombs:                            # 每个墓碑都指向保留段 A
            self.assertEqual(t["merged_into"], r1["segment"]["id"])
            self.assertIsNone(t["segment"])
        final = page["changes"][-1]
        self.assertEqual(final["type"], "upsert")
        self.assertEqual(final["segment_id"], r1["segment"]["id"])
        self.assertEqual(final["segment"]["event_count"], 5)
        ids = [c["change_id"] for c in page["changes"]]
        self.assertEqual(ids, sorted(ids))         # 全局单调递增
        self.assertEqual(len(set(ids)), len(ids))

    def test_pagination_no_miss_no_dup_while_ingesting(self):
        for i in range(5):                         # 5 个独立段 → 5 条 upsert
            self.submit("s1", f"e{i}", 1000.0 + i * 100.0)

        page1 = self.changes(after=0, limit=2)
        self.assertEqual([c["change_id"] for c in page1["changes"]], [1, 2])
        self.assertEqual(page1["next_after"], 2)

        self.submit("s1", "e5", 2000.0)            # 分页期间又有新事件

        page2 = self.changes(after=page1["next_after"], limit=2)
        page3 = self.changes(after=page2["next_after"], limit=100)
        seen = [c["change_id"] for p in (page1, page2, page3) for c in p["changes"]]
        self.assertEqual(seen, [1, 2, 3, 4, 5, 6])  # 不漏读
        self.assertEqual(len(seen), len(set(seen)))  # 不重复

        empty = self.changes(after=page3["next_after"])  # 已读完：空页
        self.assertEqual(empty["changes"], [])
        self.assertEqual(empty["next_after"], page3["next_after"])  # 游标不动

    def test_empty_changes_next_after_echoes_after(self):
        page = self.changes(after=0)
        self.assertEqual(page, {"changes": [], "next_after": 0})
        page = self.changes(after=42)
        self.assertEqual(page, {"changes": [], "next_after": 42})

    # ---- 误报事件撤回（retract_event）----

    def test_retract_unknown_event_returns_none(self):
        self.submit("s1", "e1", 1000.0)
        before_changes = self.changes()
        self.assertIsNone(self.retract("s1", "ghost"))
        self.assertIsNone(self.retract("ghost", "e1"))
        # 未写任何变更、未删任何段
        self.assertEqual(self.changes(), before_changes)
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn)), 1)

    def test_retract_sole_event_deletes_segment_and_writes_tombstone(self):
        r = self.submit("s1", "e1", 1000.0, "warning")
        seg_id = r["segment"]["id"]
        before = self.changes()

        out = self.retract("s1", "e1")
        self.assertEqual(out, {"source": "s1", "event_id": "e1", "segments": []})

        self.assertIsNone(self.get(seg_id))
        with session(self.db_path) as conn:
            self.assertEqual(service.list_segments(conn, "s1"), [])
            n = conn.execute("SELECT COUNT(*) AS c FROM events").fetchone()["c"]
            self.assertEqual(n, 0)

        new = self.changes(after=before["next_after"])["changes"]
        self.assertEqual(len(new), 1)
        ch = new[0]
        self.assertEqual(ch["type"], "deleted")
        self.assertEqual(ch["segment_id"], seg_id)
        self.assertIsNone(ch["merged_into"])
        self.assertIsNone(ch["revision"])
        self.assertIsNone(ch["segment"])

    def test_retract_inner_event_shrinks_segment_and_bumps_revision(self):
        # [0, 0]、[5, 5]（在窗口内）→ 段 [0,5]；撤回 0 处事件后仅剩 5
        r = self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 5.0, "critical")
        seg_id = r["segment"]["id"]
        self.assertEqual(self.get(seg_id)["revision"], 2)

        out = self.retract("s1", "e1")
        self.assertEqual(len(out["segments"]), 1)
        seg = out["segments"][0]
        self.assertEqual(seg["id"], seg_id)          # 沿用原 id
        self.assertEqual(seg["event_count"], 1)
        self.assertEqual(seg["revision"], 3)         # 撤回必递增，哪怕仍是同段
        self.assertEqual(seg["max_severity"], "critical")
        self.assertEqual(seg["started_at"], seg["ended_at"])
        self.assertEqual(seg["ended_at"], "1970-01-01T00:00:05+00:00")

        # 事件仍挂在原段
        with session(self.db_path) as conn:
            row = conn.execute(
                "SELECT segment_id FROM events WHERE event_id = 'e2'"
            ).fetchone()
            self.assertEqual(row["segment_id"], seg_id)

        new = self.changes(after=2)["changes"]
        self.assertEqual([c["type"] for c in new], ["upsert"])
        self.assertEqual(new[0]["segment"], seg)

    def test_retract_bridge_event_splits_segment_earliest_keeps_id(self):
        # 0 与 20 各成段（间隔 > 10），10 桥接合并为一段 [0,20]
        a = self.submit("s1", "e1", 0.0)["segment"]
        self.submit("s1", "e2", 20.0)
        self.submit("s1", "e3", 10.0)
        seg_id = a["id"]
        self.assertEqual(self.get(seg_id)["event_count"], 3)
        self.assertEqual(self.get(seg_id)["revision"], 2)
        changes_before = len(self.changes()["changes"])

        out = self.retract("s1", "e3")               # 撤掉桥接事件
        self.assertEqual(len(out["segments"]), 2)
        kept, created = out["segments"]
        self.assertEqual(kept["id"], seg_id)         # 最早子段沿用原 id
        self.assertEqual(kept["event_count"], 1)
        self.assertEqual(kept["revision"], 3)        # revision 递增
        self.assertEqual(kept["started_at"], "1970-01-01T00:00:00+00:00")
        self.assertNotIn(created["id"], (seg_id,))
        self.assertEqual(created["event_count"], 1)
        self.assertEqual(created["revision"], 1)     # 新段 revision 从 1 起
        self.assertEqual(created["started_at"], "1970-01-01T00:00:20+00:00")

        with session(self.db_path) as conn:
            segs = service.list_segments(conn, "s1")
            self.assertEqual({s["id"] for s in segs}, {kept["id"], created["id"]})
            e1 = conn.execute(
                "SELECT segment_id FROM events WHERE event_id = 'e1'"
            ).fetchone()
            e2 = conn.execute(
                "SELECT segment_id FROM events WHERE event_id = 'e2'"
            ).fetchone()
            self.assertEqual(e1["segment_id"], kept["id"])
            self.assertEqual(e2["segment_id"], created["id"])
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM events WHERE event_id = 'e3'"
                ).fetchone()
            )

        # 变更流：保留段 upsert 在前、新段 upsert 在后，change_id 保序
        new = self.changes(after=changes_before)["changes"]
        self.assertEqual([c["type"] for c in new], ["upsert", "upsert"])
        self.assertEqual(new[0]["segment_id"], kept["id"])
        self.assertEqual(new[0]["revision"], 3)
        self.assertEqual(new[1]["segment_id"], created["id"])
        self.assertEqual(new[1]["revision"], 1)
        self.assertEqual(new[1]["segment"], created)
        self.assertLess(new[0]["change_id"], new[1]["change_id"])

    def test_retract_bridge_splits_segment_into_two_clusters(self):
        # [0,5] 与 [20,25] 两段（间隔 15 > 10），12 处事件桥接合并为 [0,25]
        self.submit("s1", "e1", 0.0)
        a = self.submit("s1", "e2", 5.0)["segment"]
        self.submit("s1", "e3", 20.0)
        self.submit("s1", "e4", 25.0)
        self.submit("s1", "e5", 12.0)                # 桥接两段
        seg_id = a["id"]
        self.assertEqual(self.get(seg_id)["event_count"], 5)
        prior_revision = self.get(seg_id)["revision"]
        changes_before = len(self.changes()["changes"])

        out = self.retract("s1", "e5")               # 撤掉桥接事件
        self.assertEqual(len(out["segments"]), 2)
        kept, created = out["segments"]
        self.assertEqual(kept["id"], seg_id)
        self.assertEqual(kept["event_count"], 2)     # [0,5]
        self.assertEqual(kept["revision"], prior_revision + 1)
        self.assertEqual(created["event_count"], 2)  # [20,25]
        self.assertEqual(created["revision"], 1)
        self.assertEqual(
            kept["started_at"], "1970-01-01T00:00:00+00:00"
        )
        self.assertEqual(
            created["started_at"], "1970-01-01T00:00:20+00:00"
        )

    def test_retract_does_not_remerge_with_neighbouring_segments(self):
        # s1：[0,0] 与 [10.5, 20.5] 两段；撤回 0 段唯一事件 → 该段消失，
        # 不会与另一相邻段发生合并重算
        self.submit("s1", "e1", 0.0)
        other = self.submit("s1", "e2", 10.5)["segment"]
        out = self.retract("s1", "e1")
        self.assertEqual(out["segments"], [])
        with session(self.db_path) as conn:
            segs = service.list_segments(conn, "s1")
        self.assertEqual([s["id"] for s in segs], [other["id"]])
        self.assertEqual(segs[0]["event_count"], 1)

    def test_retract_is_isolated_by_source(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s2", "e1", 0.0)
        out = self.retract("s1", "e1")
        self.assertEqual(out["segments"], [])
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn)), 1)
            self.assertEqual(service.list_segments(conn)[0]["source"], "s2")

    def test_retract_then_reingest_same_event_id(self):
        r = self.submit("s1", "e1", 0.0)
        old_id = r["segment"]["id"]
        self.retract("s1", "e1")
        # 误报修正后以同 id 重新上报（不同时间）：全新段，不复用已删 id
        again = self.submit("s1", "e1", 100.0)
        self.assertFalse(again["deduplicated"])
        self.assertNotEqual(again["segment"]["id"], old_id)
        self.assertEqual(again["segment"]["revision"], 1)

    def test_retract_recomputes_max_severity(self):
        self.submit("s1", "e1", 0.0, "critical")
        self.submit("s1", "e2", 5.0, "info")
        out = self.retract("s1", "e1")
        self.assertEqual(out["segments"][0]["max_severity"], "info")

    def test_retract_rollback_entirely_on_failure(self):
        from app.db import connect

        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 20.0)
        bridge = self.submit("s1", "e3", 10.0)["segment"]
        seg_id = bridge["id"]
        changes_before = self.changes()

        # 在受控事务内让“写变更流”一步失败，随后显式回滚：
        # 事件删除、拆段 UPDATE/INSERT 必须全部撤销
        conn = connect(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            original_record = service._record_change

            def boom(*args, **kwargs):
                raise RuntimeError("change stream write failed")

            service._record_change = boom
            try:
                with self.assertRaises(RuntimeError):
                    service.retract_event(
                        conn, source="s1", event_id="e3", silence=SILENCE
                    )
            finally:
                service._record_change = original_record
            conn.rollback()
        finally:
            conn.close()

        seg = self.get(seg_id)
        self.assertEqual(seg["event_count"], 3)
        self.assertEqual(seg["revision"], 2)
        with session(self.db_path) as conn2:
            self.assertEqual(len(service.list_segments(conn2, "s1")), 1)
            self.assertEqual(
                conn2.execute("SELECT COUNT(*) AS c FROM events").fetchone()["c"], 3
            )
        self.assertEqual(self.changes(), changes_before)

    # ---- 静默间隔重分段预演（纯读）----

    def preview(self, source, silence):
        with read_only_session(self.db_path) as conn:
            return service.preview_resegmentation(
                conn, source=source, silence=silence
            )

    def test_preview_smaller_interval_splits_segments(self):
        # 当前 SILENCE=10：0、8、20、28、40 → 三段 [0,8]、[20,28]、[40]
        a = self.submit("s1", "e1", 0.0)["segment"]
        self.submit("s1", "e2", 8.0)
        b = self.submit("s1", "e3", 20.0)["segment"]
        self.submit("s1", "e4", 28.0)
        c = self.submit("s1", "e5", 40.0)["segment"]

        r = self.preview("s1", 5.0)
        # 间隔 5：8→20(12)、28→40(12) 切开；0→8(8) 切开 → 五段
        sims = r["simulated_segments"]
        self.assertEqual([s["temp_id"] for s in sims], [1, 2, 3, 4, 5])
        self.assertEqual(
            [s["event_count"] for s in sims], [1, 1, 1, 1, 1]
        )
        self.assertTrue(all(s["source"] == "s1" for s in sims))

        by_id = {m["segment_id"]: m for m in r["mappings"]}
        self.assertEqual(by_id[a["id"]]["relation"], "split")
        self.assertEqual(by_id[a["id"]]["simulated_temp_ids"], [1, 2])
        self.assertEqual(by_id[b["id"]]["relation"], "split")
        self.assertEqual(by_id[b["id"]]["simulated_temp_ids"], [3, 4])
        self.assertEqual(by_id[c["id"]]["relation"], "unchanged")
        self.assertEqual(by_id[c["id"]]["simulated_temp_ids"], [5])

        summary = r["summary"]
        self.assertEqual(summary["split_existing_segment_count"], 2)
        self.assertEqual(summary["merged_existing_segment_count"], 0)
        self.assertEqual(summary["unchanged_existing_segment_count"], 1)
        self.assertEqual(summary["changed_existing_segment_count"], 2)
        self.assertEqual(summary["existing_segment_count"], 3)
        self.assertEqual(summary["simulated_segment_count"], 5)
        self.assertEqual(r["event_count"], 5)

    def test_preview_larger_interval_merges_segments(self):
        # 0、8、20、28 → 现有两段 [0,8]、[20,28]
        a = self.submit("s1", "e1", 0.0)["segment"]
        self.submit("s1", "e2", 8.0)
        b = self.submit("s1", "e3", 20.0)["segment"]
        self.submit("s1", "e4", 28.0)

        r = self.preview("s1", 100.0)
        sims = r["simulated_segments"]
        self.assertEqual(len(sims), 1)
        self.assertEqual(sims[0]["temp_id"], 1)
        self.assertEqual(sims[0]["event_count"], 4)
        self.assertEqual(sims[0]["started_at"], "1970-01-01T00:00:00+00:00")
        self.assertEqual(sims[0]["max_severity"], "info")

        by_id = {m["segment_id"]: m for m in r["mappings"]}
        self.assertEqual(by_id[a["id"]]["relation"], "merged")
        self.assertEqual(by_id[a["id"]]["simulated_temp_ids"], [1])
        self.assertEqual(by_id[b["id"]]["relation"], "merged")
        self.assertEqual(by_id[b["id"]]["simulated_temp_ids"], [1])
        self.assertEqual(r["summary"]["merged_existing_segment_count"], 2)
        self.assertEqual(r["summary"]["changed_existing_segment_count"], 2)
        self.assertEqual(r["summary"]["split_existing_segment_count"], 0)
        self.assertEqual(r["summary"]["unchanged_existing_segment_count"], 0)

    def test_preview_equal_interval_all_unchanged(self):
        a = self.submit("s1", "e1", 0.0)["segment"]
        self.submit("s1", "e2", 8.0)
        b = self.submit("s1", "e3", 20.0)["segment"]

        r = self.preview("s1", SILENCE)
        self.assertEqual(len(r["simulated_segments"]), 2)
        self.assertTrue(all(m["relation"] == "unchanged" for m in r["mappings"]))
        self.assertEqual(
            [m["simulated_temp_ids"] for m in r["mappings"]], [[1], [2]]
        )
        self.assertEqual(r["summary"]["changed_existing_segment_count"], 0)
        self.assertEqual(r["summary"]["unchanged_existing_segment_count"], 2)
        # 模拟段与现有段边界一致
        sims = {s["temp_id"]: s for s in r["simulated_segments"]}
        self.assertEqual(sims[1]["event_count"], 2)
        self.assertEqual(sims[2]["event_count"], 1)

    def test_preview_boundary_gap_equal_to_interval_joins(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", SILENCE)  # 恰好等于间隔 → 同段
        r = self.preview("s1", SILENCE)
        self.assertEqual(len(r["simulated_segments"]), 1)

    def test_preview_max_severity_and_timestamps(self):
        self.submit("s1", "e1", 100.0, "info")
        self.submit("s1", "e2", 103.0, "critical")
        self.submit("s1", "e3", 95.0, "warning")  # 乱序到达
        r = self.preview("s1", 1000.0)
        seg = r["simulated_segments"][0]
        self.assertEqual(seg["started_at"], "1970-01-01T00:01:35+00:00")  # 95
        self.assertEqual(seg["ended_at"], "1970-01-01T00:01:43+00:00")    # 103
        self.assertEqual(seg["max_severity"], "critical")
        self.assertEqual(seg["event_count"], 3)

    def test_preview_stable_sort_tiebreaks_same_timestamp_by_event_id(self):
        self.submit("s1", "b", 0.0)
        self.submit("s1", "a", 0.0)    # 同刻事件
        self.submit("s1", "c", 100.0)  # 超静默 → 第二段
        r = self.preview("s1", 5.0)
        self.assertEqual(
            [s["temp_id"] for s in r["simulated_segments"]], [1, 2]
        )
        # 同刻事件必在同一段；temp_id 编号按 (occurred_at, event_id) 次序确定
        self.assertEqual(r["simulated_segments"][0]["event_count"], 2)

    def test_preview_is_isolated_by_source(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s2", "e2", 0.0)
        self.submit("s2", "e3", 100.0)
        r = self.preview("s2", 5.0)
        self.assertEqual(r["source"], "s2")
        self.assertEqual(r["event_count"], 2)
        self.assertEqual(len(r["simulated_segments"]), 2)
        self.assertEqual(r["summary"]["existing_segment_count"], 2)
        self.assertTrue(all(s["source"] == "s2" for s in r["simulated_segments"]))

    def test_preview_unknown_source_is_empty(self):
        self.submit("s1", "e1", 0.0)
        r = self.preview("nope", 10.0)
        self.assertEqual(r["event_count"], 0)
        self.assertEqual(r["simulated_segments"], [])
        self.assertEqual(r["mappings"], [])
        self.assertEqual(
            r["summary"],
            {
                "changed_existing_segment_count": 0,
                "split_existing_segment_count": 0,
                "merged_existing_segment_count": 0,
                "unchanged_existing_segment_count": 0,
                "existing_segment_count": 0,
                "simulated_segment_count": 0,
            },
        )

    def test_preview_repeated_requests_are_identical(self):
        for i in range(6):
            self.submit("s1", f"e{i}", float(i) * 7)
        r1 = self.preview("s1", 5.0)
        r2 = self.preview("s1", 5.0)
        self.assertEqual(r1, r2)

    def test_preview_does_not_mutate_data(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 8.0)
        self.submit("s1", "e3", 20.0)
        with session(self.db_path) as conn:
            segs_before = service.list_segments(conn)
            changes_before = service.list_segment_changes(conn)
            events_before = conn.execute("SELECT * FROM events ORDER BY id").fetchall()

        for candidate in (0.0, 5.0, 100.0):
            self.preview("s1", candidate)

        with session(self.db_path) as conn:
            segs_after = service.list_segments(conn)
            changes_after = service.list_segment_changes(conn)
            events_after = conn.execute("SELECT * FROM events ORDER BY id").fetchall()
        self.assertEqual(segs_after, segs_before)
        self.assertEqual(changes_after, changes_before)
        self.assertEqual(
            [tuple(e) for e in events_after], [tuple(e) for e in events_before]
        )

    def test_preview_read_only_session_rejects_writes(self):
        from app.db import read_only_session

        self.submit("s1", "e1", 0.0)
        with read_only_session(self.db_path) as conn:
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute(
                    "INSERT INTO segments "
                    "(source, start_ts, end_ts, max_severity, event_count, "
                    " revision, created_at, updated_at) "
                    "VALUES ('x', 0, 0, 'info', 1, 1, 0, 0)"
                )
        # 回滚释放后库仍可正常写入
        self.submit("s1", "e2", 8.0)

    # ---- 旧库变更流迁移（deleted 类型上线）----

    def test_legacy_changes_table_migrates_and_keeps_change_id_order(self):
        import sqlite3

        legacy = os.path.join(self.tmp.name, "legacy.db")
        conn = sqlite3.connect(legacy)
        conn.executescript(
            """
            CREATE TABLE segment_changes (
                change_id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL CHECK (type IN ('upsert', 'merged')),
                segment_id INTEGER NOT NULL, merged_into INTEGER,
                revision INTEGER, source TEXT, start_ts REAL, end_ts REAL,
                max_severity TEXT, event_count INTEGER, created_at REAL NOT NULL
            );
            INSERT INTO segment_changes
                (change_id, type, segment_id, merged_into, revision, source,
                 start_ts, end_ts, max_severity, event_count, created_at)
            VALUES
                (1, 'upsert', 1, NULL, 1, 's1', 0, 0, 'info', 1, 0),
                (2, 'merged', 2, 1, NULL, NULL, NULL, NULL, NULL, NULL, 0);
            """
        )
        conn.commit()
        conn.close()

        # init_db 幂等建表并检测到旧 CHECK → 同事务迁移
        init_db(legacy)
        with session(legacy) as conn:
            rows = conn.execute(
                "SELECT change_id, type, segment_id, merged_into "
                "FROM segment_changes ORDER BY change_id"
            ).fetchall()
            self.assertEqual(
                [tuple(r) for r in rows],
                [(1, "upsert", 1, None), (2, "merged", 2, 1)],
            )

        # 迁移后可写 deleted 墓碑，且 change_id 接续历史最大值
        with session(legacy) as conn:
            conn.execute("BEGIN IMMEDIATE")
            service._record_change(
                conn, change_type="deleted", segment_id=1, now=0.0
            )
        with session(legacy) as conn:
            row = conn.execute(
                "SELECT change_id, type FROM segment_changes "
                "WHERE type = 'deleted'"
            ).fetchone()
            self.assertEqual(tuple(row), (3, "deleted"))

        # 再次 init_db 为幂等 no-op，数据不丢不重
        init_db(legacy)
        with session(legacy) as conn:
            n = conn.execute("SELECT COUNT(*) AS c FROM segment_changes").fetchone()
            self.assertEqual(n["c"], 3)


if __name__ == "__main__":
    unittest.main()
