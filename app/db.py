"""SQLite 连接与 schema 初始化（幂等，服务启动时自动执行）。"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

# 段变更流表 DDL：供下游增量同步。与事件写入同事务追加，change_id 单调递增。
# 单独成常量，便于既有库迁移时重建（误报撤回在 CHECK 中新增了 deleted 类型）。
SEGMENT_CHANGES_DDL = """
CREATE TABLE IF NOT EXISTS segment_changes (
    change_id    INTEGER PRIMARY KEY AUTOINCREMENT,  -- 单调递增的变更序号（分页游标）
    type         TEXT    NOT NULL CHECK (type IN ('upsert', 'merged', 'deleted')),
    segment_id   INTEGER NOT NULL,          -- upsert: 目标段；墓碑: 被删除段
    merged_into  INTEGER,                   -- 仅 merged：指向保留段 id
    revision     INTEGER,                   -- 仅 upsert：变更后的 revision
    source       TEXT,                      -- 以下仅 upsert：变更时刻的段快照
    start_ts     REAL,
    end_ts       REAL,
    max_severity TEXT,
    event_count  INTEGER,
    created_at   REAL    NOT NULL
);
-- 查询模式为 WHERE change_id > ? ORDER BY change_id，主键即索引，无需额外索引
"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS segments (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT    NOT NULL,
    start_ts      REAL    NOT NULL,          -- 段内最早 occurred_at（epoch 秒）
    end_ts        REAL    NOT NULL,          -- 段内最晚 occurred_at（epoch 秒）
    max_severity  TEXT    NOT NULL,
    event_count   INTEGER NOT NULL,
    revision      INTEGER NOT NULL,
    created_at    REAL    NOT NULL,
    updated_at    REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_segments_source ON segments (source, start_ts, end_ts);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    segment_id  INTEGER NOT NULL REFERENCES segments (id),
    source      TEXT    NOT NULL,
    event_id    TEXT    NOT NULL,
    occurred_at REAL    NOT NULL,            -- epoch 秒
    severity    TEXT    NOT NULL,
    received_at REAL    NOT NULL,
    UNIQUE (source, event_id)                -- 幂等去重键
);
CREATE INDEX IF NOT EXISTS idx_events_segment ON events (segment_id);

""" + SEGMENT_CHANGES_DDL


def _needs_changes_migration(conn: sqlite3.Connection) -> bool:
    """既有库的 segment_changes 仍为旧 schema（CHECK 不含 deleted）时返回真。"""
    sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'segment_changes'"
    ).fetchone()
    return bool(sql) and "'deleted'" not in sql[0]


def _migrate_segment_changes(conn: sqlite3.Connection) -> None:
    """放宽变更流 CHECK 到 upsert/merged/deleted。

    换表并按原 change_id 逐条复制，AUTOINCREMENT 序列取 MAX(change_id)，
    保证撤回产生的新记录接在历史之后、change_id 顺序不乱。
    """
    conn.execute("ALTER TABLE segment_changes RENAME TO segment_changes_old")
    conn.execute(SEGMENT_CHANGES_DDL)
    conn.execute(
        """
        INSERT INTO segment_changes
            (change_id, type, segment_id, merged_into, revision, source,
             start_ts, end_ts, max_severity, event_count, created_at)
        SELECT change_id, type, segment_id, merged_into, revision, source,
               start_ts, end_ts, max_severity, event_count, created_at
        FROM segment_changes_old
        ORDER BY change_id
        """
    )
    conn.execute("DROP TABLE segment_changes_old")
    # 重建 AUTOINCREMENT 序列，使下一条 change_id 接续历史最大值；表为空时
    # 删除可能存在的旧序列行，让下一条记录从 1 开始
    max_id = conn.execute(
        "SELECT MAX(change_id) AS m FROM segment_changes"
    ).fetchone()["m"]
    conn.execute("DELETE FROM sqlite_sequence WHERE name = 'segment_changes'")
    if max_id is not None:
        conn.execute(
            "INSERT INTO sqlite_sequence (name, seq) VALUES ('segment_changes', ?)",
            (max_id,),
        )


def connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(db_path: str) -> None:
    """建表，幂等；Docker 启动时即完成数据库初始化（含旧库轻量迁移）。"""
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
        if _needs_changes_migration(conn):
            # 部署在持久卷中的既有库仍为旧 CHECK（无 deleted）：同事务迁移
            _migrate_segment_changes(conn)


@contextmanager
def session(db_path: str):
    """事务上下文：成功提交、异常回滚、保证关闭连接。"""
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def read_only_session(db_path: str):
    """只读事务上下文：所有读在同一快照内完成，结束以回滚释放事务。

    - 显式 BEGIN 开启延迟读事务，多次 SELECT 共享同一快照；
    - query_only=ON 使任何 INSERT/UPDATE/DELETE/DDL 直接报错，
      从机制上杜绝预演逻辑误写事件、段或变更流。
    """
    conn = connect(db_path)
    try:
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        yield conn
        conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
