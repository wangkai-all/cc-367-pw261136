"""存储层：SQLite schema 与连接管理。

三个独立进程（接入 / 批处理 / 查询）通过同一个 WAL 模式的 SQLite 文件协作。
所有写操作都在短事务内完成，batch 的逐桶提交也是独立事务，因此提交中断后
可以从锚点记录精确恢复，不会出现"半批"状态。
"""
import sqlite3
import os
import time

from . import config

SCHEMA = """
-- 业务事件主表：每个逻辑交易一行，保存最新状态；更正/删除通过版本号迭代。
CREATE TABLE IF NOT EXISTS events (
    txn_id       TEXT NOT NULL,
    version      INTEGER NOT NULL,
    op           TEXT NOT NULL CHECK (op IN ('upsert','delete')),
    dim          TEXT NOT NULL,            -- 业务维度（演示中为商户号）
    event_time   INTEGER NOT NULL,         -- 业务事件时间（秒）
    bucket       INTEGER NOT NULL,         -- 事件时间对齐的小时桶起点
    amount       INTEGER NOT NULL,         -- 最新状态金额（分）；删除后为 0
    status       TEXT NOT NULL CHECK (status IN ('active','deleted')),
    ingest_seq   INTEGER NOT NULL,         -- 全局接入序号（交接快照用）
    ingested_at  INTEGER NOT NULL,
    PRIMARY KEY (txn_id, version)
);

-- 逐条贡献台账：每个版本产生一条或多条【带符号】贡献。
-- 更正 = 反向旧贡献 + 正向新贡献；删除 = 反向贡献。
-- batch_id 为 NULL 表示该行仍由流式侧持有（未锚定），非空表示已交接给某批次。
CREATE TABLE IF NOT EXISTS ledger (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    txn_id      TEXT NOT NULL,
    version     INTEGER NOT NULL,
    dim         TEXT NOT NULL,
    bucket      INTEGER NOT NULL,
    delta       INTEGER NOT NULL,          -- 带符号贡献（分）
    op          TEXT NOT NULL,
    ingest_seq  INTEGER NOT NULL,
    batch_id    INTEGER,                   -- NULL=流式区；否则锚定到的批次
    FOREIGN KEY (batch_id) REFERENCES batch_runs(id)
);
CREATE INDEX IF NOT EXISTS idx_ledger_anchor ON ledger(batch_id, bucket);
CREATE INDEX IF NOT EXISTS idx_ledger_bucket ON ledger(bucket, dim);
CREATE INDEX IF NOT EXISTS idx_ledger_txn ON ledger(txn_id);

-- 流式侧物化汇总（仅累加未锚定贡献），交接时随锚定一起扣减。
CREATE TABLE IF NOT EXISTS stream_agg (
    bucket      INTEGER NOT NULL,
    dim         TEXT NOT NULL,
    amount      INTEGER NOT NULL,
    PRIMARY KEY (bucket, dim)
);

-- 批次运行表：交接边界的三元组在这里留痕，可解释、可审计。
CREATE TABLE IF NOT EXISTS batch_runs (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    state                  TEXT NOT NULL CHECK (state IN
                           ('running','committed','crashed','superseded')),
    mode                   TEXT NOT NULL CHECK (mode IN ('ledger','replace')),
    watermark_event_time   INTEGER NOT NULL,  -- 边界①：只接管该时间之前的桶
    snapshot_ingest_seq    INTEGER NOT NULL,  -- 边界②：只接管该序号之前的接入
    bucket_count           INTEGER NOT NULL DEFAULT 0,
    anchored_rows          INTEGER NOT NULL DEFAULT 0,
    anchored_amount        INTEGER NOT NULL DEFAULT 0,
    fail_stage             TEXT,              -- 注入的故障点（演示用）
    started_at             INTEGER NOT NULL,
    finished_at            INTEGER
);

-- 逐桶锚点记录：每个桶的交接是一个独立事务，崩溃恢复以此为准。
CREATE TABLE IF NOT EXISTS batch_anchors (
    batch_id    INTEGER NOT NULL,
    bucket      INTEGER NOT NULL,
    dim         TEXT NOT NULL,
    row_count   INTEGER NOT NULL,
    amount      INTEGER NOT NULL,
    PRIMARY KEY (batch_id, bucket, dim),
    FOREIGN KEY (batch_id) REFERENCES batch_runs(id)
);

-- 对照方案（按区间整体替换）产生的快照表：整桶覆写，用于代价对比与缺陷演示。
CREATE TABLE IF NOT EXISTS repl_snapshot (
    batch_id    INTEGER NOT NULL,
    bucket      INTEGER NOT NULL,
    dim         TEXT NOT NULL,
    total       INTEGER NOT NULL,
    PRIMARY KEY (batch_id, bucket, dim)
);

-- replace 模式下每个桶当前的所有权归属（同一桶任一时刻只属于一个批次）。
-- ledger 批次接管该桶时删除对应行；新的 replace 批次覆写对应行（旧批次 superseded）。
CREATE TABLE IF NOT EXISTS replace_coverage (
    bucket        INTEGER PRIMARY KEY,
    batch_id      INTEGER NOT NULL,
    snapshot_seq  INTEGER NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES batch_runs(id)
);

-- 批处理器控制面：进程轮询这张表实现暂停/恢复/故障注入。
CREATE TABLE IF NOT EXISTS batch_control (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    state       TEXT NOT NULL CHECK (state IN ('running','paused')),
    fail_stage  TEXT,           -- none / before_commit / after_commit / mid_anchor
    fail_bucket INTEGER         -- 在第几个桶触发（NULL=第一个桶）
);

-- 通用 KV 元数据（虚拟时钟、接入序号等）。
CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);

-- 审计日志：所有关键动作留痕。
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    detail TEXT NOT NULL
);
"""


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or config.DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    cur = conn.execute("SELECT COUNT(*) c FROM batch_control")
    if cur.fetchone()["c"] == 0:
        conn.execute(
            "INSERT INTO batch_control(id, state, fail_stage, fail_bucket) "
            "VALUES (1, 'running', 'none', NULL)"
        )
    conn.execute(
        "INSERT OR IGNORE INTO meta(k, v) VALUES ('ingest_seq', '0')"
    )


def next_ingest_seq(conn: sqlite3.Connection) -> int:
    """在事务内领取全局接入序号。"""
    conn.execute("UPDATE meta SET v = CAST(CAST(v AS INTEGER) + 1 AS TEXT) WHERE k='ingest_seq'")
    return int(conn.execute("SELECT v FROM meta WHERE k='ingest_seq'").fetchone()["v"])


def get_meta(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = conn.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
    return row["v"] if row else default


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta(k,v) VALUES(?,?) "
        "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (key, value),
    )


def audit(conn: sqlite3.Connection, actor: str, action: str, detail: str) -> None:
    conn.execute(
        "INSERT INTO audit_log(ts, actor, action, detail) VALUES(?,?,?,?)",
        (int(time.time()), actor, action, detail),
    )
