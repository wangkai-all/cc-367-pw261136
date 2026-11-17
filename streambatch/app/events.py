"""事件模型与接入逻辑。

核心语义（三个进程共同遵守的业务规则）：
- 每个逻辑交易 txn_id 有多个版本，版本单调递增；
- 更正（upsert 新版本）= 对旧版本贡献做【反向冲销】+ 写入新版本正向贡献；
- 删除（delete）= 只做反向冲销，事件状态置 deleted；
- 所有贡献都是带符号的整数 delta，落在【事件时间】所在的小时桶。
  因此迟到的更正/删除天然可以落到已经交接过的旧桶，由批处理重算兜底。
"""
from __future__ import annotations
import sqlite3
import time

from . import config, store


def bucket_of(event_time: int) -> int:
    return (event_time // config.BUCKET_SECONDS) * config.BUCKET_SECONDS


def clock_now(conn: sqlite3.Connection) -> int:
    """演示用虚拟时钟；未初始化时跟随真实墙钟。"""
    v = store.get_meta(conn, "clock_now")
    return int(v) if v is not None else int(time.time())


def set_clock(conn: sqlite3.Connection, ts: int) -> None:
    store.set_meta(conn, "clock_now", str(int(ts)))


def advance_clock(conn: sqlite3.Connection, seconds: int) -> int:
    now = clock_now(conn)
    now += int(seconds)
    set_clock(conn, now)
    return now


def _apply_stream_delta(conn: sqlite3.Connection, bucket: int, dim: str, delta: int) -> None:
    conn.execute(
        "INSERT INTO stream_agg(bucket, dim, amount) VALUES(?,?,?) "
        "ON CONFLICT(bucket,dim) DO UPDATE SET amount = amount + excluded.amount",
        (bucket, dim, delta),
    )
    row = conn.execute(
        "SELECT amount FROM stream_agg WHERE bucket=? AND dim=?", (bucket, dim)
    ).fetchone()
    if row is not None and row["amount"] == 0:
        conn.execute("DELETE FROM stream_agg WHERE bucket=? AND dim=?", (bucket, dim))


def _write_ledger(conn: sqlite3.Connection, *, txn_id, version, dim, bucket, delta, op, seq) -> None:
    conn.execute(
        "INSERT INTO ledger(txn_id, version, dim, bucket, delta, op, ingest_seq, batch_id) "
        "VALUES (?,?,?,?,?,?,?,NULL)",
        (txn_id, version, dim, bucket, delta, op, seq),
    )
    _apply_stream_delta(conn, bucket, dim, delta)


def latest_event(conn: sqlite3.Connection, txn_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM events WHERE txn_id=? ORDER BY version DESC LIMIT 1", (txn_id,)
    ).fetchone()


def ingest(conn: sqlite3.Connection, *, txn_id: str, dim: str, amount: int,
           event_time: int | None = None, op: str = "upsert") -> dict:
    """接入一笔事件（新交易 / 更正 / 删除）。整段在一个 IMMEDIATE 事务内。"""
    if op not in ("upsert", "delete"):
        raise ValueError("op 必须是 upsert 或 delete")
    amount = int(amount)
    if op == "upsert" and amount < 0:
        raise ValueError("金额不能为负；冲销由系统内部以带符号 delta 表达")

    conn.execute("BEGIN IMMEDIATE")
    try:
        seq = store.next_ingest_seq(conn)
        if event_time is None:
            event_time = clock_now(conn)
        event_time = int(event_time)
        bucket = bucket_of(event_time)
        prior = latest_event(conn, txn_id)
        version = (prior["version"] + 1) if prior else 1
        deltas: list[dict] = []

        if op == "delete" and prior is None:
            raise ValueError(f"交易 {txn_id} 不存在，无法删除")
        if op == "delete" and prior["status"] == "deleted":
            raise ValueError(f"交易 {txn_id} 已经是删除状态")

        # 第一步：旧版本若仍有效，先反向冲销其全部贡献
        if prior is not None and prior["status"] == "active":
            rev = {
                "txn_id": txn_id, "version": version, "dim": prior["dim"],
                "bucket": prior["bucket"], "delta": -prior["amount"],
                "op": "reverse", "ingest_seq": seq,
            }
            deltas.append(rev)
            _write_ledger(conn, txn_id=txn_id, version=version, dim=prior["dim"],
                          bucket=prior["bucket"], delta=-prior["amount"],
                          op="reverse", seq=seq)

        # 第二步：upsert 写入新版本正向贡献；delete 不产生正向贡献
        if op == "upsert":
            fwd = {
                "txn_id": txn_id, "version": version, "dim": dim,
                "bucket": bucket, "delta": amount,
                "op": "forward", "ingest_seq": seq,
            }
            deltas.append(fwd)
            _write_ledger(conn, txn_id=txn_id, version=version, dim=dim,
                          bucket=bucket, delta=amount, op="forward", seq=seq)

        new_status = "deleted" if op == "delete" else "active"
        new_amount = 0 if op == "delete" else amount
        conn.execute(
            "INSERT INTO events(txn_id, version, op, dim, event_time, bucket, "
            "amount, status, ingest_seq, ingested_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (txn_id, version, op, dim, event_time, bucket, new_amount,
             new_status, seq, clock_now(conn)),
        )
        store.audit(conn, "ingest", op,
                    f"txn={txn_id} v{version} seq={seq} bucket={bucket} deltas={len(deltas)}")
        conn.execute("COMMIT")
        return {
            "txn_id": txn_id, "version": version, "ingest_seq": seq,
            "bucket": bucket, "status": new_status, "deltas": deltas,
        }
    except Exception:
        conn.execute("ROLLBACK")
        raise
