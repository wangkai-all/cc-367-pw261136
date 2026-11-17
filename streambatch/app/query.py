"""查询端：所有读路径。流/批分区在这里对用户可见、可解释。"""
from __future__ import annotations
import sqlite3

from .reconcile import full_reconcile


def _fmt_rows(rows) -> list[dict]:
    return [dict(r) for r in rows]


def overview(conn: sqlite3.Connection) -> dict:
    mat = materialized_groups(conn)
    stream_total = conn.execute(
        "SELECT COALESCE(SUM(amount),0) s FROM stream_agg").fetchone()["s"]
    anchor_total = conn.execute(
        "SELECT COALESCE(SUM(amount),0) s FROM batch_anchors").fetchone()["s"]
    repl_total = conn.execute(
        "SELECT COALESCE(SUM(t.total + COALESCE(l.s,0)),0) s FROM replace_coverage c "
        "JOIN repl_snapshot t ON t.batch_id=c.batch_id AND t.bucket=c.bucket "
        "LEFT JOIN (SELECT bucket, dim, SUM(delta) s FROM ledger "
        "WHERE batch_id IS NULL GROUP BY bucket, dim) l "
        "ON l.bucket=t.bucket AND l.dim=t.dim").fetchone()["s"]
    ledger_total = conn.execute(
        "SELECT COALESCE(SUM(delta),0) s FROM ledger").fetchone()["s"]
    counts = {
        "events": conn.execute("SELECT COUNT(*) c FROM events").fetchone()["c"],
        "active_txns": conn.execute(
            "SELECT COUNT(*) c FROM events e JOIN ("
            "SELECT txn_id, MAX(version) mv FROM events GROUP BY txn_id) t "
            "ON e.txn_id=t.txn_id AND e.version=t.mv WHERE e.status='active'"
        ).fetchone()["c"],
        "ledger_rows": conn.execute("SELECT COUNT(*) c FROM ledger").fetchone()["c"],
        "stream_ledger_rows": conn.execute(
            "SELECT COUNT(*) c FROM ledger WHERE batch_id IS NULL").fetchone()["c"],
        "batch_ledger_rows": conn.execute(
            "SELECT COUNT(*) c FROM ledger WHERE batch_id IS NOT NULL").fetchone()["c"],
    }
    return {"stream_total": stream_total,
            "batch_total": anchor_total + repl_total,
            "grand_total": sum(mat.values()),
            "ledger_grand_total": ledger_total, "counts": counts}


def materialized_groups(conn) -> dict[tuple[int, str], int]:
    from .reconcile import materialized_recompute
    return materialized_recompute(conn)


def bucket_summary(conn: sqlite3.Connection) -> list[dict]:
    """每个桶一行：流式部分、批处理部分（含 replace 快照+迟到增量）、合计与属主。"""
    covered = {r["bucket"]: dict(r) for r in conn.execute(
        "SELECT * FROM replace_coverage")}
    agg: dict[int, dict] = {}

    def cell(bucket: int) -> dict:
        return agg.setdefault(bucket, {"bucket": bucket, "stream_amount": 0,
                                       "batch_amount": 0, "batch_ids": [],
                                       "handed_off": False, "mode": None})

    # ledger 桶
    for r in conn.execute("SELECT bucket, SUM(amount) a FROM stream_agg "
                          "WHERE bucket NOT IN (SELECT bucket FROM replace_coverage) "
                          "GROUP BY bucket"):
        cell(r["bucket"])["stream_amount"] += r["a"]
    for r in conn.execute(
            "SELECT a.bucket, a.amount, a.batch_id FROM batch_anchors a "
            "JOIN batch_runs r ON r.id=a.batch_id WHERE r.state='committed' "
            "AND a.bucket NOT IN (SELECT bucket FROM replace_coverage)"):
        d = cell(r["bucket"])
        d["batch_amount"] += r["amount"]
        d["batch_ids"].append(r["batch_id"])
        d["handed_off"] = True
        d["mode"] = "ledger"

    # replace 桶：快照 + 迟到增量（迟到增量即该桶未锚定 ledger，体现在 stream 侧概念里）
    for bucket, cov in covered.items():
        d = cell(bucket)
        d["mode"] = "replace"
        d["handed_off"] = True
        d["batch_ids"] = [cov["batch_id"]]
        snap = conn.execute(
            "SELECT COALESCE(SUM(total),0) s FROM repl_snapshot WHERE batch_id=? AND bucket=?",
            (cov["batch_id"], bucket)).fetchone()["s"]
        late = conn.execute(
            "SELECT COALESCE(SUM(delta),0) s FROM ledger WHERE bucket=? "
            "AND batch_id IS NULL AND ingest_seq>?",
            (bucket, cov["snapshot_seq"])).fetchone()["s"]
        d["batch_amount"] += snap
        d["stream_amount"] += late  # 快照之后的迟到更正仍在流式区

    out = []
    for d in sorted(agg.values(), key=lambda x: x["bucket"]):
        d["total"] = d["stream_amount"] + d["batch_amount"]
        out.append(d)
    return out


def ledger_detail(conn: sqlite3.Connection, bucket: int | None = None,
                  dim: str | None = None, side: str | None = None,
                  txn_id: str | None = None) -> list[dict]:
    """逐条贡献明细，可按 桶/维度/流批侧/交易 过滤。side: stream | batch"""
    sql = ("SELECT l.id, l.txn_id, l.version, l.op, l.dim, l.bucket, l.delta, "
           "l.ingest_seq, l.batch_id, e.status AS txn_status, e.event_time "
           "FROM ledger l LEFT JOIN events e "
           "ON e.txn_id=l.txn_id AND e.version=l.version WHERE 1=1")
    args: list = []
    if bucket is not None:
        sql += " AND l.bucket=?"; args.append(bucket)
    if dim is not None:
        sql += " AND l.dim=?"; args.append(dim)
    if txn_id is not None:
        sql += " AND l.txn_id=?"; args.append(txn_id)
    if side == "stream":
        sql += " AND l.batch_id IS NULL"
    elif side == "batch":
        sql += " AND l.batch_id IS NOT NULL"
    sql += " ORDER BY l.bucket, l.id"
    return _fmt_rows(conn.execute(sql, args).fetchall())


def event_list(conn: sqlite3.Connection, txn_id: str | None = None) -> list[dict]:
    sql = ("SELECT txn_id, version, op, dim, event_time, bucket, amount, status, "
           "ingest_seq, ingested_at FROM events")
    args: list = []
    if txn_id:
        sql += " WHERE txn_id=?"; args.append(txn_id)
    sql += " ORDER BY txn_id, version"
    return _fmt_rows(conn.execute(sql, args).fetchall())


def batch_runs(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT * FROM batch_runs ORDER BY id DESC").fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["boundary_explained"] = (
            f"本批只接管【事件时间 < {r['watermark_event_time']}】且"
            f"【接入序号 ≤ {r['snapshot_ingest_seq']}】的未锚定贡献；"
            f"之后新到事件的 batch_id 恒为 NULL，留在流式区，不会被覆盖。"
        )
        out.append(d)
    return out


def batch_detail(conn: sqlite3.Connection, run_id: int) -> dict:
    run = conn.execute("SELECT * FROM batch_runs WHERE id=?", (run_id,)).fetchone()
    if run is None:
        return {}
    anchors = _fmt_rows(conn.execute(
        "SELECT * FROM batch_anchors WHERE batch_id=? ORDER BY bucket, dim",
        (run_id,)).fetchall())
    contributions = _fmt_rows(conn.execute(
        "SELECT id, txn_id, version, op, dim, bucket, delta, ingest_seq "
        "FROM ledger WHERE batch_id=? ORDER BY bucket, id", (run_id,)).fetchall())
    snaps = _fmt_rows(conn.execute(
        "SELECT * FROM repl_snapshot WHERE batch_id=? ORDER BY bucket, dim",
        (run_id,)).fetchall())
    run_dict = dict(run)
    run_dict["boundary_explained"] = (
        f"本批只接管【事件时间 < {run['watermark_event_time']}】且"
        f"【接入序号 ≤ {run['snapshot_ingest_seq']}】的未锚定贡献；"
        f"之后新到事件的 batch_id 恒为 NULL，留在流式区，不会被覆盖。"
    )
    return {"run": run_dict, "anchors": anchors,
            "contributions": contributions, "replace_snapshots": snaps}


def control_state(conn: sqlite3.Connection) -> dict:
    return dict(conn.execute("SELECT * FROM batch_control WHERE id=1").fetchone())


def reconcile(conn: sqlite3.Connection) -> dict:
    return full_reconcile(conn)


def trace(conn: sqlite3.Connection, bucket: int | None = None,
          dim: str | None = None) -> list[dict]:
    from .reconcile import trace_difference
    return trace_difference(conn, bucket, dim)


def audit_tail(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    return _fmt_rows(conn.execute(
        "SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,)).fetchall())
