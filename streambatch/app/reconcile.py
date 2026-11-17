"""对账与差额追踪。

正确性不靠某个总数对得上，而是三条互相独立的计算路径逐桶逐维度比对：

  A. 原始事实重算 ：只信 events 表每个交易的【最新版本状态】，独立 SUM；
  B. 台账重算     ：只信 ledger 带符号贡献，按桶/维度 SUM；
  C. 物化视图重算 ：流式 stream_agg + 批侧 batch_anchors（或 repl_snapshot）。

任何一方被篡改或丢行，都会在具体 (bucket, dim) 上暴露差额，并可继续下钻到
txn_id：对比"该交易在 events 中的应有净贡献"与"ledger 中实际记录的贡献"，
直接定位是哪一个事件导致了差额。
"""
from __future__ import annotations
import sqlite3

from .events import bucket_of  # noqa: F401  (保持桶规则单一来源)


def raw_recompute(conn: sqlite3.Connection) -> dict[tuple[int, str], int]:
    """路径 A：从原始事件最新状态独立重算（完全不看 ledger / 物化表）。"""
    rows = conn.execute(
        "SELECT e.bucket, e.dim, e.amount FROM events e JOIN ("
        "  SELECT txn_id, MAX(version) mv FROM events GROUP BY txn_id"
        ") t ON e.txn_id=t.txn_id AND e.version=t.mv WHERE e.status='active'"
    ).fetchall()
    out: dict[tuple[int, str], int] = {}
    for r in rows:
        key = (r["bucket"], r["dim"])
        out[key] = out.get(key, 0) + r["amount"]
    return out


def ledger_recompute(conn: sqlite3.Connection) -> dict[tuple[int, str], int]:
    """路径 B：逐条带符号贡献求和。"""
    rows = conn.execute(
        "SELECT bucket, dim, SUM(delta) s FROM ledger GROUP BY bucket, dim"
    ).fetchall()
    return {(r["bucket"], r["dim"]): r["s"] for r in rows}


def materialized_recompute(conn: sqlite3.Connection) -> dict[tuple[int, str], int]:
    """路径 C：按桶所有权合成的物化真值。

    - ledger 桶：stream_agg（迟到残留）+ batch_anchors；
    - replace 桶：repl_snapshot 冻结值（seq<=S）+ 该桶 seq>S 的迟到 ledger 增量，
      即「整体快照 + 迟到逐条贡献」。同一桶只有一个 coverage 属主，不会累加多批快照。
    """
    out: dict[tuple[int, str], int] = {}

    def add(key, val):
        out[key] = out.get(key, 0) + val

    covered = {r["bucket"]: r for r in conn.execute(
        "SELECT bucket, batch_id, snapshot_seq FROM replace_coverage")}
    covered_set = set(covered)

    # 未被 replace 覆盖的桶：流式物化 + ledger 锚点
    for r in conn.execute(
            "SELECT bucket, dim, amount FROM stream_agg WHERE bucket NOT IN "
            "(SELECT bucket FROM replace_coverage)"):
        add((r["bucket"], r["dim"]), r["amount"])
    for r in conn.execute(
            "SELECT a.bucket, a.dim, a.amount FROM batch_anchors a "
            "WHERE a.bucket NOT IN (SELECT bucket FROM replace_coverage)"):
        add((r["bucket"], r["dim"]), r["amount"])

    # replace 桶：快照 + 快照序号之后的迟到增量（来自 ledger，未锚定）
    for bucket, cov in covered.items():
        for r in conn.execute(
                "SELECT dim, total FROM repl_snapshot WHERE batch_id=? AND bucket=?",
                (cov["batch_id"], bucket)):
            add((bucket, r["dim"]), r["total"])
        late = conn.execute(
            "SELECT dim, SUM(delta) s FROM ledger WHERE bucket=? AND batch_id IS NULL "
            "AND ingest_seq>? GROUP BY dim", (bucket, cov["snapshot_seq"])).fetchall()
        for r in late:
            add((bucket, r["dim"]), r["s"])
    return {k: v for k, v in out.items() if v != 0}


def replace_invariants(conn: sqlite3.Connection) -> list[dict]:
    """replace 桶专属守恒：快照(seq<=S) + 迟到增量(seq>S) 必须等于原始事实重算。"""
    raw = raw_recompute(conn)
    out = []
    for cov in conn.execute("SELECT * FROM replace_coverage"):
        b, seq = cov["bucket"], cov["snapshot_seq"]
        snap = {r["dim"]: r["total"] for r in conn.execute(
            "SELECT dim, total FROM repl_snapshot WHERE batch_id=? AND bucket=?",
            (cov["batch_id"], b))}
        late_rows = conn.execute(
            "SELECT dim, SUM(delta) s FROM ledger WHERE bucket=? AND batch_id IS NULL "
            "AND ingest_seq>? GROUP BY dim", (b, seq)).fetchall()
        mat: dict[str, int] = dict(snap)
        for r in late_rows:
            mat[r["dim"]] = mat.get(r["dim"], 0) + r["s"]
        dims = set(mat) | {k[1] for k in raw if k[0] == b}
        for dim in dims:
            want = raw.get((b, dim), 0)
            got = mat.get(dim, 0)
            if want != got:
                out.append({"bucket": b, "dim": dim, "raw": want,
                            "snapshot_plus_late": got, "difference": got - want})
    return out


def expected_net_per_txn(conn: sqlite3.Connection) -> dict[str, int]:
    """每个交易按 events 最新状态【应有】的净贡献（带交易自身桶/维度信息另查）。"""
    rows = conn.execute(
        "SELECT e.txn_id, e.bucket, e.dim, e.amount, e.status FROM events e JOIN ("
        "  SELECT txn_id, MAX(version) mv FROM events GROUP BY txn_id"
        ") t ON e.txn_id=t.txn_id AND e.version=t.mv"
    ).fetchall()
    return {r["txn_id"]: (r["amount"] if r["status"] == "active" else 0)
            for r in rows}


def actual_net_per_txn(conn: sqlite3.Connection) -> dict[str, int]:
    """每个交易在 ledger 中【实际】记录的净贡献。"""
    rows = conn.execute(
        "SELECT txn_id, SUM(delta) s FROM ledger GROUP BY txn_id").fetchall()
    return {r["txn_id"]: r["s"] for r in rows}


def trace_difference(conn: sqlite3.Connection, bucket: int | None = None,
                     dim: str | None = None) -> list[dict]:
    """下钻：逐交易比对应有净贡献与台账净贡献，返回所有对不上的事件。"""
    latest = conn.execute(
        "SELECT e.txn_id, e.bucket, e.dim, e.amount, e.status, e.version FROM events e "
        "JOIN (SELECT txn_id, MAX(version) mv FROM events GROUP BY txn_id) t "
        "ON e.txn_id=t.txn_id AND e.version=t.mv").fetchall()
    actual = actual_net_per_txn(conn)
    findings = []
    for r in latest:
        if bucket is not None and r["bucket"] != bucket:
            continue
        if dim is not None and r["dim"] != dim:
            continue
        expected = r["amount"] if r["status"] == "active" else 0
        got = actual.get(r["txn_id"], 0)
        if expected != got:
            findings.append({
                "txn_id": r["txn_id"], "bucket": r["bucket"], "dim": r["dim"],
                "version": r["version"], "status": r["status"],
                "expected_from_events": expected,
                "actual_in_ledger": got,
                "difference": got - expected,
            })
    return findings


def full_reconcile(conn: sqlite3.Connection) -> dict:
    a = raw_recompute(conn)
    b = ledger_recompute(conn)
    c = materialized_recompute(conn)
    keys = sorted(set(a) | set(b) | set(c))
    diffs = []
    for k in keys:
        va, vb, vc = a.get(k, 0), b.get(k, 0), c.get(k, 0)
        if not (va == vb == vc):
            diffs.append({
                "bucket": k[0], "dim": k[1],
                "raw_events": va, "ledger_sum": vb, "materialized": vc,
                "ledger_minus_raw": vb - va,
                "materialized_minus_raw": vc - va,
            })
    # 分区守恒（限定在非 replace 覆盖桶；replace 桶由 replace_invariants 单独验证）
    not_cov = "bucket NOT IN (SELECT bucket FROM replace_coverage)"
    stream_part = conn.execute(
        f"SELECT COALESCE(SUM(delta),0) s FROM ledger WHERE batch_id IS NULL AND {not_cov}"
    ).fetchone()["s"]
    stream_mat = conn.execute(
        f"SELECT COALESCE(SUM(amount),0) s FROM stream_agg WHERE {not_cov}"
    ).fetchone()["s"]
    anchor_part = conn.execute(
        f"SELECT COALESCE(SUM(delta),0) s FROM ledger WHERE batch_id IS NOT NULL AND {not_cov}"
    ).fetchone()["s"]
    anchor_mat = conn.execute(
        f"SELECT COALESCE(SUM(amount),0) s FROM batch_anchors WHERE {not_cov}"
    ).fetchone()["s"]
    repl_diffs = replace_invariants(conn)
    return {
        "ok": (not diffs and not repl_diffs and stream_part == stream_mat
               and anchor_part == anchor_mat),
        "group_differences": diffs,
        "replace_bucket_differences": repl_diffs,
        "partition_invariants": {
            "stream_ledger_vs_materialized": {"ledger": stream_part,
                                              "materialized": stream_mat,
                                              "ok": stream_part == stream_mat},
            "batch_ledger_vs_anchors": {"ledger": anchor_part,
                                        "anchors": anchor_mat,
                                        "ok": anchor_part == anchor_mat},
            "replace_snapshot_plus_late_vs_raw": {"violations": len(repl_diffs),
                                                  "ok": not repl_diffs},
        },
        "totals": {
            "raw_events": sum(a.values()), "ledger": sum(b.values()),
            "materialized": sum(c.values()),
        },
        "trace": trace_difference(conn),
    }
