"""批处理器：流批交接的执行体，作为独立进程轮询运行。

交接边界（可解释的三元组，全部落库到 batch_runs）：
  1. watermark_event_time : 只接管事件时间严格小于该水位的小时桶；
  2. snapshot_ingest_seq  : 批次启动瞬间冻结接入序号，只接管该序号之前的接入；
  3. 逐桶独立事务锚定     : 每处理一个桶提交一次，崩溃后凭 batch_anchors 续算。

两种交接方式：
  - ledger（选定方案）：把未锚定的逐条贡献原子改为本批持有，并同步扣减流式汇总。
    新事件的 ledger 行 batch_id 始终为 NULL，不可能被覆盖；迟到更正进入旧桶时，
    若桶已锚定则触发整桶失效重算（见 reanchor_late）。
  - replace（对照方案）：重算整桶并整体覆写到 repl_snapshot，不保留逐条贡献。
"""
from __future__ import annotations
import sqlite3
import time

from . import store

POLL_INTERVAL = 0.5


class InjectedCrash(Exception):
    """模拟"提交中断"：进程以非零码退出，留下 crashed 批次等待恢复。"""


# ---------------------------------------------------------------- 控制面
def get_control(conn: sqlite3.Connection) -> dict:
    row = conn.execute("SELECT * FROM batch_control WHERE id=1").fetchone()
    return dict(row)


def set_paused(conn: sqlite3.Connection, paused: bool) -> None:
    conn.execute("UPDATE batch_control SET state=? WHERE id=1",
                 ("paused" if paused else "running",))
    store.audit(conn, "control", "pause" if paused else "resume",
                "批处理器" + ("已暂停" if paused else "已恢复"))


def arm_fault(conn: sqlite3.Connection, fail_stage: str, fail_bucket: int | None) -> None:
    if fail_stage not in ("none", "before_commit", "mid_anchor"):
        raise ValueError("未知故障点")
    conn.execute(
        "UPDATE batch_control SET fail_stage=?, fail_bucket=? WHERE id=1",
        (fail_stage, fail_bucket),
    )
    store.audit(conn, "control", "arm_fault",
                f"故障注入已武装: {fail_stage} @ 桶序号 {fail_bucket}")


def _consume_fault(conn: sqlite3.Connection, stage: str, bucket_index: int) -> None:
    ctl = get_control(conn)
    if ctl["fail_stage"] == stage and (ctl["fail_bucket"] is None or
                                       ctl["fail_bucket"] == bucket_index):
        # 一次性：触发后立即解除武装，避免恢复后再次崩溃
        conn.execute(
            "UPDATE batch_control SET fail_stage='none', fail_bucket=NULL WHERE id=1"
        )
        store.audit(conn, "batch", "crash_injected",
                    f"在 {stage}（第 {bucket_index} 桶）模拟提交中断")
        raise InjectedCrash(f"注入故障：{stage} @ 桶{bucket_index}")


# ---------------------------------------------------------------- 批次创建
def create_run(conn: sqlite3.Connection, mode: str = "ledger",
               watermark: int | None = None) -> int:
    if mode not in ("ledger", "replace"):
        raise ValueError("mode 必须是 ledger 或 replace")
    if watermark is None:
        watermark = int(store.get_meta(conn, "clock_now", str(int(time.time()))))
    seq = int(store.get_meta(conn, "ingest_seq", "0"))
    cur = conn.execute(
        "INSERT INTO batch_runs(state, mode, watermark_event_time, "
        "snapshot_ingest_seq, started_at) VALUES ('running',?,?,?,?)",
        (mode, watermark, seq, int(time.time())),
    )
    run_id = cur.lastrowid
    store.audit(conn, "batch", "create_run",
                f"run={run_id} mode={mode} 边界: event_time<{watermark} 且 seq<={seq}")
    return run_id


def pending_buckets(conn: sqlite3.Connection, run: sqlite3.Row) -> list[tuple[int, int]]:
    """返回 (bucket, 未锚定行数) 列表：水位之内、快照之内、仍由流式持有的桶。"""
    rows = conn.execute(
        "SELECT bucket, COUNT(*) c FROM ledger "
        "WHERE batch_id IS NULL AND bucket < ? AND ingest_seq <= ? "
        "GROUP BY bucket ORDER BY bucket",
        (run["watermark_event_time"], run["snapshot_ingest_seq"]),
    ).fetchall()
    return [(r["bucket"], r["c"]) for r in rows]


# ---------------------------------------------------------------- ledger 锚定
def _reset_stream_agg(conn: sqlite3.Connection, bucket: int) -> None:
    """确定性重建某桶的流式物化汇总：其真值恒等于该桶未锚定贡献之和。

    交接/重锚后统一调用，避免增量加减在反复更正下累积漂移。
    """
    conn.execute("DELETE FROM stream_agg WHERE bucket=?", (bucket,))
    rows = conn.execute(
        "SELECT dim, SUM(delta) s FROM ledger WHERE batch_id IS NULL AND bucket=? "
        "GROUP BY dim HAVING s <> 0", (bucket,)).fetchall()
    for r in rows:
        conn.execute("INSERT INTO stream_agg(bucket,dim,amount) VALUES(?,?,?)",
                     (bucket, r["dim"], r["s"]))


def _anchor_bucket_ledger(conn: sqlite3.Connection, run_id: int, bucket: int,
                          run: sqlite3.Row) -> dict:
    conn.execute("BEGIN IMMEDIATE")
    rows = conn.execute(
        "SELECT id, dim, delta FROM ledger WHERE batch_id IS NULL AND bucket=? "
        "AND ingest_seq<=? ORDER BY id",
        (bucket, run["snapshot_ingest_seq"]),
    ).fetchall()
    n_rows = len(rows)
    amount = sum(r["delta"] for r in rows)
    if n_rows:
        conn.execute("UPDATE ledger SET batch_id=? WHERE batch_id IS NULL AND bucket=? "
                     "AND ingest_seq<=?", (run_id, bucket, run["snapshot_ingest_seq"]))
        by_dim: dict[str, int] = {}
        cnt: dict[str, int] = {}
        for r in rows:
            by_dim[r["dim"]] = by_dim.get(r["dim"], 0) + r["delta"]
            cnt[r["dim"]] = cnt.get(r["dim"], 0) + 1
        for dim, amt in by_dim.items():
            conn.execute(
                "INSERT INTO batch_anchors(batch_id,bucket,dim,row_count,amount) "
                "VALUES(?,?,?,?,?) ON CONFLICT(batch_id,bucket,dim) "
                "DO UPDATE SET row_count=excluded.row_count, amount=excluded.amount",
                (run_id, bucket, dim, cnt[dim], amt))
    # 该桶若此前被 replace 模式覆盖，ledger 接管后收回所有权并清理旧快照
    conn.execute("DELETE FROM replace_coverage WHERE bucket=?", (bucket,))
    conn.execute("DELETE FROM repl_snapshot WHERE bucket=?", (bucket,))
    _reset_stream_agg(conn, bucket)
    conn.execute("COMMIT")
    return {"bucket": bucket, "rows": n_rows, "amount": amount}


# ---------------------------------------------------------------- replace 对照
def _replace_bucket(conn: sqlite3.Connection, run_id: int, bucket: int,
                    run: sqlite3.Row) -> dict:
    """整体替换：从 events 当前状态重算整桶后覆写快照（不保留贡献明细）。

    所有权规则：同一桶只登记一个 replace_coverage 行；再次 replace 时旧批次
    被标记 superseded 且旧快照整桶删除，杜绝多批次快照累加双算。
    快照只冻结 seq<=S 的事实；seq>S 的迟到贡献保留在 ledger/stream_agg，
    查询时以「快照 + 迟到增量」合成当前真值。
    """
    conn.execute("BEGIN IMMEDIATE")
    # 关键：先在【全局、seq<=S】范围找每个交易的最新版本（不能按桶限定子查询，
    # 否则更正后迁到其他桶的交易，其旧桶版本会被误当作有效事实重算进来），
    # 再按当前桶与有效状态过滤。
    latest = conn.execute(
        "SELECT e.* FROM events e JOIN ("
        "  SELECT txn_id, MAX(version) mv FROM events "
        "  WHERE ingest_seq<=? GROUP BY txn_id"
        ") t ON e.txn_id=t.txn_id AND e.version=t.mv "
        "WHERE e.bucket=? AND e.status='active'",
        (run["snapshot_ingest_seq"], bucket),
    ).fetchall()
    by_dim: dict[str, int] = {}
    for r in latest:
        by_dim[r["dim"]] = by_dim.get(r["dim"], 0) + r["amount"]

    old = conn.execute("SELECT batch_id FROM replace_coverage WHERE bucket=?",
                       (bucket,)).fetchone()
    if old is not None:
        conn.execute("DELETE FROM repl_snapshot WHERE bucket=?", (bucket,))
        conn.execute("UPDATE batch_runs SET state='superseded' WHERE id=?",
                     (old["batch_id"],))
    conn.execute(
        "INSERT INTO replace_coverage(bucket,batch_id,snapshot_seq) VALUES(?,?,?) "
        "ON CONFLICT(bucket) DO UPDATE SET batch_id=excluded.batch_id, "
        "snapshot_seq=excluded.snapshot_seq",
        (bucket, run_id, run["snapshot_ingest_seq"]))
    for dim, total in by_dim.items():
        conn.execute(
            "INSERT INTO repl_snapshot(batch_id,bucket,dim,total) VALUES(?,?,?,?)",
            (run_id, bucket, dim, total),
        )
    # ledger 行不锚定（这正是 replace 方案丢失逐条可追溯性的原因）；
    # 流式物化只保留快照序号之后的迟到增量
    _reset_stream_agg(conn, bucket)
    conn.execute("COMMIT")
    return {"bucket": bucket, "rows": len(latest), "amount": sum(by_dim.values())}


# ---------------------------------------------------------------- 迟到更正重锚
def reanchor_late(conn: sqlite3.Connection) -> list[dict]:
    """已锚定桶出现迟到贡献（更正/删除）时，把该桶整体重锚到一个新批次。

    旧批次标记 superseded（仍保留历史），新批次继承边界并只重算受影响桶。
    这是 ledger 方案支持迟到更正的关键路径。
    """
    # 迟到是【事件时间】概念：未锚定行的事件时间桶已被某个【已提交】批次接管。
    # 正在运行批次所辖桶不参与（其快照之后的在线事件必须留在流式区）。
    late = conn.execute(
        "SELECT DISTINCT l.bucket FROM ledger l WHERE l.batch_id IS NULL AND EXISTS ("
        "  SELECT 1 FROM ledger x JOIN batch_runs br ON br.id=x.batch_id "
        "  WHERE x.bucket=l.bucket AND br.state IN ('committed','superseded'))"
    ).fetchall()
    results = []
    for row in late:
        bucket = row["bucket"]
        old = conn.execute(
            "SELECT DISTINCT batch_id FROM ledger WHERE bucket=? AND batch_id IS NOT NULL",
            (bucket,),
        ).fetchall()
        # 新批次冻结【当前】接入序号：吸收此刻为止该桶的全部迟到贡献；
        # 此后到达的在线事件序号更大，继续留在流式区，边界不可回溯。
        snap_seq = int(store.get_meta(conn, "ingest_seq", "0"))
        conn.execute("BEGIN IMMEDIATE")
        wm = bucket + 3600
        cur = conn.execute(
            "INSERT INTO batch_runs(state,mode,watermark_event_time,"
            "snapshot_ingest_seq,started_at,finished_at) "
            "VALUES('running','ledger',?,?,?,?)",
            (wm, snap_seq, int(time.time()), int(time.time())),
        )
        new_id = cur.lastrowid
        # 若该桶此前是 replace 覆盖，先收回其快照所有权，避免双份计数
        conn.execute("DELETE FROM replace_coverage WHERE bucket=?", (bucket,))
        conn.execute("DELETE FROM repl_snapshot WHERE bucket=?", (bucket,))
        # 旧锚定行 + 当前序号以内的未锚定迟到行重锚到新批次；更晚的在线行保持 NULL
        conn.execute(
            "UPDATE ledger SET batch_id=? WHERE bucket=? AND "
            "(batch_id IS NOT NULL OR ingest_seq<=?)",
            (new_id, bucket, snap_seq))
        conn.execute("DELETE FROM batch_anchors WHERE bucket=? AND batch_id IN "
                     "({})".format(",".join(str(o["batch_id"]) for o in old) or "0"),
                     (bucket,))
        agg = conn.execute(
            "SELECT dim, COUNT(*) c, SUM(delta) s FROM ledger WHERE bucket=? "
            "AND batch_id=? GROUP BY dim", (bucket, new_id)).fetchall()
        for a in agg:
            conn.execute(
                "INSERT INTO batch_anchors(batch_id,bucket,dim,row_count,amount) "
                "VALUES(?,?,?,?,?)", (new_id, bucket, a["dim"], a["c"], a["s"]))
        _reset_stream_agg(conn, bucket)
        for o in old:
            conn.execute("UPDATE batch_runs SET state='superseded' WHERE id=?",
                         (o["batch_id"],))
        conn.execute("UPDATE batch_runs SET state='committed', finished_at=? WHERE id=?",
                     (int(time.time()), new_id))
        conn.execute("COMMIT")
        results.append({"bucket": bucket, "new_run": new_id,
                        "superseded": [o["batch_id"] for o in old]})
        store.audit(conn, "batch", "reanchor_late",
                    f"桶{bucket} 迟到更正重锚: 新批次 {new_id}")
    return results


# ---------------------------------------------------------------- 主执行
def run_pending(conn: sqlite3.Connection, run_id: int) -> dict:
    run = conn.execute("SELECT * FROM batch_runs WHERE id=?", (run_id,)).fetchone()
    if run is None:
        raise ValueError(f"批次 {run_id} 不存在")
    if run["state"] == "committed":
        return {"run_id": run_id, "already": True}

    # 先收敛上一轮交接后、落在已锚定旧桶的迟到更正/删除（幂等：无迟到则为空操作）
    reanchor_late(conn)
    run = conn.execute("SELECT * FROM batch_runs WHERE id=?", (run_id,)).fetchone()
    done = {r["bucket"] for r in conn.execute(
        "SELECT DISTINCT bucket FROM batch_anchors WHERE batch_id=?", (run_id,))}
    todo = [b for b, _ in pending_buckets(conn, run) if b not in done]
    total_rows = total_amount = 0

    for idx, bucket in enumerate(todo):
        while True:  # 暂停轮询
            ctl = get_control(conn)
            if ctl["state"] == "running":
                break
            time.sleep(POLL_INTERVAL)

        if idx == 0 and not done:
            _consume_fault(conn, "before_commit", idx)
        if run["mode"] == "ledger":
            res = _anchor_bucket_ledger(conn, run_id, bucket, run)
        else:
            res = _replace_bucket(conn, run_id, bucket, run)
        total_rows += res["rows"]
        total_amount += res["amount"]
        conn.execute(
            "UPDATE batch_runs SET bucket_count=?, anchored_rows=?, anchored_amount=? "
            "WHERE id=?", (len(done) + idx + 1,
                          _sum_rows(conn, run_id),
                          _sum_amount(conn, run_id, run["mode"]), run_id))
        _consume_fault(conn, "mid_anchor", idx)  # 第 idx 个桶提交后触发（0 基，与武装值一致）

    conn.execute(
        "UPDATE batch_runs SET state='committed', finished_at=? WHERE id=?",
        (int(time.time()), run_id))
    store.audit(conn, "batch", "commit_run", f"run={run_id} 完成交接")
    # 注意：不在本批提交后立即 reanchor_late——那会回溯吸收本批快照之后的在线事件。
    # 迟到桶的收敛统一发生在【下一批次开始时】（见本函数开头），边界严格不回溯。
    return {"run_id": run_id, "buckets": len(todo),
            "rows": total_rows, "amount": total_amount}


def _sum_rows(conn: sqlite3.Connection, run_id: int) -> int:
    r = conn.execute("SELECT COALESCE(SUM(row_count),0) s FROM batch_anchors "
                     "WHERE batch_id=?", (run_id,)).fetchone()
    return r["s"]


def _sum_amount(conn: sqlite3.Connection, run_id: int, mode: str) -> int:
    if mode == "replace":
        r = conn.execute("SELECT COALESCE(SUM(total),0) s FROM repl_snapshot "
                         "WHERE batch_id=?", (run_id,)).fetchone()
    else:
        r = conn.execute("SELECT COALESCE(SUM(amount),0) s FROM batch_anchors "
                         "WHERE batch_id=?", (run_id,)).fetchone()
    return r["s"]


def mark_crashed(conn: sqlite3.Connection, run_id: int) -> None:
    conn.execute("UPDATE batch_runs SET state='crashed' WHERE id=? AND state='running'",
                 (run_id,))


def worker_loop(conn: sqlite3.Connection, once: bool = False) -> None:
    """独立进程入口：发现 running/crashed 批次就推进，否则等待触发。

    once=True 时注入故障会向上抛出（由入口以退出码 7 结束，模拟真实进程死亡）；
    常驻模式 once=False 时模拟守护监督：标记 crashed 后在循环内自动重新接管，
    凭 batch_anchors 从已提交桶的下一个桶续算，演示"提交中断 → 自动恢复"。
    """
    while True:
        run = conn.execute(
            "SELECT * FROM batch_runs WHERE state IN ('running','crashed') "
            "ORDER BY id LIMIT 1").fetchone()
        if run is not None:
            try:
                run_pending(conn, run["id"])
            except InjectedCrash:
                mark_crashed(conn, run["id"])
                if once:
                    raise
                # 常驻守护：不退出，下一轮从锚点恢复该 crashed 批次
                continue
        if once:
            return
        time.sleep(POLL_INTERVAL)
