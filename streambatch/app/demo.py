"""演示场景 CLI（不依赖 HTTP，直接调用同一套库逻辑；HTTP 路径由网页覆盖）。

子命令：
  reset-demo   清库并生成一个白天的随机交易
  run-batch    以独立子进程方式跑一个批次（--mode ledger|replace）
  late         交接后注入迟到更正与删除，触发已锚定桶重算
  pause-demo   暂停批处理 → 在线继续输入 → 恢复 → 新批次
  crash-demo   武装提交中断 → 批次崩溃 → 再启动恢复，验证幂等续算
  reconcile    三方对账
  tamper       故意篡改物化汇总，演示差额下钻到具体事件
  benchmark    对比 ledger 锚定与 replace 整体替换的写放大量
"""
from __future__ import annotations
import argparse
import os
import subprocess
import sys
import tempfile
import time

from . import config, store, events, batch, reconcile
from .generator import seed_day, random_corrections, random_deletes


def _conn():
    c = store.connect()
    store.init_db(c)
    return c


def _hr(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def _show_recon(c) -> None:
    r = reconcile.full_reconcile(c)
    print(f"三方总额: 原始事件={r['totals']['raw_events']:,}  "
          f"台账={r['totals']['ledger']:,}  "
          f"物化视图={r['totals']['materialized']:,}  分")
    inv = r["partition_invariants"]
    print(f"流式分区守恒: {inv['stream_ledger_vs_materialized']}")
    print(f"批处理分区守恒: {inv['batch_ledger_vs_anchors']}")
    print("对账结果:", "✅ 全部一致" if r["ok"] else f"❌ 发现 {len(r['group_differences'])} 组差额")
    return r


def cmd_reset_demo(args) -> None:
    if os.path.exists(config.DB_PATH):
        os.remove(config.DB_PATH)
    for ext in ("-wal", "-shm"):
        p = config.DB_PATH + ext
        if os.path.exists(p):
            os.remove(p)
    c = _conn()
    start = args.clock_start
    created = seed_day(c, n=args.n, clock_start=start,
                       spread_seconds=args.spread, seed=args.seed)
    ids = [e["txn_id"] for e in created]
    store.set_meta(c, "demo_txn_ids", ",".join(ids))
    _hr(f"白天流式交易已生成: {len(created)} 笔（随机种子 {args.seed}）")
    ov = c.execute("SELECT COUNT(*) c, COALESCE(SUM(delta),0) s FROM ledger").fetchone()
    print(f"ledger 行数={ov['c']}  流式累计={ov['s']:,} 分")
    print("事件时间跨度: 多个小时桶，便于观察逐桶交接")
    print("前 3 笔交易:", ids[:3])


def _run_batch_subprocess(once: bool = True) -> int:
    cmd = [sys.executable, "-m", "app.run_batch"]
    if once:
        cmd.append("--once")
    proc = subprocess.run(cmd, cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return proc.returncode


def cmd_run_batch(args) -> None:
    c = _conn()
    run_id = batch.create_run(c, mode=args.mode)
    run = c.execute("SELECT * FROM batch_runs WHERE id=?", (run_id,)).fetchone()
    _hr(f"创建批次 #{run_id}（{args.mode}）— 交接边界")
    print(run["boundary_explained"] if "boundary_explained" in run.keys()
          else f"水位 event_time<{run['watermark_event_time']}, 快照 seq<={run['snapshot_ingest_seq']}")
    c.close()
    rc = _run_batch_subprocess()
    c = _conn()
    done = c.execute("SELECT * FROM batch_runs WHERE id=?", (run_id,)).fetchone()
    print(f"子进程退出码={rc}  批次状态={done['state']}  "
          f"锚定桶={done['bucket_count']} 行={done['anchored_rows']} "
          f"金额={done['anchored_amount']:,}")
    _show_recon(c)


def cmd_late(args) -> None:
    c = _conn()
    ids = store.get_meta(c, "demo_txn_ids", "").split(",")
    old_buckets = [r["bucket"] for r in c.execute(
        "SELECT DISTINCT bucket FROM ledger WHERE batch_id IS NOT NULL")]
    _hr("夜间交接后，迟到的更正与删除到达（事件时间回填到已交接旧桶）")
    cor = random_corrections(c, ids, n=args.n_corr, seed=args.seed,
                             backfill_buckets=old_buckets)
    dele = random_deletes(c, [e["txn_id"] for e in cor] or ids,
                          n=args.n_del, seed=args.seed,
                          backfill_buckets=old_buckets)
    print(f"迟到更正 {len(cor)} 笔、删除 {len(dele)} 笔；全部落在已锚定旧桶 {sorted(set(old_buckets))}")
    moved = batch.reanchor_late(c)
    print(f"触发旧桶重锚 {len(moved)} 个:")
    for m in moved:
        print(f"  桶 {m['bucket']} -> 新批次 {m['new_run']}，旧批次 {m['superseded']} 标 superseded")
    _show_recon(c)


def _fresh_scenario_db(label: str, n: int = 36) -> tuple:
    """构造一个自包含的临时演示库，返回 (连接, 交易id列表, 旧桶列表)。"""
    tmp = tempfile.mkdtemp(prefix=f"sb_{label}_")
    db_path = os.path.join(tmp, "demo.db")
    c = store.connect(db_path)
    store.init_db(c)
    created = seed_day(c, n=n, clock_start=44 * 3600,
                       spread_seconds=3 * 3600, seed=20261117)
    ids = [e["txn_id"] for e in created]
    old_buckets = [r["bucket"] for r in c.execute(
        "SELECT DISTINCT bucket FROM ledger ORDER BY bucket")]
    return c, ids, old_buckets, db_path


def cmd_pause_demo(args) -> None:
    c, ids, old_buckets, db_path = _fresh_scenario_db("pause")
    _hr("暂停批处理，验证在线输入不中断、且不被本批覆盖")
    # 先制造一批"暂停前已在线"的事件，它们落在水位内、应当被本批锚定
    pre = random_corrections(c, ids, n=4, seed=11, backfill_buckets=old_buckets)
    batch.set_paused(c, True)
    run_id = batch.create_run(c, mode="ledger")
    run = c.execute("SELECT * FROM batch_runs WHERE id=?", (run_id,)).fetchone()
    snap_seq = run["snapshot_ingest_seq"]
    env = dict(os.environ, SB_DB=db_path)
    proc = subprocess.Popen([sys.executable, "-m", "app.run_batch", "--once"],
                            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            env=env)
    time.sleep(1.0)
    print(f"批次 #{run_id} 已创建并冻结边界: seq<={snap_seq}；"
          f"batch 进程 {proc.pid} 处于暂停等待…")
    # 暂停期间接入服务照常写入：这些事件 seq 大于快照序号，必须留在流式区
    post = random_corrections(c, ids, n=3, seed=22, backfill_buckets=old_buckets)
    post_max_seq = max(e["ingest_seq"] for e in post)
    print(f"暂停期间在线写入 {len(post)} 笔事件（seq 至 {post_max_seq} > 快照 {snap_seq}）")
    batch.set_paused(c, False)
    print("恢复批处理…")
    rc = proc.wait()
    run = c.execute("SELECT * FROM batch_runs WHERE id=?", (run_id,)).fetchone()
    print(f"退出码={rc} 批次状态={run['state']} 锚定桶={run['bucket_count']} 行={run['anchored_rows']}")
    covered_after_snapshot = c.execute(
        "SELECT COUNT(*) c FROM ledger WHERE batch_id=? AND ingest_seq>?",
        (run_id, snap_seq)).fetchone()["c"]
    still_stream = c.execute(
        "SELECT COUNT(*) c FROM ledger WHERE batch_id IS NULL AND ingest_seq>?",
        (snap_seq,)).fetchone()["c"]
    expected_rows = sum(len(e["deltas"]) for e in post)
    print(f"边界校验: 快照后事件被误锚定 {covered_after_snapshot} 行（必须为 0）；"
          f"仍在流式区 {still_stream} 行（{len(post)} 笔更正共 {expected_rows} 条带符号贡献）")
    assert covered_after_snapshot == 0 and still_stream == expected_rows
    _show_recon(c)


def cmd_crash_demo(args) -> None:
    c, ids, old_buckets, db_path = _fresh_scenario_db("crash")
    env = dict(os.environ, SB_DB=db_path)
    _hr("模拟提交中断：在第 2 个桶锚定后崩溃，再从锚点恢复")
    run_id = batch.create_run(c, mode="ledger")
    todo_before = batch.pending_buckets(c, c.execute(
        "SELECT * FROM batch_runs WHERE id=?", (run_id,)).fetchone())
    print(f"批次 #{run_id} 待处理 {len(todo_before)} 个桶: {[b for b, _ in todo_before]}")
    batch.arm_fault(c, "mid_anchor", 1)  # 第 2 个桶（索引 1）提交后触发
    c.close()
    proc = subprocess.run([sys.executable, "-m", "app.run_batch", "--once"],
                          cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          env=env)
    c = store.connect(db_path)
    crashed = c.execute("SELECT * FROM batch_runs WHERE id=?", (run_id,)).fetchone()
    anchored = c.execute(
        "SELECT COUNT(DISTINCT bucket) b, COUNT(*) r FROM batch_anchors WHERE batch_id=?",
        (run_id,)).fetchone()
    print(f"第一次运行退出码={proc.returncode}（7=模拟崩溃） 批次状态={crashed['state']}")
    print(f"崩溃前已安全提交 {anchored['b']} 个桶 / {anchored['r']} 行锚点（逐桶事务保证不丢）")
    _hr("重新启动批处理进程：从锚点恢复，未提交桶继续，已提交桶不重复")
    proc2 = subprocess.run([sys.executable, "-m", "app.run_batch", "--once"],
                           cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           env=env)
    run = c.execute("SELECT * FROM batch_runs WHERE id=?", (run_id,)).fetchone()
    print(f"恢复运行退出码={proc2.returncode} 批次状态={run['state']} "
          f"桶={run['bucket_count']} 行={run['anchored_rows']}")
    dup = c.execute(
        "SELECT COUNT(*) c FROM (SELECT id FROM ledger WHERE batch_id=? GROUP BY id HAVING COUNT(*)>1)",
        (run_id,)).fetchone()["c"]
    print(f"重复锚定检查: {dup} 行（必须为 0）；待处理桶应全部完成: "
          f"{len(todo_before)} == {run['bucket_count']}")
    assert dup == 0 and run["bucket_count"] == len(todo_before)
    _show_recon(c)


def cmd_reconcile(args) -> None:
    c = _conn()
    r = _show_recon(c)
    if r["group_differences"]:
        _hr("差额下钻到交易")
        for f in r["trace"]:
            print(f)


def cmd_tamper(args) -> None:
    c = _conn()
    _hr("故意篡改一条批处理锚点（模拟存储损坏/错误覆写），观察追踪")
    row = c.execute(
        "SELECT * FROM batch_anchors ORDER BY RANDOM() LIMIT 1").fetchone()
    if row is None:
        print("没有已锚定数据，请先 run-batch")
        return
    print(f"篡改目标: batch={row['batch_id']} bucket={row['bucket']} "
          f"dim={row['dim']} 原金额={row['amount']:,}")
    c.execute("BEGIN IMMEDIATE")
    c.execute("UPDATE batch_anchors SET amount=amount+? WHERE batch_id=? AND bucket=? AND dim=?",
              (args.delta, row["batch_id"], row["bucket"], row["dim"]))
    store.audit(c, "tamper", "corrupt_anchor", f"bucket={row['bucket']} dim={row['dim']}")
    c.execute("COMMIT")
    r = reconcile.full_reconcile(c)
    print("对账结果:", "❌ 捕获差额" if not r["ok"] else "意外通过")
    for d in r["group_differences"]:
        print(f"  桶={d['bucket']} 维度={d['dim']}: "
              f"原始={d['raw_events']:,} 台账={d['ledger_sum']:,} 物化={d['materialized']:,} "
              f"物化-原始差额={d['materialized_minus_raw']:,}")
    _hr("逐交易下钻（应有净贡献 vs 台账净贡献）")
    trace = reconcile.trace_difference(c, bucket=row["bucket"], dim=row["dim"])
    # 锚点被篡改但 ledger 完好时，逐交易层面对得上——这恰好证明差额来自物化层，
    # 并可列出该桶全部交易贡献作为修复依据
    print(f"交易级差额 {len(trace)} 项；若为 0 说明 ledger 完好，可用 ledger 重算修复锚点")
    if not trace:
        c.execute("BEGIN IMMEDIATE")
        c.execute(
            "UPDATE batch_anchors SET amount=("
            "SELECT COALESCE(SUM(delta),0) FROM ledger "
            "WHERE ledger.batch_id=batch_anchors.batch_id AND ledger.bucket=batch_anchors.bucket "
            "AND ledger.dim=batch_anchors.dim) "
            "WHERE batch_id=? AND bucket=? AND dim=?",
            (row["batch_id"], row["bucket"], row["dim"]))
        c.execute("COMMIT")
        print("已按 ledger 逐条贡献自动修复锚点")
    fixed = reconcile.full_reconcile(c)
    print("修复后对账:", "✅ 全部一致" if fixed["ok"] else "❌ 仍有差额")


def cmd_benchmark(args) -> None:
    c = _conn()
    _hr("两种交接方式代价对比（同一批数据，实测写放大）")
    ids = store.get_meta(c, "demo_txn_ids", "").split(",")
    if not ids or ids == [""]:
        print("请先 reset-demo")
        return
    # 用一个新库跑对照，避免互相干扰
    path_l = os.path.join(os.path.dirname(config.DB_PATH), "bench_ledger.db")
    path_r = os.path.join(os.path.dirname(config.DB_PATH), "bench_replace.db")
    for p in (path_l, path_r):
        for ext in ("", "-wal", "-shm"):
            if os.path.exists(p + ext):
                os.remove(p + ext)

    class CountingConn:
        """统计写 DML 次数的连接包装；store/batch 模块的函数都通过 conn.execute 访问。"""
        def __init__(self, real):
            self._r = real
            self.writes = 0

        def execute(self, sql, *a, **kw):
            if sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
                self.writes += 1
            return self._r.execute(sql, *a, **kw)

        def executescript(self, script):
            return self._r.executescript(script)

        def __getattr__(self, name):
            return getattr(self._r, name)

    def _build(db_path: str, mode: str) -> dict:
        real = store.connect(db_path)
        store.init_db(real)
        cc = CountingConn(real)
        # 热桶设计：大量交易集中在少数几个桶，更正只命中其中 1 个，
        # 这样"整桶全量替换"与"只动涉及贡献行"的代价差异最明显。
        seed_day(cc, n=args.n, clock_start=args.clock_start,
                 spread_seconds=int(1.6 * 3600), seed=args.seed)
        real.execute("PRAGMA wal_checkpoint(TRUNCATE)")

        rid = batch.create_run(cc, mode=mode)
        w0 = cc.writes
        batch.run_pending(cc, rid)
        first_writes = cc.writes - w0

        # 只往【一个热桶】注入 k 笔迟到更正
        ids = [r["txn_id"] for r in real.execute(
            "SELECT DISTINCT txn_id FROM events")]
        hot = [real.execute("SELECT MIN(bucket) b FROM ledger").fetchone()["b"]]
        k = 6
        w1 = cc.writes
        random_corrections(cc, ids, n=k, seed=args.seed + 1, backfill_buckets=hot)
        ingest_writes = cc.writes - w1  # 两模式相同的接入成本，对比时剔除

        rid2 = batch.create_run(cc, mode=mode)
        w2 = cc.writes
        batch.run_pending(cc, rid2)
        batch.reanchor_late(cc)
        late_writes = cc.writes - w2
        real.execute("PRAGMA wal_checkpoint(TRUNCATE)")

        # 受影响热桶的活跃事件数：replace 必须为这些事件全部重算
        hot_events = real.execute(
            "SELECT COUNT(*) c FROM events e JOIN ("
            "SELECT txn_id, MAX(version) mv FROM events GROUP BY txn_id) t "
            "ON e.txn_id=t.txn_id AND e.version=t.mv "
            "WHERE e.status='active' AND e.bucket=?", (hot[0],)).fetchone()["c"]
        cc.close()
        return {"first_writes": first_writes, "ingest_writes": ingest_writes,
                "late_writes": late_writes, "hot_events": hot_events,
                "late_corrections": k,
                "bytes": os.path.getsize(db_path)}

    l = _build(path_l, "ledger")
    r = _build(path_r, "replace")
    print(f"数据规模: {args.n} 笔随机交易集中在少数热桶；向 1 个热桶注入 "
          f"{l['late_corrections']} 笔迟到更正\n")
    print("[ledger 逐条贡献锚定] —— 选定方案")
    print(f"  首次交接 DML 写入 {l['first_writes']} 次（行级 UPDATE batch_id，明细全保留）")
    print(f"  {l['late_corrections']} 笔迟到更正后的再交接 DML 写入 {l['late_writes']} 次"
          f"（只重锚该桶涉及的贡献行 + 锚点）")
    print(f"  库文件 {l['bytes']:,} 字节；任何差额可下钻到具体 txn_id/version/op")
    print("\n[replace 区间整体替换] —— 对照方案")
    print(f"  首次交接 DML 写入 {r['first_writes']} 次（整桶 DELETE+INSERT 快照，无明细）")
    print(f"  同样 {r['late_corrections']} 笔更正后的再交接 DML 写入 {r['late_writes']} 次"
          f"——热桶内 {r['hot_events']} 个活跃事件被【全部】重算覆写，与更正数无关")
    print(f"  库文件 {r['bytes']:,} 字节；快照只有桶合计，无法回答'差额由哪笔交易造成'")
    print("\n代价结论（迟到更正是夜间批补算的常态）:")
    print(f"  再交接写入次数: ledger {l['late_writes']} vs replace {r['late_writes']}")
    print(f"  ledger 的成本随【实际更正涉及的贡献】增长；"
          f"replace 的成本随【桶内事件总量】线性增长，热桶越热越亏，且永久丢失逐条可追溯性。")


def main() -> None:
    p = argparse.ArgumentParser(description="流批交接演示")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("reset-demo"); s.set_defaults(func=cmd_reset_demo)
    s.add_argument("--n", type=int, default=40)
    s.add_argument("--seed", type=int, default=20261117)
    s.add_argument("--clock-start", type=int, default=44 * 3600)
    s.add_argument("--spread", type=int, default=3 * 3600)

    s = sub.add_parser("run-batch"); s.set_defaults(func=cmd_run_batch)
    s.add_argument("--mode", choices=["ledger", "replace"], default="ledger")

    s = sub.add_parser("late"); s.set_defaults(func=cmd_late)
    s.add_argument("--n-corr", type=int, default=5)
    s.add_argument("--n-del", type=int, default=2)
    s.add_argument("--seed", type=int, default=77)

    sub.add_parser("pause-demo").set_defaults(func=cmd_pause_demo)
    sub.add_parser("crash-demo").set_defaults(func=cmd_crash_demo)
    sub.add_parser("reconcile").set_defaults(func=cmd_reconcile)

    s = sub.add_parser("tamper"); s.set_defaults(func=cmd_tamper)
    s.add_argument("--delta", type=int, default=12345)

    s = sub.add_parser("benchmark"); s.set_defaults(func=cmd_benchmark)
    s.add_argument("--n", type=int, default=60)
    s.add_argument("--seed", type=int, default=4242)
    s.add_argument("--clock-start", type=int, default=44 * 3600)
    s.add_argument("--spread", type=int, default=3 * 3600)

    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
