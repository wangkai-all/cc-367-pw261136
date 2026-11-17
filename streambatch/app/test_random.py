"""随机化正确性测试：不使用任何固定期望值，而是在随机操作流上持续断言不变量。

每一步随机执行：新交易 / 更正 / 删除 / 批次交接 / 迟到更正，
每次动作后都跑三方对账，并额外用 Python 端独立维护的参考模型交叉验证。
失败时打印最小上下文（种子 + 具体交易），可直接复现。
"""
from __future__ import annotations
import os
import random
import sys
import tempfile

from . import store, events, batch, reconcile, config


class ReferenceModel:
    """完全独立于 SQL 的 Python 参考模型：txn -> (dim, bucket, amount, status)。"""

    def __init__(self):
        self.txns: dict[str, dict] = {}

    def apply(self, *, txn_id, dim, amount, bucket, op):
        self.txns[txn_id] = {
            "dim": dim, "bucket": bucket,
            "amount": amount if op == "upsert" else 0,
            "status": "active" if op == "upsert" else "deleted",
        }

    def expected_groups(self) -> dict[tuple[int, str], int]:
        out: dict[tuple[int, str], int] = {}
        for t in self.txns.values():
            if t["status"] == "active":
                key = (t["bucket"], t["dim"])
                out[key] = out.get(key, 0) + t["amount"]
        return out


def run_scenario(seed: int, steps: int, verbose: bool = False) -> None:
    rng = random.Random(seed)
    tmp = tempfile.mkdtemp(prefix=f"sb_test_{seed}_")
    db_path = os.path.join(tmp, "test.db")
    conn = store.connect(db_path)
    store.init_db(conn)

    ref = ReferenceModel()
    clock = 10 * 3600
    events.set_clock(conn, clock)
    active_ids: list[str] = []
    serial = 0

    def check(label: str):
        r = reconcile.full_reconcile(conn)
        assert r["ok"], (
            f"[seed={seed}] 三方对账失败于 {label}:\n"
            f"组差额={r['group_differences']}\n"
            f"分区不变量={r['partition_invariants']}\n"
            f"交易级追踪={r['trace'][:5]}"
        )
        # 与独立参考模型逐桶逐维度比对（不是只比总数）
        got = reconcile.raw_recompute(conn)
        want = ref.expected_groups()
        assert got == want, (
            f"[seed={seed}] 与参考模型不一致于 {label}\n"
            f"缺失键={set(want) - set(got)} 多余键={set(got) - set(want)}\n"
            f"差异={[(k, want.get(k), got.get(k)) for k in set(want) | set(got) if want.get(k) != got.get(k)][:5]}"
        )

    check("开局空库")

    for step in range(steps):
        roll = rng.random()
        if roll < 0.55 or not active_ids:
            # 新交易，事件时间随机落在过去 3 小时内的任意秒
            serial += 1
            txn_id = f"TXN-{seed}-{serial}"
            et = clock - rng.randint(0, 3 * 3600 - 60)
            dim = f"M-{rng.randint(1, 5):02d}"
            amount = rng.randint(1, 500_000)
            events.ingest(conn, txn_id=txn_id, dim=dim, amount=amount, event_time=et)
            ref.apply(txn_id=txn_id, dim=dim, amount=amount,
                      bucket=events.bucket_of(et), op="upsert")
            active_ids.append(txn_id)
        elif roll < 0.8:
            # 更正：随机维度、随机金额，事件时间也可能回填（迟到）
            txn_id = rng.choice(active_ids)
            et = clock - rng.randint(0, 4 * 3600 - 60)
            dim = f"M-{rng.randint(1, 5):02d}"
            amount = rng.randint(1, 500_000)
            events.ingest(conn, txn_id=txn_id, dim=dim, amount=amount, event_time=et)
            ref.apply(txn_id=txn_id, dim=dim, amount=amount,
                      bucket=events.bucket_of(et), op="upsert")
        else:
            # 删除
            txn_id = rng.choice(active_ids)
            prior = ref.txns[txn_id]
            if prior["status"] == "deleted":
                continue
            et = clock - rng.randint(0, 4 * 3600 - 60)
            events.ingest(conn, txn_id=txn_id, dim=prior["dim"], amount=0,
                          event_time=et, op="delete")
            ref.apply(txn_id=txn_id, dim=prior["dim"], amount=0,
                      bucket=events.bucket_of(et), op="delete")

        check(f"step {step} 写入后")

        # 每 12 步做一次批次交接，水位随机，刻意只切走一部分桶
        if step > 0 and step % 12 == 11:
            clock += 3600
            events.set_clock(conn, clock)
            mode = "ledger" if rng.random() < 0.8 else "replace"
            run_id = batch.create_run(conn, mode=mode)
            batch.run_pending(conn, run_id)
            batch.reanchor_late(conn)
            check(f"step {step} 批次交接({mode})后")

            # 交接后立刻继续写新事件，验证新事件绝不被旧批覆盖
            txn_id = f"TXN-{seed}-AFTER-{serial + 1}"
            serial += 1
            et = clock - rng.randint(0, 1800)
            dim, amount = "M-01", rng.randint(1, 99999)
            events.ingest(conn, txn_id=txn_id, dim=dim, amount=amount, event_time=et)
            ref.apply(txn_id=txn_id, dim=dim, amount=amount,
                      bucket=events.bucket_of(et), op="upsert")
            active_ids.append(txn_id)
            row = conn.execute(
                "SELECT COUNT(*) c FROM ledger WHERE txn_id=? AND batch_id IS NOT NULL",
                (txn_id,)).fetchone()
            assert row["c"] == 0, f"[seed={seed}] 新事件被旧批次覆盖！"
            check(f"step {step} 交接后新写入")

    # 崩溃恢复性质：武装故障后跑到崩溃，再恢复，最终必须仍然全部一致
    run_id = batch.create_run(conn, mode="ledger")
    batch.arm_fault(conn, "mid_anchor", 0)
    try:
        batch.run_pending(conn, run_id)
    except batch.InjectedCrash:
        batch.mark_crashed(conn, run_id)
    batch.worker_loop(conn, once=True)  # 恢复
    check("崩溃恢复后")
    final = conn.execute("SELECT state FROM batch_runs WHERE id=?", (run_id,)).fetchone()
    assert final["state"] == "committed", f"[seed={seed}] 恢复后批次未提交: {final['state']}"

    conn.close()
    if verbose:
        print(f"  seed={seed}: {steps} 步随机操作 + 崩溃恢复，全部不变量成立 ✅")


def main() -> None:
    seeds = [int(x) for x in sys.argv[1:]] or [1, 7, 42, 1009, 20261117]
    print(f"随机化测试: {len(seeds)} 个独立种子，每个 60 步混合操作流")
    for seed in seeds:
        run_scenario(seed, steps=60, verbose=True)
    print(f"\n全部 {len(seeds)} 个随机场景通过：三方对账 + 独立参考模型 + 崩溃恢复")


if __name__ == "__main__":
    main()
