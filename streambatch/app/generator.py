"""虚构交易生成器：金额、商户、时间间隔全部随机，演示不依赖任何固定数值。"""
from __future__ import annotations
import random
import string

from . import events

MERCHANTS = [f"M-{n:03d}" for n in range(1, 9)]


def random_txn_id(rng: random.Random) -> str:
    return "T-" + "".join(rng.choices(string.ascii_uppercase + string.digits, k=8))


def seed_day(conn, *, n: int = 40, clock_start: int, spread_seconds: int = 3 * 3600,
             seed: int | None = None) -> list[dict]:
    """生成一个白天的流式交易：事件时间散布在 spread_seconds 内（跨多个小时桶）。

    生成结束后把虚拟时钟拨到 spread 之后 1 小时——模拟"入夜"，
    这样随后创建的夜间批次水位天然高于全部白天事件。
    """
    rng = random.Random(seed)
    events.set_clock(conn, clock_start)
    created = []
    t = clock_start
    for _ in range(n):
        t += rng.randint(5, max(6, spread_seconds // n))
        txn = random_txn_id(rng)
        amount = rng.randint(100, 999_999)  # 分为单位
        created.append(events.ingest(
            conn, txn_id=txn, dim=rng.choice(MERCHANTS),
            amount=amount, event_time=t))
    events.advance_clock(conn, spread_seconds + 3600)
    return created


def _backfill_time(rng: random.Random, backfill_buckets: list[int] | None) -> int | None:
    if not backfill_buckets:
        return None  # 用当前时钟（落到新桶）
    b = rng.choice(backfill_buckets)
    return b + rng.randint(0, 3599)


def random_corrections(conn, txn_ids: list[str], *, n: int = 5,
                       seed: int | None = None,
                       backfill_buckets: list[int] | None = None) -> list[dict]:
    """随机挑交易做更正（新金额随机）。

    提供 backfill_buckets 时，更正的事件时间回填到这些【已交接旧桶】内，
    从而在旧桶制造迟到贡献，触发整桶重锚；否则事件时间取当前时钟（新桶）。
    """
    rng = random.Random(seed)
    out = []
    for txn in rng.sample(txn_ids, min(n, len(txn_ids))):
        out.append(events.ingest(
            conn, txn_id=txn, dim=rng.choice(MERCHANTS),
            amount=rng.randint(100, 999_999),
            event_time=_backfill_time(rng, backfill_buckets)))
    return out


def random_deletes(conn, txn_ids: list[str], *, n: int = 3,
                   seed: int | None = None,
                   backfill_buckets: list[int] | None = None) -> list[dict]:
    rng = random.Random(seed)
    out = []
    for txn in rng.sample(txn_ids, min(n, len(txn_ids))):
        prior = events.latest_event(conn, txn)
        out.append(events.ingest(
            conn, txn_id=txn, dim=prior["dim"], amount=0,
            event_time=_backfill_time(rng, backfill_buckets), op="delete"))
    return out
