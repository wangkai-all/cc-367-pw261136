"""批处理器独立进程入口。

用法：
  python -m app.run_batch            # 常驻轮询，自动接管 running/crashed 批次
  python -m app.run_batch --once     # 处理完当前批次后退出（便于演示恢复）
崩溃恢复：--once 模式下注入故障会以退出码 7 结束；再次启动同一命令，
进程从 batch_anchors 记录的锚点继续，已提交的桶不会重复锚定。
"""
import sys

from . import store, batch


def main() -> None:
    once = "--once" in sys.argv
    conn = store.connect()
    store.init_db(conn)
    try:
        batch.worker_loop(conn, once=once)
    except batch.InjectedCrash as e:
        print(f"[batch] 提交中断（模拟崩溃）: {e}")
        print("[batch] 本批已标记 crashed，已提交的桶保留锚点；重新启动本进程即可恢复")
        sys.exit(7)
    print("[batch] 本轮处理结束" if once else "[batch] 常驻运行中…")


if __name__ == "__main__":
    main()
