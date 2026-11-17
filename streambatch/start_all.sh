#!/usr/bin/env bash
# 一次性拉起三个独立进程：接入服务、批处理器、查询端 Web。
# 日志各自写到 data/logs/，PID 写到 data/run/。
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p data/logs data/run

start() {
  local name="$1"; shift
  if [ -f "data/run/$name.pid" ] && kill -0 "$(cat data/run/$name.pid)" 2>/dev/null; then
    echo "$name 已在运行 (pid $(cat data/run/$name.pid))"
    return
  fi
  python3 -m "$@" >"data/logs/$name.log" 2>&1 &
  echo $! > "data/run/$name.pid"
  echo "启动 $name (pid $!)"
}

start ingest app.ingest
start batch  app.run_batch
start web    app.web
echo
echo "接入服务 : http://127.0.0.1:8101/health"
echo "查询网页 : http://127.0.0.1:8102/"
