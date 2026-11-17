#!/usr/bin/env bash
# 停止三个服务。
cd "$(dirname "$0")"
for name in ingest batch web; do
  if [ -f "data/run/$name.pid" ]; then
    pid="$(cat data/run/$name.pid)"
    kill "$pid" 2>/dev/null && echo "停止 $name (pid $pid)" || true
    rm -f "data/run/$name.pid"
  fi
done
