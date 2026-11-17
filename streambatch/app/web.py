"""查询端 Web 服务：独立进程，只负责读与控制面，绝不直接写业务事件。

批处理的启动/暂停/故障武装都通过控制表交给独立的 batch 进程执行，
本服务不运行任何批次逻辑，保证三个服务职责隔离。
"""
from __future__ import annotations
import json
import os
import time
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from . import config, store, batch, query

INGEST_URL = f"http://127.0.0.1:{config.INGEST_PORT}"

STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "static")


def _json(handler, status: int, payload) -> None:
    body = json.dumps(payload, ensure_ascii=False, default=str).encode()
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class WebHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _read_json(self) -> dict:
        n = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(n).decode()) if n else {}

    def _qs(self) -> dict:
        return {k: v[0] for k, v in parse_qs(urlparse(self.path).query).items()}

    # ------------------------------------------------------------ GET
    def do_GET(self):
        path = urlparse(self.path).path
        conn = store.connect()
        try:
            store.init_db(conn)
            q = self._qs()
            if path == "/" or path == "/index.html":
                with open(os.path.join(STATIC_DIR, "index.html"), "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif path == "/api/overview":
                _json(self, 200, query.overview(conn))
            elif path == "/api/buckets":
                _json(self, 200, query.bucket_summary(conn))
            elif path == "/api/ledger":
                _json(self, 200, query.ledger_detail(
                    conn,
                    bucket=int(q["bucket"]) if q.get("bucket") else None,
                    dim=q.get("dim"),
                    side=q.get("side"),
                    txn_id=q.get("txn"),
                ))
            elif path == "/api/events":
                _json(self, 200, query.event_list(conn, q.get("txn")))
            elif path == "/api/batches":
                _json(self, 200, query.batch_runs(conn))
            elif path.startswith("/api/batches/"):
                _json(self, 200, query.batch_detail(conn, int(path.rsplit("/", 1)[1])))
            elif path == "/api/control":
                _json(self, 200, query.control_state(conn))
            elif path == "/api/reconcile":
                _json(self, 200, query.reconcile(conn))
            elif path == "/api/trace":
                _json(self, 200, query.trace(
                    conn,
                    bucket=int(q["bucket"]) if q.get("bucket") else None,
                    dim=q.get("dim"),
                ))
            elif path == "/api/audit":
                _json(self, 200, query.audit_tail(conn, int(q.get("limit", "30"))))
            elif path == "/health":
                _json(self, 200, {"service": "web", "ok": True})
            else:
                _json(self, 404, {"error": "not found"})
        except Exception as e:  # noqa: BLE001
            _json(self, 500, {"error": repr(e)})
        finally:
            conn.close()

    # ------------------------------------------------------------ POST
    def do_POST(self):
        path = urlparse(self.path).path
        conn = store.connect()
        try:
            store.init_db(conn)
            p = self._read_json()
            if path == "/api/control":
                batch.set_paused(conn, bool(p.get("paused")))
                _json(self, 200, {"ok": True, "control": query.control_state(conn)})
            elif path == "/api/fault":
                batch.arm_fault(conn, p.get("fail_stage", "none"),
                                p.get("fail_bucket"))
                _json(self, 200, {"ok": True, "control": query.control_state(conn)})
            elif path == "/api/batches":
                run_id = batch.create_run(
                    conn, mode=p.get("mode", "ledger"),
                    watermark=p.get("watermark"))
                _json(self, 200, {"ok": True, "run_id": run_id,
                                  "note": "批次已创建，等待 batch 进程接管"})
            elif path.startswith("/api/proxy/"):
                # 业务事件只允许由接入服务落库：web 仅做同源转发，自身不写事件
                target = INGEST_URL + "/" + path[len("/api/proxy/"):]
                last_err = None
                for attempt in (1, 2):  # 瞬时连接/锁抖动重试一次
                    req = urllib.request.Request(
                        target, data=json.dumps(p).encode(),
                        headers={"Content-Type": "application/json"}, method="POST")
                    try:
                        with urllib.request.urlopen(req, timeout=15) as resp:
                            _json(self, resp.status, json.loads(resp.read().decode()))
                            last_err = None
                            break
                    except urllib.error.HTTPError as e:
                        # 业务错误（4xx）不重试，原样透传
                        _json(self, e.code, json.loads(e.read().decode()))
                        last_err = None
                        break
                    except (urllib.error.URLError, TimeoutError) as e:
                        last_err = repr(e)
                        if attempt == 1:
                            time.sleep(0.2)
                if last_err is not None:
                    _json(self, 503, {"error": f"接入服务暂时不可达：{last_err}"})
            else:
                _json(self, 404, {"error": "not found"})
        except ValueError as e:
            _json(self, 400, {"error": str(e)})
        except Exception as e:  # noqa: BLE001
            _json(self, 500, {"error": repr(e)})
        finally:
            conn.close()


def main() -> None:
    conn = store.connect()
    store.init_db(conn)
    conn.close()
    srv = ThreadingHTTPServer(("127.0.0.1", config.WEB_PORT), WebHandler)
    print(f"[web] 查询端监听 http://127.0.0.1:{config.WEB_PORT}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
