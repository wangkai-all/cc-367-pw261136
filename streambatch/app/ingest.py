"""接入服务：独立进程，唯一的事件写入入口。

提供 POST /events（新交易/更正/删除）、POST /events/bulk（批量虚构交易）、
GET /health。写库逻辑全部委托 events.ingest，保证语义只有一个实现。
"""
from __future__ import annotations
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import config, store, events


def _json(handler: BaseHTTPRequestHandler, status: int, payload: dict) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode()
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class IngestHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # 安静一点
        pass

    def _read_json(self) -> dict:
        n = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(n).decode()) if n else {}

    def do_GET(self):
        if self.path == "/health":
            _json(self, 200, {"service": "ingest", "ok": True})
        else:
            _json(self, 404, {"error": "not found"})

    def do_POST(self):
        conn = store.connect()
        try:
            store.init_db(conn)
            if self.path == "/events":
                p = self._read_json()
                res = events.ingest(
                    conn,
                    txn_id=str(p["txn_id"]),
                    dim=str(p.get("dim", "M-DEFAULT")),
                    amount=int(p.get("amount", 0)),
                    event_time=p.get("event_time"),
                    op=p.get("op", "upsert"),
                )
                _json(self, 200, {"ok": True, "event": res})
            elif self.path == "/events/bulk":
                p = self._read_json()
                created = []
                for e in p.get("events", []):
                    created.append(events.ingest(
                        conn,
                        txn_id=str(e["txn_id"]),
                        dim=str(e.get("dim", "M-DEFAULT")),
                        amount=int(e.get("amount", 0)),
                        event_time=e.get("event_time"),
                        op=e.get("op", "upsert"),
                    ))
                _json(self, 200, {"ok": True, "count": len(created), "events": created})
            elif self.path == "/clock":
                p = self._read_json()
                if "advance_seconds" in p:
                    now = events.advance_clock(conn, int(p["advance_seconds"]))
                else:
                    events.set_clock(conn, int(p["event_time"]))
                    now = int(p["event_time"])
                _json(self, 200, {"ok": True, "clock_now": now})
            else:
                _json(self, 404, {"error": "not found"})
        except KeyError as e:
            _json(self, 400, {"ok": False, "error": f"缺少字段 {e}"})
        except ValueError as e:
            _json(self, 400, {"ok": False, "error": str(e)})
        except Exception as e:  # noqa: BLE001
            _json(self, 500, {"ok": False, "error": repr(e)})
        finally:
            conn.close()


def main() -> None:
    conn = store.connect()
    store.init_db(conn)
    conn.close()
    srv = ThreadingHTTPServer(("127.0.0.1", config.INGEST_PORT), IngestHandler)
    print(f"[ingest] 接入服务监听 http://127.0.0.1:{config.INGEST_PORT}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
