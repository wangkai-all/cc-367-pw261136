"""全局配置：三个进程共用同一个 SQLite 文件，但各自独立运行。"""
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.environ.get("SB_DATA_DIR", os.path.join(BASE_DIR, "data"))
DB_PATH = os.environ.get("SB_DB", os.path.join(DATA_DIR, "streambatch.db"))

INGEST_PORT = int(os.environ.get("SB_INGEST_PORT", "8101"))
WEB_PORT = int(os.environ.get("SB_WEB_PORT", "8102"))

# 事件时间按小时对齐成桶，交接边界以桶为单位
BUCKET_SECONDS = 3600

# 演示用：时钟可以被外部拨快（虚拟时间），未设置时用真实墙钟
def now_ts() -> int:
    return int(__import__("time").time())
