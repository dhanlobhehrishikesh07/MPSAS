"""
debug_logger.py
───────────────
Centralised logger for One Click Server – Phase 2.
  • Writes rotating file  →  <app_dir>/logs/server_debug.log  (5 MB × 3)
  • Pushes every record to log_queue so the GUI live-console can display it
  • Safe on every platform and path (spaces, OneDrive, etc.)
"""

import logging
import queue
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

# ── Locate log folder relative to THIS file (works after zip/copy) ─────────────
APP_DIR  = Path(__file__).parent.resolve()
LOG_DIR  = APP_DIR / "logs"
LOG_FILE = LOG_DIR / "server_debug.log"

try:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    import tempfile
    LOG_DIR  = Path(tempfile.gettempdir()) / "OneClickServer_logs"
    LOG_FILE = LOG_DIR / "server_debug.log"
    LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Queue GUI reads from ────────────────────────────────────────────────────────
log_queue: queue.Queue = queue.Queue(maxsize=5000)

# ── Custom queue handler ────────────────────────────────────────────────────────
class _QueueHandler(logging.Handler):
    COLORS = {
        "DEBUG":    "#7ecfff",
        "INFO":     "#a8ff78",
        "WARNING":  "#ffe066",
        "ERROR":    "#ff6b6b",
        "CRITICAL": "#ff3333",
    }
    def emit(self, record):
        try:
            msg = self.format(record)
            log_queue.put_nowait({
                "msg":   msg,
                "color": self.COLORS.get(record.levelname, "#ffffff"),
                "level": record.levelname,
            })
        except queue.Full:
            pass   # never block the calling thread

# ── Build the root logger ───────────────────────────────────────────────────────
_FMT = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-8s | %(module)-22s | %(funcName)-28s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("OCS")
logger.setLevel(logging.DEBUG)
logger.propagate = False

# File handler
try:
    _fh = RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3,
        encoding="utf-8", delay=False,
    )
    _fh.setFormatter(_FMT)
    _fh.setLevel(logging.DEBUG)
    logger.addHandler(_fh)
except Exception as _e:
    print(f"[WARN] Could not open log file {LOG_FILE}: {_e}")

# Queue handler (GUI)
_qh = _QueueHandler()
_qh.setFormatter(_FMT)
_qh.setLevel(logging.DEBUG)
logger.addHandler(_qh)

# Console handler (terminal / run.bat)
_ch = logging.StreamHandler(sys.stdout)
_ch.setFormatter(_FMT)
_ch.setLevel(logging.INFO)
logger.addHandler(_ch)

# ── Convenience wrappers ────────────────────────────────────────────────────────
def debug(msg, *a, **kw):    logger.debug(msg, *a, **kw)
def info(msg, *a, **kw):     logger.info(msg, *a, **kw)
def warning(msg, *a, **kw):  logger.warning(msg, *a, **kw)
def error(msg, *a, **kw):    logger.error(msg, *a, **kw)
def critical(msg, *a, **kw): logger.critical(msg, *a, **kw)

def section(title: str):
    bar = "─" * 64
    logger.info(f"\n{bar}\n  {title}\n{bar}")