"""
main.py
───────
One Click Server – Phase 2 Entry Point.

On EVERY launch:
  1. Logger starts immediately (writes to logs/server_debug.log)
  2. Setup runs: checks Python, installs firebase-admin + Google API libs if missing
  3. serviceAccountKey.json and client_secrets.json auto-detected next to main.py
  4. GUI launches

Run:  python main.py
  or: double-click run.bat  (Windows)
"""

import sys
import os
import traceback
from pathlib import Path

# ── Make sure the app folder is always in sys.path ────────────────────────────
_APP_DIR = Path(__file__).parent.resolve()
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))

# ── Change CWD to app folder so all relative paths work ───────────────────────
os.chdir(str(_APP_DIR))

# ── Logger (no external dependencies) ────────────────────────────────────────
import debug_logger as log

log.section("APPLICATION BOOT  [Phase 2]")
log.info(f"Python : {sys.version}")
log.info(f"App dir: {_APP_DIR}")
log.info(f"CWD    : {os.getcwd()}")
log.info(f"Platform: {sys.platform}")

# ── Setup (installs packages if needed) ──────────────────────────────────────
try:
    import setup as _setup
    _setup.run()
except Exception as _e:
    log.error(f"Setup error: {_e}", exc_info=True)

# ── Check credential files ────────────────────────────────────────────────────
_sak = _APP_DIR / "serviceAccountKey.json"
_cs  = _APP_DIR / "client_secrets.json"
log.info(f"serviceAccountKey.json : {'FOUND' if _sak.exists() else 'NOT FOUND'}")
log.info(f"client_secrets.json    : {'FOUND' if _cs.exists() else 'NOT FOUND'}")

# ── Launch GUI ────────────────────────────────────────────────────────────────
try:
    from gui import main
    main()
except Exception as _fatal:
    msg = f"Fatal error starting One Click Server Phase 2:\n\n{traceback.format_exc()}"
    log.critical(msg)
    try:
        import tkinter as _tk
        from tkinter import messagebox as _mb
        _root = _tk.Tk(); _root.withdraw()
        _mb.showerror("One Click Server Phase 2 – Fatal Error", msg)
        _root.destroy()
    except Exception:
        print(msg, file=sys.stderr)
    sys.exit(1)