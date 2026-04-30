"""
gui.py
──────
One Click Server – Tkinter GUI  (Phase 2)

Phase 2 Tab Structure:
  ✓ Dashboard  – kept fully
  ✓ Servers    – kept, REMOVED portable transfer section
  ✓ Projects   – kept, REMOVED allowed-servers field (all servers allowed)
  ✗ Files      – REMOVED entirely
  ✗ Changes    – REMOVED entirely
  ✓ Sync       – NEW: per-project sync status, inventory viewer, manual sync
  ✓ Debug Log  – kept, updated for Phase 2 events
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import datetime
import os
import subprocess
import sys
from pathlib import Path

import debug_logger as log
import core
import threads as tm

# ── Palette ────────────────────────────────────────────────────────────────────
BG     = "#0d1117"
BG2    = "#161b22"
BG3    = "#21262d"
BORDER = "#30363d"
FG     = "#e6edf3"
FG2    = "#8b949e"
ACCENT = "#58a6ff"
GREEN  = "#3fb950"
RED    = "#f85149"
YELLOW = "#d29922"
PURPLE = "#bc8cff"
ORANGE = "#ffa657"
TEAL   = "#39d353"

FT = ("Consolas", 18, "bold")
FH = ("Consolas", 11, "bold")
FB = ("Consolas", 10)
FS = ("Consolas",  9)
FL = ("Courier New", 9)


# ── Widget helpers ─────────────────────────────────────────────────────────────

def _frame(parent, **kw):
    kw.setdefault("bg", BG2); kw.setdefault("bd", 0)
    kw.setdefault("highlightthickness", 1)
    kw.setdefault("highlightbackground", BORDER)
    return tk.Frame(parent, **kw)

def _btn(parent, text, cmd, color=ACCENT, **kw):
    kw.setdefault("font", FB); kw.setdefault("cursor", "hand2")
    kw.setdefault("relief", "flat"); kw.setdefault("padx", 14); kw.setdefault("pady", 6)
    return tk.Button(parent, text=text, command=cmd,
                     bg=color, fg="#0d1117",
                     activebackground=color, activeforeground="#0d1117", **kw)

def _lbl(parent, text, color=FG, font=FB, **kw):
    return tk.Label(parent, text=text, bg=parent["bg"], fg=color, font=font, **kw)

def _entry(parent, width=30, **kw):
    return tk.Entry(parent, width=width, bg=BG3, fg=FG,
                    insertbackground=ACCENT, relief="flat",
                    highlightthickness=1, highlightbackground=BORDER,
                    highlightcolor=ACCENT, font=FB, **kw)

def _combo(parent, values=None, width=28, **kw):
    return ttk.Combobox(parent, values=values or [], width=width,
                        font=FB, state="readonly", **kw)

def _style_tree():
    s = ttk.Style()
    s.configure("Treeview", background=BG3, foreground=FG,
                 fieldbackground=BG3, rowheight=24, font=("Consolas", 9))
    s.configure("Treeview.Heading", background=BG2, foreground=ACCENT,
                 font=("Consolas", 9, "bold"))
    s.map("Treeview", background=[("selected", "#1f4173")])


# ══════════════════════════════════════════════════════════════════════════════
# Application
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# Application
# ══════════════════════════════════════════════════════════════════════════════

class App(tk.Tk):

    def __init__(self):
        super().__init__()
        log.section("GUI INIT  (Phase 2)")
        self.title("One Click Server – Phase 2")
        self.configure(bg=BG)
        self.geometry("1200x820")
        self.minsize(960, 660)

        self._srv_var    = tk.StringVar(value=core.get_device_name())
        self._running    = False
        self._autoscroll = True

        core.set_gui_root(self)
        core.set_gui_callback(self._on_firebase_update)

        self._build_header()
        self._build_status_bar()
        self._build_notebook()

        threading.Thread(target=self._bg_firebase_init, daemon=True).start()
        self._poll_log()
        self._poll_status()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        log.info("GUI ready  (Phase 2)")

        # ── ADD THESE 3 LINES FOR PHASE 3 AUTO-START ──
        import sys
        if "--auto-start" in sys.argv:
            self.after(1000, self._start)
        # ──────────────────────────────────────────────

    # ── Firebase stream callback ───────────────────────────────────────────────

    def _on_firebase_update(self):
        """Called via root.after() whenever Firebase stream delivers new data."""
        self._dash_refresh()

    # ── Firebase stream callback ───────────────────────────────────────────────

    def _on_firebase_update(self):
        """Called via root.after() whenever Firebase stream delivers new data."""
        self._dash_refresh()
        self._refresh_srv_list()
        self._refresh_proj_list()
        self._refresh_sync_tab()

    # ── Header ────────────────────────────────────────────────────────────────

    def _build_header(self):
        h = tk.Frame(self, bg=BG, pady=10)
        h.pack(fill="x", padx=20)
        _lbl(h, "⬡  ONE CLICK SERVER  ·  Phase 2", color=ACCENT, font=FT).pack(side="left")

        r = tk.Frame(h, bg=BG); r.pack(side="right")
        _lbl(r, "Device:", color=FG2, font=FS).pack(side="left", padx=(0, 4))
        _entry(r, width=24, textvariable=self._srv_var).pack(side="left", padx=(0, 10))
        self._btn_start = _btn(r, "▶  Start Server", self._start, color=GREEN)
        self._btn_start.pack(side="left", padx=4)
        self._btn_stop = _btn(r, "■  Stop", self._stop, color=RED)
        self._btn_stop.pack(side="left", padx=4)
        self._btn_stop.config(state="disabled")
        tk.Frame(self, bg=BORDER, height=1).pack(fill="x")

    # ── Status bar ────────────────────────────────────────────────────────────

    def _build_status_bar(self):
        bar = tk.Frame(self, bg=BG3, height=26)
        bar.pack(fill="x", side="bottom")
        self._lbl_srv = tk.Label(bar, text="○  OFFLINE", fg=RED, bg=BG3, font=FS)
        self._lbl_srv.pack(side="left", padx=12)
        tk.Label(bar, text="|", fg=BORDER, bg=BG3, font=FS).pack(side="left")
        self._lbl_fb = tk.Label(bar, text="Firebase: —", fg=FG2, bg=BG3, font=FS)
        self._lbl_fb.pack(side="left", padx=12)
        tk.Label(bar, text="|", fg=BORDER, bg=BG3, font=FS).pack(side="left")
        self._lbl_sync_status = tk.Label(bar, text="Sync: —", fg=FG2, bg=BG3, font=FS)
        self._lbl_sync_status.pack(side="left", padx=12)
        tk.Label(bar, text="|", fg=BORDER, bg=BG3, font=FS).pack(side="left")
        self._lbl_dir = tk.Label(bar, text="", fg=FG2, bg=BG3, font=FS)
        self._lbl_dir.pack(side="left", padx=12)
        self._lbl_time = tk.Label(bar, text="", fg=FG2, bg=BG3, font=FS)
        self._lbl_time.pack(side="right", padx=12)

    # ── Notebook ──────────────────────────────────────────────────────────────

    def _build_notebook(self):
        s = ttk.Style(); s.theme_use("default")
        s.configure("TNotebook", background=BG, borderwidth=0, tabmargins=0)
        s.configure("TNotebook.Tab", background=BG3, foreground=FG2,
                    font=FH, padding=[16, 8], borderwidth=0)
        s.map("TNotebook.Tab",
              background=[("selected", BG2)], foreground=[("selected", ACCENT)])

        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True)
        _style_tree()

        for label, builder in [
            (" Dashboard ",  self._tab_dashboard),
            (" Servers ",    self._tab_servers),
            (" Projects ",   self._tab_projects),
            (" Sync ",       self._tab_sync),
            (" Debug Log ",  self._tab_log),
        ]:
            f = tk.Frame(self.nb, bg=BG2)
            self.nb.add(f, text=label)
            builder(f)

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 1 – Dashboard
    # ══════════════════════════════════════════════════════════════════════════

    def _tab_dashboard(self, p):
        p.columnconfigure(0, weight=1); p.columnconfigure(1, weight=1)
        p.rowconfigure(1, weight=1)

        _lbl(p, "System Overview", font=FH, color=FG)\
            .grid(row=0, column=0, columnspan=2, sticky="w", padx=20, pady=(16, 8))

        # Left panel – stats
        sc = _frame(p)
        sc.grid(row=1, column=0, sticky="nsew", padx=(20, 6), pady=(0, 8))
        sc.columnconfigure(1, weight=1)

        self._dash_labels = {}
        rows_def = [
            ("Firebase",      "firebase",    ACCENT),
            ("Registered",    "registered",  FG),
            ("Online",        "online",      GREEN),
            ("Offline",       "offline",     RED),
            ("Projects",      "projects",    PURPLE),
            ("App Dir",       "app_dir",     FG2),
            ("Server Dir",    "server_dir",  FG2),
            ("Key File",      "key_present", YELLOW),
        ]
        for i, (label, key, color) in enumerate(rows_def):
            _lbl(sc, f"{label}:", color=FG2).grid(
                row=i, column=0, sticky="w", padx=(14, 8), pady=6)
            lv = _lbl(sc, "—", color=color)
            lv.grid(row=i, column=1, sticky="w", pady=6)
            self._dash_labels[key] = lv

        # Right panel – sync activity
        sr = _frame(p)
        sr.grid(row=1, column=1, sticky="nsew", padx=(6, 20), pady=(0, 8))
        sr.rowconfigure(1, weight=1)
        _lbl(sr, "Live Sync Activity", color=ACCENT, font=FH)\
            .grid(row=0, column=0, sticky="w", padx=14, pady=(12, 6))
        self._activity_txt = tk.Text(
            sr, bg="#0a0e14", fg=TEAL, font=FL, relief="flat",
            wrap="word", state="disabled", padx=8, pady=4)
        vsb = ttk.Scrollbar(sr, orient="vertical", command=self._activity_txt.yview)
        self._activity_txt.configure(yscrollcommand=vsb.set)
        self._activity_txt.grid(row=1, column=0, sticky="nsew", padx=(14, 0), pady=(0, 14))
        vsb.grid(row=1, column=1, sticky="ns", pady=(0, 14), padx=(0, 8))
        sr.columnconfigure(0, weight=1)

        _btn(p, "⟳  Refresh Dashboard", self._dash_refresh)\
            .grid(row=2, column=0, columnspan=2, pady=(0, 12))

        self._dash_refresh()

    def _dash_refresh(self):
        try:
            snap = core.get_status_snapshot()
            self._dash_labels["firebase"].config(text=snap["firebase"])
            self._dash_labels["registered"].config(
                text=str(len(snap["registered"])) + "  →  " + ", ".join(snap["registered"]) or "—")
            self._dash_labels["online"].config(
                text=str(len(snap["online"])) + "  →  " + ", ".join(snap["online"]) or "—")
            self._dash_labels["offline"].config(
                text=str(len(snap["offline"])) + "  →  " + ", ".join(snap["offline"]) or "—")
            self._dash_labels["projects"].config(
                text=str(len(snap["projects"])))
            self._dash_labels["app_dir"].config(text=snap["app_dir"][-60:])
            self._dash_labels["server_dir"].config(text=snap["server_dir"][-60:])
            self._dash_labels["key_present"].config(
                text="✓ Found" if snap["key_present"] else "✗ Not found",
                fg=GREEN if snap["key_present"] else RED)
            self._lbl_fb.config(text=f"Firebase: {snap['firebase'][:30]}")
            self._lbl_dir.config(text=f"Dir: {snap['server_dir'][-40:]}")

            # Sync status summary
            projs = snap["projects"]
            n_proj = len(projs)
            self._lbl_sync_status.config(
                text=f"Sync: {n_proj} project(s) mirrored",
                fg=GREEN if n_proj > 0 else FG2)
        except Exception as e:
            log.error(f"_dash_refresh: {e}", exc_info=True)

    def _add_activity(self, msg: str):
        """Append a line to the activity feed on the dashboard."""
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}]  {msg}\n"
        try:
            self._activity_txt.config(state="normal")
            self._activity_txt.insert("end", line)
            lines = int(self._activity_txt.index("end-1c").split(".")[0])
            if lines > 500:
                self._activity_txt.delete("1.0", "100.0")
            self._activity_txt.config(state="disabled")
            self._activity_txt.see("end")
        except Exception:
            pass

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 2 – Servers 
    # ══════════════════════════════════════════════════════════════════════════

    def _tab_servers(self, p):
        p.columnconfigure(0, weight=1)

        # Register section
        c1 = _frame(p); c1.pack(fill="x", padx=20, pady=(18, 8))
        _lbl(c1, "Register a Server Device", color=ACCENT, font=FH)\
            .pack(anchor="w", padx=14, pady=(12, 6))
        r1 = tk.Frame(c1, bg=BG2); r1.pack(fill="x", padx=14, pady=(0, 14))
        _lbl(r1, "Device name:").pack(side="left")
        self._e_reg = _entry(r1, width=28)
        self._e_reg.insert(0, core.get_device_name())
        self._e_reg.pack(side="left", padx=8)
        _btn(r1, "Register", self._do_register, color=GREEN).pack(side="left")

        # Firebase key section
        c2 = _frame(p); c2.pack(fill="x", padx=20, pady=(0, 8))
        _lbl(c2, "Firebase Service Account Key  (serviceAccountKey.json)",
             color=ACCENT, font=FH).pack(anchor="w", padx=14, pady=(12, 6))
        key_status = "✓ Found" if core.FIREBASE_KEY.exists() else "✗ Not found"
        key_color  = GREEN if core.FIREBASE_KEY.exists() else RED
        self._lbl_key_status = _lbl(
            c2, f"Current: {core.FIREBASE_KEY}  [{key_status}]",
            color=key_color, font=FS)
        self._lbl_key_status.pack(anchor="w", padx=14)
        r2 = tk.Frame(c2, bg=BG2); r2.pack(fill="x", padx=14, pady=(6, 14))
        self._e_key = _entry(r2, width=50)
        self._e_key.pack(side="left", padx=(0, 8))
        _btn(r2, "Browse…", self._browse_key).pack(side="left", padx=4)
        _btn(r2, "Load & Connect", self._load_key, color=PURPLE).pack(side="left", padx=4)

        # Google Drive OAuth section
        c3 = _frame(p); c3.pack(fill="x", padx=20, pady=(0, 8))
        _lbl(c3, "Google Drive OAuth  (client_secrets.json – Phase 2 Cloud RAM)",
             color=ACCENT, font=FH).pack(anchor="w", padx=14, pady=(12, 6))
        cs_status = "✓ Found" if core.CLIENT_SECRETS.exists() else "✗ Not found"
        cs_color  = GREEN if core.CLIENT_SECRETS.exists() else RED
        self._lbl_cs_status = _lbl(
            c3, f"Secrets: {core.CLIENT_SECRETS}  [{cs_status}]",
            color=cs_color, font=FS)
        self._lbl_cs_status.pack(anchor="w", padx=14)
        tk_status = "✓ Authorized" if core.TOKEN_FILE.exists() else "○ Not yet authorized"
        tk_color  = GREEN if core.TOKEN_FILE.exists() else YELLOW
        self._lbl_tk_status = _lbl(
            c3, f"Token: {core.TOKEN_FILE}  [{tk_status}]",
            color=tk_color, font=FS)
        self._lbl_tk_status.pack(anchor="w", padx=14)
        r3 = tk.Frame(c3, bg=BG2); r3.pack(fill="x", padx=14, pady=(6, 14))
        _btn(r3, "🔑  Authorize Google Drive", self._do_gdrive_auth, color=ORANGE)\
            .pack(side="left", padx=4)
        _btn(r3, "🗑  Revoke Token", self._do_gdrive_revoke, color=BG3)\
            .pack(side="left", padx=4)

        # Server list
        c4 = _frame(p); c4.pack(fill="both", expand=True, padx=20, pady=(0, 18))
        _lbl(c4, "Registered Servers  (live from Firebase)", color=ACCENT, font=FH)\
            .pack(anchor="w", padx=14, pady=(12, 6))
        self._srv_tree = ttk.Treeview(c4, columns=("Name", "Status"), show="headings", height=6)
        for col, w in (("Name", 260), ("Status", 140)):
            self._srv_tree.heading(col, text=col)
            self._srv_tree.column(col, width=w)
        vsb = ttk.Scrollbar(c4, orient="vertical", command=self._srv_tree.yview)
        self._srv_tree.configure(yscrollcommand=vsb.set)
        self._srv_tree.pack(side="left", fill="both", expand=True, padx=(14, 0), pady=(0, 14))
        vsb.pack(side="left", fill="y", pady=(0, 14))
        _btn(c4, "⟳ Refresh", self._refresh_srv_list)\
            .pack(anchor="e", padx=14, pady=(0, 12))
        self._refresh_srv_list()

    def _do_register(self):
        name = self._e_reg.get().strip()
        if not name:
            messagebox.showwarning("Register", "Enter a device name."); return
        try:
            msg = core.register_server(name)
            messagebox.showinfo("Register Server", msg)
            self._refresh_srv_list(); self._dash_refresh()
        except Exception as e:
            log.error(f"_do_register: {e}", exc_info=True)
            messagebox.showerror("Register Server", str(e))

    def _browse_key(self):
        p = filedialog.askopenfilename(title="Select serviceAccountKey.json",
                                        filetypes=[("JSON", "*.json")])
        if p:
            self._e_key.delete(0, "end"); self._e_key.insert(0, p)

    def _load_key(self):
        src_text = self._e_key.get().strip()
        if not src_text: return
        src = Path(src_text)
        if not src.is_file():
            messagebox.showerror("Firebase Key", f"Not a file:\n{src}"); return
        try:
            dest = core.APP_DIR / "serviceAccountKey.json"
            if src.resolve() != dest.resolve():
                import shutil; shutil.copy2(str(src), str(dest))
            ok = core.init_firebase()
            if ok:
                messagebox.showinfo("Firebase", "Connected successfully!")
            else:
                messagebox.showerror("Firebase",
                    f"Connection failed.\n{core._firebase_error}\n\nSee Debug Log.")
            self._update_key_status(); self._dash_refresh()
        except Exception as e:
            log.error(f"_load_key: {e}", exc_info=True)
            messagebox.showerror("Firebase Key", str(e))

    def _update_key_status(self):
        exists = core.FIREBASE_KEY.exists()
        self._lbl_key_status.config(
            text=f"Current: {core.FIREBASE_KEY}  [{'✓ Found' if exists else '✗ Not found'}]",
            fg=GREEN if exists else RED,
        )

    def _do_gdrive_auth(self):
        def _auth():
            try:
                svc = core.get_drive_service()
                if svc:
                    self.after(0, lambda: messagebox.showinfo(
                        "Google Drive", "OAuth successful! token.json saved."))
                    self.after(0, self._update_gdrive_status)
                else:
                    self.after(0, lambda: messagebox.showerror(
                        "Google Drive", "OAuth failed. Check Debug Log."))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Google Drive", str(e)))
        threading.Thread(target=_auth, daemon=True).start()
        messagebox.showinfo("Google Drive Auth",
                            "A browser window will open for Google sign-in.\n"
                            "Please complete the login flow.")

    def _do_gdrive_revoke(self):
        if core.TOKEN_FILE.exists():
            core.TOKEN_FILE.unlink()
            messagebox.showinfo("Revoke", "token.json deleted. Drive access revoked.")
            self._update_gdrive_status()
        else:
            messagebox.showinfo("Revoke", "No token.json found.")

    def _update_gdrive_status(self):
        try:
            tk_status = "✓ Authorized" if core.TOKEN_FILE.exists() else "○ Not yet authorized"
            tk_color  = GREEN if core.TOKEN_FILE.exists() else YELLOW
            self._lbl_tk_status.config(text=f"Token: {core.TOKEN_FILE}  [{tk_status}]",
                                        fg=tk_color)
        except Exception:
            pass

    def _refresh_srv_list(self):
        try:
            for row in self._srv_tree.get_children():
                self._srv_tree.delete(row)
            snap = core.get_status_snapshot()
            for s in snap["registered"]:
                status = "● Online" if s in snap["online"] else "○ Offline"
                self._srv_tree.insert("", "end", values=(s, status))
        except Exception as e:
            log.error(f"_refresh_srv_list: {e}", exc_info=True)

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 3 – Projects
    # ══════════════════════════════════════════════════════════════════════════

    def _tab_projects(self, p):
        p.columnconfigure(0, weight=1)

        c1 = _frame(p); c1.pack(fill="x", padx=20, pady=(18, 8))
        _lbl(c1, "Create New Project", color=ACCENT, font=FH)\
            .pack(anchor="w", padx=14, pady=(12, 6))
        _lbl(c1,
             "All registered servers will automatically mirror this project folder.",
             color=FG2, font=FS).pack(anchor="w", padx=14)
        fg = tk.Frame(c1, bg=BG2); fg.pack(fill="x", padx=14, pady=(8, 14))
        _lbl(fg, "Project Name:").grid(row=0, column=0, sticky="w", pady=4)
        self._e_pname = _entry(fg, width=34)
        self._e_pname.grid(row=0, column=1, padx=8, pady=4)
        _btn(c1, "✦  Create Project", self._do_create_proj, color=GREEN)\
            .pack(anchor="w", padx=14, pady=(0, 14))

        c2 = _frame(p); c2.pack(fill="both", expand=True, padx=20, pady=(0, 18))
        _lbl(c2, "All Projects  (live from Firebase)", color=ACCENT, font=FH)\
            .pack(anchor="w", padx=14, pady=(12, 6))
        cols = ("ID", "Name", "Local Folder", "Sync Status")
        self._proj_tree = ttk.Treeview(c2, columns=cols, show="headings", height=8)
        for col, w in zip(cols, [50, 200, 360, 180]):
            self._proj_tree.heading(col, text=col)
            self._proj_tree.column(col, width=w)
        vsb = ttk.Scrollbar(c2, orient="vertical", command=self._proj_tree.yview)
        self._proj_tree.configure(yscrollcommand=vsb.set)
        self._proj_tree.pack(side="left", fill="both", expand=True, padx=(14, 0), pady=(0, 14))
        vsb.pack(side="left", fill="y", pady=(0, 14))
        br = tk.Frame(c2, bg=BG2); br.pack(fill="x", padx=14, pady=(0, 12))
        _btn(br, "⟳ Refresh", self._refresh_proj_list).pack(side="left", padx=4)
        _btn(br, "📂 Open Folder", self._open_proj_folder).pack(side="left", padx=4)
        _btn(br, "🔄 Force Replay Journal", self._do_force_replay, color=ORANGE)\
            .pack(side="left", padx=4)
        self._refresh_proj_list()

    def _do_create_proj(self):
        name = self._e_pname.get().strip()
        if not name:
            messagebox.showwarning("Create Project", "Enter a project name."); return
        try:
            msg = core.register_project(name)
            messagebox.showinfo("Create Project", msg)
            self._add_activity(f"Project created: {name}")
            self._refresh_proj_list(); self._dash_refresh()
        except Exception as e:
            messagebox.showerror("Create Project", str(e))

    def _refresh_proj_list(self):
        try:
            for row in self._proj_tree.get_children():
                self._proj_tree.delete(row)
            for row in core.read_projects():
                if len(row) >= 2:
                    proj_name = row[1].strip() if len(row) > 1 else str(row[0])
                    folder    = str(core.SERVER_DIR / proj_name)
                    folder_exists = Path(folder).exists()
                    sync_st   = "✓ Folder exists" if folder_exists else "⚠ Creating…"
                    self._proj_tree.insert("", "end", values=(row[0], proj_name, folder, sync_st))
        except Exception as e:
            log.error(f"_refresh_proj_list: {e}", exc_info=True)

    def _open_proj_folder(self):
        sel = self._proj_tree.selection()
        if not sel:
            messagebox.showinfo("Open Folder", "Select a project first."); return
        folder = Path(self._proj_tree.item(sel[0])["values"][2])
        if folder.exists():
            _explorer(folder)
        else:
            messagebox.showwarning("Open Folder", f"Folder not found:\n{folder}")

    def _do_force_replay(self):
        sel = self._proj_tree.selection()
        if not sel:
            messagebox.showinfo("Replay", "Select a project first."); return
        proj_name = str(self._proj_tree.item(sel[0])["values"][1])
        def _replay():
            core.replay_journal(proj_name)
            self.after(0, lambda: messagebox.showinfo(
                "Replay", f"Journal replay complete for: {proj_name}"))
            self.after(0, self._refresh_sync_tab)
        threading.Thread(target=_replay, daemon=True).start()
        messagebox.showinfo("Replay", f"Journal replay started for: {proj_name}\nSee Debug Log.")

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 4 – Sync
    # ══════════════════════════════════════════════════════════════════════════

    def _tab_sync(self, p):
        p.columnconfigure(0, weight=1)
        p.columnconfigure(1, weight=1)
        p.rowconfigure(2, weight=1)

        _lbl(p, "Project Sync Status  (3-Index Distributed Mirror)",
             font=FH, color=ACCENT).grid(row=0, column=0, columnspan=2,
                                           sticky="w", padx=20, pady=(16, 8))

        ct = _frame(p); ct.grid(row=1, column=0, sticky="nsew", padx=(20, 6), pady=(0, 8))
        _lbl(ct, "Active Transfers  (Google Drive Cloud RAM)", color=ACCENT, font=FH)\
            .pack(anchor="w", padx=14, pady=(12, 6))
        cols_t = ("Project", "File UUID", "File Name", "Status", "Progress")
        self._xfer_tree = ttk.Treeview(ct, columns=cols_t, show="headings", height=6)
        for col, w in zip(cols_t, [120, 100, 180, 100, 80]):
            self._xfer_tree.heading(col, text=col)
            self._xfer_tree.column(col, width=w)
        vsb_t = ttk.Scrollbar(ct, orient="vertical", command=self._xfer_tree.yview)
        self._xfer_tree.configure(yscrollcommand=vsb_t.set)
        self._xfer_tree.pack(side="left", fill="both", expand=True,
                             padx=(14, 0), pady=(0, 14))
        vsb_t.pack(side="left", fill="y", pady=(0, 14))

        ci = _frame(p); ci.grid(row=1, column=1, sticky="nsew", padx=(6, 20), pady=(0, 8))
        ci.rowconfigure(1, weight=1)
        _lbl(ci, "Inventory Index  (Flat UUID Map)", color=ACCENT, font=FH)\
            .grid(row=0, column=0, columnspan=3, sticky="w", padx=14, pady=(12, 6))

        sel_row = tk.Frame(ci, bg=BG2)
        sel_row.grid(row=0, column=0, sticky="w", padx=14, pady=(0, 6))
        _lbl(sel_row, "Project:").pack(side="left")
        self._cb_inv_proj = _combo(sel_row, width=22)
        self._cb_inv_proj.pack(side="left", padx=8)
        _btn(sel_row, "Load", self._load_inventory, color=PURPLE).pack(side="left", padx=4)

        cols_i = ("UUID", "Type", "Name", "Parent ID", "Size", "Hash")
        self._inv_tree = ttk.Treeview(ci, columns=cols_i, show="headings", height=8)
        for col, w in zip(cols_i, [110, 60, 160, 110, 80, 80]):
            self._inv_tree.heading(col, text=col)
            self._inv_tree.column(col, width=w)
        vsb_i = ttk.Scrollbar(ci, orient="vertical", command=self._inv_tree.yview)
        self._inv_tree.configure(yscrollcommand=vsb_i.set)
        self._inv_tree.grid(row=1, column=0, columnspan=2, sticky="nsew",
                            padx=(14, 0), pady=(0, 14))
        vsb_i.grid(row=1, column=2, sticky="ns", pady=(0, 14), padx=(0, 8))
        ci.columnconfigure(0, weight=1)

        cj = _frame(p)
        cj.grid(row=2, column=0, columnspan=2, sticky="nsew", padx=20, pady=(0, 18))
        cj.rowconfigure(1, weight=1)
        _lbl(cj, "Journal Index  (Change History)", color=ACCENT, font=FH)\
            .grid(row=0, column=0, sticky="w", padx=14, pady=(12, 6))
        sel_row2 = tk.Frame(cj, bg=BG2)
        sel_row2.grid(row=0, column=1, sticky="w", padx=14, pady=(0, 6))
        self._cb_jrn_proj = _combo(sel_row2, width=22)
        self._cb_jrn_proj.pack(side="left", padx=8)
        _btn(sel_row2, "Load Journal", self._load_journal, color=TEAL).pack(side="left", padx=4)
        _btn(sel_row2, "⟳ Refresh All", self._refresh_sync_tab).pack(side="left", padx=4)

        cols_j = ("GSID", "Action", "UUID", "Origin", "Timestamp")
        self._jrn_tree = ttk.Treeview(cj, columns=cols_j, show="headings", height=8)
        for col, w in zip(cols_j, [200, 100, 110, 140, 140]):
            self._jrn_tree.heading(col, text=col)
            self._jrn_tree.column(col, width=w)
        vsb_j = ttk.Scrollbar(cj, orient="vertical", command=self._jrn_tree.yview)
        self._jrn_tree.configure(yscrollcommand=vsb_j.set)
        self._jrn_tree.grid(row=1, column=0, columnspan=2, sticky="nsew",
                            padx=(14, 0), pady=(0, 14))
        vsb_j.grid(row=1, column=2, sticky="ns", pady=(0, 14), padx=(0, 8))
        cj.columnconfigure(0, weight=1)

        self._refresh_sync_tab()

    def _refresh_sync_tab(self):
        try:
            proj_names = [r[1] for r in core.read_projects() if len(r) > 1]
            for cb in (self._cb_inv_proj, self._cb_jrn_proj):
                cb["values"] = proj_names
                if proj_names and not cb.get():
                    cb.current(0)
        except Exception as e:
            log.error(f"_refresh_sync_tab: {e}", exc_info=True)

        self._refresh_transfer_list()

    def _refresh_transfer_list(self):
        try:
            for row in self._xfer_tree.get_children():
                self._xfer_tree.delete(row)

            for proj_name in [r[1] for r in core.read_projects() if len(r) > 1]:
                transfer_data = core._fb_get(core.transfer_path(proj_name))
                if not isinstance(transfer_data, dict):
                    continue
                for file_uuid, entry in transfer_data.items():
                    if not isinstance(entry, dict):
                        continue
                    status   = entry.get("status", "?")
                    fname    = entry.get("file_name", "?")
                    total    = entry.get("total_chunks", 1)
                    
                    chunks_raw = entry.get("chunks", {})
                    if isinstance(chunks_raw, dict):
                        chunks_iterable = chunks_raw.values()
                    else:
                        chunks_iterable = chunks_raw
                    
                    done_c = len([c for c in chunks_iterable 
                                  if isinstance(c, dict) and c.get("status") == "READY"])
                    
                    progress = f"{done_c}/{total}"
                    self._xfer_tree.insert("", "end",
                        values=(proj_name, file_uuid[:12], fname, status, progress))
        except Exception as e:
            log.error(f"_refresh_transfer_list: {e}", exc_info=True)

    def _load_inventory(self):
        proj = self._cb_inv_proj.get().strip()
        if not proj:
            messagebox.showinfo("Inventory", "Select a project."); return
        try:
            for row in self._inv_tree.get_children():
                self._inv_tree.delete(row)
            inv = core.get_local_inventory(proj)
            for uid, meta in inv.get("files", {}).items():
                self._inv_tree.insert("", "end", values=(
                    uid[:12], "FILE",
                    meta.get("name", "?"),
                    (meta.get("parent_id", "?") or "root")[:12],
                    meta.get("size_bytes", 0),
                    (meta.get("checksum", "?") or "")[:8],
                ))
            for uid, meta in inv.get("folders", {}).items():
                self._inv_tree.insert("", "end", values=(
                    uid[:12], "FOLDER",
                    meta.get("name", "?"),
                    (meta.get("parent_id", "?") or "root")[:12],
                    "—", "—",
                ))
        except Exception as e:
            log.error(f"_load_inventory: {e}", exc_info=True)
            messagebox.showerror("Inventory", str(e))

    def _load_journal(self):
        proj = self._cb_jrn_proj.get().strip()
        if not proj:
            messagebox.showinfo("Journal", "Select a project."); return
        def _fetch():
            try:
                data = core._fb_get(core.journal_path(proj))
                self.after(0, lambda: self._populate_journal(data))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Journal", str(e)))
        threading.Thread(target=_fetch, daemon=True).start()

    def _populate_journal(self, data):
        try:
            for row in self._jrn_tree.get_children():
                self._jrn_tree.delete(row)
            if not isinstance(data, dict):
                return
            for gsid, entry in sorted(data.items(), reverse=True):
                if not isinstance(entry, dict):
                    continue
                ts = entry.get("timestamp", 0)
                ts_str = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") \
                         if ts else "?"
                self._jrn_tree.insert("", "end", values=(
                    gsid[:24],
                    entry.get("action", "?"),
                    (entry.get("uuid", "?") or "")[:12],
                    entry.get("origin", "?"),
                    ts_str,
                ))
        except Exception as e:
            log.error(f"_populate_journal: {e}", exc_info=True)

    # ══════════════════════════════════════════════════════════════════════════
    # TAB 5 – Debug Log
    # ══════════════════════════════════════════════════════════════════════════

    def _tab_log(self, p):
        p.rowconfigure(1, weight=1); p.columnconfigure(0, weight=1)

        tb = tk.Frame(p, bg=BG2); tb.grid(row=0, column=0, sticky="ew")
        _lbl(tb, "  Live Debug Console  (Phase 2)", color=ACCENT, font=FH)\
            .pack(side="left", padx=8, pady=8)

        self._log_filter = tk.StringVar(value="ALL")
        for lvl, col in [("ALL", FG2), ("DEBUG", ACCENT), ("INFO", GREEN),
                          ("WARNING", YELLOW), ("ERROR", RED)]:
            tk.Radiobutton(tb, text=lvl, variable=self._log_filter, value=lvl,
                           command=self._refilter_log,
                           bg=BG2, fg=col, selectcolor=BG3,
                           activebackground=BG2, font=FS).pack(side="left", padx=4)

        _btn(tb, "🗑 Clear",      self._clear_log,    color=BG3).pack(side="right", padx=8)
        _btn(tb, "📄 Open File",  self._open_log_file          ).pack(side="right", padx=4)
        _btn(tb, "↕ Auto-scroll", self._toggle_scroll, color=BG3).pack(side="right", padx=4)
        _btn(tb, "📁 Open Log Dir", self._open_log_dir, color=BG3).pack(side="right", padx=4)

        lf = tk.Frame(p, bg=BG); lf.grid(row=1, column=0, sticky="nsew")
        lf.rowconfigure(0, weight=1); lf.columnconfigure(0, weight=1)

        self._log_txt = tk.Text(lf, bg="#0a0e14", fg=FG, font=FL,
                                 relief="flat", wrap="word", state="disabled",
                                 padx=8, pady=4, selectbackground=BG3)
        vsb = ttk.Scrollbar(lf, orient="vertical", command=self._log_txt.yview)
        self._log_txt.configure(yscrollcommand=vsb.set)
        self._log_txt.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")

        for tag, col in [("DEBUG", ACCENT), ("INFO", GREEN),
                          ("WARNING", YELLOW), ("ERROR", RED), ("CRITICAL", "#ff3333")]:
            self._log_txt.tag_config(tag, foreground=col)

        self._log_entries: list = []

    def _append_log_entry(self, level: str, msg: str):
        self._log_entries.append((level, msg))
        if len(self._log_entries) > 5000:
            self._log_entries = self._log_entries[-4000:]
        filt = self._log_filter.get()
        if filt != "ALL" and level != filt:
            return
        self._log_txt.config(state="normal")
        self._log_txt.insert("end", msg + "\n", level)
        lines = int(self._log_txt.index("end-1c").split(".")[0])
        if lines > 3200:
            self._log_txt.delete("1.0", "800.0")
        self._log_txt.config(state="disabled")
        if self._autoscroll:
            self._log_txt.see("end")

        if level in ("INFO", "WARNING", "ERROR"):
            for kw in ("[Sync]", "[Watcher]", "[Journal]", "[Transfer]", "[Upload]", "[Download]",
                       "[Apply]", "[Replay]", "[Janitor]"):
                if kw in msg:
                    self._add_activity(msg.split("|")[-1].strip())
                    break

    def _refilter_log(self):
        self._log_txt.config(state="normal")
        self._log_txt.delete("1.0", "end")
        filt = self._log_filter.get()
        for level, msg in self._log_entries:
            if filt == "ALL" or level == filt:
                self._log_txt.insert("end", msg + "\n", level)
        self._log_txt.config(state="disabled")
        if self._autoscroll: self._log_txt.see("end")

    def _clear_log(self):
        self._log_entries.clear()
        self._log_txt.config(state="normal"); self._log_txt.delete("1.0", "end")
        self._log_txt.config(state="disabled")

    def _toggle_scroll(self):
        self._autoscroll = not self._autoscroll

    def _open_log_file(self):
        _explorer(log.LOG_FILE)

    def _open_log_dir(self):
        _explorer(log.LOG_DIR)

    def _poll_log(self):
        try:
            while True:
                e = log.log_queue.get_nowait()
                self._append_log_entry(e["level"], e["msg"])
        except Exception:
            pass
        self.after(150, self._poll_log)

    def _poll_status(self):
        self._lbl_time.config(text=datetime.datetime.now().strftime("%H:%M:%S"))
        if self._running:
            self._lbl_srv.config(text=f"● ONLINE  {self._srv_var.get()}", fg=GREEN)
            self._dash_refresh()
        else:
            self._lbl_srv.config(text="○  OFFLINE", fg=RED)
        self.after(2000, self._poll_status)

    def _start(self):
        name = self._srv_var.get().strip()
        if not name:
            messagebox.showwarning("Start", "Enter a device name."); return
        if self._running: return
        try:
            core.ensure_dirs()
            core.register_server(name)
            core.mark_online(name)
            tm.thread_manager.start(name)
            self._running = True
            self._btn_start.config(state="disabled")
            self._btn_stop.config(state="normal")
            log.info(f"Server '{name}' started  [Phase 2]")
            self._dash_refresh(); self._refresh_srv_list()
            self._add_activity(f"Server started: {name}")
        except Exception as e:
            log.error(f"_start: {e}", exc_info=True)
            messagebox.showerror("Start Server", str(e))

    def _stop(self):
        name = self._srv_var.get().strip()
        try:
            tm.thread_manager.stop()
            core.mark_offline(name)
            self._running = False
            self._btn_start.config(state="normal")
            self._btn_stop.config(state="disabled")
            log.info(f"Server '{name}' stopped")
            self._dash_refresh(); self._refresh_srv_list()
            self._add_activity(f"Server stopped: {name}")
        except Exception as e:
            log.error(f"_stop: {e}", exc_info=True)

    def _bg_firebase_init(self):
        log.debug("Background Firebase init started")
        core.init_firebase()
        self.after(0, self._dash_refresh)
        self.after(0, self._update_key_status)

    def _on_close(self):
        if self._running:
            if not messagebox.askyesno("Exit", "Server is running. Stop and exit?"):
                return
            self._stop()
        log.info("Application closing"); self.destroy()


def _explorer(path: Path):
    try:
        if sys.platform == "win32":
            os.startfile(str(path))
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])
    except Exception as e:
        messagebox.showerror("Open", f"Cannot open:\n{path}\n\n{e}")


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()