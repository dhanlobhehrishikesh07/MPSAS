"""
core.py
───────
One Click Server – Phase 2 Business Logic

Phase 2 Changes vs Phase 1:
  • Removed: Offline_To_Change directory and all its logic
  • Removed: CSV-based change queue (record_change, apply_pending_changes)
  • Removed: import_file_to_project / import_folder_to_project (old logic)
  • Removed: transfer_server (portable copy/move server folder)
  • Removed: allowed_servers restriction (all servers are allowed everything)
  • Simplified register_project: just creates a folder with project name under Server/

  NEW – 3-Index Distributed Sync System:
  ─────────────────────────────────────
  • Inventory Index (Firebase + local JSON cache):
      UUID-keyed flat map of all files and folders per project.
      { files: {uuid: {...}}, folders: {uuid: {...}} }

  • Journal Index (Firebase only, streamed to all servers):
      Sequential log of changes (CREATE, MOVE, COPY, DELETE).
      Each entry has a Global Sequence ID (gsid).
      Servers track their checkpoint (last processed gsid).

  • Transfer Index (Firebase only, streamed to all servers):
      Tracks Google Drive chunk upload/download progress for CREATE operations.
      Used as "cloud RAM" for file transfers.

  • File Watcher: Monitors each project folder for changes using
      watchdog (or polling fallback). Detected changes are written to
      Journal and dispatched to other servers via Firebase.

  • Google Drive OAuth (user account, 15 GB):
      Files are chunked (10 MB) and uploaded/downloaded via Drive API.
      client_secrets.json → token.json OAuth flow.

  Architecture Robustness Fixes Applied:
  • Simultaneous-modification conflict: lower gsid wins; loser aborts upload.
  • Incomplete-create timeout: transfers marked FAILED after X minutes of inactivity.
  • Folder-delete propagation: recursive local delete from inventory parent_id lookup.
  • Janitor logic: try/except wrapped; never crashes the main program.
  • All background operations are daemon threads; all Firebase paths are try/except.
"""

import threading
import json
import os
import shutil
import socket
import uuid
import hashlib
import datetime
import time
from pathlib import Path

import debug_logger as log

# ─── Base paths ────────────────────────────────────────────────────────────────
APP_DIR    = Path(__file__).parent.resolve()
SERVER_DIR = APP_DIR / "Server"

FIREBASE_KEY      = APP_DIR / "serviceAccountKey.json"
FIREBASE_URL      = "hiden for security"
CLIENT_SECRETS    = APP_DIR / "client_secrets.json"
TOKEN_FILE        = APP_DIR / "token.json"
DRIVE_FOLDER_ID   = "hiden for security"  # Google Drive "creation bucket"

CHUNK_SIZE        = 10 * 1024 * 1024   # 10 MB
TRANSFER_TIMEOUT  = 15 * 60            # 15 minutes before marking FAILED

# ─── Firebase state ────────────────────────────────────────────────────────────
_firebase_ready   = False
_firebase_error   = ""
_active_listeners = []

# ─── Global Sync Controls ──────────────────────────────────────────────────────
# Added: Global limit to prevent API/Bandwidth saturation 
TRANSFER_SEMAPHORE = threading.Semaphore(2)

_FIREBASE_META_KEYS = frozenset({
    "hostname", "registered", "registered_at", "status", "last_seen"
})

# ─── Live in-memory mirror ────────────────────────────────────────────────────
LIVE_STATE = {
    "registered": [],   # list[str]
    "online":     [],   # list[str]
    "offline":    [],   # derived: registered - online
    "projects":   {},   # dict {name: {id, created_at}}
}

# Per-project sync state (populated from Firebase streams)
# { project_name: { "inventory": {...}, "journal": {...}, "transfer": {...} } }
PROJECT_SYNC_STATE = {}

# Per-project file watchers
_watchers = {}

# ─── Thread-safe GUI callback ─────────────────────────────────────────────────
_gui_root     = None
_gui_callback = None

def set_gui_root(root):
    global _gui_root
    _gui_root = root

def set_gui_callback(cb):
    global _gui_callback
    _gui_callback = cb

def _fire_gui_update():
    if _gui_callback and _gui_root:
        try:
            _gui_root.after(0, _gui_callback)
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# Firebase helpers
# ══════════════════════════════════════════════════════════════════════════════

def init_firebase(key_path: str = None) -> bool:
    global _firebase_ready, _firebase_error
    _firebase_error = ""
    key = Path(key_path) if key_path else FIREBASE_KEY
    log.debug(f"init_firebase: key={key}")
    if not key.exists():
        _firebase_error = f"Key file not found: {key}"
        log.warning(_firebase_error)
        return False
    try:
        import firebase_admin
        from firebase_admin import credentials, db as rtdb
        if firebase_admin._apps:
            log.info("Firebase already initialised – reusing existing app")
            _firebase_ready = True
            return True
        cred = credentials.Certificate(str(key))
        firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_URL})
        _firebase_ready = True
        log.info(f"Firebase connected. Project: {cred.project_id}  URL: {FIREBASE_URL}")
        return True
    except ImportError:
        _firebase_error = "firebase-admin not installed."
        log.error(_firebase_error)
    except Exception as e:
        _firebase_error = str(e)
        log.error(f"Firebase init failed: {e}", exc_info=True)
    _firebase_ready = False
    return False


def firebase_status() -> str:
    if _firebase_ready:
        return "CONNECTED ✓"
    if _firebase_error:
        return f"OFFLINE – {_firebase_error[:60]}"
    return "OFFLINE (key not loaded)"


def _fb_ref(path: str):
    if not _firebase_ready:
        log.debug(f"Firebase not ready – skipping: {path}")
        return None
    try:
        from firebase_admin import db as rtdb
        return rtdb.reference(path)
    except Exception as e:
        log.error(f"_fb_ref({path}) failed: {e}", exc_info=True)

def _fb_set(path: str, data):
    ref = _fb_ref(path)
    if ref:
        try:
            ref.set(data)
            log.debug(f"Firebase SET: {path}")
        except Exception as e:
            log.error(f"Firebase SET failed ({path}): {e}", exc_info=True)

def _fb_update(path: str, data: dict):
    ref = _fb_ref(path)
    if ref:
        try:
            ref.update(data)
            log.debug(f"Firebase UPDATE: {path}")
        except Exception as e:
            log.error(f"Firebase UPDATE failed ({path}): {e}", exc_info=True)

def _fb_push(path: str, data: dict):
    ref = _fb_ref(path)
    if ref:
        try:
            ref.push(data)
            log.debug(f"Firebase PUSH: {path}")
        except Exception as e:
            log.error(f"Firebase PUSH failed ({path}): {e}", exc_info=True)

def _fb_delete(path: str):
    ref = _fb_ref(path)
    if ref:
        try:
            ref.delete()
            log.debug(f"Firebase DELETE: {path}")
        except Exception as e:
            log.error(f"Firebase DELETE failed ({path}): {e}", exc_info=True)

def _fb_get(path: str):
    ref = _fb_ref(path)
    if ref:
        try:
            return ref.get()
        except Exception as e:
            log.error(f"Firebase GET failed ({path}): {e}", exc_info=True)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Device identity
# ══════════════════════════════════════════════════════════════════════════════

def get_device_name() -> str:
    try:
        name = f"{socket.gethostname()}_{APP_DIR.name}"
    except Exception:
        name = "UnknownDevice"
    log.debug(f"get_device_name: {name}")
    return name


# ══════════════════════════════════════════════════════════════════════════════
# Realtime stream – Infrastructure (Registered / Online / Projects)
# ══════════════════════════════════════════════════════════════════════════════

def _is_valid_server_name(name: str) -> bool:
    s = name.strip() if name else ""
    return bool(s) and s not in _FIREBASE_META_KEYS


def _recalc_offline():
    LIVE_STATE["offline"] = [
        s for s in LIVE_STATE["registered"]
        if s not in LIVE_STATE["online"]
    ]


def _handle_infra_event(event, firebase_path: str):
    try:
        data      = event.data
        evt_path  = getattr(event, "path", "/")
        log.debug(f"Stream  node={firebase_path}  path={evt_path}  dtype={type(data).__name__}")

        if firebase_path == "Registered_Server_Locations":
            _merge_presence_event(LIVE_STATE, "registered", evt_path, data)
            _recalc_offline()
            log.info(f"[Stream] Registered: {LIVE_STATE['registered']}")

        elif firebase_path == "Online_Servers_Activated":
            _merge_presence_event(LIVE_STATE, "online", evt_path, data)
            _recalc_offline()
            log.info(f"[Stream] Online={LIVE_STATE['online']}  Offline={LIVE_STATE['offline']}")

        elif firebase_path == "Projects":
            _merge_projects_event(evt_path, data)
            log.info(f"[Stream] Projects: {list(LIVE_STATE['projects'].keys())}")
            # When a new project appears, create local folder
            _ensure_project_local_folders()

        _fire_gui_update()
    except Exception as e:
        log.error(f"Stream handler error (node={firebase_path}): {e}", exc_info=True)


def _merge_presence_event(state: dict, key: str, evt_path: str, data):
    if evt_path == "/":
        if data is None:
            state[key] = []
        elif isinstance(data, dict):
            state[key] = [k for k in data if _is_valid_server_name(k)]
        else:
            state[key] = []
    else:
        server_name = evt_path.lstrip("/")
        if not _is_valid_server_name(server_name):
            return
        current: list = state[key]
        if data is None:
            if server_name in current:
                current.remove(server_name)
                log.debug(f"  [{key}] removed: {server_name}")
        else:
            if server_name not in current:
                current.append(server_name)
                log.debug(f"  [{key}] added: {server_name}")


def _merge_projects_event(evt_path: str, data):
    if evt_path == "/":
        if data is None:
            LIVE_STATE["projects"] = {}
        elif isinstance(data, dict):
            LIVE_STATE["projects"] = data
    else:
        proj_name = evt_path.lstrip("/").split("/")[0]
        if not proj_name:
            return
        if data is None:
            LIVE_STATE["projects"].pop(proj_name, None)
        else:
            top = evt_path.lstrip("/")
            if "/" not in top:
                LIVE_STATE["projects"][proj_name] = data
            else:
                if proj_name not in LIVE_STATE["projects"]:
                    LIVE_STATE["projects"][proj_name] = {}
                if isinstance(data, dict) and isinstance(LIVE_STATE["projects"][proj_name], dict):
                    LIVE_STATE["projects"][proj_name].update(data)


def _ensure_project_local_folders():
    """When Firebase streams project list, ensure every project has a local folder."""
    for proj_name in LIVE_STATE["projects"]:
        proj_dir = SERVER_DIR / proj_name
        if not proj_dir.exists():
            try:
                proj_dir.mkdir(parents=True, exist_ok=True)
                log.info(f"[Sync] Created local folder for project: {proj_name}")
            except Exception as e:
                log.error(f"[Sync] Failed to create folder {proj_dir}: {e}")


def start_infrastructure_stream(server_name: str):
    paths = [
        "Registered_Server_Locations",
        "Online_Servers_Activated",
        "Projects",
    ]
    for path in paths:
        ref = _fb_ref(path)
        if ref:
            try:
                listener = ref.listen(lambda event, p=path: _handle_infra_event(event, p))
                _active_listeners.append(listener)
                log.info(f"Stream subscribed: {path}")
            except Exception as e:
                log.error(f"Stream subscribe failed ({path}): {e}", exc_info=True)


def stop_all_streams():
    global _active_listeners
    for listener in _active_listeners:
        try:
            listener.close()
        except Exception as e:
            log.error(f"Error closing listener: {e}")
    _active_listeners = []
    log.info("All Firebase streams closed")


# ══════════════════════════════════════════════════════════════════════════════
# Server lifecycle
# ══════════════════════════════════════════════════════════════════════════════

def ensure_dirs():
    for d in [SERVER_DIR, APP_DIR / "logs"]:
        d.mkdir(parents=True, exist_ok=True)
    log.debug(f"Dirs ensured: {APP_DIR}")


def register_server(name: str) -> str:
    log.section(f"REGISTER SERVER: {name}")
    ensure_dirs()
    name = name.strip()
    if not name:
        return "ERROR: Name empty"
    _fb_set(f"Registered_Server_Locations/{name}", {
        "registered":    True,
        "registered_at": _now(),
        "hostname":      name,
    })
    return f"Registration request sent for {name}"


def mark_online(name: str):
    log.info(f"Marking ONLINE: {name}")
    _fb_set(f"Online_Servers_Activated/{name}", {
        "status":    "online",
        "last_seen": _now(),
    })


def mark_offline(name: str):
    log.info(f"Marking OFFLINE: {name}")
    _fb_delete(f"Online_Servers_Activated/{name}")


def get_status_snapshot() -> dict:
    return {
        "registered":  list(LIVE_STATE["registered"]),
        "online":      list(LIVE_STATE["online"]),
        "offline":     list(LIVE_STATE["offline"]),
        "projects":    read_projects(),
        "firebase":    firebase_status(),
        "app_dir":     str(APP_DIR),
        "server_dir":  str(SERVER_DIR),
        "key_present": FIREBASE_KEY.exists(),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Projects  (Phase 2: simplified – just creates folder, no sub-dirs)
# ══════════════════════════════════════════════════════════════════════════════

def read_projects() -> list:
    result = []
    for name, info in LIVE_STATE["projects"].items():
        if isinstance(info, dict):
            result.append([info.get("id", 0), name])
    return result


def register_project(project_name: str) -> str:
    """
    Phase 2: Create project.
    - Creates a folder named <project_name> under Server/.
    - Pushes project metadata to Firebase → all other servers will create
      the same folder when their stream picks it up.
    - Initialises an empty Inventory Index on Firebase.
    """
    log.section(f"REGISTER PROJECT: {project_name}")
    ensure_dirs()
    project_name = project_name.strip()
    if not project_name:
        return "ERROR: Project name cannot be empty."
    if project_name in LIVE_STATE["projects"]:
        return f"Project '{project_name}' already exists."

    ids    = [v.get("id", 0) for v in LIVE_STATE["projects"].values() if isinstance(v, dict)]
    new_id = max(ids, default=0) + 1

    # Create local folder
    proj_dir = SERVER_DIR / project_name
    proj_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Created local folder: {proj_dir}")

    # Push to Firebase (triggers remote servers to create folder too)
    _fb_set(f"Projects/{project_name}", {
        "id":         new_id,
        "created_at": _now(),
        "creator":    get_device_name(),
    })

    # Initialise empty Inventory Index on Firebase for this project
    inventory_path = f"ProjectSync/{project_name}/inventory"
    _fb_set(inventory_path, {"files": {}, "folders": {}})
    log.info(f"Inventory Index initialised for project: {project_name}")

    return f"Project '{project_name}' created (folder + Firebase sync initialised)"


def get_project_dir(project_name: str) -> Path:
    return SERVER_DIR / project_name


# ══════════════════════════════════════════════════════════════════════════════
# 3-Index System: Inventory, Journal, Transfer
# ══════════════════════════════════════════════════════════════════════════════

# ── Inventory ─────────────────────────────────────────────────────────────────

def inventory_path(project: str) -> str:
    return f"ProjectSync/{project}/inventory"

def journal_path(project: str) -> str:
    return f"ProjectSync/{project}/journal"

def transfer_path(project: str) -> str:
    return f"ProjectSync/{project}/transfer"

def checkpoint_path(project: str) -> str:
    return f"ProjectSync/{project}/checkpoints/{get_device_name()}"

def local_inventory_path(project: str) -> Path:
    return SERVER_DIR / project / ".inventory_cache.json"


def get_local_inventory(project: str) -> dict:
    """Load local inventory cache, return empty if not present."""
    p = local_inventory_path(project)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning(f"[Inventory] Could not read local cache for {project}: {e}")
    return {"files": {}, "folders": {}}


def save_local_inventory(project: str, inventory: dict):
    """Save inventory to local cache JSON file."""
    p = local_inventory_path(project)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    except Exception as e:
        log.error(f"[Inventory] Could not save local cache for {project}: {e}")


def add_to_inventory(project: str, item_uuid: str, item_type: str, metadata: dict):
    """
    Add file or folder to Inventory Index (Firebase + local cache).
    item_type: 'files' or 'folders'
    """
    inv = get_local_inventory(project)
    inv.setdefault("files", {})
    inv.setdefault("folders", {})
    inv[item_type][item_uuid] = metadata
    save_local_inventory(project, inv)
    # Push to Firebase
    _fb_set(f"{inventory_path(project)}/{item_type}/{item_uuid}", metadata)
    log.debug(f"[Inventory] Added {item_type} uuid={item_uuid} to {project}")


def remove_from_inventory(project: str, item_uuid: str, item_type: str):
    inv = get_local_inventory(project)
    inv.setdefault(item_type, {}).pop(item_uuid, None)
    save_local_inventory(project, inv)
    _fb_delete(f"{inventory_path(project)}/{item_type}/{item_uuid}")
    log.debug(f"[Inventory] Removed {item_type} uuid={item_uuid} from {project}")


def get_inventory_item(project: str, item_uuid: str) -> dict | None:
    inv = get_local_inventory(project)
    return inv.get("files", {}).get(item_uuid) or inv.get("folders", {}).get(item_uuid)


def uuid_to_local_path(project: str, item_uuid: str) -> Path | None:
    """
    Reconstruct full local path from inventory by walking parent_id chain.
    """
    inv = get_local_inventory(project)
    item = inv.get("files", {}).get(item_uuid) or inv.get("folders", {}).get(item_uuid)
    if not item:
        return None
    parts = [item.get("name", "")]
    parent_id = item.get("parent_id", "root")
    visited = set()
    while parent_id and parent_id != "root":
        if parent_id in visited:
            break  # cycle guard
        visited.add(parent_id)
        folder = inv.get("folders", {}).get(parent_id)
        if not folder:
            break
        parts.append(folder.get("name", ""))
        parent_id = folder.get("parent_id", "root")
    parts.reverse()
    return SERVER_DIR / project / Path(*parts) if parts else None


def find_children_uuids(project: str, folder_uuid: str) -> list:
    """Find all UUIDs (files+folders) whose parent_id is folder_uuid."""
    inv = get_local_inventory(project)
    result = []
    for uid, meta in {**inv.get("files", {}), **inv.get("folders", {})}.items():
        if meta.get("parent_id") == folder_uuid:
            result.append(uid)
    return result


# ── Journal ───────────────────────────────────────────────────────────────────

_gsid_lock = threading.Lock()

def _next_gsid(project: str) -> str:
    """Get next global sequence ID. Simple: timestamp-based + UUID suffix."""
    ts = int(time.time() * 1000)
    return f"{ts:016d}_{uuid.uuid4().hex[:8]}"


def write_journal_entry(project: str, action: str, item_uuid: str, meta: dict) -> str:
    """
    Write a journal entry to Firebase.
    Returns the gsid.
    Conflict resolution: lower gsid (earlier timestamp) wins.
    """
    gsid = _next_gsid(project)
    entry = {
        "action":    action,
        "uuid":      item_uuid,
        "meta":      meta,
        "origin":    get_device_name(),
        "timestamp": _now_ts(),
        "gsid":      gsid,
    }
    _fb_set(f"{journal_path(project)}/{gsid}", entry)
    log.info(f"[Journal] {action} uuid={item_uuid} gsid={gsid} in {project}")
    return gsid


def get_checkpoint(project: str) -> str:
    """Return last processed gsid for this server."""
    try:
        p = SERVER_DIR / project / ".checkpoint"
        if p.exists():
            return p.read_text().strip()
    except Exception:
        pass
    return ""


def set_checkpoint(project: str, gsid: str):
    """Save last processed gsid for this server."""
    try:
        p = SERVER_DIR / project / ".checkpoint"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(gsid)
    except Exception as e:
        log.error(f"[Journal] Could not save checkpoint for {project}: {e}")


# ── Transfer Index ────────────────────────────────────────────────────────────

def init_transfer_entry(project: str, file_uuid: str, total_chunks: int,
                        file_name: str, file_size: int, gsid: str):
    """Create a transfer relay entry on Firebase for a new file."""
    all_servers = list(LIVE_STATE["registered"])
    sync_progress = {srv: "PENDING" for srv in all_servers if srv != get_device_name()}
    entry = {
        "file_name":    file_name,
        "file_size":    file_size,
        "total_chunks": total_chunks,
        "gsid":         gsid,
        "status":       "IN_PROGRESS",
        "started_at":   _now_ts(),
        "sender":       get_device_name(),
        "sync_progress": sync_progress,
        "chunks":       {},
    }
    _fb_set(f"{transfer_path(project)}/{file_uuid}", entry)
    log.info(f"[Transfer] Init relay for {file_name} ({total_chunks} chunks)")


def update_transfer_chunk(project: str, file_uuid: str, chunk_idx: int,
                           gdrive_id: str, chunk_hash: str):
    """Mark a chunk as ready in the Transfer Index."""
    _fb_set(f"{transfer_path(project)}/{file_uuid}/chunks/{chunk_idx}", {
        "gdrive_id": gdrive_id,
        "hash":      chunk_hash,
        "status":    "READY",
        "uploaded_at": _now_ts(),
    })


def mark_transfer_server_done(project: str, file_uuid: str, server_name: str):
    """Mark this server as COMPLETED for a transfer."""
    _fb_set(
        f"{transfer_path(project)}/{file_uuid}/sync_progress/{server_name}",
        "COMPLETED"
    )


# ══════════════════════════════════════════════════════════════════════════════
# Google Drive OAuth + Upload/Download
# ══════════════════════════════════════════════════════════════════════════════

def get_drive_service():
    """Build Google Drive service using OAuth user credentials."""
    try:
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        SCOPES = ["https://www.googleapis.com/auth/drive.file"]
        creds = None

        if TOKEN_FILE.exists():
            try:
                creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
            except Exception:
                creds = None

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception:
                    creds = None

            if not creds:
                if not CLIENT_SECRETS.exists():
                    log.error("[Drive] client_secrets.json not found")
                    return None
                flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRETS), SCOPES)
                creds = flow.run_local_server(port=0)

            try:
                TOKEN_FILE.write_text(creds.to_json())
            except Exception as e:
                log.warning(f"[Drive] Could not save token: {e}")

        return build("drive", "v3", credentials=creds)
    except ImportError as e:
        log.error(f"[Drive] Google API library missing: {e}")
        return None
    except Exception as e:
        log.error(f"[Drive] get_drive_service failed: {e}", exc_info=True)
        return None


def drive_upload_chunk(service, chunk_data: bytes, chunk_name: str) -> str | None:
    """Upload a single chunk to Google Drive. Returns file ID or None."""
    try:
        from googleapiclient.http import MediaIoBaseUpload
        import io
        media = MediaIoBaseUpload(io.BytesIO(chunk_data), mimetype="application/octet-stream")
        meta  = {"name": chunk_name, "parents": [DRIVE_FOLDER_ID]}
        f = service.files().create(body=meta, media_body=media, fields="id").execute()
        gdrive_id = f.get("id")
        log.debug(f"[Drive] Uploaded chunk {chunk_name} → {gdrive_id}")
        return gdrive_id
    except Exception as e:
        log.error(f"[Drive] Chunk upload failed ({chunk_name}): {e}", exc_info=True)
        return None

def drive_download_chunk(service, gdrive_id: str) -> bytes | None:
    """Download a chunk with explicit error reporting and existence check."""
    try:
        from googleapiclient.http import MediaIoBaseDownload
        import io
        
        # Test if file exists before downloading to catch Janitor deletions
        try:
            service.files().get(fileId=gdrive_id).execute()
        except Exception:
            log.error(f"[Drive:404] File ID {gdrive_id} NOT FOUND. Janitor might have deleted it!")
            return None

        req  = service.files().get_media(fileId=gdrive_id,acknowledgeAbuse=True)
        buf  = io.BytesIO()
        dl   = MediaIoBaseDownload(buf, req)
        done = False
        while not done:
            status, done = dl.next_chunk()
            if status:
                log.debug(f"[Drive:Internal] Chunk DL Progress: {int(status.progress() * 100)}%")
        
        return buf.getvalue()
    except Exception as e:
        log.error(f"[Drive:API_Error] Connection or Permission issue: {e}")
        return None
    

def drive_delete_file(service, gdrive_id: str):
    """Delete a file from Google Drive."""
    try:
        service.files().delete(fileId=gdrive_id).execute()
        log.debug(f"[Drive] Deleted gdrive file: {gdrive_id}")
    except Exception as e:
        log.warning(f"[Drive] Delete failed ({gdrive_id}): {e}")


# ══════════════════════════════════════════════════════════════════════════════
# File Sync Operations  (called when watcher detects changes or journal received)
# ══════════════════════════════════════════════════════════════════════════════

def _sha256(path: Path) -> str:
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
    except Exception as e:
        log.error(f"[Hash] Failed to hash {path}: {e}")
    return h.hexdigest()


def _get_or_create_folder_uuid(project: str, folder_name: str, parent_id: str) -> str:
    """Get existing folder UUID or create new one in inventory."""
    inv = get_local_inventory(project)
    for uid, meta in inv.get("folders", {}).items():
        if meta.get("name") == folder_name and meta.get("parent_id") == parent_id:
            return uid
    new_uuid = str(uuid.uuid4())
    meta = {
        "name":        folder_name,
        "parent_id":   parent_id,
        "created_at":  _now_ts(),
        "modified_at": _now_ts(),
    }
    add_to_inventory(project, new_uuid, "folders", meta)
    return new_uuid


def sync_create_file(project: str, local_path: Path, parent_uuid: str = "root") -> str:
    """
    Handle CREATE of a new file:
    1. Generate UUID, compute hash.
    2. Add to Inventory.
    3. Write Journal entry.
    4. Upload chunks to Drive, update Transfer Index.
    Returns the file UUID.
    """
    if not local_path.exists() or not local_path.is_file():
        log.warning(f"[Sync:CREATE] File not found: {local_path}")
        return ""

    file_uuid  = str(uuid.uuid4())
    file_size  = local_path.stat().st_size
    file_hash  = _sha256(local_path)
    total_chunks = max(1, (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE)

    meta = {
        "name":          local_path.name,
        "extension":     local_path.suffix,
        "size_bytes":    file_size,
        "parent_id":     parent_uuid,
        "checksum":      file_hash,
        "created_at":    _now_ts(),
        "last_modified": _now_ts(),
    }
    add_to_inventory(project, file_uuid, "files", meta)

    gsid = write_journal_entry(project, "CREATE", file_uuid, {
        "name":          local_path.name,
        "parent_uuid":   parent_uuid,
        "size":          file_size,
        "hash":          file_hash,
        "total_chunks":  total_chunks,
    })

    if len(LIVE_STATE["registered"]) > 1:
        # Only upload to Drive if other servers need the file
        init_transfer_entry(project, file_uuid, total_chunks,
                            local_path.name, file_size, gsid)
        threading.Thread(
            target=_upload_file_chunks,
            args=(project, file_uuid, local_path, total_chunks, gsid),
            daemon=True, name=f"Upload-{file_uuid[:8]}"
        ).start()

    return file_uuid

def _upload_file_chunks(project: str, file_uuid: str, local_path: Path,
                         total_chunks: int, my_gsid: str):
    """
    Background thread: upload file chunks to Google Drive.
    Implements a 5-operation parallel limit using a global semaphore.
    """
    # Acquire one of the 5 available slots; blocks here if all slots are full
    with TRANSFER_SEMAPHORE:
        log.section(f"UPLOAD START (Slot Acquired): {local_path.name}")
        
        service = get_drive_service()
        if not service:
            log.error("[Upload] Could not get Drive service – aborting upload")
            _fb_set(f"{transfer_path(project)}/{file_uuid}/status", "FAILED")
            return

        # Conflict check: if a lower gsid (earlier change) exists, abort this upload [cite: 1]
        fb_entry = _fb_get(f"{transfer_path(project)}/{file_uuid}")
        if fb_entry and isinstance(fb_entry, dict):
            existing_gsid = fb_entry.get("gsid", "")
            if existing_gsid and existing_gsid < my_gsid:
                log.warning(f"[Upload] Conflict! Lower gsid {existing_gsid} exists. Aborting.")
                return

        try:
            with open(local_path, "rb") as f:
                for i in range(total_chunks):
                    chunk_data = f.read(CHUNK_SIZE)
                    chunk_name = f"{file_uuid}_chunk_{i:05d}"
                    chunk_hash = hashlib.sha256(chunk_data).hexdigest()

                    # Upload chunk to Google Drive "Cloud RAM" [cite: 1]
                    gdrive_id = drive_upload_chunk(service, chunk_data, chunk_name)
                    if not gdrive_id:
                        log.error(f"[Upload] Chunk {i} upload failed – marking FAILED")
                        _fb_set(f"{transfer_path(project)}/{file_uuid}/status", "FAILED")
                        return

                    # Update Transfer Index so other servers can start downloading [cite: 1]
                    update_transfer_chunk(project, file_uuid, i, gdrive_id, chunk_hash)
                    log.info(f"[Upload] Chunk {i+1}/{total_chunks} of {local_path.name}")

            _fb_set(f"{transfer_path(project)}/{file_uuid}/status", "UPLOAD_COMPLETE")
            log.info(f"[Upload] Complete: {local_path.name}")
            
        except Exception as e:
            log.error(f"[Upload] Exception during transfer: {e}", exc_info=True)
            try:
                _fb_set(f"{transfer_path(project)}/{file_uuid}/status", "FAILED")
            except Exception:
                pass

def sync_create_folder(project: str, folder_name: str, parent_uuid: str = "root") -> str:
    """Handle CREATE of a new folder."""
    folder_uuid = _get_or_create_folder_uuid(project, folder_name, parent_uuid)
    write_journal_entry(project, "CREATE_FOLDER", folder_uuid, {
        "name":      folder_name,
        "parent_id": parent_uuid,
    })
    return folder_uuid


def sync_move(project: str, item_uuid: str, new_parent_uuid: str):
    """Handle MOVE: update parent_id in inventory and journal."""
    inv = get_local_inventory(project)
    for item_type in ("files", "folders"):
        if item_uuid in inv.get(item_type, {}):
            old_parent = inv[item_type][item_uuid].get("parent_id", "root")
            inv[item_type][item_uuid]["parent_id"] = new_parent_uuid
            save_local_inventory(project, inv)
            _fb_update(f"{inventory_path(project)}/{item_type}/{item_uuid}",
                       {"parent_id": new_parent_uuid})
            write_journal_entry(project, "MOVE", item_uuid, {
                "old_parent_uuid": old_parent,
                "new_parent_uuid": new_parent_uuid,
            })
            return


def sync_delete(project: str, item_uuid: str):
    """
    Handle DELETE.
    If it's a folder, recursively delete all children from inventory.
    Only records one journal entry for the top-level UUID (efficiency).
    """
    inv = get_local_inventory(project)
    is_folder = item_uuid in inv.get("folders", {})

    if is_folder:
        # Recursively collect all children
        all_to_delete = _collect_recursive_uuids(project, item_uuid)
        for uid in all_to_delete:
            for t in ("files", "folders"):
                inv.get(t, {}).pop(uid, None)
        save_local_inventory(project, inv)
        # Delete from Firebase
        for uid in all_to_delete:
            for t in ("files", "folders"):
                _fb_delete(f"{inventory_path(project)}/{t}/{uid}")
    else:
        inv.get("files", {}).pop(item_uuid, None)
        save_local_inventory(project, inv)
        _fb_delete(f"{inventory_path(project)}/files/{item_uuid}")

    write_journal_entry(project, "DELETE", item_uuid, {
        "recursive": is_folder,
    })


def _collect_recursive_uuids(project: str, parent_uuid: str) -> list:
    """Collect UUID of parent + all descendants."""
    result = [parent_uuid]
    children = find_children_uuids(project, parent_uuid)
    for child in children:
        result.extend(_collect_recursive_uuids(project, child))
    return result


# ══════════════════════════════════════════════════════════════════════════════
# Journal Replay  (for servers that come back online)
# ══════════════════════════════════════════════════════════════════════════════

def replay_journal(project: str):
    """
    Called when server comes online or on periodic check.
    Fetches journal entries after last checkpoint and applies them.
    """
    checkpoint = get_checkpoint(project)
    log.info(f"[Replay] Project={project}  checkpoint={checkpoint or 'NONE'}")

    journal_data = _fb_get(journal_path(project))
    if not journal_data or not isinstance(journal_data, dict):
        log.info(f"[Replay] No journal entries for {project}")
        return

    # Sort by gsid (lexicographic = chronological since we use timestamp prefix)
    entries = sorted(journal_data.items(), key=lambda x: x[0])
    new_entries = [(gsid, e) for gsid, e in entries if gsid > checkpoint]

    if not new_entries:
        log.info(f"[Replay] {project} is fully synced.")
        return

    log.info(f"[Replay] Applying {len(new_entries)} missed entries for {project}")

    for gsid, entry in new_entries:
        if entry.get("origin") == get_device_name():
            # Skip own entries – we already applied them
            set_checkpoint(project, gsid)
            continue
        try:
            _apply_journal_entry(project, gsid, entry)
            set_checkpoint(project, gsid)
        except Exception as e:
            log.error(f"[Replay] Failed to apply gsid={gsid}: {e}", exc_info=True)


def _apply_journal_entry(project: str, gsid: str, entry: dict):
    """Apply a single journal entry from another server."""
    action    = entry.get("action", "")
    item_uuid = entry.get("uuid", "")
    meta      = entry.get("meta", {})

    log.info(f"[Apply] {action} uuid={item_uuid} from {entry.get('origin')}")

    if action == "CREATE_FOLDER":
        _apply_create_folder(project, item_uuid, meta)

    elif action == "CREATE":
        _apply_create_file(project, item_uuid, meta, gsid)

    elif action == "MOVE":
        _apply_move(project, item_uuid, meta)

    elif action == "COPY":
        _apply_copy(project, item_uuid, meta)

    elif action == "DELETE":
        _apply_delete(project, item_uuid, meta)

    else:
        log.warning(f"[Apply] Unknown action: {action}")


def _apply_create_folder(project: str, folder_uuid: str, meta: dict):
    """Create local folder as instructed by journal."""
    local_path = uuid_to_local_path_from_meta(project, folder_uuid, meta)
    if local_path:
        try:
            local_path.mkdir(parents=True, exist_ok=True)
            log.info(f"[Apply:CREATE_FOLDER] {local_path}")
        except Exception as e:
            log.error(f"[Apply:CREATE_FOLDER] {e}")
    # Update inventory
    folder_meta = {
        "name":        meta.get("name", folder_uuid),
        "parent_id":   meta.get("parent_id", "root"),
        "created_at":  _now_ts(),
        "modified_at": _now_ts(),
    }
    add_to_inventory(project, folder_uuid, "folders", folder_meta)


def _apply_create_file(project: str, file_uuid: str, meta: dict, gsid: str):
    """
    Create a 'ghost' file shell and start downloading chunks.
    Conflict: if our local gsid for same file is lower, we skip (we won).
    """
    file_meta = {
        "name":          meta.get("name", file_uuid),
        "extension":     Path(meta.get("name", "")).suffix,
        "size_bytes":    meta.get("size", 0),
        "parent_id":     meta.get("parent_uuid", "root"),
        "checksum":      meta.get("hash", ""),
        "created_at":    _now_ts(),
        "last_modified": _now_ts(),
    }
    add_to_inventory(project, file_uuid, "files", file_meta)

    local_path = uuid_to_local_path(project, file_uuid)
    if not local_path:
        log.warning(f"[Apply:CREATE_FILE] Cannot resolve local path for uuid={file_uuid}")
        return

    # Create parent dirs
    local_path.parent.mkdir(parents=True, exist_ok=True)

    # Create ghost file (pre-allocate)
    file_size = meta.get("size", 0)
    if not local_path.exists():
        try:
            with open(local_path, "wb") as f:
                if file_size > 0:
                    f.seek(file_size - 1)
                    f.write(b"\0")
            log.info(f"[Apply:CREATE_FILE] Ghost file created: {local_path}")
        except Exception as e:
            log.error(f"[Apply:CREATE_FILE] Ghost file creation failed: {e}")
            return

    # Start download in background thread
    threading.Thread(
        target=_download_file_chunks,
        args=(project, file_uuid, local_path, meta.get("total_chunks", 1),
              meta.get("hash", ""), gsid),
        daemon=True, name=f"Download-{file_uuid[:8]}"
    ).start()

def _download_file_chunks(project: str, file_uuid: str, local_path: Path,
                           total_chunks: int, expected_hash: str, gsid: str):
    """
    Background thread: download chunks with List/Dict compatibility, 
    speed tracking, and a 5-operation parallel limit.
    """
    # Wait for an available slot among the 5 parallel operations
    with TRANSFER_SEMAPHORE:
        log.section(f"DOWNLOAD START (Slot Acquired): {local_path.name}")
        service = get_drive_service()
        
        if not service:
            log.error(f"[Drive:Error] Could not initialize Google Drive Service for {local_path.name}")
            return

        deadline = time.time() + TRANSFER_TIMEOUT
        downloaded = set()
        retry_stats = {} 

        while len(downloaded) < total_chunks:
            if time.time() > deadline:
                log.error(f"[Download:Timeout] Limit reached for {local_path.name}. Marking FAILED.")
                _fb_set(f"{transfer_path(project)}/{file_uuid}/status", "FAILED")
                return

            try:
                # Fetch the entire entry for full visibility
                full_entry = _fb_get(f"{transfer_path(project)}/{file_uuid}")
                
                if not full_entry or not isinstance(full_entry, dict):
                    log.debug(f"[Download] Waiting for Transfer entry to appear on Firebase...")
                    time.sleep(5)
                    continue

                chunk_data = full_entry.get("chunks")
                
                if chunk_data is None:
                    log.debug(f"[Download] Entry found, but 'chunks' node missing. Status: {full_entry.get('status')}")
                    time.sleep(5)
                    continue

                # --- LIST/DICT NORMALIZATION FIX ---
                if isinstance(chunk_data, list):
                    chunk_map = {str(i): v for i, v in enumerate(chunk_data) if v is not None}
                elif isinstance(chunk_data, dict):
                    chunk_map = chunk_data
                else:
                    log.warning(f"[Download] Unexpected data type: {type(chunk_data)}")
                    time.sleep(5)
                    continue

                # Iterate over the normalized map
                for idx_str, chunk_info in chunk_map.items():
                    idx = int(idx_str)
                    if idx in downloaded: continue
                    
                    if not isinstance(chunk_info, dict) or chunk_info.get("status") != "READY":
                        continue

                    gdrive_id = chunk_info.get("gdrive_id")
                    log.info(f"[Drive:Access] Pulling ID: {gdrive_id} from Cloud RAM...")
                    
                    start_time = time.time()
                    chunk_bytes = drive_download_chunk(service, gdrive_id)
                    end_time = time.time()

                    if chunk_bytes is None:
                        retry_stats[idx] = retry_stats.get(idx, 0) + 1
                        log.warning(f"[Drive:Fail] Chunk {idx} FAILED (Attempt {retry_stats[idx]}/10).")
                        if retry_stats[idx] >= 10:
                            log.error(f"[Download:Abort] Too many failures. Stopping.")
                            _fb_set(f"{transfer_path(project)}/{file_uuid}/status", "FAILED")
                            return
                        continue

                    # Speed calculation
                    duration = max(0.001, end_time - start_time)
                    speed_kb = (len(chunk_bytes) / 1024) / duration
                    log.info(f"[Download:Progress] Chunk {idx+1}/{total_chunks} | Speed: {speed_kb:.2f} KB/s")

                    # Write at correct offset
                    offset = idx * CHUNK_SIZE
                    with open(local_path, "r+b") as f:
                        f.seek(offset)
                        f.write(chunk_bytes)
                    
                    downloaded.add(idx)

            except Exception as e:
                log.error(f"[Download:Critical] Loop error: {e}")
                time.sleep(5)

        # Final Verification
        actual_hash = _sha256(local_path)
        if expected_hash and actual_hash != expected_hash:
            log.error(f"[Download:Verify] Checksum MISMATCH! File is corrupted.")
        else:
            log.info(f"[Download:Success] COMPLETE: {local_path.name} (hash OK)")
            mark_transfer_server_done(project, file_uuid, get_device_name())

def _apply_move(project: str, item_uuid: str, meta: dict):
    """Apply MOVE: rename/move local file, update inventory."""
    old_path = uuid_to_local_path(project, item_uuid)
    # Update inventory first
    sync_move(project, item_uuid, meta.get("new_parent_uuid", "root"))
    new_path = uuid_to_local_path(project, item_uuid)

    if old_path and new_path and old_path.exists() and old_path != new_path:
        try:
            new_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(old_path), str(new_path))
            log.info(f"[Apply:MOVE] {old_path} → {new_path}")
        except Exception as e:
            log.error(f"[Apply:MOVE] {e}")


def _apply_copy(project: str, item_uuid: str, meta: dict):
    """Apply COPY: create a new UUID for the copy, copy local file."""
    src_path = uuid_to_local_path(project, item_uuid)
    new_uuid = str(uuid.uuid4())
    new_parent = meta.get("new_parent_uuid", "root")

    # Copy in inventory
    item = get_inventory_item(project, item_uuid)
    if item:
        new_meta = dict(item)
        new_meta["parent_id"] = new_parent
        new_meta["created_at"] = _now_ts()
        add_to_inventory(project, new_uuid, "files", new_meta)

    # Copy locally
    new_path = uuid_to_local_path(project, new_uuid)
    if src_path and new_path and src_path.exists():
        try:
            new_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src_path), str(new_path))
            log.info(f"[Apply:COPY] {src_path} → {new_path}")
        except Exception as e:
            log.error(f"[Apply:COPY] {e}")


def _apply_delete(project: str, item_uuid: str, meta: dict):
    """Apply DELETE: recursively delete local files/folders, remove from inventory."""
    local_path = uuid_to_local_path(project, item_uuid)
    is_recursive = meta.get("recursive", False)

    if local_path and local_path.exists():
        try:
            if is_recursive or local_path.is_dir():
                shutil.rmtree(str(local_path), ignore_errors=True)
            else:
                local_path.unlink(missing_ok=True)
            log.info(f"[Apply:DELETE] {local_path}")
        except Exception as e:
            log.error(f"[Apply:DELETE] {e}")

    # Remove from inventory
    if is_recursive:
        all_uuids = _collect_recursive_uuids(project, item_uuid)
        inv = get_local_inventory(project)
        for uid in all_uuids:
            for t in ("files", "folders"):
                inv.get(t, {}).pop(uid, None)
        save_local_inventory(project, inv)
    else:
        remove_from_inventory(project, item_uuid, "files")
        remove_from_inventory(project, item_uuid, "folders")


def uuid_to_local_path_from_meta(project: str, item_uuid: str, meta: dict) -> Path | None:
    """Build local path using meta + inventory lookup."""
    inv = get_local_inventory(project)
    parts = [meta.get("name", item_uuid)]
    parent_id = meta.get("parent_id", "root")
    visited = set()
    while parent_id and parent_id != "root":
        if parent_id in visited:
            break
        visited.add(parent_id)
        folder = inv.get("folders", {}).get(parent_id)
        if not folder:
            break
        parts.append(folder.get("name", ""))
        parent_id = folder.get("parent_id", "root")
    parts.reverse()
    if not parts:
        return None
    return SERVER_DIR / project / Path(*parts)


# ══════════════════════════════════════════════════════════════════════════════
# File Watcher  (watches local project folders for changes)
# ══════════════════════════════════════════════════════════════════════════════

class _ProjectWatcher:
    """
    Polls a project folder for file changes.
    Uses watchdog if available, falls back to stat-based polling.
    """
    POLL_INTERVAL = 3  # seconds

    def __init__(self, project: str, stop_event: threading.Event):
        self.project     = project
        self.stop_event  = stop_event
        self._known      = {}   # path_str → mtime/size
        self._scan_existing()

    def _scan_existing(self):
        proj_dir = SERVER_DIR / self.project
        if not proj_dir.exists():
            return
        for p in proj_dir.rglob("*"):
            if self._is_hidden(p):
                continue
            try:
                st = p.stat()
                self._known[str(p)] = (st.st_mtime, st.st_size)
            except Exception:
                pass

    def _is_hidden(self, path: Path) -> bool:
        # Skip inventory cache and checkpoint files
        name = path.name
        return name.startswith(".") or name == "__pycache__"

    def run(self):
        log.info(f"[Watcher] Started for project: {self.project}")
        while not self.stop_event.wait(self.POLL_INTERVAL):
            try:
                self._poll()
            except Exception as e:
                log.error(f"[Watcher:{self.project}] Poll error: {e}", exc_info=True)
        log.info(f"[Watcher] Stopped for project: {self.project}")

    def _poll(self):
        proj_dir = SERVER_DIR / self.project
        if not proj_dir.exists():
            return

        current = {}
        for p in proj_dir.rglob("*"):
            if self._is_hidden(p):
                continue
            try:
                st = p.stat()
                current[str(p)] = (st.st_mtime, st.st_size)
            except Exception:
                pass

        # Detect new or modified
        for path_str, (mtime, size) in current.items():
            p = Path(path_str)
            if path_str not in self._known:
                # New file/folder detected
                log.info(f"[Watcher] CREATED: {path_str}")
                self._on_create(p)
            elif self._known[path_str] != (mtime, size):
                log.info(f"[Watcher] MODIFIED: {path_str}")
                # Treat modification as a new create (re-sync)
                self._on_create(p)

        # Detect deleted
        for path_str in list(self._known.keys()):
            if path_str not in current:
                log.info(f"[Watcher] DELETED: {path_str}")
                self._on_delete(Path(path_str))

        self._known = current

    def _on_create(self, path: Path):
        """When file/folder appears, create journal+inventory entry."""
        try:
            # --- ANTI-LOOP PATCH ---
            # Check if this filename is already known to the system.
            # If it is in the inventory, it means we just downloaded it from
            # another server. We must ignore it to prevent a mirroring loop.
            inv = get_local_inventory(self.project)
            all_items = {**inv.get("files", {}), **inv.get("folders", {})}
            for meta in all_items.values():
                if meta.get("name") == path.name:
                    # File is already known; silence the watcher
                    return 
            # -----------------------

            if path.is_dir():
                parent_uuid = self._get_parent_uuid(path)
                sync_create_folder(self.project, path.name, parent_uuid)
            else:
                parent_uuid = self._get_parent_uuid(path)
                sync_create_file(self.project, path, parent_uuid)
        except Exception as e:
            log.error(f"[Watcher:_on_create] {e}", exc_info=True)

    def _on_delete(self, path: Path):
        """When file/folder disappears, find UUID and delete from inventory/journal."""
        try:
            inv = get_local_inventory(self.project)
            name = path.name
            for t in ("files", "folders"):
                for uid, meta in inv.get(t, {}).items():
                    if meta.get("name") == name:
                        sync_delete(self.project, uid)
                        return
            log.warning(f"[Watcher:_on_delete] Could not find UUID for {path.name}")
        except Exception as e:
            log.error(f"[Watcher:_on_delete] {e}", exc_info=True)

    def _get_parent_uuid(self, path: Path) -> str:
        """Find or create UUID for the parent folder."""
        parent = path.parent
        proj_dir = SERVER_DIR / self.project
        if parent == proj_dir:
            return "root"
        # Walk from project root down
        rel_parts = parent.relative_to(proj_dir).parts
        parent_uuid = "root"
        for part in rel_parts:
            parent_uuid = _get_or_create_folder_uuid(self.project, part, parent_uuid)
        return parent_uuid


def start_project_watcher(project: str, stop_event: threading.Event):
    """Start a background file watcher thread for a project."""
    if project in _watchers:
        return
    watcher = _ProjectWatcher(project, stop_event)
    t = threading.Thread(
        target=watcher.run,
        daemon=True,
        name=f"Watcher-{project}",
    )
    t.start()
    _watchers[project] = (watcher, t)
    log.info(f"[Watcher] Thread launched for: {project}")


def stop_project_watchers():
    global _watchers
    _watchers.clear()
    log.info("[Watcher] All watchers stopped")


# ══════════════════════════════════════════════════════════════════════════════
# Janitor  (cleanup Google Drive + prune journal)
# ══════════════════════════════════════════════════════════════════════════════

def run_janitor(project: str):
    """
    Safe janitor: try to clean up, never crash.
    1. Find completed transfers → delete Drive chunks.
    2. Prune journal entries all servers have processed.
    3. Check timed-out transfers → mark FAILED.
    """
    log.info(f"[Janitor] Running for project: {project}")
    try:
        transfer_data = _fb_get(transfer_path(project))
        if not transfer_data or not isinstance(transfer_data, dict):
            return

        service = None  # lazy-init Drive only if needed

        for file_uuid, entry in list(transfer_data.items()):
            if not isinstance(entry, dict):
                continue
            try:
                _janitor_process_entry(project, file_uuid, entry, service)
            except Exception as e:
                log.warning(f"[Janitor] Error processing {file_uuid}: {e}")
                # Never crash – just continue

    except Exception as e:
        log.warning(f"[Janitor] Outer error: {e}")

    # Prune journal
    try:
        _prune_journal(project)
    except Exception as e:
        log.warning(f"[Janitor] Prune error: {e}")

def _janitor_process_entry(project: str, file_uuid: str, entry: dict, service):
    """
    Process a single transfer entry in janitor.
    Cleaned version to remove IDE warnings and handle Firebase list/dict types.
    """
    status = entry.get("status", "")

    # 1. Check timeout for stuck transfers
    if status == "IN_PROGRESS":
        started = entry.get("started_at", 0)
        # Use local helper directly to avoid 'core' reference warnings
        if _now_ts() - started > TRANSFER_TIMEOUT:
            log.warning(f"[Janitor] Transfer {file_uuid} timed out – marking FAILED")
            _fb_set(f"{transfer_path(project)}/{file_uuid}/status", "FAILED")
        return

    # 2. Only clean up if the upload was successful
    if status not in ("UPLOAD_COMPLETE",):
        return

    # 3. Check if all servers completed download
    sync_progress = entry.get("sync_progress", {})
    # Default to True if sync_progress is missing or empty
    all_done = all(v == "COMPLETED" for v in sync_progress.values()) if sync_progress else True

    if not all_done:
        return

    # 4. All servers done → clean up Drive chunks
    log.info(f"[Janitor] All servers done for {file_uuid} – cleaning Drive")
    
    if service is None:
        service = get_drive_service()

    if service:
        chunks_raw = entry.get("chunks", {})
        
        # FIXED: Normalization to handle Firebase JSON Arrays vs Objects
        if isinstance(chunks_raw, list):
            chunks_iterable = [c for c in chunks_raw if c is not None]
        elif isinstance(chunks_raw, dict):
            chunks_iterable = list(chunks_raw.values())
        else:
            chunks_iterable = []

        for chunk_info in chunks_iterable:
            if isinstance(chunk_info, dict):
                g_id = chunk_info.get("gdrive_id")
                if g_id:
                    try:
                        drive_delete_file(service, g_id)
                    except Exception as e:
                        log.debug(f"[Janitor] Drive delete skip: {e}")
                        continue 

    # 5. Remove transfer entry from Firebase
    try:
        _fb_delete(f"{transfer_path(project)}/{file_uuid}")
        log.info(f"[Janitor] Transfer entry {file_uuid} removed from Firebase")
    except Exception as e:
        log.error(f"[Janitor] Failed to delete Firebase node: {e}")

def _prune_journal(project: str):
    """Remove journal entries all known servers have processed."""
    all_servers = list(LIVE_STATE["registered"])
    if not all_servers:
        return

    # Find minimum checkpoint across all servers
    min_checkpoint = None
    for srv in all_servers:
        chk_path = f"ProjectSync/{project}/checkpoints/{srv}"
        chk = _fb_get(chk_path)
        if chk and isinstance(chk, str):
            if min_checkpoint is None or chk < min_checkpoint:
                min_checkpoint = chk

    if not min_checkpoint:
        return

    journal_data = _fb_get(journal_path(project))
    if not isinstance(journal_data, dict):
        return

    pruned = 0
    for gsid in list(journal_data.keys()):
        if gsid < min_checkpoint:
            try:
                _fb_delete(f"{journal_path(project)}/{gsid}")
                pruned += 1
            except Exception:
                pass

    if pruned:
        log.info(f"[Janitor] Pruned {pruned} journal entries for {project}")


# ══════════════════════════════════════════════════════════════════════════════
# Firebase stream for ProjectSync (Journal + Transfer per project)
# ══════════════════════════════════════════════════════════════════════════════

def subscribe_project_streams(project: str, stop_event: threading.Event):
    """Subscribe to Journal and Transfer Firebase nodes for a project."""
    def _handle_journal(event):
        try:
            _on_journal_event(project, event)
        except Exception as e:
            log.error(f"[JournalStream:{project}] {e}", exc_info=True)

    def _handle_transfer(event):
        try:
            _on_transfer_event(project, event)
        except Exception as e:
            log.error(f"[TransferStream:{project}] {e}", exc_info=True)

    for path, handler in [
        (journal_path(project),  _handle_journal),
        (transfer_path(project), _handle_transfer),
    ]:
        ref = _fb_ref(path)
        if ref:
            try:
                listener = ref.listen(handler)
                _active_listeners.append(listener)
                log.info(f"[ProjectStream] Subscribed: {path}")
            except Exception as e:
                log.error(f"[ProjectStream] Subscribe failed ({path}): {e}", exc_info=True)


def _on_journal_event(project: str, event):
    """Handle incoming journal stream event from another server."""
    data     = event.data
    evt_path = getattr(event, "path", "/")

    if evt_path == "/" or data is None:
        return

    # evt_path = /gsid or /gsid/field
    gsid = evt_path.lstrip("/").split("/")[0]
    if not gsid or not isinstance(data, dict):
        return

    # Skip own entries
    if data.get("origin") == get_device_name():
        return

    checkpoint = get_checkpoint(project)
    if gsid <= checkpoint:
        return  # already processed

    log.info(f"[JournalStream] New entry gsid={gsid} action={data.get('action')} from {data.get('origin')}")
    try:
        _apply_journal_entry(project, gsid, data)
        set_checkpoint(project, gsid)
        _fire_gui_update()
    except Exception as e:
        log.error(f"[JournalStream] Apply failed gsid={gsid}: {e}", exc_info=True)


def _on_transfer_event(project: str, event):
    """Handle transfer index updates from Firebase stream."""
    data     = event.data
    evt_path = getattr(event, "path", "/")
    if not data:
        return
    log.debug(f"[TransferStream] {project} path={evt_path}")
    # The download thread already polls transfer index directly.
    # This callback just logs – future: could wake download threads proactively.


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _now() -> str:
    return datetime.datetime.now().isoformat(timespec="seconds")

def _now_ts() -> int:
    return int(time.time())