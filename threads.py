"""
threads.py
──────────
Background thread manager for One Click Server – Phase 2.

Phase 2 threads:
  1. Firebase infrastructure stream (Registered/Online/Projects)
  2. Per-project file watchers
  3. Per-project Firebase journal/transfer streams
  4. Journal replay on startup (catch up after being offline)
  5. Periodic Janitor (cleanup Drive + prune journal)
"""

import threading
import debug_logger as log
import core

JANITOR_INTERVAL   = 5 * 60   # run janitor every 5 minutes
REPLAY_INTERVAL    = 60        # re-check journal every 60 s (in case of missed events)


class ServerThreadManager:

    def __init__(self):
        self._stop         = threading.Event()
        self._threads: list[threading.Thread] = []
        self.server_name   = ""
        self._watcher_stop = threading.Event()
        log.debug("ServerThreadManager created")

    def start(self, server_name: str):
        if self.is_running():
            log.warning("Thread manager already running – ignoring start()")
            return

        self.server_name = server_name
        self._stop.clear()
        self._watcher_stop.clear()

        # 1. Firebase infrastructure streams (Registered / Online / Projects)
        try:
            core.start_infrastructure_stream(server_name)
        except Exception as e:
            log.error(f"Infrastructure stream start failed: {e}", exc_info=True)

        # 2. Start per-project streams + watchers for existing projects
        self._start_project_threads()

        # 3. Janitor thread
        t_janitor = threading.Thread(
            name="Janitor",
            target=self._loop_janitor,
            daemon=True,
        )
        t_janitor.start()
        self._threads.append(t_janitor)

        # 4. Project discovery thread
        # (handles projects created AFTER server start via Firebase stream)
        t_proj = threading.Thread(
            name="ProjectDiscovery",
            target=self._loop_project_discovery,
            daemon=True,
        )
        t_proj.start()
        self._threads.append(t_proj)

        log.info(f"Thread manager started for server: {server_name}")

    def stop(self):
        log.section("THREAD MANAGER STOP")
        self._stop.set()
        self._watcher_stop.set()
        core.stop_all_streams()
        core.stop_project_watchers()
        for t in self._threads:
            if t.is_alive():
                t.join(timeout=2)
        self._threads.clear()
        log.info("All background tasks stopped")

    def is_running(self) -> bool:
        return bool(self._threads) and not self._stop.is_set()

    def _start_project_threads(self):
        """Start watcher + stream + journal replay for each known project."""
        for proj_name in list(core.LIVE_STATE["projects"].keys()):
            self._init_project(proj_name)

    def _init_project(self, proj_name: str):
        """Initialise all sync infrastructure for a single project."""
        try:
            # Subscribe to journal + transfer streams
            core.subscribe_project_streams(proj_name, self._stop)

            # Start file watcher
            core.start_project_watcher(proj_name, self._watcher_stop)

            # Replay missed journal entries
            t_replay = threading.Thread(
                name=f"Replay-{proj_name}",
                target=core.replay_journal,
                args=(proj_name,),
                daemon=True,
            )
            t_replay.start()
            self._threads.append(t_replay)

            log.info(f"[ThreadManager] Project init complete: {proj_name}")
        except Exception as e:
            log.error(f"[ThreadManager] Project init failed for {proj_name}: {e}", exc_info=True)

    def _loop_janitor(self):
        """Periodically clean up Drive and prune journal."""
        log.debug("Janitor: started")
        while not self._stop.wait(JANITOR_INTERVAL):
            try:
                for proj_name in list(core.LIVE_STATE["projects"].keys()):
                    core.run_janitor(proj_name)
            except Exception as e:
                log.error(f"[Janitor] Error: {e}", exc_info=True)
        log.debug("Janitor: exiting")

    def _loop_project_discovery(self):
        """
        Watches for new projects added to Firebase after startup
        and initialises their threads.
        """
        log.debug("ProjectDiscovery: started")
        known_projects = set(core.LIVE_STATE["projects"].keys())

        while not self._stop.wait(10):  # check every 10 s
            try:
                current_projects = set(core.LIVE_STATE["projects"].keys())
                new_projects = current_projects - known_projects
                for proj in new_projects:
                    log.info(f"[Discovery] New project detected: {proj}")
                    self._init_project(proj)
                known_projects = current_projects
            except Exception as e:
                log.error(f"[ProjectDiscovery] {e}", exc_info=True)
        log.debug("ProjectDiscovery: exiting")


# Module-level singleton
thread_manager = ServerThreadManager()