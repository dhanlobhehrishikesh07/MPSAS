"""
setup.py
────────
One Click Server – Phase 2 Setup / Dependency Installer.

Runs once on first launch (or whenever a package is missing).
Installs all required packages for Phase 2 including Google Drive API.
"""

import sys
import subprocess
import importlib

REQUIRED_PACKAGES = [
    ("firebase_admin",           "firebase-admin"),
    ("google.oauth2.credentials","google-auth"),
    ("google_auth_oauthlib",     "google-auth-oauthlib"),
    ("google.auth.transport",    "google-auth-httplib2"),
    ("googleapiclient",          "google-api-python-client"),
]


def _is_installed(import_name: str) -> bool:
    try:
        importlib.import_module(import_name)
        return True
    except ImportError:
        return False


def _install(pkg_name: str):
    print(f"  Installing {pkg_name}...")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--quiet", pkg_name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print(f"  ✓ {pkg_name} installed")


def run():
    print("=== One Click Server Phase 2 – Dependency Check ===")
    all_ok = True
    for import_name, pkg_name in REQUIRED_PACKAGES:
        if _is_installed(import_name):
            print(f"  ✓ {pkg_name}")
        else:
            print(f"  ✗ {pkg_name} missing – installing…")
            try:
                _install(pkg_name)
            except Exception as e:
                print(f"  ERROR: Could not install {pkg_name}: {e}")
                all_ok = False

    # Check Python version
    if sys.version_info < (3, 10):
        print(f"  WARNING: Python 3.10+ recommended. Current: {sys.version}")

    print("=== Setup complete ===" if all_ok else "=== Setup complete (some errors) ===")


if __name__ == "__main__":
    run()