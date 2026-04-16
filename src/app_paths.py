from __future__ import annotations

import sys
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def app_root() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return _project_root()


def data_root() -> Path:
    path = app_root() / "portable_data"
    path.mkdir(parents=True, exist_ok=True)
    return path


def database_path(filename: str) -> str:
    return str(data_root() / filename)


def settings_path() -> Path:
    path = data_root() / "settings.ini"
    if not path.exists():
        path.touch()
    return path


def startup_profile_log_path() -> Path:
    return data_root() / "startup_profile.log"
