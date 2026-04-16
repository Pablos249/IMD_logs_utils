import os
import sys
import time
from pathlib import Path
from PyQt5 import QtCore, QtWidgets

# Allow both `python -m src.main` and `python src/main.py`.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.app_paths import settings_path, startup_profile_log_path
from src.app_info import APP_NAME, APP_VERSION
from src.ui.main_window import MainWindow


def _startup_profile_enabled() -> bool:
    return os.environ.get("IMD_STARTUP_PROFILE", "").strip().lower() in {"1", "true", "yes", "on"}


def _startup_profile_log_path() -> Path:
    return startup_profile_log_path()


def _append_profile_log(message: str):
    with _startup_profile_log_path().open("a", encoding="utf-8") as log_file:
        log_file.write(message + "\n")


def _profile_log(enabled: bool, message: str, start_time: float, last_time: float) -> float:
    if enabled:
        now = time.perf_counter()
        log_line = f"[startup] {message}: +{now - last_time:.3f}s (total {now - start_time:.3f}s)"
        print(log_line, file=sys.stderr, flush=True)
        _append_profile_log(log_line)
        return now
    return last_time


def main():
    startup_profile = _startup_profile_enabled()
    start_time = time.perf_counter()
    last_time = start_time

    if startup_profile:
        _startup_profile_log_path().write_text("", encoding="utf-8")

    QtCore.QSettings.setDefaultFormat(QtCore.QSettings.IniFormat)
    QtCore.QSettings.setPath(
        QtCore.QSettings.IniFormat,
        QtCore.QSettings.UserScope,
        str(settings_path().parent),
    )

    app = QtWidgets.QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    app.setApplicationVersion(APP_VERSION)
    last_time = _profile_log(startup_profile, "QApplication created", start_time, last_time)

    window = MainWindow()
    last_time = _profile_log(startup_profile, "MainWindow created", start_time, last_time)

    window.show()
    last_time = _profile_log(startup_profile, "MainWindow shown", start_time, last_time)

    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
