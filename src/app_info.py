from __future__ import annotations

from datetime import datetime
from pathlib import Path

APP_NAME = "IMD Log Utils"
APP_VERSION = "0.1.0"
APP_DESCRIPTION = (
    "Narzedzie do importu, przegladania i wizualizacji logow IMD, CLC, "
    "Conditioning, CCS oraz EOS."
)
APP_COMPANY = "IMD"


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def build_display_version() -> str:
    return f"{APP_NAME} v{APP_VERSION}"


def about_html() -> str:
    year = datetime.now().year
    return (
        f"<h3>{APP_NAME}</h3>"
        f"<p><b>Wersja:</b> {APP_VERSION}</p>"
        f"<p>{APP_DESCRIPTION}</p>"
        "<p><b>Funkcje:</b> import logow, filtrowanie danych, "
        "kasowanie importow z bazy oraz wizualizacja serii czasowych.</p>"
        "<p><b>Tryb danych:</b> portable, dane i ustawienia sa zapisywane obok aplikacji.</p>"
        f"<p><b>Copyright:</b> {year} {APP_COMPANY}</p>"
    )
