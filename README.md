# IMD Log Utils

A cross-platform GUI application to:

1. Load and decode CAN log dump files.
2. Authenticate to an HTTP service and download EV charging session data via REST API.
3. Correlate CAN data with session data and generate time-series plots.

## Dokumentacja po polsku

Pelna dokumentacja uzytkownika:

- [docs/instrukcja_uzytkownika_pl.md](docs/instrukcja_uzytkownika_pl.md)

Szybki one-pager z obrazkami:

- [docs/one_pager_wykresy_pl.md](docs/one_pager_wykresy_pl.md)

## Structure

```
imd_log_utils/
├── .venv/           # Python virtual environment
├── src/
│   ├── main.py      # Application entry point
│   ├── ui/
│   │   └── main_window.py  # Qt5 main window and tabs
│   └── modules/
│       ├── can_logs.py     # CAN log parser
│       ├── http_client.py  # HTTP/REST client
│       └── data_analysis.py# Correlation and plotting
├── tests/           # Unit tests
├── requirements.txt # Dependencies
└── README.md
```

## Getting started

1. Activate the virtual environment:
   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run the unit tests (requires `pytest` - install if necessary):
   ```bash
   pip install pytest
   pytest tests
   ```
4. Run the application:
   ```bash
   python -m src.main
   ```

> **Note:** the list of charging stations you add via the `+ Add QP` button is persisted using Qt settings. When you restart the program your previously entered stations will reappear in the dropdown and the last selected station will be re-selected automatically.

## Packaging

We use **PyInstaller** to build a standalone Windows executable.

1. Activate the virtual environment:
   ```powershell
   .\.venv\Scripts\Activate.ps1
   ```
2. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```
3. Build the executable:
   ```powershell
   .\build_exe.ps1
   ```
4. The packaged app will be available in:
   ```text
   dist\IMDLogUtils\IMDLogUtils.exe
   ```

This is currently a `one-folder` build, which is more reliable for PyQt5 and matplotlib than `one-file` during early development.

## Portable build

To prepare a portable package that can be copied to another Windows machine:

```powershell
.\build_portable.ps1
```

This creates:

```text
dist\IMDLogUtils\
dist\IMDLogUtils-portable.zip
```

The portable build stores its local data next to the executable in:

```text
dist\IMDLogUtils\portable_data\
```

That folder contains:

- `settings.ini` with the saved station list and last selected station
- `*.db` SQLite databases created by the log tabs
- `startup_profile.log` when startup profiling is enabled

## Versioning and releases

Application version is kept in:

```text
src\app_info.py
```

To create a new release and automatically bump the version:

```powershell
.\release.ps1
```

Default behavior increases the patch version, for example `0.1.0 -> 0.1.1`.

You can also choose the bump type:

```powershell
.\release.ps1 -Bump minor
.\release.ps1 -Bump major
```

Or set an explicit version:

```powershell
.\release.ps1 -Version 1.0.0
```

The release script:

- updates the application version
- validates the edited files
- builds the portable executable
- creates `dist\IMDLogUtils-portable.zip`
- creates a versioned archive like `dist\IMDLogUtils-0.1.1-portable.zip`

> **Persistent data:**
> - In development mode, app data is stored in `portable_data\` in the project root.
> - In the packaged portable build, app data is stored in `portable_data\` next to `IMDLogUtils.exe`.
> - Data persists between runs; importing a new log simply appends to the database.
> - Use the "Delete from DB" button on the CAN Logs tab to remove records imported from a particular file.

---

Next steps: implement each tab's UI and wiring to modules.
