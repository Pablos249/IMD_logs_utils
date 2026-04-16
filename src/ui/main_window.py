import os
import sys
import time

from PyQt5 import QtWidgets, QtCore
from src.app_paths import settings_path, startup_profile_log_path
from src.app_info import APP_DESCRIPTION, APP_NAME, APP_VERSION, about_html, build_display_version
from src.ui.analysis_tab import AnalysisTab, SinglePlotAnalysisTab
from src.ui.bulk_import_tab import BulkImportTab
from src.ui.ccs_logs_tab import CCSLogsTab
from src.ui.can_logs_tab import CANLogsTab
from src.ui.clc_logs_tab import CLCLogsTab
from src.ui.conditioning_logs_tab import ConditioningLogsTab
from src.ui.log_converter_tab import LogConverterTab
from src.ui.eos_logs_tab import EOSLogsTab


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        self._startup_profile_enabled = os.environ.get("IMD_STARTUP_PROFILE", "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self._startup_started_at = time.perf_counter()
        self._startup_last_checkpoint = self._startup_started_at
        super().__init__()
        self.setWindowTitle(build_display_version())
        self.resize(1200, 700)
        self._startup_checkpoint("QMainWindow base initialized")
        self._busy_depth = 0
        self._create_menu()
        self._startup_checkpoint("Menu created")
        self.status_bar = self.statusBar()
        self.busy_progress = QtWidgets.QProgressBar()
        self.busy_progress.setVisible(False)
        self.busy_progress.setFixedWidth(220)
        self.busy_progress.setTextVisible(False)
        self.busy_progress.setRange(0, 0)
        self.version_label = QtWidgets.QLabel(f"v{APP_VERSION}")
        self.version_label.setStyleSheet("padding-left: 8px; color: #555;")
        self.status_bar.addPermanentWidget(self.version_label)
        self.status_bar.addPermanentWidget(self.busy_progress)
        self._startup_checkpoint("Status bar initialized")
        
        # Store selected charging station globally
        self.selected_station = None
        self.stations = self._load_stations()
        self._startup_checkpoint("Stations loaded")
        
        self._create_ui()
        self._startup_checkpoint("Main UI created")
        
        # Add status bar for user feedback
        self.status_bar.showMessage("Ready")
        self._startup_checkpoint("MainWindow ready")

    def _create_menu(self):
        help_menu = self.menuBar().addMenu("&Pomoc")

        about_action = QtWidgets.QAction("&O aplikacji...", self)
        about_action.triggered.connect(self._show_about_dialog)
        help_menu.addAction(about_action)

        version_action = QtWidgets.QAction("Informacje o &wersji", self)
        version_action.triggered.connect(self._show_version_dialog)
        help_menu.addAction(version_action)

        help_menu.addSeparator()

        portable_action = QtWidgets.QAction("Gdzie sa zapisane dane?", self)
        portable_action.triggered.connect(self._show_data_location_dialog)
        help_menu.addAction(portable_action)

    def _startup_checkpoint(self, label: str):
        if not self._startup_profile_enabled:
            return
        now = time.perf_counter()
        log_line = (
            f"[startup] {label}: +{now - self._startup_last_checkpoint:.3f}s "
            f"(window {now - self._startup_started_at:.3f}s)"
        )
        print(log_line, file=sys.stderr, flush=True)
        self._append_startup_log(log_line)
        self._startup_last_checkpoint = now

    def _append_startup_log(self, message: str):
        log_path = startup_profile_log_path()
        with log_path.open("a", encoding="utf-8") as log_file:
            log_file.write(message + "\n")

    def _settings(self):
        return QtCore.QSettings(str(settings_path()), QtCore.QSettings.IniFormat)

    def _load_stations(self):
        """Load list of charging stations from settings/config"""
        settings = self._settings()
        size = settings.beginReadArray("stations")
        stations = []
        for i in range(size):
            settings.setArrayIndex(i)
            stations.append({
                "id": settings.value("id", type=int),
                "name": settings.value("name", type=str),
            })
        settings.endArray()

        # restore last selected station if stored
        last = settings.value("last_selected", type=int)
        if last is not None:
            self.selected_station = last
        return stations

    def _create_ui(self):
        """Setup main window layout"""
        # Central widget with tabs
        central_widget = QtWidgets.QWidget()
        self.setCentralWidget(central_widget)
        self._startup_checkpoint("Central widget created")
        
        main_layout = QtWidgets.QVBoxLayout(central_widget)
        
        # Top bar: Station selector and Add button
        top_bar = QtWidgets.QHBoxLayout()
        top_bar.addWidget(QtWidgets.QLabel("Charging Station:"))
        
        self.station_combo = QtWidgets.QComboBox()
        self.station_combo.currentIndexChanged.connect(self._on_station_changed)
        self._update_station_combo()
        top_bar.addWidget(self.station_combo)
        
        self.add_station_btn = QtWidgets.QPushButton("+ Add QP")
        self.add_station_btn.clicked.connect(self._add_new_station)
        top_bar.addWidget(self.add_station_btn)

        self.help_btn = QtWidgets.QPushButton("Pomoc")
        self.help_btn.clicked.connect(self._show_about_dialog)
        top_bar.addWidget(self.help_btn)
        
        top_bar.addStretch()
        main_layout.addLayout(top_bar)
        self._startup_checkpoint("Top bar created")
        
        # Tabs
        self.tabs = QtWidgets.QTabWidget()
        
        # CAN Logs tab
        self.tab_can = CANLogsTab(self)
        self._startup_checkpoint("CAN Logs tab created")

        # CLC Logs tab
        self.tab_clc = CLCLogsTab(self)
        self._startup_checkpoint("CLC Logs tab created")

        # Conditioning Logs tab
        self.tab_conditioning = ConditioningLogsTab(self)
        self._startup_checkpoint("Conditioning Logs tab created")

        # CCS Logs tab
        self.tab_ccs = CCSLogsTab(self)
        self._startup_checkpoint("CCS Logs tab created")
        
        # Log converter tab
        self.tab_converter = LogConverterTab(self)
        self._startup_checkpoint("Log converter tab created")

        # EOS Logs tab
        self.tab_eos = EOSLogsTab(self)
        self._startup_checkpoint("EOS Logs tab created")

        # Bulk import tab
        self.tab_bulk_import = BulkImportTab(self)
        self._startup_checkpoint("Bulk import tab created")

        # HTTP Session tab (placeholder)
        self.tab_http = QtWidgets.QWidget()
        http_layout = QtWidgets.QVBoxLayout()
        http_layout.addWidget(QtWidgets.QLabel("HTTP session functionality coming soon"))
        self.tab_http.setLayout(http_layout)
        self._startup_checkpoint("HTTP Session tab created")
        
        # Analysis tab
        self.tab_analysis = AnalysisTab(self)
        self._startup_checkpoint("Visualization 3-plot tab created")
        self.tab_analysis_single = SinglePlotAnalysisTab(self)
        self._startup_checkpoint("Visualization single-plot tab created")

        self.tabs.addTab(self.tab_can, "IMD Logs")
        self.tabs.addTab(self.tab_clc, "CLC Logs")
        self.tabs.addTab(self.tab_conditioning, "Conditioning Logs")
        self.tabs.addTab(self.tab_ccs, "CCS Logs")
        self.tabs.addTab(self.tab_converter, "Konwerter")
        self.tabs.addTab(self.tab_bulk_import, "Wczytaj dane")
        self.tabs.addTab(self.tab_eos, "Wczytaj dane z EOS")
        self.tabs.addTab(self.tab_http, "HTTP Session")
        self.tabs.addTab(self.tab_analysis, "Wizualizacja (3 wykresy)")
        self.tabs.addTab(self.tab_analysis_single, "Wizualizacja")
        
        main_layout.addWidget(self.tabs)
        self._startup_checkpoint("Tabs added to layout")

    def _show_about_dialog(self):
        QtWidgets.QMessageBox.about(
            self,
            f"O aplikacji {APP_NAME}",
            about_html(),
        )

    def _show_version_dialog(self):
        data_path = settings_path().parent
        QtWidgets.QMessageBox.information(
            self,
            "Informacje o wersji",
            (
                f"{APP_NAME}\n"
                f"Wersja: {APP_VERSION}\n\n"
                f"{APP_DESCRIPTION}\n\n"
                f"Katalog danych: {data_path}"
            ),
        )

    def _show_data_location_dialog(self):
        data_path = settings_path().parent
        QtWidgets.QMessageBox.information(
            self,
            "Dane aplikacji",
            (
                "Aplikacja działa w trybie portable.\n\n"
                f"Wszystkie ustawienia i bazy danych są zapisywane tutaj:\n{data_path}"
            ),
        )

    def _update_station_combo(self):
        """Refresh station dropdown"""
        self.station_combo.blockSignals(True)
        self.station_combo.clear()
        
        if not self.stations:
            self.station_combo.addItem("-- Select or Add Station --", None)
        else:
            for station in self.stations:
                self.station_combo.addItem(station["name"], station["id"])
        
        self.station_combo.blockSignals(False)
        # if we restored a selection above, apply it
        if self.selected_station is not None:
            # find index with matching data
            for idx in range(self.station_combo.count()):
                if self.station_combo.itemData(idx) == self.selected_station:
                    self.station_combo.setCurrentIndex(idx)
                    break
        self._on_station_changed()

    def _on_station_changed(self):
        """Update selected station globally"""
        self.selected_station = self.station_combo.currentData()
        # persist selection immediately so that tab changes and restarts remember it
        self._save_stations()

    def _add_new_station(self):
        """Show dialog to add new charging station"""
        self.status_bar.showMessage("Adding new station...")
        dialog = QtWidgets.QDialog(self)
        dialog.setWindowTitle("Add New Charging Station")
        dialog.setModal(True)
        
        layout = QtWidgets.QVBoxLayout()
        
        # Station name/number input
        name_label = QtWidgets.QLabel("Station Number/Serial:")
        name_input = QtWidgets.QLineEdit()
        layout.addWidget(name_label)
        layout.addWidget(name_input)
        
        # Buttons
        btn_layout = QtWidgets.QHBoxLayout()
        ok_btn = QtWidgets.QPushButton("Add")
        cancel_btn = QtWidgets.QPushButton("Cancel")
        
        def on_ok():
            station_name = name_input.text().strip()
            if not station_name:
                QtWidgets.QMessageBox.warning(dialog, "Error", "Station name cannot be empty")
                return
            
            # Check for duplicates
            if any(s["name"] == station_name for s in self.stations):
                QtWidgets.QMessageBox.warning(dialog, "Error", "Station already exists")
                return
            
            # Add new station
            new_station = {
                "id": len(self.stations) + 1,
                "name": station_name
            }
            self.stations.append(new_station)
            self._save_stations()
            self._update_station_combo()
            
            # Select the newly added station
            for i in range(self.station_combo.count()):
                if self.station_combo.itemData(i) == new_station["id"]:
                    self.station_combo.setCurrentIndex(i)
                    break
            
            dialog.accept()
            self.status_bar.showMessage("Station added")
        
        ok_btn.clicked.connect(on_ok)
        cancel_btn.clicked.connect(dialog.reject)
        btn_layout.addWidget(ok_btn)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)
        
        dialog.setLayout(layout)
        dialog.exec_()
        self.status_bar.showMessage("Ready")

    def _save_stations(self):
        """Save stations to persistent storage"""
        settings = self._settings()
        settings.beginWriteArray("stations")
        for i, station in enumerate(self.stations):
            settings.setArrayIndex(i)
            settings.setValue("id", station.get("id"))
            settings.setValue("name", station.get("name"))
        settings.endArray()
        # remember last selection as well
        if self.selected_station is not None:
            settings.setValue("last_selected", self.selected_station)

    def get_selected_station(self):
        """Get currently selected station"""
        return self.selected_station

    def get_station_name(self, station_id):
        """Get station name by ID"""
        for station in self.stations:
            if station["id"] == station_id:
                return station["name"]
        return "Unknown"

    def begin_busy(self, message: str = "Working..."):
        """Show a global indeterminate progress bar in the status bar."""
        self._busy_depth += 1
        self.status_bar.showMessage(message)
        self.busy_progress.setVisible(True)
        self.busy_progress.setRange(0, 0)
        QtWidgets.QApplication.processEvents()

    def end_busy(self, message: str = "Ready"):
        """Hide the global progress bar."""
        self._busy_depth = max(0, self._busy_depth - 1)
        if self._busy_depth == 0:
            self.busy_progress.setVisible(False)
            self.status_bar.showMessage(message)
        QtWidgets.QApplication.processEvents()

