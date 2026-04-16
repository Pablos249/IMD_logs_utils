"""Bulk import tab for mixed log folders."""

from __future__ import annotations

import os
from collections import Counter

from PyQt5 import QtWidgets
from PyQt5.QtWidgets import (
    QCheckBox,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QProgressBar,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from src.modules.log_importer import MixedLogImporter


class BulkImportTab(QtWidgets.QWidget):
    """Import mixed logs from a selected directory."""

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.importer = MixedLogImporter(
            can_parser=self.main_window.tab_can.parser,
            clc_parser=self.main_window.tab_clc.parser,
            conditioning_parser=self.main_window.tab_conditioning.parser,
            ccs_parser=self.main_window.tab_ccs.parser,
            eos_parser=self.main_window.tab_eos.parser,
        )
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        info_label = QLabel(
            "Choose a folder with mixed log files. The app will detect the log type "
            "and import each file to the correct tab."
        )
        info_label.setWordWrap(True)
        layout.addWidget(info_label)

        folder_layout = QHBoxLayout()
        folder_layout.addWidget(QLabel("Folder:"))
        self.folder_edit = QLineEdit()
        self.folder_edit.setReadOnly(True)
        folder_layout.addWidget(self.folder_edit)

        self.browse_btn = QPushButton("Choose folder")
        self.browse_btn.clicked.connect(self._choose_folder)
        folder_layout.addWidget(self.browse_btn)
        layout.addLayout(folder_layout)

        options_layout = QHBoxLayout()
        self.recursive_checkbox = QCheckBox("Scan subfolders")
        self.recursive_checkbox.setChecked(True)
        options_layout.addWidget(self.recursive_checkbox)

        self.import_btn = QPushButton("Import folder")
        self.import_btn.clicked.connect(self._import_folder)
        options_layout.addWidget(self.import_btn)
        options_layout.addStretch()
        layout.addLayout(options_layout)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        self.summary_label = QLabel("No import started yet.")
        self.summary_label.setWordWrap(True)
        layout.addWidget(self.summary_label)

        self.results_table = QTableWidget()
        self.results_table.setColumnCount(4)
        self.results_table.setHorizontalHeaderLabels(["File", "Type", "Status", "Details"])
        self.results_table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.results_table)

    def _choose_folder(self):
        folder = QFileDialog.getExistingDirectory(
            self,
            "Choose folder with logs",
            self.folder_edit.text() or ".",
        )
        if folder:
            self.folder_edit.setText(folder)

    def _import_folder(self):
        station_id = self.main_window.get_selected_station()
        if station_id is None:
            QtWidgets.QMessageBox.warning(
                self,
                "No Station Selected",
                "Please select or add a charging station first.",
            )
            return

        folder = self.folder_edit.text().strip()
        if not folder or not os.path.isdir(folder):
            QtWidgets.QMessageBox.warning(self, "No folder selected", "Please choose a valid folder.")
            return

        file_paths = self._collect_file_paths(folder)
        if not file_paths:
            QtWidgets.QMessageBox.information(self, "No files found", "No files were found in the selected folder.")
            return

        self.main_window.begin_busy("Importing mixed logs...")
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.import_btn.setEnabled(False)
        self.browse_btn.setEnabled(False)
        try:
            results = self.importer.import_files(
                file_paths,
                station_id=station_id,
                progress_callback=self._on_import_progress,
            )
            self._populate_results(results)
            self._refresh_affected_tabs(results)
        finally:
            self.progress_bar.setVisible(False)
            self.import_btn.setEnabled(True)
            self.browse_btn.setEnabled(True)
            self.main_window.end_busy("Bulk import finished.")

    def _collect_file_paths(self, folder: str):
        collected = []
        if self.recursive_checkbox.isChecked():
            for root, _, files in os.walk(folder):
                for name in files:
                    collected.append(os.path.join(root, name))
        else:
            for name in os.listdir(folder):
                path = os.path.join(folder, name)
                if os.path.isfile(path):
                    collected.append(path)
        return sorted(collected)

    def _on_import_progress(self, current: int, total: int, file_name: str, log_type: str):
        if total > 0:
            percent = int((current / total) * 100)
            self.progress_bar.setValue(percent)
        pretty_type = log_type.upper() if log_type and log_type != "unknown" else "detecting"
        self.main_window.status_bar.showMessage(
            f"Importing mixed logs... {current}/{total} {file_name} [{pretty_type}]"
        )
        QtWidgets.QApplication.processEvents()

    def _populate_results(self, results):
        self.results_table.setRowCount(len(results))
        status_counter = Counter(result.status for result in results)
        type_counter = Counter(result.log_type for result in results if result.log_type != "unknown")

        for row_index, result in enumerate(results):
            details = result.details or (f"{result.inserted} rows imported" if result.inserted else "")
            values = [
                os.path.basename(result.file_path),
                result.log_type.upper(),
                result.status,
                details,
            ]
            for column, value in enumerate(values):
                self.results_table.setItem(row_index, column, QTableWidgetItem(str(value)))

        self.results_table.resizeColumnsToContents()
        type_summary = ", ".join(f"{log_type.upper()}: {count}" for log_type, count in sorted(type_counter.items()))
        if not type_summary:
            type_summary = "no recognized logs"
        self.summary_label.setText(
            "Processed "
            f"{len(results)} file(s). Imported: {status_counter.get('imported', 0)}, "
            f"skipped: {status_counter.get('skipped', 0)}, "
            f"unknown: {status_counter.get('unknown', 0)}, "
            f"errors: {status_counter.get('error', 0)}. "
            f"Detected types: {type_summary}."
        )

    def _refresh_affected_tabs(self, results):
        imported_types = {result.log_type for result in results if result.status == "imported"}
        if MixedLogImporter.TYPE_CAN in imported_types:
            self.main_window.tab_can.refresh_for_current_station()
        if MixedLogImporter.TYPE_CLC in imported_types:
            self.main_window.tab_clc.refresh_for_current_station()
        if MixedLogImporter.TYPE_CONDITIONING in imported_types:
            self.main_window.tab_conditioning.refresh_for_current_station()
        if MixedLogImporter.TYPE_CCS in imported_types:
            self.main_window.tab_ccs.refresh_for_current_station()
        if MixedLogImporter.TYPE_EOS in imported_types:
            self.main_window.tab_eos.refresh_for_current_station()
