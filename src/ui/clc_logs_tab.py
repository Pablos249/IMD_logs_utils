"""UI for CLC logs module."""

import os

from PyQt5 import QtCore, QtWidgets
from PyQt5.QtWidgets import (
    QComboBox,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from src.modules.clc_logs import CLCLogParser


class CLCLogsTab(QtWidgets.QWidget):
    ROWS_PER_PAGE = 100

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.parser = CLCLogParser()
        self.current_page = 0
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        file_layout = QHBoxLayout()
        self.file_label = QLabel("No file loaded")
        self.load_btn = QPushButton("Load CLC Logs")
        self.load_btn.clicked.connect(self._load_file)
        file_layout.addWidget(QLabel("CLC Log File:"))
        file_layout.addWidget(self.file_label)
        file_layout.addWidget(self.load_btn)
        layout.addLayout(file_layout)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        ctrl_layout = QHBoxLayout()
        ctrl_layout.addWidget(QLabel("Loaded files:"))
        self.file_combo = QComboBox()
        self.file_combo.addItem("-- all files --", None)
        self.file_combo.currentIndexChanged.connect(self._on_filter_changed)
        ctrl_layout.addWidget(self.file_combo)

        self.delete_file_btn = QPushButton("Delete from DB")
        self.delete_file_btn.clicked.connect(self._delete_file_logs)
        ctrl_layout.addWidget(self.delete_file_btn)
        ctrl_layout.addStretch()
        layout.addLayout(ctrl_layout)

        pagination_layout = QHBoxLayout()
        self.info_label = QLabel("No data")
        self.prev_btn = QPushButton("< Previous")
        self.prev_btn.clicked.connect(self._prev_page)
        self.next_btn = QPushButton("Next >")
        self.next_btn.clicked.connect(self._next_page)
        self.page_spinbox = QSpinBox()
        self.page_spinbox.setMinimum(1)
        self.page_spinbox.valueChanged.connect(self._on_page_changed)

        pagination_layout.addWidget(self.info_label)
        pagination_layout.addStretch()
        pagination_layout.addWidget(QLabel("Page:"))
        pagination_layout.addWidget(self.page_spinbox)
        pagination_layout.addWidget(self.prev_btn)
        pagination_layout.addWidget(self.next_btn)
        layout.addLayout(pagination_layout)

        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["Station", "Timestamp", "Logger", "Level", "Message"])
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        self.main_window.station_combo.currentIndexChanged.connect(self._on_station_switched)
        if self.main_window.get_selected_station() is not None:
            self._on_station_switched()

    def _load_file(self):
        if self.main_window.get_selected_station() is None:
            QtWidgets.QMessageBox.warning(
                self,
                "No Station Selected",
                "Please select or add a charging station first.",
            )
            return

        filepaths, _ = QFileDialog.getOpenFileNames(
            self,
            "Open CLC Log Files",
            "input_data/clc_logs",
            "CLC Log Files (*.log *.txt *.1 *.2 *.3 *.4 *.5 *.6 *.7 *.8 *.9);;All Files (*.*)",
        )
        if not filepaths:
            return

        try:
            self.main_window.begin_busy("Loading CLC log files...")
            self.main_window.status_bar.showMessage("Loading CLC log files...")
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            self.load_btn.setEnabled(False)

            station_id = self.main_window.get_selected_station()
            imported_lines = 0
            imported_files = 0
            skipped_files = 0
            for index, filepath in enumerate(filepaths, start=1):
                self.main_window.status_bar.showMessage(
                    f"Loading CLC log files... ({index}/{len(filepaths)}) {os.path.basename(filepath)}"
                )
                inserted = self.parser.parse(
                    filepath,
                    station_id=station_id,
                    progress_callback=self._on_parse_progress,
                )
                imported_lines += inserted
                if inserted == 0:
                    skipped_files += 1
                else:
                    imported_files += 1

            self.current_page = 0
            self._populate_file_combo()
            self._reset_pagination()
            self._display_page()

            self.file_label.setText(
                f"{imported_files} imported, {skipped_files} skipped ({imported_lines} lines)"
            )
            self.main_window.status_bar.showMessage(
                f"Loaded {imported_lines} CLC lines from {imported_files} file(s), skipped {skipped_files}"
            )

            self.progress_bar.setValue(100)
            QtCore.QTimer.singleShot(1000, lambda: self.progress_bar.setVisible(False))
        except Exception as exc:
            self.main_window.status_bar.showMessage("Failed to load CLC log files")
            QtWidgets.QMessageBox.critical(self, "Error", f"Failed to load file: {exc}")
        finally:
            self.load_btn.setEnabled(True)
            self.main_window.end_busy("Ready")

    def _on_parse_progress(self, current: int, total: int):
        if total > 0:
            percent = int((current / total) * 100)
            self.progress_bar.setValue(percent)
            self.main_window.status_bar.showMessage(f"Loading CLC log file... {percent}%")
        QtWidgets.QApplication.processEvents()

    def _populate_file_combo(self):
        self.file_combo.blockSignals(True)
        self.file_combo.clear()
        self.file_combo.addItem("-- all files --", None)

        station_id = self.main_window.get_selected_station()
        if station_id is not None:
            for fname in self.parser.get_files(station_id=station_id):
                self.file_combo.addItem(fname, fname)

        self.file_combo.blockSignals(False)

    def _on_station_switched(self):
        self._begin_busy("Refreshing CLC view...")
        try:
            self.current_page = 0
            self.file_label.setText("")
            self._populate_file_combo()
            self._reset_pagination()
            self._display_page()
            self.page_spinbox.setValue(1)
        finally:
            self._end_busy("CLC view refreshed.")

    def refresh_for_current_station(self):
        self._on_station_switched()

    def _on_filter_changed(self):
        self._begin_busy("Refreshing CLC data...")
        try:
            self.current_page = 0
            self._reset_pagination()
            self._display_page()
        finally:
            self._end_busy("CLC data refreshed.")

    def _reset_pagination(self):
        station_id = self.main_window.get_selected_station()
        file_name = self.file_combo.currentData()
        total_count = self.parser.get_total_count(station_id=station_id, file_name=file_name)
        total_pages = (total_count + self.ROWS_PER_PAGE - 1) // self.ROWS_PER_PAGE
        self.page_spinbox.blockSignals(True)
        self.page_spinbox.setMaximum(max(1, total_pages))
        self.page_spinbox.setValue(1)
        self.page_spinbox.blockSignals(False)

    def _prev_page(self):
        if self.current_page > 0:
            self._begin_busy("Loading CLC page...")
            try:
                self.current_page -= 1
                self.page_spinbox.blockSignals(True)
                self.page_spinbox.setValue(self.current_page + 1)
                self.page_spinbox.blockSignals(False)
                self._display_page()
            finally:
                self._end_busy("CLC page loaded.")

    def _next_page(self):
        station_id = self.main_window.get_selected_station()
        file_name = self.file_combo.currentData()
        total_count = self.parser.get_total_count(station_id=station_id, file_name=file_name)
        if (self.current_page + 1) * self.ROWS_PER_PAGE < total_count:
            self._begin_busy("Loading CLC page...")
            try:
                self.current_page += 1
                self.page_spinbox.blockSignals(True)
                self.page_spinbox.setValue(self.current_page + 1)
                self.page_spinbox.blockSignals(False)
                self._display_page()
            finally:
                self._end_busy("CLC page loaded.")

    def _on_page_changed(self, page: int):
        self._begin_busy("Loading CLC page...")
        try:
            self.current_page = page - 1
            self._display_page()
        finally:
            self._end_busy("CLC page loaded.")

    def _display_page(self):
        station_id = self.main_window.get_selected_station()
        file_name = self.file_combo.currentData()
        total_count = self.parser.get_total_count(station_id=station_id, file_name=file_name)
        entries = self.parser.get_entries_paginated(
            self.current_page + 1,
            self.ROWS_PER_PAGE,
            station_id=station_id,
            file_name=file_name,
        )

        if total_count > 0:
            start_idx = self.current_page * self.ROWS_PER_PAGE + 1
            end_idx = min(start_idx + len(entries) - 1, total_count)
            self.info_label.setText(f"Showing {start_idx}-{end_idx} of {total_count}")
        else:
            self.info_label.setText("No data")

        self.table.setRowCount(len(entries))
        for row_index, entry in enumerate(entries):
            station_name = (
                self.main_window.get_station_name(entry["station_id"])
                if entry["station_id"]
                else "N/A"
            )
            self.table.setItem(row_index, 0, QTableWidgetItem(station_name))
            self.table.setItem(row_index, 1, QTableWidgetItem(entry.get("timestamp", "")))
            self.table.setItem(row_index, 2, QTableWidgetItem(entry.get("logger_name", "")))
            self.table.setItem(row_index, 3, QTableWidgetItem(entry.get("level", "")))
            self.table.setItem(row_index, 4, QTableWidgetItem(entry.get("message", "")))

        self.table.resizeColumnsToContents()
        self.prev_btn.setEnabled(self.current_page > 0)
        self.next_btn.setEnabled((self.current_page + 1) * self.ROWS_PER_PAGE < total_count)

    def _delete_file_logs(self):
        file_name = self.file_combo.currentData()
        station_id = self.main_window.get_selected_station()
        if not file_name:
            QtWidgets.QMessageBox.warning(self, "No file selected", "Please select a file to delete.")
            return

        reply = QtWidgets.QMessageBox.question(
            self,
            "Confirm Deletion",
            f"Delete all CLC entries imported from '{file_name}'?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
        )
        if reply != QtWidgets.QMessageBox.Yes:
            return

        self._begin_busy("Deleting CLC data...")
        try:
            deleted = self.parser.delete_logs_by_file(file_name, station_id=station_id)
            QtWidgets.QMessageBox.information(self, "Deleted", f"Removed {deleted} rows.")
            self._populate_file_combo()
            self._reset_pagination()
            self._display_page()
        finally:
            self._end_busy("CLC data deleted.")

    def _begin_busy(self, message: str):
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)
        self.main_window.begin_busy(message)

    def _end_busy(self, message: str):
        self.progress_bar.setVisible(False)
        self.progress_bar.setRange(0, 100)
        self.main_window.end_busy(message)
