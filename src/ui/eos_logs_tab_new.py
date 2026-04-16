"""UI for EOS logs module."""

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

from src.modules.eos_logs import EOSLogParser


class EOSLogsTab(QtWidgets.QWidget):
    ROWS_PER_PAGE = 100

    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self.parser = EOSLogParser()
        self.current_page = 0
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        file_layout = QHBoxLayout()
        self.file_label = QLabel("No file loaded")
        self.load_btn = QPushButton("Load EOS Log")
        self.load_btn.clicked.connect(self._load_file)
        file_layout.addWidget(QLabel("EOS Log File:"))
        file_layout.addWidget(self.file_label)
        file_layout.addWidget(self.load_btn)
        layout.addLayout(file_layout)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        ctrl_layout = QHBoxLayout()
        ctrl_layout.addWidget(QLabel("Filter by Transaction:"))
        self.tx_combo = QComboBox()
        self.tx_combo.addItem("All", None)
        self.tx_combo.currentIndexChanged.connect(self._on_filter_changed)
        ctrl_layout.addWidget(self.tx_combo)
        ctrl_layout.addSpacing(20)

        ctrl_layout.addWidget(QLabel("Loaded files:"))
        self.file_combo = QComboBox()
        self.file_combo.addItem("-- none --", None)
        self.file_combo.currentIndexChanged.connect(self._on_filter_changed)
        ctrl_layout.addWidget(self.file_combo)

        self.hide_empty_checkbox = QtWidgets.QCheckBox("Hide empty rows")
        self.hide_empty_checkbox.stateChanged.connect(self._on_filter_changed)
        ctrl_layout.addWidget(self.hide_empty_checkbox)

        self.delete_file_btn = QPushButton("Delete from DB")
        self.delete_file_btn.clicked.connect(self._delete_file_logs)
        ctrl_layout.addWidget(self.delete_file_btn)

        ctrl_layout.addStretch()
        layout.addLayout(ctrl_layout)

        pagination_layout = QHBoxLayout()
        self.info_label = QLabel("No data")
        self.prev_btn = QPushButton("â—€ Previous")
        self.prev_btn.clicked.connect(self._prev_page)
        self.next_btn = QPushButton("Next â–¶")
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
        self.table.setColumnCount(0)
        self.table.setHorizontalHeaderLabels([])
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

        self.main_window.station_combo.currentIndexChanged.connect(self._on_station_switched)

        if self.main_window.get_selected_station() is not None:
            self._on_station_switched()

    def _load_file(self):
        if self.main_window.get_selected_station() is None:
            QtWidgets.QMessageBox.warning(
                self, "No Station Selected", "Please select or add a charging station first."
            )
            return

        filepath, _ = QFileDialog.getOpenFileName(
            self,
            "Open EOS Log File",
            "input_data/eos_logs",
            "EOS Log Files (*.csv);;All Files (*.*)",
        )
        if not filepath:
            return

        try:
            self.main_window.begin_busy("Loading EOS log file...")
            self.main_window.status_bar.showMessage("Loading EOS log file...")
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            self.load_btn.setEnabled(False)

            station_id = self.main_window.get_selected_station()
            count = self.parser.parse(
                filepath, station_id=station_id, progress_callback=self._on_parse_progress
            )

            self.file_label.setText(f"{filepath} ({count} entries)")
            self.current_page = 0
            self._populate_tx_combo()
            self._populate_file_combo()

            total_count = self.parser.get_total_count(station_id=station_id)
            total_pages = (total_count + self.ROWS_PER_PAGE - 1) // self.ROWS_PER_PAGE
            self.page_spinbox.setMaximum(max(1, total_pages))
            self.page_spinbox.setValue(1)

            self._display_page()

            self.progress_bar.setValue(100)
            QtCore.QTimer.singleShot(1000, lambda: self.progress_bar.setVisible(False))
            self.main_window.status_bar.showMessage(
                f"Loaded {count} entries from {os.path.basename(filepath)}"
            )
        except Exception as exc:
            self.main_window.status_bar.showMessage("Failed to load EOS log file")
            QtWidgets.QMessageBox.critical(self, "Error", f"Failed to load file: {exc}")
        finally:
            self.load_btn.setEnabled(True)
            self.main_window.end_busy("Ready")

    def _populate_tx_combo(self):
        self.tx_combo.blockSignals(True)
        self.tx_combo.clear()
        self.tx_combo.addItem("All", None)

        station_id = self.main_window.get_selected_station()
        if station_id is None:
            self.tx_combo.blockSignals(False)
            return

        for tx in self.parser.get_transactions(station_id=station_id):
            label = (
                f"{tx['transaction_id']} "
                f"({tx.get('start_time', '')[:16]} - {tx.get('stop_time', '')[:16]})"
            )
            self.tx_combo.addItem(label, tx["transaction_id"])

        self.tx_combo.blockSignals(False)

    def _populate_file_combo(self):
        self.file_combo.blockSignals(True)
        self.file_combo.clear()
        self.file_combo.addItem("-- none --", None)

        station_id = self.main_window.get_selected_station()
        if station_id is None:
            self.file_combo.blockSignals(False)
            return

        for fname in self.parser.get_files(station_id=station_id):
            self.file_combo.addItem(fname, fname)

        self.file_combo.blockSignals(False)

    def _on_parse_progress(self, current: int, total: int):
        if total > 0:
            percent = int((current / total) * 100)
            self.progress_bar.setValue(percent)
            self.main_window.status_bar.showMessage(f"Loading EOS log file... {percent}%")
        QtWidgets.QApplication.processEvents()

    def _on_station_switched(self):
        self._begin_busy("Refreshing EOS view...")
        try:
            self.current_page = 0
            self.file_label.setText("")
            self._populate_tx_combo()
            self._populate_file_combo()
            self._display_page()
            self.page_spinbox.setValue(1)
        finally:
            self._end_busy("EOS view refreshed.")

    def _on_filter_changed(self):
        self._begin_busy("Refreshing EOS data...")
        try:
            self.current_page = 0
            station_id = self.main_window.get_selected_station()
            tx_id = self.tx_combo.currentData()
            file_filter = self.file_combo.currentData()

            total_count = self.parser.get_total_count(
                station_id=station_id, transaction_id=tx_id, file_name=file_filter
            )
            total_pages = (total_count + self.ROWS_PER_PAGE - 1) // self.ROWS_PER_PAGE
            self.page_spinbox.blockSignals(True)
            self.page_spinbox.setMaximum(max(1, total_pages))
            self.page_spinbox.setValue(1)
            self.page_spinbox.blockSignals(False)
            self._display_page()
        finally:
            self._end_busy("EOS data refreshed.")

    def _prev_page(self):
        if self.current_page > 0:
            self._begin_busy("Loading EOS page...")
            try:
                self.current_page -= 1
                self.page_spinbox.blockSignals(True)
                self.page_spinbox.setValue(self.current_page + 1)
                self.page_spinbox.blockSignals(False)
                self._display_page()
            finally:
                self._end_busy("EOS page loaded.")

    def _next_page(self):
        station_id = self.main_window.get_selected_station()
        tx_id = self.tx_combo.currentData()
        file_filter = self.file_combo.currentData()
        total_count = self.parser.get_total_count(
            station_id=station_id, transaction_id=tx_id, file_name=file_filter
        )
        if (self.current_page + 1) * self.ROWS_PER_PAGE < total_count:
            self._begin_busy("Loading EOS page...")
            try:
                self.current_page += 1
                self.page_spinbox.blockSignals(True)
                self.page_spinbox.setValue(self.current_page + 1)
                self.page_spinbox.blockSignals(False)
                self._display_page()
            finally:
                self._end_busy("EOS page loaded.")

    def _on_page_changed(self, page):
        self._begin_busy("Loading EOS page...")
        try:
            self.current_page = page - 1
            self._display_page()
        finally:
            self._end_busy("EOS page loaded.")

    def _display_page(self):
        station_id = self.main_window.get_selected_station()
        tx_id = self.tx_combo.currentData()
        file_filter = self.file_combo.currentData()

        total_count = self.parser.get_total_count(
            station_id=station_id, transaction_id=tx_id, file_name=file_filter
        )
        messages = self.parser.get_entries_paginated(
            self.current_page + 1,
            self.ROWS_PER_PAGE,
            station_id=station_id,
            transaction_id=tx_id,
            file_name=file_filter,
        )

        if self.hide_empty_checkbox.isChecked():
            messages = [message for message in messages if not self._is_empty_row(message)]

        if total_count > 0:
            start_idx = self.current_page * self.ROWS_PER_PAGE + 1
            end_idx = min(start_idx + len(messages) - 1, total_count)
            if self.hide_empty_checkbox.isChecked():
                self.info_label.setText(
                    f"Showing {start_idx}-{end_idx} of {total_count} (empty rows hidden)"
                )
            else:
                self.info_label.setText(f"Showing {start_idx}-{end_idx} of {total_count}")
        else:
            self.info_label.setText("No data")

        columns = []
        for message in messages:
            for key in message.keys():
                if key == "raw_data":
                    continue
                if key not in columns:
                    columns.append(key)

        preferred_order = ["station_id", "file_name", "transaction_id", "Date", "timestamp"]
        ordered_columns = [column for column in preferred_order if column in columns]
        ordered_columns += [column for column in columns if column not in ordered_columns]
        columns = ordered_columns

        self.table.setColumnCount(len(columns))
        self.table.setHorizontalHeaderLabels(columns)
        self.table.setRowCount(len(messages))

        for row_idx, message in enumerate(messages):
            for col_idx, column_name in enumerate(columns):
                self.table.setItem(
                    row_idx,
                    col_idx,
                    QTableWidgetItem(str(message.get(column_name, ""))),
                )

        self.table.resizeColumnsToContents()
        self.prev_btn.setEnabled(self.current_page > 0)
        self.next_btn.setEnabled((self.current_page + 1) * self.ROWS_PER_PAGE < total_count)

    def _is_empty_row(self, row: dict) -> bool:
        ignore_keys = {"station_id", "file_name", "transaction_id"}
        for key, value in row.items():
            if key in ignore_keys:
                continue
            if value is None:
                continue
            if isinstance(value, str) and value.strip() == "":
                continue
            return False
        return True

    def _delete_file_logs(self):
        fname = self.file_combo.currentData()
        station_id = self.main_window.get_selected_station()
        if not fname:
            QtWidgets.QMessageBox.warning(
                self, "No file selected", "Please select a file to delete."
            )
            return

        reply = QtWidgets.QMessageBox.question(
            self,
            "Confirm Deletion",
            f"Delete all entries imported from '{fname}'?",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
        )
        if reply != QtWidgets.QMessageBox.Yes:
            return

        self._begin_busy("Deleting EOS data...")
        try:
            deleted = self.parser.delete_logs_by_file(fname, station_id=station_id)
            QtWidgets.QMessageBox.information(self, "Deleted", f"Removed {deleted} rows.")
            self._populate_tx_combo()
            self._populate_file_combo()
            self._on_filter_changed()
        finally:
            self._end_busy("EOS data deleted.")

    def _begin_busy(self, message: str):
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)
        self.main_window.begin_busy(message)

    def _end_busy(self, message: str):
        self.progress_bar.setVisible(False)
        self.progress_bar.setRange(0, 100)
        self.main_window.end_busy(message)
