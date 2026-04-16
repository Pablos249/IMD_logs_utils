"""UI for CAN logs module"""

import os

from PyQt5 import QtWidgets, QtCore
from PyQt5.QtWidgets import QVBoxLayout, QHBoxLayout, QPushButton, QTableWidget, QTableWidgetItem, QFileDialog, QLabel, QProgressBar, QSpinBox, QComboBox
from PyQt5.QtCore import Qt
from src.modules.can_logs import CANLogParser


class CANLogsTab(QtWidgets.QWidget):
    ROWS_PER_PAGE = 100  # Display 100 rows at a time
    
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        # single parser for the entire application; keeps DB open and persistent
        self.parser = CANLogParser()
        self.current_page = 0
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # Top section: file loader
        file_layout = QHBoxLayout()
        self.file_label = QLabel("No file loaded")
        self.load_btn = QPushButton("Load CAN Logs")
        self.load_btn.clicked.connect(self._load_file)
        file_layout.addWidget(QLabel("CAN Log File:"))
        file_layout.addWidget(self.file_label)
        file_layout.addWidget(self.load_btn)
        layout.addLayout(file_layout)

        # Progress bar (hidden by default)
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        # Controls row: filter by CAN ID and file operations
        ctrl_layout = QHBoxLayout()
        ctrl_layout.addWidget(QLabel("Filter by CAN ID:"))
        self.filter_combo = QComboBox()
        self.filter_combo.addItem("All", None)
        self.filter_combo.currentIndexChanged.connect(self._on_filter_changed)
        ctrl_layout.addWidget(self.filter_combo)
        ctrl_layout.addSpacing(20)

        # file selector for deletion
        ctrl_layout.addWidget(QLabel("Loaded files:"))
        self.file_combo = QComboBox()
        self.file_combo.addItem("-- none --", None)
        self.file_combo.currentIndexChanged.connect(self._on_filter_changed)
        ctrl_layout.addWidget(self.file_combo)
        self.delete_file_btn = QPushButton("Delete from DB")
        self.delete_file_btn.clicked.connect(self._delete_file_logs)
        ctrl_layout.addWidget(self.delete_file_btn)

        ctrl_layout.addStretch()
        layout.addLayout(ctrl_layout)

        # Pagination controls
        pagination_layout = QHBoxLayout()
        self.info_label = QLabel("No data")
        self.prev_btn = QPushButton("◀ Previous")
        self.prev_btn.clicked.connect(self._prev_page)
        self.next_btn = QPushButton("Next ▶")
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

        # clear tab contents if the selected station is changed
        self.main_window.station_combo.currentIndexChanged.connect(self._on_station_switched)
        
        # Table to display parsed messages
        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(["Station", "Timestamp", "CAN ID", "Description", "Raw Data", "Decoded Values"])
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)
        
        # if a station is already selected on startup, populate the combos
        if self.main_window.get_selected_station() is not None:
            self._on_station_switched()

    def _load_file(self):
        """Open file dialog to select a CAN log file"""
        # Check if station is selected
        if self.main_window.get_selected_station() is None:
            QtWidgets.QMessageBox.warning(self, "No Station Selected", 
                "Please select or add a charging station first.")
            return
        
        filepaths, _ = QFileDialog.getOpenFileNames(
            self,
            "Open CAN Log Files",
            "input_data/clc_logs",
            "CAN Log Files (*.can *.log);;All Files (*.*)"
        )

        if not filepaths:
            return

        try:
            self.main_window.begin_busy("Loading CAN log files...")
            self.main_window.status_bar.showMessage("Loading CAN log files...")
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            self.load_btn.setEnabled(False)

            station_id = self.main_window.get_selected_station()
            imported_messages = 0
            imported_files = []
            failed_files = []
            for index, filepath in enumerate(filepaths, start=1):
                self.main_window.status_bar.showMessage(
                    f"Loading CAN log files... ({index}/{len(filepaths)}) {os.path.basename(filepath)}"
                )
                count = self.parser.parse(
                    filepath,
                    station_id=station_id,
                    progress_callback=self._on_parse_progress,
                )
                imported_messages += count
                imported_files.append(filepath)
            
            self.file_label.setText(
                f"{len(imported_files)} file(s) imported ({imported_messages} messages)"
            )
            self.current_page = 0
            
            # Populate filter dropdown and file list for the station
            self._populate_filter_combo()
            self._populate_file_combo()
            
            # Setup pagination
            total_count = self.parser.get_total_count()
            total_pages = (total_count + self.ROWS_PER_PAGE - 1) // self.ROWS_PER_PAGE
            self.page_spinbox.setMaximum(max(1, total_pages))
            self.page_spinbox.setValue(1)
            
            self._display_page()
            
            self.progress_bar.setValue(100)
            QtCore.QTimer.singleShot(1000, lambda: self.progress_bar.setVisible(False))
            self.main_window.status_bar.showMessage(
                f"Loaded {imported_messages} CAN messages from {len(imported_files)} file(s)"
            )
        except Exception as e:
            self.main_window.status_bar.showMessage("Failed to load CAN log files")
            QtWidgets.QMessageBox.critical(self, "Error", f"Failed to load file: {str(e)}")
        finally:
            self.load_btn.setEnabled(True)
            self.main_window.end_busy("Ready")

    def _populate_filter_combo(self):
        """Populate filter dropdown with CAN IDs for current station"""
        self.filter_combo.blockSignals(True)
        self.filter_combo.clear()
        self.filter_combo.addItem("All", None)
        
        station = self.main_window.get_selected_station()
        if station is None:
            # nothing to show until a station is selected
            self.filter_combo.blockSignals(False)
            return
        can_ids = self.parser.get_can_ids(station_id=station)
        for can_id in can_ids:
            self.filter_combo.addItem(can_id, can_id)
        
        self.filter_combo.blockSignals(False)

    def _populate_file_combo(self):
        """Populate file dropdown for the current station"""
        self.file_combo.blockSignals(True)
        self.file_combo.clear()
        self.file_combo.addItem("-- none --", None)
        station = self.main_window.get_selected_station()
        if station is None:
            self.file_combo.blockSignals(False)
            return
        files = self.parser.get_files(station_id=station)
        for fname in files:
            self.file_combo.addItem(fname, fname)
        self.file_combo.blockSignals(False)

    def _on_parse_progress(self, current: int, total: int):
        """Callback for parser progress updates"""
        if total > 0:
            percent = int((current / total) * 100)
            self.progress_bar.setValue(percent)
            self.main_window.status_bar.showMessage(f"Loading CAN log file... {percent}%")
        QtWidgets.QApplication.processEvents()  # Keep UI responsive

    def _on_station_switched(self):
        """Reset view when user switches charging station"""
        self._begin_busy("Refreshing IMD view...")
        try:
            self.current_page = 0
            self.file_label.setText("")
            # repopulate filters/files based on existing DB entries for selected station
            self._populate_filter_combo()
            self._populate_file_combo()
            self._display_page()
            # ensure page spinbox is reset
            self.page_spinbox.setValue(1)
        finally:
            self._end_busy("IMD view refreshed.")

    def refresh_for_current_station(self):
        self._on_station_switched()

    def _on_filter_changed(self):
        """Handle CAN ID filter change"""
        self._begin_busy("Refreshing IMD data...")
        try:
            self.current_page = 0
            filter_can_id = self.filter_combo.currentData()
            station = self.main_window.get_selected_station()
            file_filter = self.file_combo.currentData()
            
            if self.parser:
                total_count = self.parser.get_total_count(station_id=station,
                                                        file_name=file_filter,
                                                        filter_can_id=filter_can_id)
                total_pages = (total_count + self.ROWS_PER_PAGE - 1) // self.ROWS_PER_PAGE
                self.page_spinbox.blockSignals(True)
                self.page_spinbox.setMaximum(max(1, total_pages))
                self.page_spinbox.setValue(1)
                self.page_spinbox.blockSignals(False)
                self._display_page()
        finally:
            self._end_busy("IMD data refreshed.")

    def _prev_page(self):
        """Go to previous page"""
        if self.current_page > 0:
            self._begin_busy("Loading IMD page...")
            try:
                self.current_page -= 1
                self.page_spinbox.blockSignals(True)
                self.page_spinbox.setValue(self.current_page + 1)
                self.page_spinbox.blockSignals(False)
                self._display_page()
            finally:
                self._end_busy("IMD page loaded.")

    def _delete_file_logs(self):
        """Remove logs from selected file"""
        fname = self.file_combo.currentData()
        station = self.main_window.get_selected_station()
        if not fname:
            QtWidgets.QMessageBox.warning(self, "No file selected", "Please select a file to delete.")
            return
        reply = QtWidgets.QMessageBox.question(self, "Confirm Deletion",
                        f"Remove all entries imported from '{fname}'?", 
                        QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No)
        if reply != QtWidgets.QMessageBox.Yes:
            return
        self._begin_busy("Deleting IMD data...")
        try:
            self.main_window.status_bar.showMessage("Deleting logs from file...")
            deleted = self.parser.delete_logs_by_file(fname, station_id=station)
            QtWidgets.QMessageBox.information(self, "Deleted", f"Removed {deleted} rows.")
            self.main_window.status_bar.showMessage(f"Deleted {deleted} rows from {fname}")
            # refresh file list and view
            self._populate_file_combo()
            self._populate_filter_combo()
            self._on_filter_changed()
        finally:
            self._end_busy("IMD data deleted.")

    def _next_page(self):
        """Go to next page"""
        if self.parser:
            filter_can_id = self.filter_combo.currentData()
            station = self.main_window.get_selected_station()
            file_filter = self.file_combo.currentData()
            total_count = self.parser.get_total_count(
                station_id=station,
                file_name=file_filter,
                filter_can_id=filter_can_id
            )
            if (self.current_page + 1) * self.ROWS_PER_PAGE < total_count:
                self._begin_busy("Loading IMD page...")
                try:
                    self.current_page += 1
                    self.page_spinbox.blockSignals(True)
                    self.page_spinbox.setValue(self.current_page + 1)
                    self.page_spinbox.blockSignals(False)
                    self._display_page()
                finally:
                    self._end_busy("IMD page loaded.")

    def _on_page_changed(self, page):
        """Handle page spinbox change"""
        self._begin_busy("Loading IMD page...")
        try:
            self.current_page = page - 1
            self._display_page()
        finally:
            self._end_busy("IMD page loaded.")

    def _display_page(self):
        """Display current page of messages"""
        if not self.parser:
            self.info_label.setText("No data")
            self.table.setRowCount(0)
            return

        filter_can_id = self.filter_combo.currentData()
        station = self.main_window.get_selected_station()
        file_filter = self.file_combo.currentData()
        messages, total_count = self.parser.get_messages_page(
            self.current_page,
            self.ROWS_PER_PAGE,
            station_id=station,
            file_name=file_filter,
            filter_can_id=filter_can_id
        )

        # Update info label
        if total_count > 0:
            start_idx = self.current_page * self.ROWS_PER_PAGE + 1
            end_idx = min(start_idx + len(messages) - 1, total_count)
            self.info_label.setText(f"Showing {start_idx}-{end_idx} of {total_count}")
        else:
            self.info_label.setText("No data")

        # Display messages in table
        self.table.setRowCount(len(messages))

        for row, msg in enumerate(messages):
            station_name = self.main_window.get_station_name(msg['station_id']) if msg['station_id'] else "N/A"
            station_item = QTableWidgetItem(station_name)
            
            timestamp_item = QTableWidgetItem(msg['timestamp'])
            can_id_item = QTableWidgetItem(msg['can_id'])
            description_item = QTableWidgetItem(msg['description'])
            raw_data_item = QTableWidgetItem(msg['raw_data'].hex().upper())

            # Build decoded values text from database fields
            decoded_parts = []
            if msg['voltage_V'] is not None:
                decoded_parts.append(f"voltage_V: {msg['voltage_V']}")
            if msg['capacitance_nF'] is not None:
                decoded_parts.append(f"capacitance_nF: {msg['capacitance_nF']}")
            if msg['resistance_ohm'] is not None:
                decoded_parts.append(f"resistance_ohm: {msg['resistance_ohm']}")
            if msg['status_byte'] is not None:
                decoded_parts.append(f"status_byte: {msg['status_byte']}")
            if msg['status_binary'] is not None:
                decoded_parts.append(f"status_binary: {msg['status_binary']}")
            
            decoded_text = ", ".join(decoded_parts)
            decoded_item = QTableWidgetItem(decoded_text)

            self.table.setItem(row, 0, station_item)
            self.table.setItem(row, 1, timestamp_item)
            self.table.setItem(row, 2, can_id_item)
            self.table.setItem(row, 3, description_item)
            self.table.setItem(row, 4, raw_data_item)
            self.table.setItem(row, 5, decoded_item)

        # Resize columns to content
        self.table.resizeColumnsToContents()
        
        # Update button states
        self.prev_btn.setEnabled(self.current_page > 0)
        self.next_btn.setEnabled((self.current_page + 1) * self.ROWS_PER_PAGE < total_count)

    def _begin_busy(self, message: str):
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)
        self.main_window.begin_busy(message)

    def _end_busy(self, message: str):
        self.progress_bar.setVisible(False)
        self.progress_bar.setRange(0, 100)
        self.main_window.end_busy(message)
