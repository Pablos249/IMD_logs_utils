"""UI for converting logs to CSV."""

from PyQt5 import QtWidgets, QtCore
from PyQt5.QtWidgets import (
    QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QFileDialog, QTextEdit, QGroupBox, QProgressBar
)

from src.modules.log_converter import convert_imd_log_to_csv, convert_generic_log_to_csv


class _ConversionThread(QtCore.QThread):
    progress = QtCore.pyqtSignal(int, int)
    finished = QtCore.pyqtSignal(int, str)
    error = QtCore.pyqtSignal(str)

    def __init__(self, func, input_path: str, output_path: str):
        super().__init__()
        self.func = func
        self.input_path = input_path
        self.output_path = output_path

    def run(self):
        try:
            count = self.func(self.input_path, self.output_path, progress_callback=self._on_progress)
            self.finished.emit(count, self.output_path)
        except Exception as e:
            self.error.emit(str(e))

    def _on_progress(self, current: int, total: int):
        self.progress.emit(current, total)


class LogConverterTab(QtWidgets.QWidget):
    def __init__(self, main_window):
        super().__init__()
        self.main_window = main_window
        self._setup_ui()
        self._thread = None

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # IMD log converter section
        imd_group = QGroupBox("Konwerter logów IMD -> CSV")
        imd_layout = QVBoxLayout(imd_group)

        self.imd_file_label = QLabel("Brak wybranego pliku")
        imd_layout.addWidget(self.imd_file_label)

        imd_btn_layout = QHBoxLayout()
        self.imd_choose_btn = QPushButton("Wybierz plik IMD")
        self.imd_choose_btn.clicked.connect(self._choose_imd_file)
        imd_btn_layout.addWidget(self.imd_choose_btn)

        self.imd_convert_btn = QPushButton("Konwertuj do CSV")
        self.imd_convert_btn.clicked.connect(self._convert_imd)
        self.imd_convert_btn.setEnabled(False)
        imd_btn_layout.addWidget(self.imd_convert_btn)

        imd_layout.addLayout(imd_btn_layout)

        # Other device converter section
        other_group = QGroupBox("Konwerter innego logu -> CSV")
        other_layout = QVBoxLayout(other_group)

        self.other_file_label = QLabel("Brak wybranego pliku")
        other_layout.addWidget(self.other_file_label)

        other_btn_layout = QHBoxLayout()
        self.other_choose_btn = QPushButton("Wybierz plik")
        self.other_choose_btn.clicked.connect(self._choose_other_file)
        other_btn_layout.addWidget(self.other_choose_btn)

        self.other_convert_btn = QPushButton("Konwertuj do CSV")
        self.other_convert_btn.clicked.connect(self._convert_other)
        self.other_convert_btn.setEnabled(False)
        other_btn_layout.addWidget(self.other_convert_btn)

        other_layout.addLayout(other_btn_layout)

        # Status / log output
        self.output_box = QTextEdit()
        self.output_box.setReadOnly(True)

        layout.addWidget(imd_group)
        layout.addWidget(other_group)
        layout.addWidget(QLabel("Wyjście:"))
        layout.addWidget(self.output_box)

        # Progress bar (hidden by default)
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setValue(0)
        layout.addWidget(self.progress_bar)

        self.imd_path = None
        self.other_path = None

    def _choose_imd_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Wybierz plik logu IMD", "input_data/imd_log", "Logi (*.log *.txt);;Wszystkie pliki (*)")
        if not path:
            return
        self.imd_path = path
        self.imd_file_label.setText(path)
        self.imd_convert_btn.setEnabled(True)
        self._log(f"Wybrano plik IMD: {path}")

    def _choose_other_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Wybierz inny plik logu", "input_data/clc_logs", "Logi (*.log *.txt);;Wszystkie pliki (*)")
        if not path:
            return
        self.other_path = path
        self.other_file_label.setText(path)
        self.other_convert_btn.setEnabled(True)
        self._log(f"Wybrano inny plik: {path}")

    def _convert_imd(self):
        if not self.imd_path:
            return

        out_path, _ = QFileDialog.getSaveFileName(self, "Zapisz jako CSV", "", "CSV (*.csv)")
        if not out_path:
            return

        self.imd_convert_btn.setEnabled(False)
        self.other_convert_btn.setEnabled(False)
        self._log(f"Rozpoczynam konwersję IMD -> CSV: {out_path}")
        self.main_window.status_bar.showMessage("Rozpoczynam konwersję IMD...")

        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)

        self._thread = _ConversionThread(convert_imd_log_to_csv, self.imd_path, out_path)
        self._thread.progress.connect(self._on_progress)
        self._thread.finished.connect(self._on_finished)
        self._thread.error.connect(self._on_error)
        self._thread.start()

    def _convert_other(self):
        if not self.other_path:
            return

        out_path, _ = QFileDialog.getSaveFileName(self, "Zapisz jako CSV", "", "CSV (*.csv)")
        if not out_path:
            return

        self.imd_convert_btn.setEnabled(False)
        self.other_convert_btn.setEnabled(False)
        self._log(f"Rozpoczynam konwersję innego logu -> CSV: {out_path}")
        self.main_window.status_bar.showMessage("Rozpoczynam konwersję innego logu...")

        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)

        self._thread = _ConversionThread(convert_generic_log_to_csv, self.other_path, out_path)
        self._thread.progress.connect(self._on_progress)
        self._thread.finished.connect(self._on_finished)
        self._thread.error.connect(self._on_error)
        self._thread.start()

    def _on_progress(self, current: int, total: int):
        if total > 0:
            pct = int((current / total) * 100)
            self.progress_bar.setValue(pct)
            self.main_window.status_bar.showMessage(f"Konwersja... {pct}%")

    def _on_finished(self, count: int, out_path: str):
        self._log(f"Zakończono. Zapisano {count} wierszy do {out_path}")
        self.imd_convert_btn.setEnabled(True)
        self.other_convert_btn.setEnabled(True)
        self.progress_bar.setVisible(False)
        self.main_window.status_bar.showMessage(f"Konwersja zakończona. Zapisano {count} wierszy.")

    def _on_error(self, message: str):
        self._log(f"Błąd podczas konwersji: {message}")
        self.imd_convert_btn.setEnabled(True)
        self.other_convert_btn.setEnabled(True)
        self.progress_bar.setVisible(False)
        self.main_window.status_bar.showMessage("Błąd podczas konwersji.")

    def _log(self, msg: str):
        self.output_box.append(msg)
