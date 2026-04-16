"""Visualization tab for IMD, CLC, Conditioning and EOS time series."""

from __future__ import annotations

from datetime import datetime
from math import isclose

from PyQt5 import QtCore, QtWidgets
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDialog,
    QDoubleSpinBox,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPushButton,
    QProgressBar,
    QSlider,
    QSpinBox,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib import dates as mdates
from matplotlib.figure import Figure

from src.modules.data_analysis import DataAnalyzer


class PointLogsDialog(QDialog):
    """Modeless dialog with raw logs nearest to a selected sample."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Logs near selected point")
        self.resize(1100, 520)
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        self.header_label = QLabel("No sample selected.")
        self.header_label.setWordWrap(True)
        layout.addWidget(self.header_label)

        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(
            ["Source", "Delta [ms]", "Timestamp", "File", "Context", "Message"]
        )
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.table.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table)

    def update_entries(self, anchor_time: datetime, entries):
        self.header_label.setText(
            f"Selected sample: {anchor_time.isoformat(sep=' ', timespec='milliseconds')}"
        )
        self.table.setRowCount(len(entries))
        nearest_row = None
        for row_index, entry in enumerate(entries):
            delta_ms = int(round((entry.get("delta_seconds") or 0.0) * 1000.0))
            values = [
                (entry.get("source_type") or "").upper(),
                str(delta_ms),
                entry.get("timestamp", ""),
                entry.get("file_name", "") or "",
                entry.get("context", "") or "",
                entry.get("message", "") or "",
            ]
            for column, value in enumerate(values):
                self.table.setItem(row_index, column, QTableWidgetItem(value))
            if nearest_row is None:
                nearest_row = row_index

        self.table.resizeColumnsToContents()
        if nearest_row is not None:
            self.table.selectRow(nearest_row)
            self.table.scrollToItem(self.table.item(nearest_row, 0))


class PlotSelectorWidget(QGroupBox):
    """Selector for one plot with filtering and quick actions."""

    def __init__(self, title: str):
        super().__init__(title)
        self._all_items = []
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        description = QLabel("Choose any series from IMD, CLC, Conditioning or EOS.")
        description.setWordWrap(True)
        layout.addWidget(description)

        filter_layout = QHBoxLayout()
        filter_layout.addWidget(QLabel("Filter:"))
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("source, metric, scope...")
        self.search_edit.textChanged.connect(self._apply_filter)
        filter_layout.addWidget(self.search_edit)
        layout.addLayout(filter_layout)

        actions_layout = QHBoxLayout()
        self.select_visible_btn = QPushButton("Select visible")
        self.select_visible_btn.clicked.connect(self._select_visible)
        actions_layout.addWidget(self.select_visible_btn)

        self.clear_selection_btn = QPushButton("Clear")
        self.clear_selection_btn.clicked.connect(self._clear_selection)
        actions_layout.addWidget(self.clear_selection_btn)
        actions_layout.addStretch()
        layout.addLayout(actions_layout)

        self.list_widget = QListWidget()
        self.list_widget.setSelectionMode(QListWidget.MultiSelection)
        layout.addWidget(self.list_widget)

    def set_items(self, items):
        self._all_items = list(items)
        self._apply_filter()

    def clear_items(self):
        self._all_items = []
        self.list_widget.clear()
        self.search_edit.blockSignals(True)
        self.search_edit.clear()
        self.search_edit.blockSignals(False)

    def selected_payloads(self):
        return [item.data(Qt.UserRole) for item in self.list_widget.selectedItems()]

    def _apply_filter(self):
        search_text = self.search_edit.text().strip().lower()
        selected_keys = {
            self._selection_key(item.data(Qt.UserRole))
            for item in self.list_widget.selectedItems()
        }

        self.list_widget.clear()
        for label, payload in self._all_items:
            if search_text and search_text not in label.lower():
                continue
            item = QListWidgetItem(label)
            item.setData(Qt.UserRole, payload)
            if self._selection_key(payload) in selected_keys:
                item.setSelected(True)
            self.list_widget.addItem(item)

    def _select_visible(self):
        for index in range(self.list_widget.count()):
            self.list_widget.item(index).setSelected(True)

    def _clear_selection(self):
        self.list_widget.clearSelection()

    def _selection_key(self, payload: dict):
        return (
            payload.get("source_type"),
            payload.get("metric_name"),
            payload.get("metric_scope"),
        )


class BaseAnalysisTab(QtWidgets.QWidget):
    """Time-series visualization across IMD, CLC, Conditioning and EOS logs."""

    MODE_FULL = "full_range"
    MODE_SESSION = "session"
    DETAIL_MULTIPLIER = 4
    MAX_DETAIL_POINTS = 200000
    MARKER_LEFT_COLOR = "#1565C0"
    MARKER_RIGHT_COLOR = "#C2185B"
    MARKER_DELTA_COLOR = "#6A1B9A"
    GAP_THRESHOLD_OPTIONS = [
        "1 s",
        "5 s",
        "30 s",
        "1 min",
        "5 min",
        "10 min",
        "15 min",
        "30 min",
        "1 h",
    ]

    def __init__(self, main_window, plot_count: int = 3):
        super().__init__()
        self.main_window = main_window
        self.plot_count = max(1, plot_count)
        self.analyzer = DataAnalyzer()
        self._series_catalog = {"imd": [], "clc": [], "conditioning": [], "eos": []}
        self._current_bundles_per_plot = [[] for _ in range(self.plot_count)]
        self._marker_times = []
        self._marker_lines = {}
        self._full_time_range = None
        self._current_time_range = None
        self._current_mode = self.MODE_FULL
        self._current_station_id = None
        self._current_transaction_id = None
        self._current_session_window = None
        self._current_plot_max_points = self.analyzer.DEFAULT_MAX_PLOT_POINTS
        self._custom_y_ranges = {}
        self._gap_threshold_seconds = None
        self._snap_distance_threshold = 0.08
        self._point_logs_dialog = None
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        controls = QHBoxLayout()
        controls.addWidget(QLabel("Mode:"))
        self.mode_combo = QComboBox()
        self.mode_combo.addItem("Full time horizon", self.MODE_FULL)
        self.mode_combo.addItem("EOS charging session", self.MODE_SESSION)
        self.mode_combo.currentIndexChanged.connect(self._on_mode_changed)
        controls.addWidget(self.mode_combo)

        controls.addWidget(QLabel("Transaction:"))
        self.transaction_combo = QComboBox()
        self.transaction_combo.addItem("-- select transaction --", None)
        controls.addWidget(self.transaction_combo)

        controls.addWidget(QLabel("Padding [min]:"))
        self.padding_spin = QSpinBox()
        self.padding_spin.setRange(0, 120)
        self.padding_spin.setValue(5)
        controls.addWidget(self.padding_spin)

        self.refresh_btn = QPushButton("Refresh series")
        self.refresh_btn.clicked.connect(self.refresh_context)
        controls.addWidget(self.refresh_btn)

        self.plot_btn = QPushButton("Plot selected series")
        self.plot_btn.clicked.connect(self._plot_selected_series)
        controls.addWidget(self.plot_btn)

        self.clear_markers_btn = QPushButton("Clear markers")
        self.clear_markers_btn.clicked.connect(self._clear_markers)
        controls.addWidget(self.clear_markers_btn)

        self.snap_to_sample_checkbox = QCheckBox("Snap clicks to sample + logs")
        controls.addWidget(self.snap_to_sample_checkbox)

        self.gap_checkbox = QCheckBox("Break lines on gaps")
        self.gap_checkbox.toggled.connect(self._on_gap_control_changed)
        controls.addWidget(self.gap_checkbox)

        self.step_plot_checkbox = QCheckBox("Rectangular signal")
        self.step_plot_checkbox.toggled.connect(self._redraw_current_bundles)
        controls.addWidget(self.step_plot_checkbox)

        self.gap_threshold_combo = QComboBox()
        self.gap_threshold_combo.setEditable(True)
        self.gap_threshold_combo.addItems(self.GAP_THRESHOLD_OPTIONS)
        self.gap_threshold_combo.setCurrentText("1 min")
        self.gap_threshold_combo.currentTextChanged.connect(self._on_gap_control_changed)
        self.gap_threshold_combo.setEnabled(False)
        controls.addWidget(self.gap_threshold_combo)

        self.more_detail_btn = QPushButton("More points in view")
        self.more_detail_btn.setEnabled(False)
        self.more_detail_btn.clicked.connect(self._load_more_detail)
        controls.addWidget(self.more_detail_btn)
        controls.addStretch()
        layout.addLayout(controls)

        self.info_label = QLabel("Select a station and load data to start plotting.")
        self.info_label.setWordWrap(True)
        layout.addWidget(self.info_label)

        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setRange(0, 0)
        layout.addWidget(self.progress_bar)

        time_controls = QGroupBox("Visible time range")
        time_layout = QVBoxLayout(time_controls)

        self.time_range_label = QLabel("Load data to enable time range controls.")
        self.time_range_label.setWordWrap(True)
        time_layout.addWidget(self.time_range_label)

        slider_row = QHBoxLayout()
        slider_row.addWidget(QLabel("Start"))
        self.start_slider = QSlider(Qt.Horizontal)
        self.start_slider.setRange(0, 1000)
        self.start_slider.setEnabled(False)
        self.start_slider.valueChanged.connect(self._on_time_slider_changed)
        slider_row.addWidget(self.start_slider)
        slider_row.addWidget(QLabel("End"))
        self.end_slider = QSlider(Qt.Horizontal)
        self.end_slider.setRange(0, 1000)
        self.end_slider.setValue(1000)
        self.end_slider.setEnabled(False)
        self.end_slider.valueChanged.connect(self._on_time_slider_changed)
        slider_row.addWidget(self.end_slider)
        time_layout.addLayout(slider_row)

        layout.addWidget(time_controls)
        layout.addWidget(self._build_y_controls())

        content_splitter = QSplitter(Qt.Horizontal)
        content_splitter.addWidget(self._build_selector_panel())
        content_splitter.addWidget(self._build_plot_panel())
        content_splitter.setStretchFactor(0, 0)
        content_splitter.setStretchFactor(1, 1)
        layout.addWidget(content_splitter)

        self.main_window.station_combo.currentIndexChanged.connect(self.refresh_context)
        self.transaction_combo.currentIndexChanged.connect(self._on_transaction_changed)
        self._on_gap_control_changed()
        self._on_mode_changed()

    def _build_selector_panel(self) -> QWidget:
        panel = QWidget()
        panel_layout = QVBoxLayout(panel)

        self.plot_selectors = []
        for index in range(self.plot_count):
            selector = PlotSelectorWidget(f"Plot {index + 1}")
            self.plot_selectors.append(selector)
            panel_layout.addWidget(selector)

        panel_layout.addStretch()
        return panel

    def _build_plot_panel(self) -> QWidget:
        panel = QWidget()
        panel_layout = QVBoxLayout(panel)

        self.values_label = QLabel(
            "Click on a plot to place up to 2 markers and inspect values plus deltas. "
            "Left click sets the left marker, right click sets the right marker."
        )
        self.values_label.setWordWrap(True)
        self.values_label.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        self.values_label.setTextFormat(Qt.RichText)
        self.values_label.setStyleSheet("QLabel { background: white; padding: 6px; }")
        panel_layout.addWidget(self.values_label)

        # Keep the single-plot view as tall as the original 3-plot area so it
        # remains readable on smaller screens.
        figure_height = max(9, 3 * self.plot_count)
        self.figure = Figure(figsize=(10, figure_height), tight_layout=True)
        self.canvas = FigureCanvas(self.figure)
        self.axes = [self.figure.add_subplot(self.plot_count, 1, 1)]
        self.axes.extend(
            self.figure.add_subplot(self.plot_count, 1, index + 1, sharex=self.axes[0])
            for index in range(1, self.plot_count)
        )
        self.canvas.mpl_connect("button_press_event", self._on_plot_clicked)
        self.canvas.mpl_connect("scroll_event", self._on_plot_scrolled)
        panel_layout.addWidget(self.canvas)
        panel_layout.setStretch(0, 0)
        panel_layout.setStretch(1, 1)
        return panel

    def _build_y_controls(self) -> QWidget:
        group = QGroupBox("Y axis range")
        layout = QHBoxLayout(group)

        layout.addWidget(QLabel("Plot:"))
        self.y_axis_plot_combo = QComboBox()
        for index in range(self.plot_count):
            self.y_axis_plot_combo.addItem(f"Plot {index + 1}", index)
        layout.addWidget(self.y_axis_plot_combo)

        layout.addWidget(QLabel("Y min:"))
        self.y_min_spin = QDoubleSpinBox()
        self.y_min_spin.setDecimals(3)
        self.y_min_spin.setRange(-1_000_000_000, 1_000_000_000)
        self.y_min_spin.setSingleStep(1.0)
        layout.addWidget(self.y_min_spin)

        layout.addWidget(QLabel("Y max:"))
        self.y_max_spin = QDoubleSpinBox()
        self.y_max_spin.setDecimals(3)
        self.y_max_spin.setRange(-1_000_000_000, 1_000_000_000)
        self.y_max_spin.setSingleStep(1.0)
        self.y_max_spin.setValue(100.0)
        layout.addWidget(self.y_max_spin)

        self.apply_y_range_btn = QPushButton("Apply Y range")
        self.apply_y_range_btn.clicked.connect(self._apply_selected_y_range)
        layout.addWidget(self.apply_y_range_btn)

        self.auto_y_range_btn = QPushButton("Auto Y")
        self.auto_y_range_btn.clicked.connect(self._reset_selected_y_range)
        layout.addWidget(self.auto_y_range_btn)

        layout.addStretch()
        return group

    def refresh_context(self):
        self._begin_progress("Refreshing available series...")
        try:
            self._refresh_context_impl()
        finally:
            self._end_progress("Series refreshed.")

    def _refresh_context_impl(self):
        station_id = self.main_window.get_selected_station()
        self._clear_plot_lists()

        if station_id is None:
            self.transaction_combo.blockSignals(True)
            self.transaction_combo.clear()
            self.transaction_combo.addItem("-- select transaction --", None)
            self.transaction_combo.blockSignals(False)
            self.info_label.setText("Select a station first.")
            self._clear_values_label()
            self._disable_time_controls("Select a station first.")
            self._clear_axes("No station selected.")
            return

        self._series_catalog = self.analyzer.get_station_series_catalog(station_id=station_id)
        self._populate_transactions(station_id)
        self._populate_series_lists()
        self._update_info_label()
        self._clear_axes("Select series and click 'Plot selected series'.")

    def _populate_transactions(self, station_id: int):
        transactions = self.analyzer.get_transaction_catalog(station_id=station_id)
        self.transaction_combo.blockSignals(True)
        self.transaction_combo.clear()
        self.transaction_combo.addItem("-- select transaction --", None)
        for tx in transactions:
            label = (
                f"{tx['transaction_id']} "
                f"({(tx.get('start_time') or '')[:16]} - {(tx.get('stop_time') or '')[:16]})"
            )
            self.transaction_combo.addItem(label, tx["transaction_id"])
        self.transaction_combo.blockSignals(False)

    def _populate_series_lists(self):
        items = []
        for source_type in ("imd", "clc", "conditioning", "eos"):
            for entry in self._series_catalog.get(source_type, []):
                points_label = entry.get("points")
                if points_label is None:
                    points_label = "?"
                label = (
                    f"{source_type.upper()} | {entry['metric_name']} | "
                    f"{entry.get('metric_scope', '')} [{entry.get('metric_unit', '')}] "
                    f"({points_label})"
                )
                items.append((label, {"source_type": source_type, **entry}))

        for selector in self.plot_selectors:
            selector.set_items(items)

    def _clear_plot_lists(self):
        for selector in self.plot_selectors:
            selector.clear_items()

    def _selected_series_for_plot(self, plot_index: int):
        return self.plot_selectors[plot_index].selected_payloads()

    def _plot_selected_series(self):
        self._begin_progress("Preparing plots...")
        try:
            self._plot_selected_series_impl()
        finally:
            self._end_progress("Plot updated.")

    def _plot_selected_series_impl(self):
        station_id = self.main_window.get_selected_station()
        if station_id is None:
            self.info_label.setText("Select a station first.")
            return

        mode = self.mode_combo.currentData()
        transaction_id = self.transaction_combo.currentData()
        selections_per_plot = [self._selected_series_for_plot(index) for index in range(self.plot_count)]

        if mode == self.MODE_SESSION and not transaction_id:
            self.info_label.setText("Select an EOS transaction to plot a charging session.")
            self._clear_values_label()
            self._disable_time_controls("Select a transaction to enable time controls.")
            self._clear_axes("No transaction selected.")
            return

        if not any(selections_per_plot):
            self.info_label.setText("Choose at least one series for one of the plots.")
            self._clear_values_label()
            self._disable_time_controls("Select at least one series to enable time controls.")
            self._clear_axes("No series selected.")
            return

        self._current_station_id = station_id
        self._current_mode = mode
        self._current_transaction_id = transaction_id
        self._current_plot_max_points = self.analyzer.DEFAULT_MAX_PLOT_POINTS

        if mode == self.MODE_SESSION:
            self._plot_session_mode(station_id, transaction_id, selections_per_plot)
        else:
            self._plot_full_mode(station_id, selections_per_plot)

    def _plot_full_mode(self, station_id: int, selections_per_plot):
        self._current_session_window = None
        all_times = []
        bundles_per_plot = []
        for axis, selections, index in zip(self.axes, selections_per_plot, range(self.plot_count)):
            axis.clear()
            if not selections:
                axis.set_title(f"Plot {index + 1}")
                axis.text(0.5, 0.5, "No series selected.", ha="center", va="center", transform=axis.transAxes)
                bundles_per_plot.append([])
                continue

            bundle = self.analyzer.get_plot_series(
                selections=selections,
                station_id=station_id,
                max_points=self._current_plot_max_points,
            )
            bundles_per_plot.append(bundle)
            all_times.extend(self._draw_series_bundle(axis, bundle, f"Plot {index + 1}"))
            self._apply_custom_y_range(index)

        self._current_bundles_per_plot = bundles_per_plot
        self._set_full_time_range(all_times)
        self._clear_values_label()
        self._update_detail_button_state()

        self.info_label.setText(
            f"Showing full time horizon for selected station. "
            f"Series are sampled to up to {self._current_plot_max_points} points for faster plotting."
        )
        self._draw_marker_lines()
        self.canvas.draw_idle()

    def _plot_session_mode(self, station_id: int, transaction_id: str, selections_per_plot):
        padding_minutes = self.padding_spin.value()
        window = self.analyzer.get_transaction_window(
            station_id=station_id,
            transaction_id=transaction_id,
            padding_minutes=padding_minutes,
        )
        if window is None:
            self.info_label.setText("The selected transaction has no valid start/stop time.")
            self._clear_values_label()
            self._clear_axes("Invalid transaction window.")
            return

        self._current_session_window = window
        all_times = []
        bundles_per_plot = []
        for axis, selections, index in zip(self.axes, selections_per_plot, range(self.plot_count)):
            axis.clear()
            if not selections:
                axis.set_title(f"Plot {index + 1}")
                axis.text(0.5, 0.5, "No series selected.", ha="center", va="center", transform=axis.transAxes)
                bundles_per_plot.append([])
                continue

            bundle = self.analyzer.get_plot_series(
                selections=selections,
                station_id=station_id,
                transaction_id=transaction_id,
                start_time=window["start_time"],
                end_time=window["stop_time"],
                max_points=self._current_plot_max_points,
            )
            bundles_per_plot.append(bundle)
            all_times.extend(self._draw_series_bundle(axis, bundle, f"Plot {index + 1}"))
            axis.axvline(self._parse_timestamp(window["session_start_time"]), color="green", linestyle="--", linewidth=1)
            axis.axvline(self._parse_timestamp(window["session_stop_time"]), color="red", linestyle="--", linewidth=1)
            self._apply_custom_y_range(index)

        self._current_bundles_per_plot = bundles_per_plot
        all_times.extend(
            [
                self._parse_timestamp(window["start_time"]),
                self._parse_timestamp(window["stop_time"]),
            ]
        )
        self._set_full_time_range(all_times)
        self._clear_values_label()
        self._update_detail_button_state()

        self.info_label.setText(
            f"Showing transaction {transaction_id} with +/- {padding_minutes} min context. "
            f"Series are sampled to up to {self._current_plot_max_points} points for faster plotting."
        )
        self._draw_marker_lines()
        self.canvas.draw_idle()

    def _draw_series_bundle(self, axis, bundle, title: str):
        plotted = 0
        unit = None
        all_times = []
        contains_state_series = False
        for series in bundle:
            points = series.get("points", [])
            if not points:
                continue

            x_values = [self._parse_timestamp(point["timestamp"]) for point in points]
            y_values = [point["metric_value"] for point in points]
            label = f"{series['source_type'].upper()} | {series['metric_name']} | {series.get('metric_scope', '')}"
            self._plot_series_with_gap_handling(axis, x_values, y_values, label)
            all_times.extend(x_values)
            if points[0].get("metric_unit"):
                unit = points[0]["metric_unit"]
            if self._is_state_unit(unit):
                contains_state_series = True
            plotted += 1

        axis.set_title(title)
        axis.set_xlabel("Timestamp")
        axis.set_ylabel(unit or "Value")
        if contains_state_series:
            self._configure_state_axis(axis)
        axis.grid(True, alpha=0.3)
        axis.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
        if plotted:
            axis.legend(fontsize=8, loc="best")
            axis.tick_params(axis="x", rotation=20)
        else:
            axis.text(0.5, 0.5, "No points available for current selection.", ha="center", va="center", transform=axis.transAxes)
        return all_times

    def _clear_axes(self, message: str):
        self._current_bundles_per_plot = [[] for _ in range(self.plot_count)]
        self._clear_markers()
        self._full_time_range = None
        self._current_time_range = None
        self._current_session_window = None
        self._update_detail_button_state()
        for index, axis in enumerate(self.axes):
            axis.clear()
            axis.set_title(f"Plot {index + 1}")
            axis.text(0.5, 0.5, message, ha="center", va="center", transform=axis.transAxes)
            axis.set_xticks([])
            axis.set_yticks([])
        self.canvas.draw_idle()

    def _set_full_time_range(self, timestamps):
        if not timestamps:
            self._disable_time_controls("No timestamps available for the selected data.")
            return
        min_time = min(timestamps)
        max_time = max(timestamps)
        self._full_time_range = (min_time, max_time)
        self._current_time_range = (min_time, max_time)
        self._apply_shared_time_range(min_time, max_time)
        self._enable_time_controls()
        self._sync_sliders_to_time_range()

    def _apply_shared_time_range(self, start_time: datetime, end_time: datetime):
        self._current_time_range = (start_time, end_time)
        for axis in self.axes:
            axis.set_xlim(start_time, end_time)

    def _enable_time_controls(self):
        self.start_slider.setEnabled(True)
        self.end_slider.setEnabled(True)
        self._update_time_range_label()

    def _disable_time_controls(self, message: str):
        self.start_slider.setEnabled(False)
        self.end_slider.setEnabled(False)
        self.time_range_label.setText(message)

    def _update_time_range_label(self):
        if self._current_time_range is None or self._full_time_range is None:
            self.time_range_label.setText("Load data to enable time range controls.")
            return
        self.time_range_label.setText(
            "Visible: "
            f"{self._format_time(self._current_time_range[0])} -> {self._format_time(self._current_time_range[1])} | "
            "Full: "
            f"{self._format_time(self._full_time_range[0])} -> {self._format_time(self._full_time_range[1])}"
        )

    def _on_time_slider_changed(self):
        if self._full_time_range is None:
            return

        start_value = self.start_slider.value()
        end_value = self.end_slider.value()
        if start_value >= end_value:
            sender = self.sender()
            if sender is self.start_slider:
                self.end_slider.blockSignals(True)
                self.end_slider.setValue(min(1000, start_value + 1))
                self.end_slider.blockSignals(False)
                end_value = self.end_slider.value()
            else:
                self.start_slider.blockSignals(True)
                self.start_slider.setValue(max(0, end_value - 1))
                self.start_slider.blockSignals(False)
                start_value = self.start_slider.value()

        start_time = self._slider_value_to_time(start_value)
        end_time = self._slider_value_to_time(end_value)
        self._apply_shared_time_range(start_time, end_time)
        self._update_time_range_label()
        self.canvas.draw_idle()

    def _sync_sliders_to_time_range(self):
        if self._current_time_range is None or self._full_time_range is None:
            return
        start_value = self._time_to_slider_value(self._current_time_range[0])
        end_value = self._time_to_slider_value(self._current_time_range[1])
        if start_value >= end_value:
            end_value = min(1000, start_value + 1)
        self.start_slider.blockSignals(True)
        self.end_slider.blockSignals(True)
        self.start_slider.setValue(start_value)
        self.end_slider.setValue(end_value)
        self.start_slider.blockSignals(False)
        self.end_slider.blockSignals(False)
        self._update_time_range_label()

    def _update_detail_button_state(self):
        self.more_detail_btn.setEnabled(
            self._current_time_range is not None and any(self._current_bundles_per_plot)
        )

    def _apply_selected_y_range(self):
        plot_index = self.y_axis_plot_combo.currentData()
        y_min = self.y_min_spin.value()
        y_max = self.y_max_spin.value()
        if y_min >= y_max:
            self.info_label.setText("Y min must be smaller than Y max.")
            return

        self._custom_y_ranges[plot_index] = (y_min, y_max)
        self._apply_custom_y_range(plot_index)
        self.canvas.draw_idle()
        self.info_label.setText(
            f"Applied Y range [{y_min}, {y_max}] to Plot {plot_index + 1}."
        )

    def _reset_selected_y_range(self):
        plot_index = self.y_axis_plot_combo.currentData()
        self._custom_y_ranges.pop(plot_index, None)
        self.axes[plot_index].relim()
        self.axes[plot_index].autoscale_view(scalex=False, scaley=True)
        self.canvas.draw_idle()
        self.info_label.setText(f"Restored automatic Y range for Plot {plot_index + 1}.")

    def _apply_custom_y_range(self, plot_index: int):
        y_range = self._custom_y_ranges.get(plot_index)
        if y_range is None:
            return
        self.axes[plot_index].set_ylim(*y_range)

    def _load_more_detail(self):
        self._begin_progress("Loading more points for the visible range...")
        try:
            self._load_more_detail_impl()
        finally:
            self._end_progress("More detail loaded.")

    def _load_more_detail_impl(self):
        if self._current_station_id is None or self._current_time_range is None:
            return

        selections_per_plot = [self._selected_series_for_plot(index) for index in range(self.plot_count)]
        if not any(selections_per_plot):
            return

        start_time = self._current_time_range[0].isoformat()
        end_time = self._current_time_range[1].isoformat()
        next_max_points = min(
            self.MAX_DETAIL_POINTS,
            max(self._current_plot_max_points + 1, self._current_plot_max_points * self.DETAIL_MULTIPLIER),
        )
        if next_max_points == self._current_plot_max_points:
            self.info_label.setText(
                f"Already at the maximum detail level ({self._current_plot_max_points} points per series)."
            )
            return

        self._current_plot_max_points = next_max_points
        all_times = []
        bundles_per_plot = []
        for axis, selections, index in zip(self.axes, selections_per_plot, range(self.plot_count)):
            axis.clear()
            if not selections:
                axis.set_title(f"Plot {index + 1}")
                axis.text(0.5, 0.5, "No series selected.", ha="center", va="center", transform=axis.transAxes)
                bundles_per_plot.append([])
                continue

            bundle = self.analyzer.get_plot_series(
                selections=selections,
                station_id=self._current_station_id,
                transaction_id=self._current_transaction_id if self._current_mode == self.MODE_SESSION else None,
                start_time=start_time,
                end_time=end_time,
                max_points=self._current_plot_max_points,
            )
            bundles_per_plot.append(bundle)
            all_times.extend(self._draw_series_bundle(axis, bundle, f"Plot {index + 1}"))
            if self._current_session_window is not None:
                axis.axvline(
                    self._parse_timestamp(self._current_session_window["session_start_time"]),
                    color="green",
                    linestyle="--",
                    linewidth=1,
                )
                axis.axvline(
                    self._parse_timestamp(self._current_session_window["session_stop_time"]),
                    color="red",
                    linestyle="--",
                    linewidth=1,
                )
            self._apply_custom_y_range(index)

        self._current_bundles_per_plot = bundles_per_plot
        if all_times:
            self._set_full_time_range(all_times)
            self._apply_shared_time_range(min(all_times), max(all_times))
            self._sync_sliders_to_time_range()
        self._clear_values_label()
        self._update_detail_button_state()
        self.info_label.setText(
            f"Loaded more detail for the visible range. "
            f"Current limit: {self._current_plot_max_points} points per series."
        )
        self._draw_marker_lines()
        self.canvas.draw_idle()

    def _on_mode_changed(self):
        session_mode = self.mode_combo.currentData() == self.MODE_SESSION
        self.transaction_combo.setEnabled(session_mode)
        self.padding_spin.setEnabled(session_mode)
        self._update_info_label()

    def _on_transaction_changed(self):
        if self.mode_combo.currentData() == self.MODE_SESSION:
            self._update_info_label()

    def _update_info_label(self):
        station_id = self.main_window.get_selected_station()
        if station_id is None:
            self.info_label.setText("Select a station first.")
            return

        mode = self.mode_combo.currentData()
        if mode == self.MODE_SESSION:
            transaction_id = self.transaction_combo.currentData()
            if transaction_id:
                self.info_label.setText(
                    f"Session mode for transaction {transaction_id}. Choose series for each plot."
                )
            else:
                self.info_label.setText("Session mode: choose an EOS transaction and select series.")
            return

        self.info_label.setText(
            "Full horizon mode: choose any IMD, CLC, Conditioning or EOS series for each plot."
        )

    def _parse_timestamp(self, value: str):
        return datetime.fromisoformat(value)

    def _on_plot_clicked(self, event):
        if event.inaxes not in self.axes or event.xdata is None:
            return
        if not any(self._current_bundles_per_plot):
            return

        selected_time = mdates.num2date(event.xdata).replace(tzinfo=None)
        clicked_plot_index = self.axes.index(event.inaxes)
        snapped_point = None
        if self.snap_to_sample_checkbox.isChecked():
            snapped_point = self._find_nearest_displayed_point(
                self._current_bundles_per_plot[clicked_plot_index],
                selected_time,
                event.ydata,
                event.inaxes,
            )
            if snapped_point is None:
                self.info_label.setText("No displayed sample close enough to the click position.")
                return
            selected_time = self._parse_timestamp(snapped_point["timestamp"])

        button = getattr(event.button, "value", event.button)
        if button == 1:
            self._set_marker(selected_time, marker_index=0)
        elif button == 3:
            self._set_marker(selected_time, marker_index=1)
        else:
            return

        if self.snap_to_sample_checkbox.isChecked():
            self._show_point_logs(selected_time)
        self.canvas.draw_idle()

    def _on_plot_scrolled(self, event):
        if event.inaxes not in self.axes or event.xdata is None:
            return
        if self._current_time_range is None or self._full_time_range is None:
            return

        current_start, current_end = self._current_time_range
        current_start_num = mdates.date2num(current_start)
        current_end_num = mdates.date2num(current_end)
        anchor = event.xdata
        current_width = current_end_num - current_start_num
        if current_width <= 0:
            return

        zoom_factor = 0.8 if event.button == "up" else 1.25
        new_width = current_width * zoom_factor

        full_start_num = mdates.date2num(self._full_time_range[0])
        full_end_num = mdates.date2num(self._full_time_range[1])
        full_width = full_end_num - full_start_num
        min_width = max(full_width / 1000.0, 1e-9)
        new_width = max(min_width, min(full_width, new_width))

        if isclose(current_width, 0.0):
            return
        anchor_ratio = (anchor - current_start_num) / current_width
        new_start = anchor - (new_width * anchor_ratio)
        new_end = new_start + new_width

        if new_start < full_start_num:
            new_end += full_start_num - new_start
            new_start = full_start_num
        if new_end > full_end_num:
            new_start -= new_end - full_end_num
            new_end = full_end_num
        new_start = max(full_start_num, new_start)
        new_end = min(full_end_num, new_end)

        if new_end - new_start < min_width:
            return

        self._apply_shared_time_range(
            mdates.num2date(new_start).replace(tzinfo=None),
            mdates.num2date(new_end).replace(tzinfo=None),
        )
        self._sync_sliders_to_time_range()
        self.canvas.draw_idle()

    def _set_marker(self, selected_time: datetime, marker_index: int):
        while len(self._marker_times) <= marker_index:
            self._marker_times.append(None)
        self._marker_times[marker_index] = selected_time
        self._draw_marker_lines()
        self._update_values_label()

    def _draw_marker_lines(self):
        stale_axes = [axis for axis in list(self._marker_lines) if axis not in self.axes]
        for axis in stale_axes:
            lines = self._marker_lines.pop(axis)
            self._remove_lines(lines)

        for axis in self.axes:
            lines = self._marker_lines.get(axis, [])
            self._remove_lines(lines)
            new_lines = []
            for index, marker_time in enumerate(self._marker_times):
                if marker_time is None:
                    continue
                color = self.MARKER_LEFT_COLOR if index == 0 else self.MARKER_RIGHT_COLOR
                line = axis.axvline(marker_time, color=color, linestyle=":", linewidth=1.2)
                new_lines.append(line)
            self._marker_lines[axis] = new_lines

    def _clear_markers(self):
        self._marker_times = []
        for lines in self._marker_lines.values():
            self._remove_lines(lines)
        self._marker_lines = {}
        if self._point_logs_dialog is not None:
            self._point_logs_dialog.hide()
        self._clear_values_label()
        self.canvas.draw_idle()

    def _remove_lines(self, lines):
        for line in lines:
            try:
                line.remove()
            except (ValueError, NotImplementedError):
                try:
                    line.set_visible(False)
                except Exception:
                    pass

    def _update_values_label(self):
        active_marker_count = len([marker for marker in self._marker_times if marker is not None])
        if active_marker_count == 0:
            self._clear_values_label()
            return

        left_lines = []
        delta_lines = []
        right_lines = []
        marker_labels = ["Left marker", "Right marker"]
        marker_targets = [left_lines, right_lines]
        for index, marker_time in enumerate(self._marker_times):
            if marker_time is None:
                continue
            marker_targets[index].append(
                f"<b>{marker_labels[index]}</b>: {marker_time.isoformat(sep=' ', timespec='milliseconds')}"
            )
        if active_marker_count == 2 and self._marker_times[0] is not None and self._marker_times[1] is not None:
            delta_seconds = (self._marker_times[1] - self._marker_times[0]).total_seconds()
            delta_lines.append(f"<b>Delta time</b>: {delta_seconds:.3f} s")

        for plot_index, bundle in enumerate(self._current_bundles_per_plot, start=1):
            left_lines.append(f"<br><b>Plot {plot_index}</b>")
            delta_lines.append(f"<br><b>Plot {plot_index}</b>")
            right_lines.append(f"<br><b>Plot {plot_index}</b>")
            series_found = False
            for series in bundle:
                marker_points = [
                    self._find_nearest_point(series.get("points", []), marker_time) if marker_time is not None else None
                    for marker_time in self._marker_times
                ]
                if marker_points[0] is None and marker_points[1] is None:
                    continue
                series_found = True
                scope = series.get("metric_scope") or "-"
                series_label = self._escape_html(
                    f"{series['source_type'].upper()} | {series['metric_name']} | {scope}"
                )
                for index, point in enumerate(marker_points):
                    if point is None:
                        continue
                    value = self._format_marker_value(point.get("metric_value"), point.get("metric_unit"))
                    unit = point.get("metric_unit") or ""
                    value_text = self._escape_html(value)
                    unit_text = self._escape_html(self._format_marker_unit(unit))
                    marker_targets[index].append(
                        f"{series_label} = {value_text}{unit_text}"
                        f"<br><span style='color:#666;'>@ {self._escape_html(point['timestamp'])}</span>"
                    )
                if marker_points[0] is not None and marker_points[1] is not None:
                    unit = marker_points[0].get("metric_unit") or marker_points[1].get("metric_unit") or ""
                    delta_value = marker_points[1].get("metric_value") - marker_points[0].get("metric_value")
                    delta_text = self._escape_html(self._format_delta_value(delta_value, unit))
                    delta_unit = self._escape_html(self._format_delta_unit(unit))
                    delta_lines.append(f"{series_label} = {delta_text}{delta_unit}")
            if not series_found:
                if self._marker_times[0] is not None:
                    left_lines.append("No displayed data.")
                if self._marker_times[0] is not None and self._marker_times[1] is not None:
                    delta_lines.append("No displayed data.")
                if len(self._marker_times) > 1 and self._marker_times[1] is not None:
                    right_lines.append("No displayed data.")

        self.values_label.setText(
            self._build_marker_summary_html(left_lines, delta_lines, right_lines)
        )

    def _find_nearest_point(self, points, selected_time: datetime):
        if not points:
            return None
        if self.step_plot_checkbox.isChecked():
            return self._find_active_step_point(points, selected_time)
        return min(
            points,
            key=lambda point: abs(self._parse_timestamp(point["timestamp"]) - selected_time),
        )

    def _find_active_step_point(self, points, selected_time: datetime):
        active_point = None
        for point in points:
            point_time = self._parse_timestamp(point["timestamp"])
            if point_time <= selected_time:
                active_point = point
                continue
            break
        return active_point or points[0]

    def _find_nearest_displayed_point(self, bundle, selected_time: datetime, selected_value, axis):
        if not bundle:
            return None

        if self.step_plot_checkbox.isChecked():
            return self._find_active_displayed_step_point(bundle, selected_time)

        x_min_num, x_max_num = axis.get_xlim()
        x_span_seconds = max((x_max_num - x_min_num) * 86400.0, 1e-9)
        y_min, y_max = axis.get_ylim()
        y_span = max(y_max - y_min, 1e-9)

        closest_point = None
        closest_score = None
        for series in bundle:
            for point in series.get("points", []):
                point_time = self._parse_timestamp(point["timestamp"])
                dx = abs((point_time - selected_time).total_seconds()) / x_span_seconds
                if selected_value is None or point.get("metric_value") is None:
                    dy = 0.0
                else:
                    dy = abs(point["metric_value"] - selected_value) / y_span
                score = (dx * dx + dy * dy) ** 0.5
                if closest_score is None or score < closest_score:
                    closest_score = score
                    closest_point = point

        if closest_score is None or closest_score > self._snap_distance_threshold:
            return None
        return closest_point

    def _find_active_displayed_step_point(self, bundle, selected_time: datetime):
        best_point = None
        best_time = None
        for series in bundle:
            point = self._find_active_step_point(series.get("points", []), selected_time)
            if point is None:
                continue
            point_time = self._parse_timestamp(point["timestamp"])
            if best_time is None or point_time > best_time:
                best_point = point
                best_time = point_time
        return best_point

    def _show_point_logs(self, selected_time: datetime):
        if self._current_station_id is None:
            return

        self._begin_progress("Loading logs near the selected point...")
        try:
            entries = self.analyzer.get_logs_near_timestamp(
                station_id=self._current_station_id,
                center_time=selected_time.isoformat(),
                transaction_id=self._current_transaction_id if self._current_mode == self.MODE_SESSION else None,
            )
            if self._point_logs_dialog is None:
                self._point_logs_dialog = PointLogsDialog(self)
            self._point_logs_dialog.update_entries(selected_time, entries)
            self._point_logs_dialog.show()
            self._point_logs_dialog.raise_()
            self._point_logs_dialog.activateWindow()
        finally:
            self._end_progress("Logs near the selected point loaded.")

    def _clear_values_label(self):
        self.values_label.setText(
            "Click on a plot to place up to 2 markers and inspect values plus deltas. "
            "Left click sets the left marker, right click sets the right marker."
        )

    def _build_marker_summary_html(self, left_lines, delta_lines, right_lines):
        sections = [
            (self.MARKER_LEFT_COLOR, left_lines),
            (self.MARKER_DELTA_COLOR, delta_lines),
            (self.MARKER_RIGHT_COLOR, right_lines),
        ]
        cells = []
        for color, lines in sections:
            body = "<br>".join(lines) if lines else "<span style='color:#888;'>No marker</span>"
            cells.append(
                "<td style='vertical-align:top; width:33%; padding-right:18px;'>"
                f"<div style='border-top:4px solid {color}; padding-top:6px;'>{body}</div>"
                "</td>"
            )
        return "<table style='width:100%;'><tr>" + "".join(cells) + "</tr></table>"

    def _on_gap_control_changed(self):
        self.gap_threshold_combo.setEnabled(self.gap_checkbox.isChecked())
        self._gap_threshold_seconds = self._parse_gap_threshold_seconds(self.gap_threshold_combo.currentText())
        self._redraw_current_bundles()

    def _parse_gap_threshold_seconds(self, text: str):
        normalized = text.strip().lower()
        if not normalized:
            return None

        parts = normalized.split()
        if len(parts) != 2:
            return None

        try:
            value = float(parts[0].replace(",", "."))
        except ValueError:
            return None

        unit = parts[1]
        multipliers = {
            "s": 1,
            "sec": 1,
            "secs": 1,
            "second": 1,
            "seconds": 1,
            "min": 60,
            "mins": 60,
            "minute": 60,
            "minutes": 60,
            "h": 3600,
            "hr": 3600,
            "hrs": 3600,
            "hour": 3600,
            "hours": 3600,
        }
        multiplier = multipliers.get(unit)
        if multiplier is None:
            return None
        return value * multiplier

    def _plot_series_with_gap_handling(self, axis, x_values, y_values, label: str):
        threshold_seconds = self._gap_threshold_seconds if self.gap_checkbox.isChecked() else None
        drawstyle = "steps-post" if self.step_plot_checkbox.isChecked() else "default"
        if threshold_seconds is None or threshold_seconds <= 0:
            axis.plot(x_values, y_values, label=label, linewidth=1.2, drawstyle=drawstyle)
            return

        segments = []
        current_x = [x_values[0]]
        current_y = [y_values[0]]
        for previous_time, current_time, current_value in zip(x_values, x_values[1:], y_values[1:]):
            delta_seconds = (current_time - previous_time).total_seconds()
            if delta_seconds > threshold_seconds:
                segments.append((current_x, current_y))
                current_x = [current_time]
                current_y = [current_value]
                continue
            current_x.append(current_time)
            current_y.append(current_value)
        segments.append((current_x, current_y))

        first_segment = True
        series_color = None
        for segment_x, segment_y in segments:
            segment_label = label if first_segment else "_nolegend_"
            if len(segment_x) == 1:
                line_kwargs = {
                    "linestyle": "None",
                    "marker": "o",
                    "markersize": 4,
                    "label": segment_label,
                }
                if series_color is not None:
                    line_kwargs["color"] = series_color
                line = axis.plot(segment_x, segment_y, **line_kwargs)[0]
            else:
                line_kwargs = {
                    "linewidth": 1.2,
                    "label": segment_label,
                    "drawstyle": drawstyle,
                }
                if series_color is not None:
                    line_kwargs["color"] = series_color
                line = axis.plot(segment_x, segment_y, **line_kwargs)[0]
            if series_color is None:
                series_color = line.get_color()
            first_segment = False

    def _redraw_current_bundles(self):
        if not any(self._current_bundles_per_plot):
            return

        for axis, bundle, index in zip(self.axes, self._current_bundles_per_plot, range(self.plot_count)):
            axis.clear()
            if not bundle:
                axis.set_title(f"Plot {index + 1}")
                axis.text(0.5, 0.5, "No series selected.", ha="center", va="center", transform=axis.transAxes)
                continue

            self._draw_series_bundle(axis, bundle, f"Plot {index + 1}")
            if self._current_session_window is not None:
                axis.axvline(
                    self._parse_timestamp(self._current_session_window["session_start_time"]),
                    color="green",
                    linestyle="--",
                    linewidth=1,
                )
                axis.axvline(
                    self._parse_timestamp(self._current_session_window["session_stop_time"]),
                    color="red",
                    linestyle="--",
                    linewidth=1,
                )
            self._apply_custom_y_range(index)

        if self._current_time_range is not None:
            self._apply_shared_time_range(*self._current_time_range)
        self._draw_marker_lines()
        self.canvas.draw_idle()

    def _is_state_unit(self, unit):
        return (unit or "").strip().lower() == "state"

    def _configure_state_axis(self, axis):
        axis.set_ylabel("State")
        axis.set_yticks([0.0, 1.0])
        axis.set_yticklabels(["OFF", "ON"])
        current_limits = axis.get_ylim()
        axis.set_ylim(min(current_limits[0], -0.1), max(current_limits[1], 1.1))

    def _format_marker_value(self, value, unit):
        if value is None:
            return "-"
        if self._is_state_unit(unit):
            return "ON" if value >= 0.5 else "OFF"
        return f"{value:.3f}"

    def _format_marker_unit(self, unit):
        if not unit or self._is_state_unit(unit):
            return ""
        return f" {unit}"

    def _format_delta_value(self, delta_value, unit):
        if self._is_state_unit(unit):
            if delta_value > 0:
                return "OFF -> ON"
            if delta_value < 0:
                return "ON -> OFF"
            return "no change"
        return f"{delta_value:+.3f}"

    def _format_delta_unit(self, unit):
        if not unit or self._is_state_unit(unit):
            return ""
        return f" {unit}"

    def _escape_html(self, value):
        text = "" if value is None else str(value)
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    def _slider_value_to_time(self, slider_value: int):
        full_start, full_end = self._full_time_range
        full_start_num = mdates.date2num(full_start)
        full_end_num = mdates.date2num(full_end)
        ratio = slider_value / 1000.0
        return mdates.num2date(full_start_num + ((full_end_num - full_start_num) * ratio)).replace(tzinfo=None)

    def _time_to_slider_value(self, timestamp: datetime):
        full_start, full_end = self._full_time_range
        full_start_num = mdates.date2num(full_start)
        full_end_num = mdates.date2num(full_end)
        timestamp_num = mdates.date2num(timestamp)
        if isclose(full_end_num, full_start_num):
            return 0
        ratio = (timestamp_num - full_start_num) / (full_end_num - full_start_num)
        return max(0, min(1000, int(round(ratio * 1000))))

    def _format_time(self, timestamp: datetime):
        return timestamp.isoformat(sep=" ", timespec="seconds")

    def _begin_progress(self, message: str):
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setValue(-1)
        if hasattr(self.main_window, "begin_busy"):
            self.main_window.begin_busy(message)
        else:
            self.main_window.status_bar.showMessage(message)
        QtWidgets.QApplication.processEvents()

    def _end_progress(self, message: str = "Ready"):
        self.progress_bar.setVisible(False)
        if hasattr(self.main_window, "end_busy"):
            self.main_window.end_busy(message)
        else:
            self.main_window.status_bar.showMessage(message)
        QtWidgets.QApplication.processEvents()


class AnalysisTab(BaseAnalysisTab):
    """Three-plot visualization tab."""

    def __init__(self, main_window):
        super().__init__(main_window, plot_count=3)


class SinglePlotAnalysisTab(BaseAnalysisTab):
    """Single-plot visualization tab."""

    def __init__(self, main_window):
        super().__init__(main_window, plot_count=1)
