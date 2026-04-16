"""Shared data access for visualization across IMD, CLC and EOS logs."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Optional

from src.modules.can_logs import CANLogParser
from src.modules.clc_logs import CLCLogParser
from src.modules.conditioning_logs import ConditioningLogParser
from src.modules.eos_logs import EOSLogParser


class DataAnalyzer:
    """Provide a unified API for time-series visualization."""

    DEFAULT_MAX_PLOT_POINTS = 5000

    def __init__(
        self,
        can_parser: Optional[CANLogParser] = None,
        clc_parser: Optional[CLCLogParser] = None,
        conditioning_parser: Optional[ConditioningLogParser] = None,
        eos_parser: Optional[EOSLogParser] = None,
    ):
        self.can_parser = can_parser or CANLogParser()
        self.clc_parser = clc_parser or CLCLogParser()
        self.conditioning_parser = conditioning_parser or ConditioningLogParser()
        self.eos_parser = eos_parser or EOSLogParser()

    def get_station_series_catalog(self, station_id: int) -> Dict[str, List[dict]]:
        """List all currently available numeric series for a station."""
        return {
            "imd": self.can_parser.get_available_series(station_id=station_id),
            "clc": self.clc_parser.get_available_series(station_id=station_id),
            "conditioning": self.conditioning_parser.get_available_series(station_id=station_id),
            "eos": self.eos_parser.get_available_series(station_id=station_id),
        }

    def get_transaction_catalog(self, station_id: int) -> List[dict]:
        """List EOS charging sessions for a station."""
        return self.eos_parser.get_transactions(station_id=station_id)

    def get_transaction_window(
        self,
        station_id: int,
        transaction_id: str,
        padding_minutes: int = 0,
    ) -> Optional[dict]:
        """Return the time window for a selected EOS transaction."""
        transactions = self.eos_parser.get_transactions(station_id=station_id)
        match = next((tx for tx in transactions if tx["transaction_id"] == transaction_id), None)
        if not match:
            return None

        start_time = self._parse_iso_datetime(match.get("start_time"))
        stop_time = self._parse_iso_datetime(match.get("stop_time"))
        if start_time is None or stop_time is None:
            bounds = self.eos_parser.get_transaction_time_bounds(
                transaction_id=transaction_id,
                station_id=station_id,
            )
            if bounds is not None:
                if start_time is None:
                    start_time = self._parse_iso_datetime(bounds["start_time"])
                if stop_time is None:
                    stop_time = self._parse_iso_datetime(bounds["stop_time"])
        if start_time is None or stop_time is None:
            return None

        padding = timedelta(minutes=padding_minutes)
        return {
            "transaction_id": transaction_id,
            "start_time": (start_time - padding).isoformat(),
            "stop_time": (stop_time + padding).isoformat(),
            "session_start_time": start_time.isoformat(),
            "session_stop_time": stop_time.isoformat(),
        }

    def get_plot_series(
        self,
        selections: List[dict],
        station_id: int,
        transaction_id: str = None,
        start_time: str = None,
        end_time: str = None,
        max_points: int = DEFAULT_MAX_PLOT_POINTS,
    ) -> List[dict]:
        """Fetch selected series from all supported sources in a common format."""
        results = []
        for selection in selections:
            source_type = selection.get("source_type")
            metric_name = selection.get("metric_name")
            metric_scope = selection.get("metric_scope")

            if source_type == "imd":
                points = self.can_parser.get_series_data(
                    metric_name=metric_name,
                    metric_scope=metric_scope,
                    station_id=station_id,
                    start_time=start_time,
                    end_time=end_time,
                    max_points=max_points,
                )
            elif source_type == "clc":
                points = self.clc_parser.get_series_data(
                    metric_name=metric_name,
                    metric_scope=metric_scope,
                    station_id=station_id,
                    start_time=start_time,
                    end_time=end_time,
                    max_points=max_points,
                )
            elif source_type == "conditioning":
                points = self.conditioning_parser.get_series_data(
                    metric_name=metric_name,
                    metric_scope=metric_scope,
                    station_id=station_id,
                    start_time=start_time,
                    end_time=end_time,
                    max_points=max_points,
                )
            elif source_type == "eos":
                points = self.eos_parser.get_series_data(
                    metric_name=metric_name,
                    station_id=station_id,
                    transaction_id=transaction_id,
                    start_time=start_time,
                    end_time=end_time,
                    max_points=max_points,
                )
            else:
                points = []

            results.append(
                {
                    "source_type": source_type,
                    "metric_name": metric_name,
                    "metric_scope": metric_scope,
                    "points": points,
                }
            )
        return results

    def get_transaction_plot_series(
        self,
        station_id: int,
        transaction_id: str,
        selections: List[dict],
        padding_minutes: int = 0,
    ) -> dict:
        """Fetch selected series constrained to a chosen EOS transaction window."""
        window = self.get_transaction_window(
            station_id=station_id,
            transaction_id=transaction_id,
            padding_minutes=padding_minutes,
        )
        if window is None:
            return {"window": None, "series": []}

        series = self.get_plot_series(
            selections=selections,
            station_id=station_id,
            transaction_id=transaction_id,
            start_time=window["start_time"],
            end_time=window["stop_time"],
        )
        return {"window": window, "series": series}

    def get_logs_near_timestamp(
        self,
        station_id: int,
        center_time: str,
        transaction_id: str = None,
        limit_per_source: int = 40,
        max_distance_seconds: float = 300.0,
    ) -> List[dict]:
        """Collect raw log entries from all sources near a selected timestamp."""
        center_dt = self._parse_iso_datetime(center_time)
        if center_dt is None:
            return []

        entries: List[dict] = []

        for row in self.can_parser.get_entries_near_timestamp(
            center_time=center_time,
            station_id=station_id,
            limit=limit_per_source,
        ):
            timestamp = self._parse_iso_datetime(row["timestamp"])
            if timestamp is None:
                continue
            entries.append(
                {
                    "source_type": "imd",
                    "timestamp": row["timestamp"],
                    "file_name": row.get("file_name"),
                    "context": row.get("can_id"),
                    "level": "",
                    "message": row.get("description") or row.get("raw_data") or "",
                    "delta_seconds": abs((timestamp - center_dt).total_seconds()),
                }
            )

        for row in self.clc_parser.get_entries_near_timestamp(
            center_time=center_time,
            station_id=station_id,
            limit=limit_per_source,
        ):
            timestamp = self._parse_iso_datetime(row["timestamp"])
            if timestamp is None:
                continue
            entries.append(
                {
                    "source_type": "clc",
                    "timestamp": row["timestamp"],
                    "file_name": row.get("file_name"),
                    "context": row.get("logger_name"),
                    "level": row.get("level") or "",
                    "message": row.get("message") or row.get("raw_line") or "",
                    "delta_seconds": abs((timestamp - center_dt).total_seconds()),
                }
            )

        for row in self.conditioning_parser.get_entries_near_timestamp(
            center_time=center_time,
            station_id=station_id,
            limit=limit_per_source,
        ):
            timestamp = self._parse_iso_datetime(row["timestamp"])
            if timestamp is None:
                continue
            entries.append(
                {
                    "source_type": "conditioning",
                    "timestamp": row["timestamp"],
                    "file_name": row.get("file_name"),
                    "context": row.get("logger_name"),
                    "level": row.get("level") or "",
                    "message": row.get("message") or row.get("raw_line") or "",
                    "delta_seconds": abs((timestamp - center_dt).total_seconds()),
                }
            )

        for row in self.eos_parser.get_entries_near_timestamp(
            center_time=center_time,
            station_id=station_id,
            transaction_id=transaction_id,
            limit=limit_per_source,
        ):
            timestamp = self._parse_iso_datetime(row["timestamp"])
            if timestamp is None:
                continue
            entries.append(
                {
                    "source_type": "eos",
                    "timestamp": row["timestamp"],
                    "file_name": row.get("file_name"),
                    "context": row.get("transaction_id") or "session",
                    "level": "",
                    "message": row.get("message") or "",
                    "delta_seconds": abs((timestamp - center_dt).total_seconds()),
                }
            )

        entries.sort(key=lambda entry: (entry["delta_seconds"], entry["timestamp"], entry["source_type"]))
        if max_distance_seconds is not None:
            entries = [
                entry
                for entry in entries
                if entry["delta_seconds"] <= max_distance_seconds
            ]
        return entries

    def _parse_iso_datetime(self, value: str) -> Optional[datetime]:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
