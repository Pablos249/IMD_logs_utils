"""Module for loading and parsing CLC log files."""

from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import os
import re
import sqlite3
from typing import Callable, Dict, List, Optional

from src.app_paths import database_path


class CLCLogParser:
    """Parser for CLC logs with simple metric extraction."""

    LINE_PATTERN = re.compile(
        r"^\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\] "
        r"\[(?P<logger>[^\]]+)\] \[(?P<level>[^\]]+)\] (?P<message>.*)$"
    )
    MAX_LIMITS_PATTERN = re.compile(
        r"Max (?P<kind>OCPP|DLBS) global limits - "
        r"P: (?P<power>-?\d+(?:\.\d+)?) \[kW\], "
        r"U: (?P<voltage>-?\d+(?:\.\d+)?) \[V\], "
        r"I: (?P<current>-?\d+(?:\.\d+)?) \[A\]"
    )
    CALCULATE_CURRENT_PATTERN = re.compile(
        r"\[(?P<side>master|slave)\] Calculate max current: "
        r"(?P<power>-?\d+(?:\.\d+)?) kW / (?P<voltage>-?\d+(?:\.\d+)?) V = "
        r"(?P<current>-?\d+(?:\.\d+)?) A"
    )
    RECTIFIER_PATTERN = re.compile(
        r"ADDRESS: (?P<address>\d+)\. Params on rectifier module "
        r"\(bus: '(?P<bus>[^']+)'\) isEnabled: (?P<enabled>true|false), "
        r"REAL: U=(?P<real_voltage>-?\d+(?:\.\d+)?) \[V\], "
        r"I=(?P<real_current>-?\d+(?:\.\d+)?) \[A\], "
        r"REQUEST: U=\s*(?P<request_voltage>-?\d+(?:\.\d+)?) \[V\], "
        r"I=\s*(?P<request_current>-?\d+(?:\.\d+)?) \[A\], "
        r"status: (?P<status>\d+), .* temp: (?P<temperature>-?\d+(?:\.\d+)?) \[C\]\."
    )
    CCS_REAL_PATTERN = re.compile(
        r"Real Voltage: (?P<voltage>-?\d+(?:\.\d+)?), "
        r"current: (?P<current>-?\d+(?:\.\d+)?), "
        r"power: (?P<power>-?\d+(?:\.\d+)?) \[kW\], "
        r"type: (?P<type>\w+)"
    )
    TEMPERATURE_PATTERN = re.compile(
        r"Temperature: '(?P<sensor>[^']+)', deviceId: (?P<device_id>\d+), "
        r"bus: (?P<bus>[^,]+), address: (?P<address>\d+), "
        r"value: (?P<value>-?\d+(?:\.\d+)?) \[[^\]]+\]\."
    )
    FAST_MEASUREMENT_PATTERN = re.compile(
        r"Fast measurement \('(?P<name>[^']+)'\) on bus '(?P<bus>[^']+)', "
        r"address: (?P<address>\d+), voltage: (?P<voltage>-?\d+(?:\.\d+)?) \[V\], "
        r"current: (?P<current>-|\-?\d+(?:\.\d+)?) \[A\]"
    )
    CONTACTOR_STATE_PATTERN = re.compile(
        r"Contactor \(id: (?P<id>\d+), type: (?P<type>AC|DC)\) state changed: "
        r"state: (?P<state>-?\d+) confirmState: (?P<confirm_state>-?\d+)"
    )

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or database_path("clc_logs.db")
        self.db_connection: Optional[sqlite3.Connection] = None
        self._init_db()

    def _init_db(self):
        if self.db_connection:
            try:
                self.db_connection.close()
            except Exception:
                pass
            self.db_connection = None

        self.db_connection = sqlite3.connect(self.db_path)
        self.db_connection.row_factory = sqlite3.Row
        cursor = self.db_connection.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS clc_log_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id INTEGER,
                file_name TEXT NOT NULL,
                file_hash TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                logger_name TEXT,
                level TEXT,
                message TEXT NOT NULL,
                raw_line TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS clc_measurements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id INTEGER,
                file_name TEXT NOT NULL,
                file_hash TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                logger_name TEXT,
                level TEXT,
                metric_name TEXT NOT NULL,
                metric_value REAL,
                metric_unit TEXT,
                metric_scope TEXT,
                raw_line TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS clc_imported_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id INTEGER,
                file_name TEXT NOT NULL,
                file_hash TEXT NOT NULL,
                imported_at TEXT NOT NULL,
                UNIQUE(station_id, file_hash)
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_clc_entries_station_time "
            "ON clc_log_entries(station_id, timestamp)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_clc_measurements_series "
            "ON clc_measurements(station_id, metric_name, metric_scope, timestamp)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_clc_measurements_catalog "
            "ON clc_measurements(station_id, metric_name, metric_scope, metric_unit)"
        )
        self.db_connection.commit()

    def parse(
        self,
        logfile: str,
        station_id: int = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Parse a CLC log file and store raw lines plus recognized measurements."""
        file_hash = self._calculate_file_hash(logfile)
        file_name = os.path.basename(logfile)
        cursor = self.db_connection.cursor()

        cursor.execute(
            "SELECT 1 FROM clc_imported_files WHERE station_id IS ? AND file_hash = ?",
            (station_id, file_hash),
        )
        if cursor.fetchone():
            return 0

        with open(logfile, "r", encoding="utf-8", errors="ignore") as handle:
            total_lines = sum(1 for _ in handle)
            handle.seek(0)

            inserted = 0
            for line_number, raw_line in enumerate(handle, start=1):
                raw_line = raw_line.rstrip("\n")
                parsed = self._parse_line(raw_line)
                if parsed is None:
                    if progress_callback:
                        progress_callback(line_number, total_lines)
                    continue

                cursor.execute(
                    """
                    INSERT INTO clc_log_entries
                    (station_id, file_name, file_hash, timestamp, logger_name, level, message, raw_line)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        station_id,
                        file_name,
                        file_hash,
                        parsed["timestamp"],
                        parsed["logger_name"],
                        parsed["level"],
                        parsed["message"],
                        raw_line,
                    ),
                )

                for measurement in self._extract_measurements(parsed):
                    cursor.execute(
                        """
                        INSERT INTO clc_measurements
                        (station_id, file_name, file_hash, timestamp, logger_name, level,
                         metric_name, metric_value, metric_unit, metric_scope, raw_line)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            station_id,
                            file_name,
                            file_hash,
                            parsed["timestamp"],
                            parsed["logger_name"],
                            parsed["level"],
                            measurement["metric_name"],
                            measurement["metric_value"],
                            measurement["metric_unit"],
                            measurement["metric_scope"],
                            raw_line,
                        ),
                    )

                inserted += 1
                if line_number % 1000 == 0:
                    self.db_connection.commit()
                if progress_callback:
                    progress_callback(line_number, total_lines)

        cursor.execute(
            """
            INSERT INTO clc_imported_files (station_id, file_name, file_hash, imported_at)
            VALUES (?, ?, ?, ?)
            """,
            (station_id, file_name, file_hash, datetime.utcnow().isoformat()),
        )
        self.db_connection.commit()
        return inserted

    def _calculate_file_hash(self, filepath: str) -> str:
        digest = hashlib.sha256()
        with open(filepath, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _parse_line(self, line: str) -> Optional[Dict[str, str]]:
        match = self.LINE_PATTERN.match(line)
        if not match:
            return None

        timestamp = datetime.strptime(match.group("timestamp"), "%Y-%m-%d %H:%M:%S.%f")
        return {
            "timestamp": timestamp.isoformat(),
            "logger_name": match.group("logger"),
            "level": match.group("level"),
            "message": match.group("message"),
        }

    def _extract_measurements(self, parsed_line: Dict[str, str]) -> List[Dict[str, Optional[float]]]:
        message = parsed_line["message"]
        measurements: List[Dict[str, Optional[float]]] = []

        match = self.MAX_LIMITS_PATTERN.search(message)
        if match:
            kind = match.group("kind").lower()
            measurements.extend(
                [
                    self._measurement(f"{kind}_power_limit", self._safe_float(match.group("power")), "kW", "global"),
                    self._measurement(f"{kind}_voltage_limit", self._safe_float(match.group("voltage")), "V", "global"),
                    self._measurement(f"{kind}_current_limit", self._safe_float(match.group("current")), "A", "global"),
                ]
            )

        match = self.CALCULATE_CURRENT_PATTERN.search(message)
        if match:
            scope = match.group("side")
            measurements.extend(
                [
                    self._measurement("calculated_power_limit", self._safe_float(match.group("power")), "kW", scope),
                    self._measurement("calculated_voltage_limit", self._safe_float(match.group("voltage")), "V", scope),
                    self._measurement("calculated_current_limit", self._safe_float(match.group("current")), "A", scope),
                ]
            )

        match = self.RECTIFIER_PATTERN.search(message)
        if match:
            scope = f"{match.group('bus')}:{match.group('address')}"
            measurements.extend(
                [
                    self._measurement("rectifier_enabled", 1.0 if match.group("enabled") == "true" else 0.0, "bool", scope),
                    self._measurement("rectifier_real_voltage", self._safe_float(match.group("real_voltage")), "V", scope),
                    self._measurement("rectifier_real_current", self._safe_float(match.group("real_current")), "A", scope),
                    self._measurement("rectifier_request_voltage", self._safe_float(match.group("request_voltage")), "V", scope),
                    self._measurement("rectifier_request_current", self._safe_float(match.group("request_current")), "A", scope),
                    self._measurement("rectifier_status", self._safe_float(match.group("status")), "state", scope),
                    self._measurement("rectifier_temperature", self._safe_float(match.group("temperature")), "C", scope),
                ]
            )

        match = self.CCS_REAL_PATTERN.search(message)
        if match:
            scope = match.group("type")
            measurements.extend(
                [
                    self._measurement("ccs_real_voltage", self._safe_float(match.group("voltage")), "V", scope),
                    self._measurement("ccs_real_current", self._safe_float(match.group("current")), "A", scope),
                    self._measurement("ccs_real_power", self._safe_float(match.group("power")), "kW", scope),
                ]
            )

        match = self.TEMPERATURE_PATTERN.search(message)
        if match:
            scope = f"{match.group('bus')}:{match.group('address')}:{match.group('sensor')}"
            measurements.append(
                self._measurement("sensor_temperature", self._safe_float(match.group("value")), "C", scope)
            )

        match = self.FAST_MEASUREMENT_PATTERN.search(message)
        if match:
            scope = f"{match.group('bus')}:{match.group('address')}:{match.group('name')}"
            measurements.append(
                self._measurement("fast_measurement_voltage", self._safe_float(match.group("voltage")), "V", scope)
            )
            measurements.append(
                self._measurement("fast_measurement_current", self._safe_float(match.group("current")), "A", scope)
            )

        match = self.CONTACTOR_STATE_PATTERN.search(message)
        if match:
            scope = f"{match.group('type')}:{match.group('id')}"
            measurements.extend(
                [
                    self._measurement("contactor_state", self._safe_float(match.group("state")), "state", scope),
                    self._measurement("contactor_confirm_state", self._safe_float(match.group("confirm_state")), "state", scope),
                ]
            )

        return measurements

    def _measurement(
        self,
        metric_name: str,
        metric_value: Optional[float],
        metric_unit: str,
        metric_scope: str,
    ) -> Dict[str, Optional[float]]:
        return {
            "metric_name": metric_name,
            "metric_value": metric_value,
            "metric_unit": metric_unit,
            "metric_scope": metric_scope,
        }

    def _safe_float(self, value: Optional[str]) -> Optional[float]:
        if value in (None, "", "-"):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def get_total_count(self, station_id: int = None, file_name: str = None) -> int:
        cursor = self.db_connection.cursor()
        where_clauses = []
        params = []
        if station_id is not None:
            where_clauses.append("station_id = ?")
            params.append(station_id)
        if file_name:
            where_clauses.append("file_name = ?")
            params.append(file_name)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        cursor.execute(f"SELECT COUNT(*) FROM clc_log_entries {where_sql}", params)
        return cursor.fetchone()[0]

    def get_entries_paginated(
        self,
        page: int,
        per_page: int = 100,
        station_id: int = None,
        file_name: str = None,
    ) -> List[dict]:
        offset = max(0, page - 1) * per_page
        cursor = self.db_connection.cursor()
        where_clauses = []
        params = []
        if station_id is not None:
            where_clauses.append("station_id = ?")
            params.append(station_id)
        if file_name:
            where_clauses.append("file_name = ?")
            params.append(file_name)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        cursor.execute(
            f"""
            SELECT *
            FROM clc_log_entries
            {where_sql}
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
            """,
            params + [per_page, offset],
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_files(self, station_id: int = None) -> List[str]:
        cursor = self.db_connection.cursor()
        if station_id is None:
            cursor.execute("SELECT DISTINCT file_name FROM clc_log_entries ORDER BY file_name")
        else:
            cursor.execute(
                "SELECT DISTINCT file_name FROM clc_log_entries WHERE station_id = ? ORDER BY file_name",
                (station_id,),
            )
        return [row[0] for row in cursor.fetchall()]

    def get_available_series(self, station_id: int = None) -> List[dict]:
        cursor = self.db_connection.cursor()
        if station_id is None:
            cursor.execute(
                """
                SELECT metric_name, metric_scope, metric_unit, COUNT(*) AS points
                FROM clc_measurements
                GROUP BY metric_name, metric_scope, metric_unit
                """
            )
        else:
            cursor.execute(
                """
                SELECT metric_name, metric_scope, metric_unit, COUNT(*) AS points
                FROM clc_measurements
                WHERE station_id = ?
                GROUP BY metric_name, metric_scope, metric_unit
                """,
                (station_id,),
            )
        rows = [dict(row) for row in cursor.fetchall()]
        rows.sort(key=lambda row: (row["metric_name"], row["metric_scope"] or ""))
        return rows

    def get_series_data(
        self,
        metric_name: str,
        metric_scope: Optional[str] = None,
        station_id: int = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        max_points: Optional[int] = None,
    ) -> List[dict]:
        cursor = self.db_connection.cursor()
        where_clauses = ["metric_name = ?"]
        params: List[object] = [metric_name]
        if metric_scope is not None:
            where_clauses.append("metric_scope = ?")
            params.append(metric_scope)
        if station_id is not None:
            where_clauses.append("station_id = ?")
            params.append(station_id)
        if start_time is not None:
            where_clauses.append("timestamp >= ?")
            params.append(start_time)
        if end_time is not None:
            where_clauses.append("timestamp <= ?")
            params.append(end_time)

        base_sql = f"""
            SELECT timestamp, metric_name, metric_value, metric_unit, metric_scope, file_name
            FROM clc_measurements
            WHERE {' AND '.join(where_clauses)}
        """
        if max_points is not None and max_points > 0:
            cursor.execute(
                f"""
                WITH filtered AS (
                    SELECT
                        timestamp,
                        metric_name,
                        metric_value,
                        metric_unit,
                        metric_scope,
                        file_name,
                        ROW_NUMBER() OVER (ORDER BY timestamp) AS row_num
                    FROM ({base_sql})
                ),
                total AS (
                    SELECT COUNT(*) AS total_rows FROM filtered
                )
                SELECT timestamp, metric_name, metric_value, metric_unit, metric_scope, file_name
                FROM filtered, total
                WHERE total.total_rows <= ?
                   OR ((filtered.row_num - 1) % CAST(((total.total_rows + ? - 1) / ?) AS INTEGER)) = 0
                ORDER BY timestamp
                """,
                params + [max_points, max_points, max_points],
            )
        else:
            cursor.execute(
                f"""
                {base_sql}
                ORDER BY timestamp
                """,
                params,
            )
        return [dict(row) for row in cursor.fetchall()]

    def get_entries_near_timestamp(
        self,
        center_time: str,
        station_id: int = None,
        file_name: str = None,
        limit: int = 50,
        window_seconds: float = 300.0,
    ) -> List[dict]:
        cursor = self.db_connection.cursor()
        center_dt = datetime.fromisoformat(center_time.replace(" ", "T"))
        start_time = (center_dt - timedelta(seconds=window_seconds)).isoformat()
        end_time = (center_dt + timedelta(seconds=window_seconds)).isoformat()
        where_clauses = []
        params: List[object] = []
        if station_id is not None:
            where_clauses.append("station_id = ?")
            params.append(station_id)
        if file_name:
            where_clauses.append("file_name = ?")
            params.append(file_name)
        where_clauses.append("timestamp >= ?")
        params.append(start_time)
        where_clauses.append("timestamp <= ?")
        params.append(end_time)
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        cursor.execute(
            f"""
            SELECT station_id, file_name, timestamp, logger_name, level, message, raw_line
            FROM clc_log_entries
            {where_sql}
            ORDER BY timestamp
            """,
            params,
        )
        rows = [dict(row) for row in cursor.fetchall()]
        rows.sort(
            key=lambda row: (
                abs((datetime.fromisoformat(row["timestamp"].replace(" ", "T")) - center_dt).total_seconds()),
                row["timestamp"],
            )
        )
        return rows[:limit]

    def delete_logs_by_file(self, file_name: str, station_id: int = None) -> int:
        cursor = self.db_connection.cursor()
        if station_id is None:
            cursor.execute("SELECT DISTINCT file_hash FROM clc_log_entries WHERE file_name = ?", (file_name,))
        else:
            cursor.execute(
                "SELECT DISTINCT file_hash FROM clc_log_entries WHERE file_name = ? AND station_id = ?",
                (file_name, station_id),
            )
        file_hashes = [row[0] for row in cursor.fetchall()]

        if station_id is None:
            cursor.execute("DELETE FROM clc_log_entries WHERE file_name = ?", (file_name,))
            deleted = cursor.rowcount
            cursor.execute("DELETE FROM clc_measurements WHERE file_name = ?", (file_name,))
        else:
            cursor.execute(
                "DELETE FROM clc_log_entries WHERE file_name = ? AND station_id = ?",
                (file_name, station_id),
            )
            deleted = cursor.rowcount
            cursor.execute(
                "DELETE FROM clc_measurements WHERE file_name = ? AND station_id = ?",
                (file_name, station_id),
            )

        for file_hash in file_hashes:
            if station_id is None:
                cursor.execute("DELETE FROM clc_imported_files WHERE file_hash = ?", (file_hash,))
            else:
                cursor.execute(
                    "DELETE FROM clc_imported_files WHERE file_hash = ? AND station_id = ?",
                    (file_hash, station_id),
                )

        self.db_connection.commit()
        return deleted

    def close(self):
        if self.db_connection:
            self.db_connection.close()
            self.db_connection = None

    def __del__(self):
        self.close()
