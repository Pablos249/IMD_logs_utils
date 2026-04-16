"""Module for loading and parsing CCS communication log files."""

from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import os
import re
import sqlite3
from typing import Callable, Dict, List, Optional

from src.app_paths import database_path


class CCSLogParser:
    """Parser for CCS / DIN70121 communication logs."""

    LINE_PATTERN = re.compile(
        r"^\[(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})\] "
        r"\[(?P<logger>[^\]]+)\] \[(?P<level>[^\]]+)\] (?P<message>.*)$"
    )
    KEY_VALUE_PATTERN = re.compile(r"^\|?\s*(?P<name>[^:]+):\s*(?P<value>.+?)\s*$")
    NUMERIC_VALUE_PATTERN = re.compile(
        r"(?P<value>-?\d+(?:\.\d+)?)\s*(?P<unit>\[[^\]]+\]|[A-Za-z%°µ]+)?"
    )

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or database_path("ccs_logs.db")
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
            CREATE TABLE IF NOT EXISTS ccs_log_entries (
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
            CREATE TABLE IF NOT EXISTS ccs_measurements (
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
            CREATE TABLE IF NOT EXISTS ccs_imported_files (
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
            "CREATE INDEX IF NOT EXISTS idx_ccs_entries_station_time "
            "ON ccs_log_entries(station_id, timestamp)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_ccs_measurements_series "
            "ON ccs_measurements(station_id, metric_name, metric_scope, timestamp)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_ccs_measurements_catalog "
            "ON ccs_measurements(station_id, metric_name, metric_scope, metric_unit)"
        )
        self.db_connection.commit()

    def parse(
        self,
        logfile: str,
        station_id: int = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        file_hash = self._calculate_file_hash(logfile)
        file_name = os.path.basename(logfile)
        cursor = self.db_connection.cursor()

        cursor.execute(
            "SELECT 1 FROM ccs_imported_files WHERE station_id IS ? AND file_hash = ?",
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
                    INSERT INTO ccs_log_entries
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
                        INSERT INTO ccs_measurements
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
            INSERT INTO ccs_imported_files (station_id, file_name, file_hash, imported_at)
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
        message = parsed_line["message"].replace("\t", " ").strip()
        match = self.KEY_VALUE_PATTERN.match(message)
        if not match:
            return []

        raw_name = self._normalize_metric_name(match.group("name"))
        raw_value = match.group("value").strip()
        scope = parsed_line["logger_name"] or "ccs"

        lowered_value = raw_value.lower()
        if lowered_value in {"true", "false"}:
            return [
                self._measurement(
                    metric_name=raw_name,
                    metric_value=1.0 if lowered_value == "true" else 0.0,
                    metric_unit="bool",
                    metric_scope=scope,
                )
            ]

        numeric_match = self.NUMERIC_VALUE_PATTERN.search(raw_value)
        if numeric_match is None:
            if lowered_value in {"on", "off"}:
                return [
                    self._measurement(
                        metric_name=raw_name,
                        metric_value=1.0 if lowered_value == "on" else 0.0,
                        metric_unit="state",
                        metric_scope=scope,
                    )
                ]
            return []

        unit = self._normalize_unit(numeric_match.group("unit"))
        return [
            self._measurement(
                metric_name=raw_name,
                metric_value=float(numeric_match.group("value")),
                metric_unit=unit,
                metric_scope=scope,
            )
        ]

    def _normalize_metric_name(self, name: str) -> str:
        normalized = name.strip().lstrip("|").strip()
        normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", normalized)
        normalized = normalized.replace("+", "_plus_")
        normalized = normalized.replace("-", "_minus_")
        normalized = normalized.replace("/", "_")
        normalized = re.sub(r"[^0-9A-Za-z]+", "_", normalized)
        normalized = re.sub(r"_+", "_", normalized)
        return normalized.strip("_").lower()

    def _normalize_unit(self, unit: Optional[str]) -> str:
        if not unit:
            return "value"
        stripped = unit.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            stripped = stripped[1:-1]
        return stripped or "value"

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
        cursor.execute(f"SELECT COUNT(*) FROM ccs_log_entries {where_sql}", params)
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
            FROM ccs_log_entries
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
            cursor.execute("SELECT DISTINCT file_name FROM ccs_log_entries ORDER BY file_name")
        else:
            cursor.execute(
                "SELECT DISTINCT file_name FROM ccs_log_entries WHERE station_id = ? ORDER BY file_name",
                (station_id,),
            )
        return [row[0] for row in cursor.fetchall()]

    def delete_logs_by_file(self, file_name: str, station_id: int = None) -> int:
        cursor = self.db_connection.cursor()
        if station_id is None:
            cursor.execute("SELECT DISTINCT file_hash FROM ccs_log_entries WHERE file_name = ?", (file_name,))
        else:
            cursor.execute(
                "SELECT DISTINCT file_hash FROM ccs_log_entries WHERE file_name = ? AND station_id = ?",
                (file_name, station_id),
            )
        file_hashes = [row[0] for row in cursor.fetchall()]

        if station_id is None:
            cursor.execute("DELETE FROM ccs_log_entries WHERE file_name = ?", (file_name,))
            deleted = cursor.rowcount
            cursor.execute("DELETE FROM ccs_measurements WHERE file_name = ?", (file_name,))
        else:
            cursor.execute(
                "DELETE FROM ccs_log_entries WHERE file_name = ? AND station_id = ?",
                (file_name, station_id),
            )
            deleted = cursor.rowcount
            cursor.execute(
                "DELETE FROM ccs_measurements WHERE file_name = ? AND station_id = ?",
                (file_name, station_id),
            )

        for file_hash in file_hashes:
            if station_id is None:
                cursor.execute("DELETE FROM ccs_imported_files WHERE file_hash = ?", (file_hash,))
            else:
                cursor.execute(
                    "DELETE FROM ccs_imported_files WHERE file_hash = ? AND station_id = ?",
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
