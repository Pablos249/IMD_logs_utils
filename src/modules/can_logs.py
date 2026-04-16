"""Module for loading and decoding CAN log files"""

from datetime import datetime, timedelta
import re
import struct
import sqlite3
import os
from typing import Dict, List, Optional, Callable

from src.app_paths import database_path


class CANLogParser:
    CAPACITANCE_SCALE = 10.0
    RESISTANCE_SCALE = 1.0

    # CAN ID to description mapping
    CAN_DESCRIPTIONS = {
        "048BC501": "IMD CCS2 measured voltage",
        "048BC502": "IMD CCS1 measured voltage",
        "0C87C501": "IMD CCS2 resistance and capacitance",
        "0C87C502": "IMD CCS1 resistance and capacitance",
        "0883C501": "IMD CCS2 status",
        "0883C502": "IMD CCS1 status",
    }
    SERIES_DEFINITIONS = [
        {
            "metric_name": "imd_voltage",
            "column": "voltage_V",
            "unit": "V",
            "description": "Measured insulation voltage",
            "scope_by_can_id": {
                "048BC501": "CCS2",
                "048BC502": "CCS1",
            },
        },
        {
            "metric_name": "imd_resistance",
            "column": "resistance_ohm",
            "unit": "kohm",
            "description": "Measured insulation resistance",
            "scope_by_can_id": {
                "0C87C501": "CCS2",
                "0C87C502": "CCS1",
            },
        },
        {
            "metric_name": "imd_capacitance",
            "column": "capacitance_nF",
            "unit": "nF",
            "description": "Measured capacitance",
            "scope_by_can_id": {
                "0C87C501": "CCS2",
                "0C87C502": "CCS1",
            },
        },
    ]

    def __init__(self, db_path: str = None):
        # use a single persistent database for the whole application
        self.db_path = db_path or database_path("can_logs.db")
        self.db_connection = None
        self._init_db()

    def _init_db(self):
        """Initialize SQLite database with schema migration support"""
        # Close existing connection if any
        if self.db_connection:
            try:
                self.db_connection.close()
            except Exception:
                pass
            self.db_connection = None
        
        # open (or create) database; do not remove it so that data persists across runs
        first_time = not os.path.exists(self.db_path)
        self.db_connection = sqlite3.connect(self.db_path)
        self.db_connection.row_factory = sqlite3.Row
        cursor = self.db_connection.cursor()
        
        # create table if missing
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS can_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id INTEGER,
                file_name TEXT,
                timestamp TEXT NOT NULL,
                can_id TEXT NOT NULL,
                description TEXT,
                raw_data BLOB NOT NULL,
                voltage_V REAL,
                capacitance_nF REAL,
                resistance_ohm INTEGER,
                status_byte TEXT,
                status_binary TEXT
            )
        """)
        # create indexes (IF NOT EXISTS handles existing)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_station_id ON can_messages(station_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_can_id ON can_messages(can_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON can_messages(timestamp)")
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_can_station_can_time "
            "ON can_messages(station_id, can_id, timestamp)"
        )
        
        # simple migration: if old table lacked new columns, add them without losing data
        if not first_time:
            cursor.execute("PRAGMA table_info(can_messages)")
            existing = {row[1] for row in cursor.fetchall()}
            # add any missing columns rather than recreating table
            if "station_id" not in existing:
                cursor.execute("ALTER TABLE can_messages ADD COLUMN station_id INTEGER")
            if "file_name" not in existing:
                cursor.execute("ALTER TABLE can_messages ADD COLUMN file_name TEXT")
            # indexes already handled by IF NOT EXISTS clauses above

        self._migrate_resistance_scale()
        self._migrate_capacitance_scale()
        self.db_connection.commit()

    def _migrate_resistance_scale(self):
        """Normalize historical resistance values so stored data matches raw IMD units."""
        cursor = self.db_connection.cursor()
        cursor.execute(
            """
            SELECT id, raw_data, resistance_ohm
            FROM can_messages
            WHERE can_id LIKE '0C87C5%'
              AND resistance_ohm IS NOT NULL
            """
        )

        rows_to_update = []
        for row in cursor.fetchall():
            raw_data = row["raw_data"]
            if raw_data is None or len(raw_data) < 4:
                continue

            resistance_raw = int.from_bytes(raw_data[2:4], byteorder="little", signed=False)
            stored_value = row["resistance_ohm"]

            # Rewrite rows that were incorrectly divided by 1000 in an earlier migration.
            if abs(stored_value - round(resistance_raw / 1000.0, 2)) < 1e-9:
                rows_to_update.append(
                    (resistance_raw, row["id"])
                )

        if rows_to_update:
            cursor.executemany(
                "UPDATE can_messages SET resistance_ohm = ? WHERE id = ?",
                rows_to_update,
            )

    def _migrate_capacitance_scale(self):
        """Normalize historical capacitance values stored without the decimal shift."""
        cursor = self.db_connection.cursor()
        cursor.execute(
            """
            SELECT id, raw_data, capacitance_nF
            FROM can_messages
            WHERE can_id LIKE '0C87C5%'
              AND capacitance_nF IS NOT NULL
            """
        )

        rows_to_update = []
        for row in cursor.fetchall():
            raw_data = row["raw_data"]
            if raw_data is None or len(raw_data) < 6:
                continue

            capacitance_raw = int.from_bytes(raw_data[4:6], byteorder="little", signed=False)
            stored_value = row["capacitance_nF"]

            # Only rewrite rows that still contain the old unscaled raw value.
            if abs(stored_value - capacitance_raw) < 1e-9:
                rows_to_update.append(
                    (round(capacitance_raw / self.CAPACITANCE_SCALE, 2), row["id"])
                )

        if rows_to_update:
            cursor.executemany(
                "UPDATE can_messages SET capacitance_nF = ? WHERE id = ?",
                rows_to_update,
            )

    def parse(self, logfile: str, station_id: int = None, progress_callback: Optional[Callable] = None) -> int:
        """Parse a given CAN log file and store its records.

        Parameters
        ----------
        logfile : str
            Path to the log file to import.
        station_id : int, optional
            Charging station identifier.
        progress_callback : callable, optional
            Called with (current_line, total_lines) during processing.

        Returns
        -------
        int
            Number of messages added.
        """
        cursor = self.db_connection.cursor()
        file_name = os.path.basename(logfile)
        
        try:
            # First pass: count total lines
            with open(logfile, 'r') as f:
                total_lines = sum(1 for _ in f)
            
            # Second pass: parse with progress tracking
            with open(logfile, 'r') as f:
                for line_num, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    msg = self._parse_line(line)
                    if msg:
                        decoded = self._decode_values(msg['can_id'], msg['data'])
                        
                        cursor.execute("""
                            INSERT INTO can_messages 
                            (station_id, file_name, timestamp, can_id, description, raw_data, voltage_V, 
                             capacitance_nF, resistance_ohm, status_byte, status_binary)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            station_id,
                            file_name,
                            msg['timestamp'],
                            msg['can_id'],
                            msg['description'],
                            msg['data'],
                            decoded.get('voltage_V'),
                            decoded.get('capacitance_nF'),
                            decoded.get('resistance_ohm'),
                            decoded.get('status_byte'),
                            decoded.get('status_binary'),
                        ))
                    
                    if line_num % 100 == 0:
                        self.db_connection.commit()
                    
                    if progress_callback:
                        progress_callback(line_num, total_lines)
            
            self.db_connection.commit()
            cursor.execute("SELECT COUNT(*) FROM can_messages")
            return cursor.fetchone()[0]
        except FileNotFoundError:
            raise


    def _parse_line(self, line: str) -> Optional[dict]:
        """Parse a single CAN log line"""
        # Format: (2026-03-11 13:48:55.004267)  can0  048BC501   [8]  01 01 00 00 00 00 00 00
        pattern = r'\((\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\)\s+can\d+\s+([0-9A-Fa-f]+)\s+\[\d+\]\s+((?:[0-9A-Fa-f]{2}\s*)*)'
        
        match = re.match(pattern, line)
        if not match:
            return None
        
        timestamp_str = match.group(1)
        can_id = match.group(2).upper()
        data_str = match.group(3)
        
        try:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
            data = bytes.fromhex(data_str.replace(" ", ""))
            description = self.CAN_DESCRIPTIONS.get(can_id, "Unknown")
            
            return {
                'timestamp': timestamp.isoformat(),
                'can_id': can_id,
                'data': data,
                'description': description
            }
        except (ValueError, KeyError):
            return None

    def _decode_values(self, can_id: str, data: bytes) -> dict:
        """Decode specific values from CAN data based on CAN ID."""
        values = {}

        # Channel: 01 -> CCS2, 02 -> CCS1
        channel = None
        if can_id.endswith("01"):
            channel = 2
        elif can_id.endswith("02"):
            channel = 1

        if can_id.startswith("048BC5"):
            # Napięcie - raw value is at bytes 2-3 (little endian), scale 0.1V
            if len(data) >= 4:
                voltage_raw = int.from_bytes(data[2:4], byteorder="little", signed=False)
                signed_voltage_raw = voltage_raw - 0x10000 if voltage_raw >= 0x8000 else voltage_raw
                voltage_v = signed_voltage_raw / 10.0
                values.update({
                    "type": "voltage",
                    "channel": channel,
                    "voltage_raw": voltage_raw,
                    "voltage_V": round(voltage_v, 2),
                })

        elif can_id.startswith("0C87C5"):
            # Riso (bytes 2-3) and pojemność (bytes 4-5)
            if len(data) >= 6:
                riso_raw = int.from_bytes(data[2:4], byteorder="little", signed=False)
                riso = round(riso_raw / self.RESISTANCE_SCALE, 2)
                capacitance_raw = int.from_bytes(data[4:6], byteorder="little", signed=False)
                capacitance = round(capacitance_raw / self.CAPACITANCE_SCALE, 2)
                values.update({
                    "type": "insulation",
                    "channel": channel,
                    "resistance_ohm": riso,
                    "capacitance_nF": capacitance,
                    "riso_raw": riso_raw,
                    "capacitance_raw": capacitance_raw,
                })

        elif can_id.startswith("0883C5"):
            # Status - status_code at byte 1, measuring at byte 4
            if len(data) >= 5:
                status_code = data[1]
                measuring = data[4]
                values.update({
                    "type": "status",
                    "channel": channel,
                    "status_byte": f"0x{status_code:02X}",
                    "status_code": status_code,
                    "status_binary": f"{measuring:08b}",
                    "measuring": measuring,
                })

        return values

    def get_messages_page(self, page: int = 0, rows_per_page: int = 100,
                           station_id: int = None, file_name: str = None,
                           filter_can_id: str = None) -> tuple:
        """Get a page of messages from database with optional filters."""
        cursor = self.db_connection.cursor()
        where_clauses = []
        params = []
        if station_id is not None:
            where_clauses.append("station_id = ?")
            params.append(station_id)
        if file_name:
            where_clauses.append("file_name = ?")
            params.append(file_name)
        if filter_can_id:
            where_clauses.append("can_id = ?")
            params.append(filter_can_id)
        
        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)
        
        # total count
        cursor.execute(f"SELECT COUNT(*) FROM can_messages {where_sql}", params)
        total_count = cursor.fetchone()[0]
        
        # page data ordered by timestamp asc
        offset = page * rows_per_page
        cursor.execute(f"""
            SELECT id, station_id, file_name, timestamp, can_id, description, raw_data, 
                   voltage_V, capacitance_nF, resistance_ohm, status_byte, status_binary
            FROM can_messages
            {where_sql}
            ORDER BY timestamp
            LIMIT ? OFFSET ?
        """, params + [rows_per_page, offset])
        messages = cursor.fetchall()
        return messages, total_count

    def get_total_count(self, station_id: int = None, file_name: str = None, filter_can_id: str = None) -> int:
        """Get total number of messages with optional filters."""
        cursor = self.db_connection.cursor()
        where_clauses = []
        params = []
        if station_id is not None:
            where_clauses.append("station_id = ?")
            params.append(station_id)
        if file_name:
            where_clauses.append("file_name = ?")
            params.append(file_name)
        if filter_can_id:
            where_clauses.append("can_id = ?")
            params.append(filter_can_id)
        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)
        cursor.execute(f"SELECT COUNT(*) FROM can_messages {where_sql}", params)
        return cursor.fetchone()[0]

    def get_can_ids(self, station_id: int = None, file_name: str = None) -> List[str]:
        """Get list of unique CAN IDs optionally filtered by station or file."""
        cursor = self.db_connection.cursor()
        where_clauses = []
        params = []
        if station_id is not None:
            where_clauses.append("station_id = ?")
            params.append(station_id)
        if file_name:
            where_clauses.append("file_name = ?")
            params.append(file_name)
        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)
        cursor.execute(f"SELECT DISTINCT can_id FROM can_messages {where_sql} ORDER BY can_id", params)
        return [row[0] for row in cursor.fetchall()]

    def get_files(self, station_id: int = None) -> List[str]:
        """List unique file names that have been imported."""
        cursor = self.db_connection.cursor()
        if station_id is not None:
            cursor.execute("SELECT DISTINCT file_name FROM can_messages WHERE station_id = ? ORDER BY file_name", (station_id,))
        else:
            cursor.execute("SELECT DISTINCT file_name FROM can_messages ORDER BY file_name")
        return [row[0] for row in cursor.fetchall()]

    def get_available_series(self, station_id: int = None, file_name: str = None) -> List[dict]:
        """Return numeric IMD series available for plotting."""
        available_can_ids = set(self.get_can_ids(station_id=station_id, file_name=file_name))
        series: List[dict] = []
        for definition in self.SERIES_DEFINITIONS:
            for can_id, scope in definition["scope_by_can_id"].items():
                if can_id not in available_can_ids:
                    continue
                series.append(
                    {
                        "metric_name": definition["metric_name"],
                        "metric_scope": scope,
                        "metric_unit": definition["unit"],
                        "description": definition["description"],
                        "points": None,
                    }
                )
        return series

    def get_series_data(
        self,
        metric_name: str,
        metric_scope: str,
        station_id: int = None,
        file_name: str = None,
        start_time: str = None,
        end_time: str = None,
        max_points: int = None,
    ) -> List[dict]:
        """Return a single IMD numeric series ordered by timestamp."""
        definition = next(
            (item for item in self.SERIES_DEFINITIONS if item["metric_name"] == metric_name),
            None,
        )
        if definition is None:
            return []

        can_id = None
        for current_can_id, scope in definition["scope_by_can_id"].items():
            if scope == metric_scope:
                can_id = current_can_id
                break
        if can_id is None:
            return []

        where_clauses = [f"{definition['column']} IS NOT NULL", "can_id = ?"]
        params: List[object] = [can_id]
        if station_id is not None:
            where_clauses.append("station_id = ?")
            params.append(station_id)
        if file_name:
            where_clauses.append("file_name = ?")
            params.append(file_name)
        if start_time is not None:
            where_clauses.append("timestamp >= ?")
            params.append(start_time)
        if end_time is not None:
            where_clauses.append("timestamp <= ?")
            params.append(end_time)

        cursor = self.db_connection.cursor()
        base_sql = f"""
            SELECT timestamp, {definition['column']} AS metric_value, file_name, can_id
            FROM can_messages
            WHERE {' AND '.join(where_clauses)}
        """
        if max_points is not None and max_points > 0:
            cursor.execute(
                f"""
                WITH filtered AS (
                    SELECT
                        timestamp,
                        metric_value,
                        file_name,
                        can_id,
                        ROW_NUMBER() OVER (ORDER BY timestamp) AS row_num
                    FROM ({base_sql})
                ),
                total AS (
                    SELECT COUNT(*) AS total_rows FROM filtered
                )
                SELECT timestamp, metric_value, file_name, can_id
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
        return [
            {
                "timestamp": row["timestamp"],
                "metric_name": metric_name,
                "metric_scope": metric_scope,
                "metric_value": row["metric_value"],
                "metric_unit": definition["unit"],
                "file_name": row["file_name"],
                "source_type": "imd",
                "source_id": row["can_id"],
            }
            for row in cursor.fetchall()
        ]

    def get_entries_near_timestamp(
        self,
        center_time: str,
        station_id: int = None,
        file_name: str = None,
        limit: int = 50,
        window_seconds: float = 300.0,
    ) -> List[dict]:
        """Return raw IMD log rows ordered by proximity to ``center_time``."""
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
            SELECT station_id, file_name, timestamp, can_id, description, raw_data
            FROM can_messages
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
        """Remove all records imported from ``file_name``. Returns count deleted."""
        cursor = self.db_connection.cursor()
        if station_id is not None:
            cursor.execute("DELETE FROM can_messages WHERE file_name = ? AND station_id = ?", (file_name, station_id))
        else:
            cursor.execute("DELETE FROM can_messages WHERE file_name = ?", (file_name,))
        deleted = cursor.rowcount
        self.db_connection.commit()
        return deleted

    def close(self):
        """Close database connection"""
        if self.db_connection:
            self.db_connection.close()

    def __del__(self):
        """Cleanup on object destruction"""
        self.close()
