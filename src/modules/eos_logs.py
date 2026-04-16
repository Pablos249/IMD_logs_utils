"""Module for loading and parsing EOS charging session logs."""

import csv
import json
import os
import sqlite3
import unicodedata
from datetime import datetime, timedelta
from typing import Callable, List, Optional

from src.app_paths import database_path


class EOSLogParser:
    """Parser for EOS charging session logs."""

    INSULATION_MONITOR_SCALE = 1000.0
    INSULATION_MONITOR_SCALE_SUFFIXES = {
        "k": 1.0,
    }

    SERIES_DEFINITIONS = [
        {"metric_name": "current_inlet_l1", "column": "current_inlet_l1", "unit": "A", "label": "Current inlet L1"},
        {"metric_name": "current_inlet_l2", "column": "current_inlet_l2", "unit": "A", "label": "Current inlet L2"},
        {"metric_name": "current_inlet_l3", "column": "current_inlet_l3", "unit": "A", "label": "Current inlet L3"},
        {"metric_name": "current_outlet", "column": "current_outlet", "unit": "A", "label": "Current outlet"},
        {"metric_name": "current_demanded_by_vehicle", "column": "current_demanded_by_vehicle", "unit": "A", "label": "Current demanded by vehicle"},
        {"metric_name": "current_inlet_power", "column": "current_inlet_power", "unit": "kW", "label": "Current inlet power"},
        {"metric_name": "current_outlet_power", "column": "current_outlet_power", "unit": "kW", "label": "Current outlet power"},
        {"metric_name": "max_power_set", "column": "max_power_set", "unit": "kW", "label": "Max power set"},
        {"metric_name": "max_power", "column": "max_power", "unit": "kW", "label": "Max power"},
        {"metric_name": "insulation_monitor", "column": "insulation_monitor", "unit": "kohm", "label": "Insulation monitor"},
        {"metric_name": "soc", "column": "soc", "unit": "%", "label": "State of charge"},
        {"metric_name": "voltage_out", "column": "voltage_out", "unit": "V", "label": "Voltage out"},
        {"metric_name": "current_voltage_out", "column": "current_voltage_out", "unit": "V", "label": "Current voltage out"},
        {"metric_name": "voltage_demanded_by_ev", "column": "voltage_demanded_by_ev", "unit": "V", "label": "Voltage demanded by EV"},
        {"metric_name": "max_voltage_vehicle", "column": "max_voltage_vehicle", "unit": "V", "label": "Max voltage vehicle"},
        {"metric_name": "max_voltage_set", "column": "max_voltage_set", "unit": "V", "label": "Max voltage set"},
        {"metric_name": "temp_air_conditioner_out", "column": "temp_air_conditioner_out", "unit": "C", "label": "Temperature air conditioner out"},
        {"metric_name": "temp_in_charger", "column": "temp_in_charger", "unit": "C", "label": "Temperature in charger"},
        {"metric_name": "temp_modules_in", "column": "temp_modules_in", "unit": "C", "label": "Temperature modules in"},
        {"metric_name": "temp_modules_out", "column": "temp_modules_out", "unit": "C", "label": "Temperature modules out"},
        {"metric_name": "temp_plug_dc_minus", "column": "temp_plug_dc_minus", "unit": "C", "label": "Temperature plug DC-"},
        {"metric_name": "temp_plug_dc_plus", "column": "temp_plug_dc_plus", "unit": "C", "label": "Temperature plug DC+"},
        {"metric_name": "temp_cable_1", "column": "temp_cable_1", "unit": "C", "label": "Temperature cable 1"},
        {"metric_name": "temp_cable_2", "column": "temp_cable_2", "unit": "C", "label": "Temperature cable 2"},
        {"metric_name": "time_to_soc_80", "column": "time_to_soc_80", "unit": "s", "label": "Time to SoC 80%"},
        {"metric_name": "time_to_soc_100", "column": "time_to_soc_100", "unit": "s", "label": "Time to SoC 100%"},
        {"metric_name": "inlet_energy_meter_reading", "column": "inlet_energy_meter_reading", "unit": "kWh", "label": "Inlet energy meter reading"},
        {"metric_name": "outlet_energy_meter_reading", "column": "outlet_energy_meter_reading", "unit": "kWh", "label": "Outlet energy meter reading"},
        {"metric_name": "inlet_energy_incremental", "column": "inlet_energy_incremental", "unit": "kWh", "label": "Inlet energy incremental"},
        {"metric_name": "outlet_energy_incremental", "column": "outlet_energy_incremental", "unit": "kWh", "label": "Outlet energy incremental"},
        {"metric_name": "energy_active_import_interval_inlet", "column": "energy_active_import_interval_inlet", "unit": "kWh", "label": "Energy active import interval inlet"},
    ]

    COLUMN_TO_HEADER = {
        "timestamp": "Date",
        "current_inlet_l1": "Current inlet L1 [A]",
        "current_inlet_l2": "Current inlet L2 [A]",
        "current_inlet_l3": "Current inlet L3 [A]",
        "current_outlet": "Current outlet [A]",
        "max_current_vehicle": "Max. current vehicle [A]",
        "max_set_current": "Max. set current [A]",
        "max_current": "Max. current",
        "present_current_outlet": "Present current outlet [A]",
        "current_demanded_by_vehicle": "Current demanded by vehicle [A]",
        "inlet_energy_incremental": "Inlet energy incremental [kWh]",
        "outlet_energy_incremental": "Outlet energy incremental [kWh]",
        "inlet_energy_meter_reading": "Inlet energy meter reading [kWh]",
        "outlet_energy_meter_reading": "Outlet energy meter reading [kWh]",
        "current_inlet_power": "Current inlet power [kW]",
        "current_outlet_power": "Current outlet power [kW]",
        "max_power_set": "Max. power set [kW]",
        "max_power": "Max. power [kW]",
        "time_to_soc_80": "Time to SoC 80% [s]",
        "time_to_soc_100": "Time to SoC 100% [s]",
        "insulation_monitor": "Insulation monitor [Ω]",
        "soc": "SoC [%]",
        "temp_air_conditioner_out": "Temp. air conditioner out. [°C]",
        "temp_in_charger": "Temp. in charger [°C]",
        "temp_modules_in": "Temp. modules in. [°C]",
        "temp_modules_out": "Temp. modules out. [°C]",
        "temp_plug_dc_minus": "Temp. plug DC- [°C]",
        "temp_plug_dc_plus": "Temp. plug DC+ [°C]",
        "voltage_in_l1": "Voltage in. L1 [V]",
        "voltage_in_l2": "Voltage in. L2 [V]",
        "voltage_in_l3": "Voltage in. L3 [V]",
        "max_voltage_vehicle": "Max. voltage - vehicle [V]",
        "max_voltage_set": "Max. voltage set [V]",
        "voltage_out": "Voltage out. [V]",
        "current_voltage_out": "Current voltage out [V]",
        "voltage_demanded_by_ev": "Voltage demanded by EV [V]",
        "evcc_status": "EVCC status",
        "secc_status": "SECC status",
        "temp_cable_1": "Temp. Cable 1 [°C]",
        "temp_cable_2": "Temp. Cable 2 [°C]",
        "energy_active_import_interval_inlet": "Energy.Active.Import.Interval.Inlet [kWh]",
    }

    NUMERIC_COLUMNS = {
        "current_inlet_l1",
        "current_inlet_l2",
        "current_inlet_l3",
        "current_outlet",
        "max_current_vehicle",
        "max_set_current",
        "max_current",
        "present_current_outlet",
        "current_demanded_by_vehicle",
        "inlet_energy_incremental",
        "outlet_energy_incremental",
        "inlet_energy_meter_reading",
        "outlet_energy_meter_reading",
        "current_inlet_power",
        "current_outlet_power",
        "max_power_set",
        "max_power",
        "time_to_soc_80",
        "time_to_soc_100",
        "insulation_monitor",
        "soc",
        "temp_air_conditioner_out",
        "temp_in_charger",
        "temp_modules_in",
        "temp_modules_out",
        "temp_plug_dc_minus",
        "temp_plug_dc_plus",
        "voltage_in_l1",
        "voltage_in_l2",
        "voltage_in_l3",
        "max_voltage_vehicle",
        "max_voltage_set",
        "voltage_out",
        "current_voltage_out",
        "voltage_demanded_by_ev",
        "temp_cable_1",
        "temp_cable_2",
        "energy_active_import_interval_inlet",
    }

    COLUMN_HEADER_ALIASES = {
        "timestamp": ["Data pomiaru"],
        "current_inlet_l1": ["PrÄ…d wejĹ›ciowy L1 [A]", "Prad wejsciowy L1 [A]"],
        "current_inlet_l2": ["PrÄ…d wejĹ›ciowy L2 [A]", "Prad wejsciowy L2 [A]"],
        "current_inlet_l3": ["PrÄ…d wejĹ›ciowy L3 [A]", "Prad wejsciowy L3 [A]"],
        "current_outlet": ["PrÄ…d wyjĹ›ciowy [A]", "Prad wyjsciowy [A]"],
        "max_current_vehicle": ["Max. prÄ…d pojazdu [A]", "Max. prad pojazdu [A]"],
        "max_set_current": ["Max. prÄ…d wyjĹ›ciowy [A]", "Max. prad wyjsciowy [A]"],
        "max_current": ["DostÄ™pny prÄ…d Ĺ‚adowania [A]", "Dostepny prad ladowania [A]"],
        "present_current_outlet": ["BieĹĽÄ…cy prÄ…d Ĺ‚adowania [A]", "Biezacy prad ladowania [A]"],
        "current_demanded_by_vehicle": ["PrÄ…d ĹĽÄ…dany przez pojazd [A]", "Prad zadany przez pojazd [A]"],
        "inlet_energy_incremental": ["Przyrost energii wejĹ›ciowej [kWh]", "Przyrost energii wejsciowej [kWh]"],
        "outlet_energy_incremental": ["Przyrost energii wyjĹ›ciowej [kWh]", "Przyrost energii wyjsciowej [kWh]"],
        "inlet_energy_meter_reading": ["Wskazanie licznika energii wejĹ›ciowej [kWh]", "Wskazanie licznika energii wejsciowej [kWh]"],
        "outlet_energy_meter_reading": ["Wskazanie licznika energii wyjĹ›ciowej [kWh]", "Wskazanie licznika energii wyjsciowej [kWh]"],
        "current_inlet_power": ["Chwilowa moc wejĹ›ciowa [kW]", "Chwilowa moc wejsciowa [kW]"],
        "current_outlet_power": ["Chwilowa moc wyjĹ›ciowa [kW]", "Chwilowa moc wyjsciowa [kW]"],
        "max_power_set": ["Max. moc wyjĹ›ciowa  [kW]", "Max. moc wyjsciowa [kW]"],
        "max_power": ["DostÄ™pna moc Ĺ‚adowania [kW]", "Dostepna moc ladowania [kW]"],
        "time_to_soc_80": ["Czas do optymalnego naĹ‚adowania [s]", "Czas do optymalnego naladowania [s]"],
        "time_to_soc_100": ["Czas do peĹ‚nego naĹ‚adowania [s]", "Czas do pelnego naladowania [s]"],
        "insulation_monitor": ["Monitor izolacji [â„¦]", "Monitor izolacji [Ω]"],
        "soc": ["Poziom baterii pojazdu [%]"],
        "temp_air_conditioner_out": ["Temp. - wyjĹ›cie wentylacji [Â°C]", "Temp. - wyjscie wentylacji [°C]"],
        "temp_in_charger": ["Temp. w Ĺ‚adowarce [Â°C]", "Temp. w ladowarce [°C]"],
        "temp_modules_in": ["Temp. - wejĹ›cie moduĹ‚Ăłw [Â°C]", "Temp. - wejscie modulow [°C]"],
        "temp_modules_out": ["Temp. - wyjĹ›cie moduĹ‚Ăłw [Â°C]", "Temp. - wyjscie modulow [°C]"],
        "temp_plug_dc_minus": ["Temp. zĹ‚Ä…cza DC- [Â°C]", "Temp. zlacza DC- [°C]"],
        "temp_plug_dc_plus": ["Temp. zĹ‚Ä…cza DC+ [Â°C]", "Temp. zlacza DC+ [°C]"],
        "voltage_in_l1": ["NapiÄ™cie wejĹ›ciowe L1 [V]", "Napiecie wejsciowe L1 [V]"],
        "voltage_in_l2": ["NapiÄ™cie wejĹ›ciowe L2 [V]", "Napiecie wejsciowe L2 [V]"],
        "voltage_in_l3": ["NapiÄ™cie wejĹ›ciowe L3 [V]", "Napiecie wejsciowe L3 [V]"],
        "max_voltage_vehicle": ["Max. napiÄ™cie pojazdu [V]", "Max. napiecie pojazdu [V]"],
        "max_voltage_set": ["Max. napiÄ™cie wyjĹ›ciowe [V]", "Max. napiecie wyjsciowe [V]"],
        "voltage_out": ["NapiÄ™cie wyjĹ›ciowe (licznik) [V]", "Napiecie wyjsciowe (licznik) [V]"],
        "current_voltage_out": ["BieĹĽÄ…ce napiÄ™cie Ĺ‚adowania [V]", "Biezace napiecie ladowania [V]"],
        "voltage_demanded_by_ev": ["NapiÄ™cie ĹĽÄ…dane przez pojazd [V]", "Napiecie zadane przez pojazd [V]"],
    }

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or database_path("eos_logs.db")
        self.db_connection = None
        self._init_db()

    def _init_db(self):
        """Initialize SQLite database schema."""
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
            CREATE TABLE IF NOT EXISTS eos_log_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id INTEGER,
                file_name TEXT,
                transaction_id TEXT,
                timestamp TEXT NOT NULL,
                current_inlet_l1 REAL,
                current_inlet_l2 REAL,
                current_inlet_l3 REAL,
                current_outlet REAL,
                max_current_vehicle REAL,
                max_set_current REAL,
                max_current REAL,
                present_current_outlet REAL,
                current_demanded_by_vehicle REAL,
                inlet_energy_incremental REAL,
                outlet_energy_incremental REAL,
                inlet_energy_meter_reading REAL,
                outlet_energy_meter_reading REAL,
                current_inlet_power REAL,
                current_outlet_power REAL,
                max_power_set REAL,
                max_power REAL,
                time_to_soc_80 REAL,
                time_to_soc_100 REAL,
                insulation_monitor REAL,
                soc REAL,
                temp_air_conditioner_out REAL,
                temp_in_charger REAL,
                temp_modules_in REAL,
                temp_modules_out REAL,
                temp_plug_dc_minus REAL,
                temp_plug_dc_plus REAL,
                voltage_in_l1 REAL,
                voltage_in_l2 REAL,
                voltage_in_l3 REAL,
                max_voltage_vehicle REAL,
                max_voltage_set REAL,
                voltage_out REAL,
                current_voltage_out REAL,
                voltage_demanded_by_ev REAL,
                evcc_status TEXT,
                secc_status TEXT,
                temp_cable_1 REAL,
                temp_cable_2 REAL,
                energy_active_import_interval_inlet REAL,
                raw_data TEXT
            )
            """
        )

        cursor.execute("PRAGMA table_info(eos_log_entries)")
        existing_cols = [r[1] for r in cursor.fetchall()]
        if "raw_data" not in existing_cols:
            cursor.execute("ALTER TABLE eos_log_entries ADD COLUMN raw_data TEXT")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS eos_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id INTEGER,
                file_name TEXT,
                transaction_id TEXT UNIQUE,
                start_time TEXT,
                stop_time TEXT,
                soc_start REAL,
                soc_stop REAL,
                energy_inlet_start REAL,
                energy_inlet_stop REAL,
                energy_outlet_start REAL,
                energy_outlet_stop REAL
            )
            """
        )

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_eos_entries_station_time "
            "ON eos_log_entries(station_id, timestamp)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_eos_entries_station_tx_time "
            "ON eos_log_entries(station_id, transaction_id, timestamp)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_eos_transactions_station_start "
            "ON eos_transactions(station_id, start_time)"
        )

        self._migrate_insulation_monitor_scale()
        self.db_connection.commit()

    def parse(
        self,
        filepath: str,
        station_id: int = None,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """Parse an EOS CSV log and store it into the database."""
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            lines = list(csv.reader(f))

        transaction_id = None
        for row in lines:
            first_cell = row[0].strip('"') if row else ""
            if len(row) >= 2 and first_cell in {"Transaction identifier:", "Identyfikator transakcji:"}:
                transaction_id = row[1].strip('"')
                break

        start_data, stop_data = self._extract_session_summary(lines)

        if transaction_id:
            cursor = self.db_connection.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO eos_transactions
                (station_id, file_name, transaction_id, start_time, stop_time,
                 soc_start, soc_stop, energy_inlet_start, energy_inlet_stop,
                 energy_outlet_start, energy_outlet_stop)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    station_id,
                    os.path.basename(filepath),
                    transaction_id,
                    start_data.get("time"),
                    stop_data.get("time"),
                    start_data.get("soc"),
                    stop_data.get("soc"),
                    start_data.get("energy_inlet"),
                    stop_data.get("energy_inlet"),
                    start_data.get("energy_outlet"),
                    stop_data.get("energy_outlet"),
                ),
            )

        header_row = None
        for idx, row in enumerate(lines):
            first_cell = row[0].strip('"') if row else ""
            if first_cell in {"Date", "Data pomiaru"}:
                header_row = idx
                break

        parsed_count = 0
        if header_row is not None:
            headers = [h.strip('"') for h in lines[header_row]]
            for i, row in enumerate(lines[header_row + 1:], start=1):
                if len(row) != len(headers):
                    continue

                data = {h: v for h, v in zip(headers, row)}
                normalized_data = {
                    self._normalize_header_name(header): value
                    for header, value in data.items()
                }

                values_by_column = {
                    "station_id": station_id,
                    "file_name": os.path.basename(filepath),
                    "transaction_id": transaction_id,
                    "raw_data": json.dumps(data, ensure_ascii=False),
                }

                for column_name, canonical_header in self.COLUMN_TO_HEADER.items():
                    raw_value = self._value_for_column(data, normalized_data, column_name, canonical_header)
                    if column_name in self.NUMERIC_COLUMNS:
                        values_by_column[column_name] = self._parse_numeric_value(column_name, raw_value)
                    else:
                        values_by_column[column_name] = raw_value

                cols = [
                    "station_id", "file_name", "transaction_id", "timestamp",
                    "current_inlet_l1", "current_inlet_l2", "current_inlet_l3",
                    "current_outlet", "max_current_vehicle", "max_set_current",
                    "max_current", "present_current_outlet", "current_demanded_by_vehicle",
                    "inlet_energy_incremental", "outlet_energy_incremental",
                    "inlet_energy_meter_reading", "outlet_energy_meter_reading",
                    "current_inlet_power", "current_outlet_power", "max_power_set",
                    "max_power", "time_to_soc_80", "time_to_soc_100", "insulation_monitor",
                    "soc", "temp_air_conditioner_out", "temp_in_charger",
                    "temp_modules_in", "temp_modules_out", "temp_plug_dc_minus",
                    "temp_plug_dc_plus", "voltage_in_l1", "voltage_in_l2", "voltage_in_l3",
                    "max_voltage_vehicle", "max_voltage_set", "voltage_out",
                    "current_voltage_out", "voltage_demanded_by_ev", "evcc_status",
                    "secc_status", "temp_cable_1", "temp_cable_2",
                    "energy_active_import_interval_inlet", "raw_data",
                ]
                values = [values_by_column.get(column_name) for column_name in cols]

                placeholders = ", ".join("?" for _ in cols)
                sql = f"INSERT INTO eos_log_entries ({', '.join(cols)}) VALUES ({placeholders})"
                cursor = self.db_connection.cursor()
                cursor.execute(sql, values)
                parsed_count += 1
                if progress_callback:
                    progress_callback(i, len(lines) - header_row - 1)

        self.db_connection.commit()
        return parsed_count

    def _migrate_insulation_monitor_scale(self):
        """Normalize historical insulation monitor values using raw JSON payloads."""
        cursor = self.db_connection.cursor()
        cursor.execute(
            """
            SELECT id, raw_data, insulation_monitor
            FROM eos_log_entries
            WHERE raw_data IS NOT NULL
              AND insulation_monitor IS NOT NULL
            """
        )

        rows_to_update = []
        for row in cursor.fetchall():
            try:
                raw_dict = json.loads(row["raw_data"])
            except Exception:
                continue

            normalized_data = {
                self._normalize_header_name(header): value
                for header, value in raw_dict.items()
            }
            raw_value = self._value_from_headers(
                raw_dict,
                normalized_data,
                self.COLUMN_TO_HEADER["insulation_monitor"],
            )
            parsed_value = self._parse_insulation_monitor(raw_value)
            if parsed_value is None:
                continue
            if abs(row["insulation_monitor"] - parsed_value) > 1e-9:
                rows_to_update.append((parsed_value, row["id"]))

        if rows_to_update:
            cursor.executemany(
                "UPDATE eos_log_entries SET insulation_monitor = ? WHERE id = ?",
                rows_to_update,
            )

    def _safe_float(self, value: Optional[str]) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    def _parse_numeric_value(self, column_name: str, value: Optional[str]) -> Optional[float]:
        if column_name == "insulation_monitor":
            return self._parse_insulation_monitor(value)
        return self._safe_float(value)

    def _parse_insulation_monitor(self, value: Optional[str]) -> Optional[float]:
        if value is None:
            return None

        text = str(value).strip().strip('"')
        if not text:
            return None

        normalized = text.replace(",", ".").replace(" ", "")
        suffix = normalized[-1].lower()
        multiplier = self.INSULATION_MONITOR_SCALE_SUFFIXES.get(suffix)
        if multiplier is not None:
            try:
                return float(normalized[:-1]) * multiplier
            except ValueError:
                return None

        parsed = self._safe_float(normalized)
        if parsed is None:
            return None
        return round(parsed / self.INSULATION_MONITOR_SCALE, 3)

    def _extract_session_summary(self, lines: List[List[str]]) -> tuple[dict, dict]:
        start_data = {}
        stop_data = {}
        pending_section = None

        for row in lines:
            normalized = [cell.strip().strip('"') for cell in row]
            non_empty = [cell for cell in normalized if cell]
            if not non_empty:
                continue

            if len(non_empty) == 1 and non_empty[0] in {"Start", "Stop", "Koniec"}:
                pending_section = "start" if non_empty[0] == "Start" else "stop"
                continue

            if pending_section and len(normalized) >= 2:
                time_value = normalized[1] if len(normalized) > 1 else None
                if not self._looks_like_datetime(time_value):
                    continue

                parsed = {
                    "soc": self._safe_float(normalized[0] if len(normalized) > 0 else None),
                    "time": time_value,
                    "energy_inlet": self._safe_float(normalized[2] if len(normalized) > 2 else None),
                    "energy_outlet": self._safe_float(normalized[3] if len(normalized) > 3 else None),
                }
                if pending_section == "start":
                    start_data = parsed
                else:
                    stop_data = parsed
                pending_section = None

        return start_data, stop_data

    def _looks_like_datetime(self, value: Optional[str]) -> bool:
        if not value:
            return False
        try:
            datetime.fromisoformat(value)
            return True
        except ValueError:
            return False

    def _normalize_header_name(self, header: str) -> str:
        normalized = unicodedata.normalize("NFKC", header)
        normalized = normalized.replace("Â", "")
        normalized = normalized.replace("â„¦", "Ω")
        normalized = " ".join(normalized.split())
        return normalized

    def _value_from_headers(self, data: dict, normalized_data: dict, canonical_header: str) -> Optional[str]:
        if canonical_header in data:
            return data[canonical_header]
        return normalized_data.get(self._normalize_header_name(canonical_header))

    def _value_for_column(
        self,
        data: dict,
        normalized_data: dict,
        column_name: str,
        canonical_header: str,
    ) -> Optional[str]:
        direct_value = self._value_from_headers(data, normalized_data, canonical_header)
        if direct_value not in (None, ""):
            return direct_value

        for alias in self.COLUMN_HEADER_ALIASES.get(column_name, []):
            alias_value = self._value_from_headers(data, normalized_data, alias)
            if alias_value not in (None, ""):
                return alias_value
        return direct_value

    def get_total_count(self, station_id: int = None, transaction_id: str = None, file_name: str = None) -> int:
        cursor = self.db_connection.cursor()

        where_clauses = []
        params = []
        if station_id is not None:
            where_clauses.append("station_id = ?")
            params.append(station_id)
        if transaction_id:
            where_clauses.append("transaction_id = ?")
            params.append(transaction_id)
        if file_name:
            where_clauses.append("file_name = ?")
            params.append(file_name)

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        cursor.execute(f"SELECT COUNT(*) FROM eos_log_entries {where_sql}", params)
        return cursor.fetchone()[0]

    def get_entries_paginated(
        self,
        page: int,
        per_page: int = 100,
        station_id: int = None,
        transaction_id: str = None,
        file_name: str = None,
    ) -> List[dict]:
        offset = (page - 1) * per_page
        cursor = self.db_connection.cursor()

        where_clauses = []
        params = []
        if station_id is not None:
            where_clauses.append("station_id = ?")
            params.append(station_id)
        if transaction_id:
            where_clauses.append("transaction_id = ?")
            params.append(transaction_id)
        if file_name:
            where_clauses.append("file_name = ?")
            params.append(file_name)

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        cursor.execute(
            f"""
            SELECT * FROM eos_log_entries
            {where_sql}
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
            """,
            params + [per_page, offset],
        )

        results = []
        for row in cursor.fetchall():
            row_dict = dict(row)
            raw_json = row_dict.pop("raw_data", None)
            if raw_json:
                try:
                    raw_dict = json.loads(raw_json)
                    row_dict.update(raw_dict)
                except Exception:
                    pass
            results.append(row_dict)

        return results

    def get_transactions(self, station_id: int = None) -> List[dict]:
        cursor = self.db_connection.cursor()
        if station_id is not None:
            cursor.execute("SELECT * FROM eos_transactions WHERE station_id = ? ORDER BY start_time DESC", (station_id,))
        else:
            cursor.execute("SELECT * FROM eos_transactions ORDER BY start_time DESC")
        return [dict(row) for row in cursor.fetchall()]

    def get_transaction_time_bounds(self, transaction_id: str, station_id: int = None) -> Optional[dict]:
        cursor = self.db_connection.cursor()
        where_clauses = ["transaction_id = ?"]
        params: List[object] = [transaction_id]
        if station_id is not None:
            where_clauses.append("station_id = ?")
            params.append(station_id)

        cursor.execute(
            f"""
            SELECT MIN(timestamp) AS start_time, MAX(timestamp) AS stop_time
            FROM eos_log_entries
            WHERE {' AND '.join(where_clauses)}
            """,
            params,
        )
        row = cursor.fetchone()
        if not row or not row["start_time"] or not row["stop_time"]:
            return None
        return {"start_time": row["start_time"], "stop_time": row["stop_time"]}

    def get_available_series(self, station_id: int = None, transaction_id: str = None, file_name: str = None) -> List[dict]:
        """Return numeric EOS series available for plotting."""
        cursor = self.db_connection.cursor()
        where_clauses: List[str] = []
        params: List[object] = []
        if station_id is not None:
            where_clauses.append("station_id = ?")
            params.append(station_id)
        if transaction_id:
            where_clauses.append("transaction_id = ?")
            params.append(transaction_id)
        if file_name:
            where_clauses.append("file_name = ?")
            params.append(file_name)

        select_sql = ", ".join(
            f'COUNT("{definition["column"]}") AS "{definition["metric_name"]}"'
            for definition in self.SERIES_DEFINITIONS
        )
        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        cursor.execute(f"SELECT {select_sql} FROM eos_log_entries {where_sql}", params)
        row = cursor.fetchone()
        if row is None:
            return []

        series = []
        for definition in self.SERIES_DEFINITIONS:
            count = row[definition["metric_name"]] or 0
            if count == 0:
                continue
            series.append(
                {
                    "metric_name": definition["metric_name"],
                    "metric_scope": transaction_id or "session",
                    "metric_unit": definition["unit"],
                    "description": definition["label"],
                    "points": count,
                }
            )
        return series

    def get_series_data(
        self,
        metric_name: str,
        station_id: int = None,
        transaction_id: str = None,
        file_name: str = None,
        start_time: str = None,
        end_time: str = None,
        max_points: int = None,
    ) -> List[dict]:
        """Return a single EOS numeric series ordered by timestamp."""
        definition = next(
            (item for item in self.SERIES_DEFINITIONS if item["metric_name"] == metric_name),
            None,
        )
        if definition is None:
            return []

        where_clauses = [f"{definition['column']} IS NOT NULL"]
        params: List[object] = []
        if station_id is not None:
            where_clauses.append("station_id = ?")
            params.append(station_id)
        if transaction_id:
            where_clauses.append("transaction_id = ?")
            params.append(transaction_id)
        if file_name:
            where_clauses.append("file_name = ?")
            params.append(file_name)
        if start_time is not None:
            where_clauses.append("timestamp >= ?")
            params.append(start_time.replace("T", " "))
        if end_time is not None:
            where_clauses.append("timestamp <= ?")
            params.append(end_time.replace("T", " "))

        cursor = self.db_connection.cursor()
        base_sql = f"""
            SELECT timestamp, transaction_id, file_name, {definition['column']} AS metric_value
            FROM eos_log_entries
            WHERE {' AND '.join(where_clauses)}
        """
        if max_points is not None and max_points > 0:
            cursor.execute(
                f"""
                WITH filtered AS (
                    SELECT
                        timestamp,
                        transaction_id,
                        file_name,
                        metric_value,
                        ROW_NUMBER() OVER (ORDER BY timestamp) AS row_num
                    FROM ({base_sql})
                ),
                total AS (
                    SELECT COUNT(*) AS total_rows FROM filtered
                )
                SELECT timestamp, transaction_id, file_name, metric_value
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
                "metric_scope": row["transaction_id"] or "session",
                "metric_value": row["metric_value"],
                "metric_unit": definition["unit"],
                "file_name": row["file_name"],
                "source_type": "eos",
                "transaction_id": row["transaction_id"],
            }
            for row in cursor.fetchall()
        ]

    def get_entries_near_timestamp(
        self,
        center_time: str,
        station_id: int = None,
        transaction_id: str = None,
        file_name: str = None,
        limit: int = 50,
        window_seconds: float = 300.0,
    ) -> List[dict]:
        cursor = self.db_connection.cursor()
        center_dt = datetime.fromisoformat(center_time.replace(" ", "T"))
        start_time = (center_dt - timedelta(seconds=window_seconds)).strftime("%Y-%m-%d %H:%M:%S.%f")
        end_time = (center_dt + timedelta(seconds=window_seconds)).strftime("%Y-%m-%d %H:%M:%S.%f")
        where_clauses = []
        params: List[object] = []
        if station_id is not None:
            where_clauses.append("station_id = ?")
            params.append(station_id)
        if transaction_id:
            where_clauses.append("transaction_id = ?")
            params.append(transaction_id)
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
            SELECT station_id, file_name, transaction_id, timestamp, raw_data
            FROM eos_log_entries
            {where_sql}
            ORDER BY timestamp
            """,
            params,
        )

        results = []
        for row in cursor.fetchall():
            row_dict = dict(row)
            raw_json = row_dict.get("raw_data")
            if raw_json:
                try:
                    raw_dict = json.loads(raw_json)
                    preview_keys = [
                        "Date",
                        "Current outlet [A]",
                        "Voltage out. [V]",
                        "SoC [%]",
                    ]
                    row_dict["message"] = " | ".join(
                        f"{key}: {raw_dict[key]}"
                        for key in preview_keys
                        if key in raw_dict and raw_dict[key] not in (None, "")
                    )
                except Exception:
                    row_dict["message"] = raw_json
            else:
                row_dict["message"] = ""
            results.append(row_dict)
        results.sort(
            key=lambda row: (
                abs((datetime.fromisoformat(row["timestamp"].replace(" ", "T")) - center_dt).total_seconds()),
                row["timestamp"],
            )
        )
        return results[:limit]

    def delete_logs_by_file(self, file_name: str, station_id: int = None) -> int:
        """Delete all entries and transactions imported from a specific file."""
        cursor = self.db_connection.cursor()
        if station_id is not None:
            cursor.execute("DELETE FROM eos_log_entries WHERE file_name = ? AND station_id = ?", (file_name, station_id))
            cursor.execute("DELETE FROM eos_transactions WHERE file_name = ? AND station_id = ?", (file_name, station_id))
        else:
            cursor.execute("DELETE FROM eos_log_entries WHERE file_name = ?", (file_name,))
            cursor.execute("DELETE FROM eos_transactions WHERE file_name = ?", (file_name,))
        deleted = cursor.rowcount
        self.db_connection.commit()
        return deleted

    def get_files(self, station_id: int = None) -> List[str]:
        """List unique file names that have been imported."""
        cursor = self.db_connection.cursor()
        if station_id is not None:
            cursor.execute("SELECT DISTINCT file_name FROM eos_log_entries WHERE station_id = ? ORDER BY file_name", (station_id,))
        else:
            cursor.execute("SELECT DISTINCT file_name FROM eos_log_entries ORDER BY file_name")
        return [row[0] for row in cursor.fetchall()]
