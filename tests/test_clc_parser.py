import shutil
import uuid
from pathlib import Path

from src.modules.clc_logs import CLCLogParser


def _make_test_dir() -> Path:
    base_dir = Path("tests/.tmp_clc")
    base_dir.mkdir(parents=True, exist_ok=True)
    run_dir = base_dir / str(uuid.uuid4())
    run_dir.mkdir()
    return run_dir


def test_parse_clc_log_and_extract_measurements():
    tmp_path = _make_test_dir()
    sample = """[2026-03-13 07:27:29.329] [SocketsManager] [trace] Max OCPP global limits - P: 50 [kW], U: 397.7 [V], I: 125.8 [A]
[2026-03-13 07:27:29.414] [global] [debug] ADDRESS: 1. Params on rectifier module (bus: 'modules') isEnabled: true, REAL: U=396.6 [V], I=75.4 [A], REQUEST: U= 413 [V], I= 75.4 [A], status: 1, state0: 0x0, state1: 0x0, state2: 0x0, temp: 12 [C].
[2026-03-13 07:27:29.614] [CCSController] [trace] Real Voltage: 397.7, current: 92.3, power: 36.7077 [kW], type: PowerModules
[2026-03-13 07:27:29.468] [global] [trace] Temperature: 'ModbusPT1000', deviceId: 3, bus: slave, address: 4, value: 12.0 [C].
[2026-03-13 07:27:29.649] [global] [info] Fast measurement ('fm:cm03:1:34') on bus 'modules', address: 34, voltage: -6.9 [V], current: - [A]
"""
    try:
        logfile = tmp_path / "sample_clc.log"
        logfile.write_text(sample, encoding="utf-8")

        parser = CLCLogParser(db_path=str(tmp_path / "clc.db"))
        inserted = parser.parse(str(logfile), station_id=7)

        assert inserted == 5
        assert parser.get_total_count(station_id=7) == 5
        assert parser.get_files(station_id=7) == ["sample_clc.log"]

        series = parser.get_available_series(station_id=7)
        series_keys = {(row["metric_name"], row["metric_scope"]) for row in series}
        assert ("ocpp_power_limit", "global") in series_keys
        assert ("rectifier_real_voltage", "modules:1") in series_keys
        assert ("ccs_real_power", "PowerModules") in series_keys
        assert ("sensor_temperature", "slave:4:ModbusPT1000") in series_keys
        assert ("fast_measurement_voltage", "modules:34:fm:cm03:1:34") in series_keys

        power_points = parser.get_series_data("ccs_real_power", metric_scope="PowerModules", station_id=7)
        assert len(power_points) == 1
        assert power_points[0]["metric_value"] == 36.7077

        rectifier_points = parser.get_series_data("rectifier_enabled", metric_scope="modules:1", station_id=7)
        assert len(rectifier_points) == 1
        assert rectifier_points[0]["metric_value"] == 1.0
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_deduplicate_same_content_by_hash():
    tmp_path = _make_test_dir()
    sample = """[2026-03-13 08:48:02.090] [SocketsManager] [trace] Max DLBS global limits - P: 65535 [kW], U: 1000 [V], I: 6553.5 [A]
"""
    try:
        first = tmp_path / "clc_a.log"
        second = tmp_path / "clc_b.log"
        first.write_text(sample, encoding="utf-8")
        second.write_text(sample, encoding="utf-8")

        parser = CLCLogParser(db_path=str(tmp_path / "clc.db"))

        assert parser.parse(str(first), station_id=2) == 1
        assert parser.parse(str(second), station_id=2) == 0
        assert parser.get_total_count(station_id=2) == 1

        points = parser.get_series_data("dlbs_power_limit", metric_scope="global", station_id=2)
        assert len(points) == 1
        assert points[0]["metric_value"] == 65535.0
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_delete_logs_by_file_removes_import_registry():
    tmp_path = _make_test_dir()
    sample = """[2026-03-13 08:48:04.444] [global] [info] Fast measurement ('fm:cm03:1:35') on bus 'modules', address: 35, voltage: -5 [V], current: - [A]
"""
    try:
        logfile = tmp_path / "delete_me.log"
        logfile.write_text(sample, encoding="utf-8")

        parser = CLCLogParser(db_path=str(tmp_path / "clc.db"))

        assert parser.parse(str(logfile), station_id=4) == 1
        assert parser.delete_logs_by_file("delete_me.log", station_id=4) == 1
        assert parser.get_total_count(station_id=4) == 0
        assert parser.parse(str(logfile), station_id=4) == 1
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_parse_contactor_state_changes():
    tmp_path = _make_test_dir()
    sample = """[2026-03-11 23:10:48.448] [Contactor] [trace] Contactor (id: 0, type: AC) state changed: state: 0 confirmState: 1
[2026-03-11 23:10:49.372] [Contactor] [trace] Contactor (id: 0, type: DC) state changed: state: 0 confirmState: 1
"""
    try:
        logfile = tmp_path / "contactor.log"
        logfile.write_text(sample, encoding="utf-8")

        parser = CLCLogParser(db_path=str(tmp_path / "clc.db"))
        assert parser.parse(str(logfile), station_id=5) == 2

        ac_state = parser.get_series_data("contactor_state", metric_scope="AC:0", station_id=5)
        dc_state = parser.get_series_data("contactor_state", metric_scope="DC:0", station_id=5)
        ac_confirm = parser.get_series_data("contactor_confirm_state", metric_scope="AC:0", station_id=5)
        dc_confirm = parser.get_series_data("contactor_confirm_state", metric_scope="DC:0", station_id=5)

        assert len(ac_state) == 1
        assert len(dc_state) == 1
        assert ac_state[0]["metric_value"] == 0.0
        assert dc_state[0]["metric_value"] == 0.0
        assert ac_confirm[0]["metric_value"] == 1.0
        assert dc_confirm[0]["metric_value"] == 1.0
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)
