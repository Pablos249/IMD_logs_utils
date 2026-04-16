import shutil
import uuid
from pathlib import Path

from src.modules.data_analysis import DataAnalyzer
from src.modules.eos_logs import EOSLogParser


def _make_test_dir() -> Path:
    base_dir = Path("tests/.tmp_eos")
    base_dir.mkdir(parents=True, exist_ok=True)
    run_dir = base_dir / str(uuid.uuid4())
    run_dir.mkdir()
    return run_dir


def test_parse_eos_transaction_start_and_stop_from_summary_section():
    tmp_path = _make_test_dir()
    sample = '''"Search criteria:"
"Transaction identifier:","15267308"

"Additional data:"
"Start"
"SoC start [%]","Start","Energy inlet start [kWh]","Energy outlet start [kWh]"
27,"2026-03-12 08:02:24",0.00,28333.79
"Stop"
"SoC finish [%]","Stop","Energy inlet stop [kWh]","Energy outlet stop [kWh]"
99,"2026-03-12 09:25:29",64.65,28395.94

"Date","Current outlet [A]","SoC [%]"
"2026-03-12 08:02:24",,27
"2026-03-12 08:03:28","185.87",28
"2026-03-12 09:25:29","0.00",99
'''
    try:
        logfile = tmp_path / "session.csv"
        logfile.write_text(sample, encoding="utf-8")

        parser = EOSLogParser(db_path=str(tmp_path / "eos.db"))
        inserted = parser.parse(str(logfile), station_id=3)

        assert inserted == 3
        transaction = parser.get_transactions(station_id=3)[0]
        assert transaction["start_time"] == "2026-03-12 08:02:24"
        assert transaction["stop_time"] == "2026-03-12 09:25:29"
        assert transaction["soc_start"] == 27
        assert transaction["soc_stop"] == 99
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_transaction_window_falls_back_to_entry_timestamps_for_invalid_summary_times():
    tmp_path = _make_test_dir()
    try:
        parser = EOSLogParser(db_path=str(tmp_path / "eos.db"))
        cursor = parser.db_connection.cursor()
        cursor.execute(
            """
            INSERT INTO eos_transactions
            (station_id, file_name, transaction_id, start_time, stop_time)
            VALUES (?, ?, ?, ?, ?)
            """,
            (5, "bad.csv", "TX-BAD", "2026-03-12 08:02:24", "8.28"),
        )
        cursor.executemany(
            """
            INSERT INTO eos_log_entries
            (station_id, file_name, transaction_id, timestamp, current_outlet)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (5, "bad.csv", "TX-BAD", "2026-03-12 08:02:24", 0.0),
                (5, "bad.csv", "TX-BAD", "2026-03-12 08:30:00", 100.0),
                (5, "bad.csv", "TX-BAD", "2026-03-12 09:25:29", 0.0),
            ],
        )
        parser.db_connection.commit()

        analyzer = DataAnalyzer(eos_parser=parser)
        window = analyzer.get_transaction_window(station_id=5, transaction_id="TX-BAD", padding_minutes=0)

        assert window is not None
        assert window["session_start_time"] == "2026-03-12T08:02:24"
        assert window["session_stop_time"] == "2026-03-12T09:25:29"
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)


def test_parse_eos_insulation_monitor_scales_k_suffix_to_kohm():
    tmp_path = _make_test_dir()
    sample = '''"Search criteria:"
"Transaction identifier:","TX-INS"

"Date","Insulation monitor [Ω]"
"2026-03-12 08:02:24","1k"
"2026-03-12 08:03:24","1.5k"
"2026-03-12 08:04:24","1000000"
'''
    try:
        logfile = tmp_path / "insulation.csv"
        logfile.write_text(sample, encoding="utf-8")

        parser = EOSLogParser(db_path=str(tmp_path / "eos.db"))
        inserted = parser.parse(str(logfile), station_id=9)

        assert inserted == 3
        series = parser.get_series_data("insulation_monitor", station_id=9, transaction_id="TX-INS")
        assert [point["metric_value"] for point in series] == [1.0, 1.5, 1000.0]
        assert all(point["metric_unit"] == "kohm" for point in series)
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)
