import shutil
import uuid
from pathlib import Path

from src.modules.can_logs import CANLogParser
from src.modules.clc_logs import CLCLogParser
from src.modules.data_analysis import DataAnalyzer
from src.modules.eos_logs import EOSLogParser


def _make_test_dir() -> Path:
    base_dir = Path("tests/.tmp_data_analysis")
    base_dir.mkdir(parents=True, exist_ok=True)
    run_dir = base_dir / str(uuid.uuid4())
    run_dir.mkdir()
    return run_dir


def test_data_analyzer_catalog_and_transaction_window():
    tmp_path = _make_test_dir()
    try:
        can_parser = CANLogParser(db_path=str(tmp_path / "can.db"))
        clc_parser = CLCLogParser(db_path=str(tmp_path / "clc.db"))
        eos_parser = EOSLogParser(db_path=str(tmp_path / "eos.db"))

        can_log = tmp_path / "imd.log"
        can_log.write_text(
            "(2026-03-12 10:00:00.000000)  can0  048BC501   [8]  01 00 8A 13 00 00 00 00\n",
            encoding="utf-8",
        )
        assert can_parser.parse(str(can_log), station_id=1) == 1

        clc_log = tmp_path / "clc.log"
        clc_log.write_text(
            "[2026-03-12 10:05:00.000] [SocketsManager] [trace] Max OCPP global limits - P: 50 [kW], U: 400 [V], I: 125 [A]\n",
            encoding="utf-8",
        )
        assert clc_parser.parse(str(clc_log), station_id=1) == 1

        eos_cursor = eos_parser.db_connection.cursor()
        eos_cursor.execute(
            """
            INSERT INTO eos_transactions
            (station_id, file_name, transaction_id, start_time, stop_time, soc_start, soc_stop)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "eos.csv", "TX-1", "2026-03-12T10:00:00", "2026-03-12T10:10:00", 20, 80),
        )
        eos_cursor.execute(
            """
            INSERT INTO eos_log_entries
            (station_id, file_name, transaction_id, timestamp, current_outlet, soc, voltage_out)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "eos.csv", "TX-1", "2026-03-12T10:03:00", 90.0, 45.0, 390.0),
        )
        eos_parser.db_connection.commit()

        analyzer = DataAnalyzer(
            can_parser=can_parser,
            clc_parser=clc_parser,
            eos_parser=eos_parser,
        )

        catalog = analyzer.get_station_series_catalog(station_id=1)
        assert any(item["metric_name"] == "imd_voltage" and item["metric_scope"] == "CCS2" for item in catalog["imd"])
        assert any(item["metric_name"] == "ocpp_power_limit" for item in catalog["clc"])
        assert any(item["metric_name"] == "current_outlet" for item in catalog["eos"])

        window = analyzer.get_transaction_window(station_id=1, transaction_id="TX-1", padding_minutes=2)
        assert window["start_time"] == "2026-03-12T09:58:00"
        assert window["stop_time"] == "2026-03-12T10:12:00"
        assert "end_time" not in window

        bundle = analyzer.get_transaction_plot_series(
            station_id=1,
            transaction_id="TX-1",
            padding_minutes=2,
            selections=[
                {"source_type": "imd", "metric_name": "imd_voltage", "metric_scope": "CCS2"},
                {"source_type": "clc", "metric_name": "ocpp_power_limit", "metric_scope": "global"},
                {"source_type": "eos", "metric_name": "current_outlet", "metric_scope": "TX-1"},
            ],
        )

        assert bundle["window"]["session_start_time"] == "2026-03-12T10:00:00"
        assert len(bundle["series"]) == 3
        assert bundle["series"][0]["points"][0]["metric_value"] == 500.2
        assert bundle["series"][1]["points"][0]["metric_value"] == 50.0
        assert bundle["series"][2]["points"][0]["metric_value"] == 90.0
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)
