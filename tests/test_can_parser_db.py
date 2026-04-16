from src.modules.can_logs import CANLogParser
import tempfile
import os

def test_db_parsing(tmp_path):
    sample = """(2026-03-11 13:48:55.004267)  can0  048BC501   [8]  01 01 00 00 00 00 00 00
(2026-03-11 13:48:55.032050)  can0  048BC502   [8]  02 00 00 00 00 00 00 00
"""
    log_file = tmp_path / "log.can"
    log_file.write_text(sample)
    parser = CANLogParser(db_path=str(tmp_path / "db.db"))
    count = parser.parse(str(log_file))
    assert count == 2
    total = parser.get_total_count()
    assert total == 2
    msgs, tot = parser.get_messages_page(0,1)
    assert tot == 2
    assert len(msgs) == 1
    # verify values stored
    assert msgs[0]["can_id"] == "048BC501"
    assert float(msgs[0]["voltage_V"]) == 0.01

    # deleting by filename should remove rows
    deleted = parser.delete_logs_by_file(os.path.basename(str(log_file)))
    assert deleted == 2
    msgs2, tot2 = parser.get_messages_page(0, 10)
    assert tot2 == 0


def test_migration_add_columns(tmp_path):
    # create an artificial old-style database (no station_id/file_name)
    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE can_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            can_id TEXT NOT NULL,
            raw_data BLOB NOT NULL
        )
    """)
    cur.execute("INSERT INTO can_messages (timestamp, can_id, raw_data) VALUES (?, ?, ?)",
                ("2026-01-01 00:00:00", "0xABC", b"\x01\x02"))
    conn.commit()
    conn.close()

    # instantiate parser pointed at existing file; migration should add missing columns
    parser2 = CANLogParser(db_path=str(db_path))
    # old row should still be visible
    msgs, tot = parser2.get_messages_page(0, 10)
    assert tot == 1
    assert msgs[0]["can_id"] == "0xABC"
    # new columns should exist (values default to NULL)
    assert "station_id" in msgs[0]
    assert "file_name" in msgs[0]
