import os
import tempfile
import pytest

from src.modules.can_logs import CANLogParser


def test_parse_simple(tmp_path):
    # verify that parser can read a simple log and store entries in the database
    sample = """(2026-03-11 13:48:55.000000)  can0  0x100   [3]  01 02 03
(2026-03-11 13:48:55.100000)  can0  0x200   [3]  04 05 06
"""
    file = tmp_path / "log.txt"
    file.write_text(sample)

    parser = CANLogParser(db_path=str(tmp_path / "can.db"))
    count = parser.parse(str(file))
    assert count == 2

    msgs, total = parser.get_messages_page(0, 10)
    assert total == 2
    assert msgs[0]["can_id"] == "0x100"
    assert msgs[1]["can_id"] == "0x200"


def test_multiple_loads(tmp_path):
    # calling parse twice should accumulate entries rather than erase them
    sample1 = """(2026-03-11 13:48:55.000000)  can0  0xAAA   [1]  FF
"""
    sample2 = """(2026-03-11 13:48:56.000000)  can0  0xBBB   [1]  EE
"""
    f1 = tmp_path / "one.can"
    f2 = tmp_path / "two.can"
    f1.write_text(sample1)
    f2.write_text(sample2)

    parser = CANLogParser(db_path=str(tmp_path / "can.db"))
    parser.parse(str(f1))
    parser.parse(str(f2))
    msgs, total = parser.get_messages_page(0, 10)
    assert total == 2
    ids = {m['can_id'] for m in msgs}
    assert ids == {"0xAAA", "0xBBB"}


def test_station_filtering(tmp_path):
    parser = CANLogParser(db_path=str(tmp_path / "can.db"))
    sample1 = """(2026-03-11 13:48:55.000000)  can0  0x111   [1]  AA
"""
    sample2 = """(2026-03-11 13:48:56.000000)  can0  0x222   [1]  BB
"""
    f1 = tmp_path / "s1.can"
    f2 = tmp_path / "s2.can"
    f1.write_text(sample1)
    f2.write_text(sample2)

    parser.parse(str(f1), station_id=1)
    parser.parse(str(f2), station_id=2)
    msgs1, tot1 = parser.get_messages_page(0, 10, station_id=1)
    msgs2, tot2 = parser.get_messages_page(0, 10, station_id=2)
    assert tot1 == 1
    assert tot2 == 1
    assert msgs1[0]["station_id"] == 1
    assert msgs2[0]["station_id"] == 2


def test_filtering_and_decoding(tmp_path):
    # make sure that decode logic is applied to stored rows
    sample = """(2026-03-11 13:48:55.000000)  can0  048BC501   [8]  01 00 00 00 00 00 00 00
"""
    file = tmp_path / "log2.txt"
    file.write_text(sample)

    parser = CANLogParser(db_path=str(tmp_path / "can.db"))
    parser.parse(str(file))
    msgs, total = parser.get_messages_page(0, 5, filter_can_id="048BC501")
    assert total == 1
    assert msgs[0]["voltage_V"] == pytest.approx(0.01)


def test_imd_voltage_wraps_large_uint16_values(tmp_path):
    sample = """(2026-03-11 13:48:55.000000)  can0  048BC501   [8]  01 00 FE FF 00 00 00 00
"""
    file = tmp_path / "wrapped_voltage.can"
    file.write_text(sample)

    parser = CANLogParser(db_path=str(tmp_path / "can.db"))
    parser.parse(str(file))

    msgs, total = parser.get_messages_page(0, 5, filter_can_id="048BC501")
    assert total == 1
    assert msgs[0]["voltage_V"] == pytest.approx(-0.2)
