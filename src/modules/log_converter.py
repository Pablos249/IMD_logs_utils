"""Logic for converting logs into CSV format."""

import csv
import os
from typing import Optional

from src.modules.can_logs import CANLogParser


def convert_imd_log_to_csv(input_path: str, output_path: str, progress_callback=None) -> int:
    """Convert IMD CAN log file to CSV.

    Writes decoded CAN values and raw fields.

    Returns number of rows written.
    """
    parser = CANLogParser(db_path=":memory:")

    # Prepare CSV header
    header = [
        "timestamp",
        "can_id",
        "description",
        "voltage_V",
        "capacitance_nF",
        "resistance_ohm",
        "status_byte",
        "status_binary",
        "raw_data",
    ]

    written = 0
    with open(input_path, "r", encoding="utf-8", errors="ignore") as fin, \
            open(output_path, "w", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=header)
        writer.writeheader()

        # Count lines for progress (optional)
        total_lines = sum(1 for _ in fin)
        fin.seek(0)

        for i, line in enumerate(fin, start=1):
            line = line.strip()
            if not line:
                continue

            parsed = parser._parse_line(line)
            if not parsed:
                continue

            decoded = parser._decode_values(parsed["can_id"], parsed["data"]) or {}

            writer.writerow({
                "timestamp": parsed["timestamp"],
                "can_id": parsed["can_id"],
                "description": parsed["description"],
                "voltage_V": decoded.get("voltage_V"),
                "capacitance_nF": decoded.get("capacitance_nF"),
                "resistance_ohm": decoded.get("resistance_ohm"),
                "status_byte": decoded.get("status_byte"),
                "status_binary": decoded.get("status_binary"),
                "raw_data": parsed["data"].hex(),
            })

            written += 1
            if progress_callback:
                progress_callback(i, total_lines)

    return written


def convert_generic_log_to_csv(input_path: str, output_path: str, progress_callback=None) -> int:
    """Convert a generic log file to a basic CSV (line-by-line)."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    written = 0

    # Count total lines for progress tracking
    with open(input_path, "r", encoding="utf-8", errors="ignore") as fin:
        total = sum(1 for _ in fin)

    with open(input_path, "r", encoding="utf-8", errors="ignore") as fin, \
            open(output_path, "w", encoding="utf-8", newline="") as fout:
        writer = csv.writer(fout)
        writer.writerow(["line_number", "raw_line"])
        for i, line in enumerate(fin, start=1):
            writer.writerow([i, line.rstrip("\n")])
            written += 1
            if progress_callback:
                progress_callback(i, total)

    return written
