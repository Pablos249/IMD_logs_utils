"""Helpers for importing multiple log files and mixed log folders."""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional


@dataclass
class ImportEntry:
    file_path: str
    log_type: str
    inserted: int
    status: str
    details: str = ""


class MixedLogImporter:
    """Route mixed log files to the correct parser based on filename and content."""

    TYPE_CAN = "can"
    TYPE_CLC = "clc"
    TYPE_CONDITIONING = "conditioning"
    TYPE_CCS = "ccs"
    TYPE_EOS = "eos"
    TYPE_UNKNOWN = "unknown"

    def __init__(self, *, can_parser, clc_parser, conditioning_parser, ccs_parser, eos_parser):
        self.can_parser = can_parser
        self.clc_parser = clc_parser
        self.conditioning_parser = conditioning_parser
        self.ccs_parser = ccs_parser
        self.eos_parser = eos_parser

    def import_files(
        self,
        file_paths: Iterable[str],
        station_id: int,
        progress_callback=None,
    ) -> List[ImportEntry]:
        entries: List[ImportEntry] = []
        normalized_paths = list(dict.fromkeys(file_paths))
        total_files = len(normalized_paths)

        for index, file_path in enumerate(normalized_paths, start=1):
            log_type = self.detect_log_type(file_path)
            file_name = os.path.basename(file_path)
            if progress_callback:
                progress_callback(index - 1, total_files, file_name, log_type)

            if log_type == self.TYPE_UNKNOWN:
                entries.append(
                    ImportEntry(
                        file_path=file_path,
                        log_type=log_type,
                        inserted=0,
                        status="unknown",
                        details="Unsupported or unrecognized log format.",
                    )
                )
                continue

            parser = self._parser_for_type(log_type)
            try:
                inserted = parser.parse(file_path, station_id=station_id)
                status = "imported" if inserted > 0 else "skipped"
                details = "Already imported or no supported entries found." if inserted == 0 else ""
                entries.append(
                    ImportEntry(
                        file_path=file_path,
                        log_type=log_type,
                        inserted=inserted,
                        status=status,
                        details=details,
                    )
                )
            except Exception as exc:
                entries.append(
                    ImportEntry(
                        file_path=file_path,
                        log_type=log_type,
                        inserted=0,
                        status="error",
                        details=str(exc),
                    )
                )

            if progress_callback:
                progress_callback(index, total_files, file_name, log_type)

        return entries

    def detect_log_type(self, file_path: str) -> str:
        lower_name = os.path.basename(file_path).lower()
        extension = os.path.splitext(lower_name)[1]

        if "eos" in lower_name or extension == ".csv":
            if self._looks_like_eos(file_path):
                return self.TYPE_EOS

        if extension == ".can" or "imd" in lower_name or "can" in lower_name:
            if self._looks_like_can(file_path):
                return self.TYPE_CAN

        if "conditioning" in lower_name:
            return self.TYPE_CONDITIONING
        if "ccs" in lower_name:
            return self.TYPE_CCS
        if "clc" in lower_name:
            return self.TYPE_CLC

        line_samples = self._read_sample_lines(file_path)
        if not line_samples:
            return self.TYPE_UNKNOWN

        if any(self.can_parser._parse_line(line) for line in line_samples):
            return self.TYPE_CAN
        if any(self._looks_like_ccs_line(line) for line in line_samples):
            return self.TYPE_CCS

        clc_score = 0
        conditioning_score = 0
        for line in line_samples:
            parsed_clc = self.clc_parser._parse_line(line)
            if parsed_clc is not None:
                clc_score += max(1, len(self.clc_parser._extract_measurements(parsed_clc)))

            parsed_conditioning = self.conditioning_parser._parse_line(line)
            if parsed_conditioning is not None:
                conditioning_score += max(
                    1,
                    len(self.conditioning_parser._extract_measurements(parsed_conditioning)),
                )

        if clc_score == 0 and conditioning_score == 0:
            return self.TYPE_UNKNOWN
        if clc_score >= conditioning_score:
            return self.TYPE_CLC
        return self.TYPE_CONDITIONING

    def _looks_like_eos(self, file_path: str) -> bool:
        try:
            with open(file_path, "r", encoding="utf-8-sig", errors="ignore", newline="") as handle:
                reader = csv.reader(handle)
                rows = []
                for _ in range(5):
                    try:
                        rows.append(next(reader))
                    except StopIteration:
                        break
        except OSError:
            return False

        eos_headers = {"date", "transaction id", "transaction_id"}
        known_columns = {
            header.lower()
            for header in self.eos_parser.COLUMN_TO_HEADER.values()
        }
        for row in rows:
            normalized = {str(cell).strip().strip('"').lower() for cell in row if str(cell).strip()}
            if normalized & eos_headers:
                return True
            if len(normalized & known_columns) >= 2:
                return True
        return False

    def _looks_like_can(self, file_path: str) -> bool:
        return any(self.can_parser._parse_line(line) for line in self._read_sample_lines(file_path))

    def _read_sample_lines(self, file_path: str, limit: int = 40) -> List[str]:
        lines: List[str] = []
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line:
                        continue
                    lines.append(line)
                    if len(lines) >= limit:
                        break
        except OSError:
            return []
        return lines

    def _parser_for_type(self, log_type: str):
        parser_map: Dict[str, object] = {
            self.TYPE_CAN: self.can_parser,
            self.TYPE_CLC: self.clc_parser,
            self.TYPE_CONDITIONING: self.conditioning_parser,
            self.TYPE_CCS: self.ccs_parser,
            self.TYPE_EOS: self.eos_parser,
        }
        return parser_map[log_type]

    def _looks_like_ccs_line(self, line: str) -> bool:
        parsed = self.ccs_parser._parse_line(line)
        if parsed is None:
            return False
        logger = (parsed.get("logger_name") or "").upper()
        message = parsed.get("message") or ""
        return (
            logger in {"DIN70121", "TCP6SERVERCONTROLLER", "ISO15118"}
            or "EVSEPresentCurrent" in message
            or "EVTargetCurrent" in message
            or "SessionID:" in message
        )
