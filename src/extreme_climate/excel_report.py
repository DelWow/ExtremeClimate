"""Generate the basic single-sheet Excel report for daily weather summaries."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Protocol, Sequence, Tuple, Union

import psycopg
from openpyxl import Workbook


DEFAULT_REPORT_PATH = Path("reports/extreme_climate_daily.xlsx")
WORKSHEET_TITLE = "Daily Weather"
REPORT_HEADERS = (
    "Region ID",
    "Summary Date",
    "Observation Count",
    "Mean Temperature (°C)",
    "Minimum Temperature (°C)",
    "Maximum Temperature (°C)",
    "Mean Humidity (%)",
    "Total Precipitation (mm)",
    "Maximum Wind Speed (m/s)",
    "Is Anomaly",
    "Anomaly Status",
    "Anomaly Details",
)

_SELECT_REPORT_ROWS_SQL = """
    SELECT
        region_id,
        summary_date,
        observation_count,
        mean_temperature_c,
        min_temperature_c,
        max_temperature_c,
        mean_humidity_percent,
        total_precipitation_mm,
        max_wind_speed_mps,
        is_anomaly,
        anomaly_details
    FROM weather_daily_summary
    WHERE summary_date >= %s
      AND summary_date < %s
    ORDER BY region_id, summary_date
"""


class ReportInputError(ValueError):
    """Raised when report rows, dates, or anomaly details are malformed."""


class ReportGenerationError(RuntimeError):
    """Raised when report data cannot be read or the workbook cannot be saved."""


@dataclass(frozen=True)
class WeatherReportRow:
    """One typed row in the basic daily-weather workbook."""

    region_id: str
    summary_date: date
    observation_count: int
    mean_temperature_c: Optional[Decimal]
    min_temperature_c: Optional[Decimal]
    max_temperature_c: Optional[Decimal]
    mean_humidity_percent: Optional[Decimal]
    total_precipitation_mm: Optional[Decimal]
    max_wind_speed_mps: Optional[Decimal]
    is_anomaly: Optional[bool]
    anomaly_status: str
    anomaly_details_json: Optional[str]


@dataclass(frozen=True)
class ReportResult:
    """Location and row count of a saved workbook."""

    output_path: Path
    row_count: int


class CursorProtocol(Protocol):
    def fetchall(self) -> Sequence[Sequence[Any]]:
        ...


class ConnectionProtocol(Protocol):
    def execute(self, query: str, params: Sequence[Any]) -> CursorProtocol:
        ...


def _validate_date_range(start_date: date, end_date: date) -> None:
    if (
        not isinstance(start_date, date)
        or isinstance(start_date, datetime)
        or not isinstance(end_date, date)
        or isinstance(end_date, datetime)
    ):
        raise ReportInputError("start_date and end_date must be dates")
    if start_date >= end_date:
        raise ReportInputError("start_date must be before end_date")


def _anomaly_fields(value: Any) -> Tuple[str, Optional[str]]:
    if value is None:
        return "not_evaluated", None
    if isinstance(value, str):
        try:
            document = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ReportInputError("anomaly_details must be valid JSON") from exc
    else:
        document = value
    if not isinstance(document, Mapping):
        raise ReportInputError("anomaly_details must be a JSON object")
    status = document.get("status")
    if not isinstance(status, str) or not status:
        raise ReportInputError("anomaly_details.status must be a non-empty string")
    try:
        details_json = json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise ReportInputError("anomaly_details must be JSON-compatible") from exc
    return status, details_json


def _report_row(row: Sequence[Any]) -> WeatherReportRow:
    if len(row) != 11:
        raise ReportInputError("report query returned an invalid row")
    anomaly_status, anomaly_details_json = _anomaly_fields(row[10])
    return WeatherReportRow(
        region_id=row[0],
        summary_date=row[1],
        observation_count=row[2],
        mean_temperature_c=row[3],
        min_temperature_c=row[4],
        max_temperature_c=row[5],
        mean_humidity_percent=row[6],
        total_precipitation_mm=row[7],
        max_wind_speed_mps=row[8],
        is_anomaly=row[9],
        anomaly_status=anomaly_status,
        anomaly_details_json=anomaly_details_json,
    )


def load_weather_report_rows(
    connection: ConnectionProtocol,
    start_date: date,
    end_date: date,
) -> Tuple[WeatherReportRow, ...]:
    """Load report rows in the half-open date interval, sorted deterministically."""

    _validate_date_range(start_date, end_date)
    try:
        cursor = connection.execute(
            _SELECT_REPORT_ROWS_SQL,
            (start_date, end_date),
        )
        rows = cursor.fetchall()
    except psycopg.Error as exc:
        raise ReportGenerationError(
            "PostgreSQL could not read daily weather report data"
        ) from exc
    return tuple(_report_row(row) for row in rows)


def _excel_number(value: Optional[Decimal]) -> Optional[float]:
    return None if value is None else float(value)


def build_weather_report_workbook(
    rows: Iterable[WeatherReportRow],
) -> Workbook:
    """Build a deterministic, single-sheet workbook without visual styling."""

    report_rows = tuple(rows)
    keys = [(row.region_id, row.summary_date) for row in report_rows]
    if len(keys) != len(set(keys)):
        raise ReportInputError("weather report rows contain duplicate keys")
    report_rows = tuple(
        sorted(report_rows, key=lambda row: (row.region_id, row.summary_date))
    )

    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = WORKSHEET_TITLE
    worksheet.append(REPORT_HEADERS)
    for row in report_rows:
        worksheet.append(
            (
                row.region_id,
                row.summary_date,
                row.observation_count,
                _excel_number(row.mean_temperature_c),
                _excel_number(row.min_temperature_c),
                _excel_number(row.max_temperature_c),
                _excel_number(row.mean_humidity_percent),
                _excel_number(row.total_precipitation_mm),
                _excel_number(row.max_wind_speed_mps),
                row.is_anomaly,
                row.anomaly_status,
                row.anomaly_details_json,
            )
        )
    return workbook


def write_weather_report(
    rows: Iterable[WeatherReportRow],
    output_path: Union[str, Path] = DEFAULT_REPORT_PATH,
) -> ReportResult:
    """Save report rows to an `.xlsx` file and return its path and row count."""

    destination = Path(output_path)
    if destination.suffix.lower() != ".xlsx":
        raise ReportInputError("output_path must end in .xlsx")
    report_rows = tuple(rows)
    workbook = build_weather_report_workbook(report_rows)
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        workbook.save(destination)
    except (OSError, ValueError) as exc:
        raise ReportGenerationError(
            f"could not save Excel report to {destination}"
        ) from exc
    finally:
        workbook.close()
    return ReportResult(output_path=destination, row_count=len(report_rows))


def generate_weather_report(
    connection: ConnectionProtocol,
    start_date: date,
    end_date: date,
    output_path: Union[str, Path] = DEFAULT_REPORT_PATH,
) -> ReportResult:
    """Read daily summaries and write their basic Excel report."""

    rows = load_weather_report_rows(connection, start_date, end_date)
    return write_weather_report(rows, output_path)
