"""Airflow task entry points for the daily Extreme Climate workflow."""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_SELECT_RAW_WEATHER_FOR_VALIDATION_SQL = """
    SELECT
        region_id,
        observed_at,
        temperature_c,
        humidity_percent,
        precipitation_mm,
        wind_speed_mps,
        source_payload
    FROM raw_weather
    WHERE region_id = ANY(%s)
      AND observed_at >= %s
      AND observed_at < %s
    ORDER BY region_id, observed_at, raw_weather_id
"""


class PipelineTaskConfigError(ValueError):
    """Raised when an Airflow task receives invalid runtime configuration."""


def _date_window(window_start: str, window_end: str) -> Tuple[date, date]:
    try:
        start_date = date.fromisoformat(window_start)
        end_date = date.fromisoformat(window_end)
    except (TypeError, ValueError) as exc:
        raise PipelineTaskConfigError(
            "window_start and window_end must be ISO dates"
        ) from exc
    if start_date >= end_date:
        raise PipelineTaskConfigError("window_start must be before window_end")
    return start_date, end_date


def _regions_path(environ: Mapping[str, str]) -> Path:
    value = environ.get(
        "REGIONS_CONFIG_PATH",
        "/opt/airflow/project/config/regions.yaml",
    ).strip()
    if not value:
        raise PipelineTaskConfigError("REGIONS_CONFIG_PATH must not be empty")
    return Path(value)


def _report_path(environ: Mapping[str, str], start_date: date) -> Path:
    value = environ.get(
        "REPORT_OUTPUT_DIR",
        "/opt/airflow/project/reports",
    ).strip()
    if not value:
        raise PipelineTaskConfigError("REPORT_OUTPUT_DIR must not be empty")
    return Path(value) / f"extreme_climate_daily_{start_date.isoformat()}.xlsx"


def _query_bounds(
    start_date: date,
    end_date: date,
    regions: Sequence[Any],
) -> Tuple[datetime, datetime]:
    zones = [ZoneInfo(region.timezone) for region in regions]
    starts = [
        datetime.combine(start_date, time.min, tzinfo=zone).astimezone(timezone.utc)
        for zone in zones
    ]
    ends = [
        datetime.combine(end_date, time.min, tzinfo=zone).astimezone(timezone.utc)
        for zone in zones
    ]
    return min(starts), max(ends)


def _event_payload(row: Sequence[Any]) -> Mapping[str, Any]:
    if len(row) != 7:
        raise PipelineTaskConfigError(
            "raw_weather validation query returned an invalid row"
        )
    observed_at = row[1]
    if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
        raise PipelineTaskConfigError(
            "raw_weather observed_at must be a timezone-aware datetime"
        )
    return {
        "region_id": row[0],
        "observed_at": observed_at.astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "temperature_c": float(row[2]),
        "humidity_percent": None if row[3] is None else float(row[3]),
        "precipitation_mm": None if row[4] is None else float(row[4]),
        "wind_speed_mps": None if row[5] is None else float(row[5]),
        "source_payload": row[6],
    }


def validate_raw_weather_window(
    connection: Any,
    start_date: date,
    end_date: date,
    regions: Sequence[Any],
) -> int:
    """Validate raw rows belonging to the configured regions and date window."""

    from extreme_climate.weather_validation import validate_weather_event

    region_ids = {region.id for region in regions}
    region_zones = {region.id: ZoneInfo(region.timezone) for region in regions}
    query_start, query_end = _query_bounds(start_date, end_date, regions)
    rows = connection.execute(
        _SELECT_RAW_WEATHER_FOR_VALIDATION_SQL,
        (sorted(region_ids), query_start, query_end),
    ).fetchall()

    validated_count = 0
    for row in rows:
        validated = validate_weather_event(
            _event_payload(row),
            allowed_region_ids=region_ids,
        )
        local_date = validated.observed_at.astimezone(
            region_zones[validated.region_id]
        ).date()
        if start_date <= local_date < end_date:
            validated_count += 1
    return validated_count


def _connect(environ: Mapping[str, str]) -> Any:
    import psycopg

    from extreme_climate.postgres_config import load_postgres_settings

    settings = load_postgres_settings(environ)
    return psycopg.connect(**settings.connection_kwargs())


def validate_weather_task(
    window_start: str,
    window_end: str,
    *,
    environ: Optional[Mapping[str, str]] = None,
) -> int:
    """Validate the raw observations that the run is about to aggregate."""

    from extreme_climate.region_config import load_regions

    source = os.environ if environ is None else environ
    start_date, end_date = _date_window(window_start, window_end)
    regions = load_regions(_regions_path(source))
    with _connect(source) as connection:
        validated_count = validate_raw_weather_window(
            connection,
            start_date,
            end_date,
            regions,
        )
    logger.info(
        "Validated %d raw weather row(s) for [%s, %s)",
        validated_count,
        start_date,
        end_date,
    )
    return validated_count


def transform_weather_task(
    window_start: str,
    window_end: str,
    *,
    environ: Optional[Mapping[str, str]] = None,
) -> int:
    """Aggregate valid observations and return the number of summary rows."""

    from extreme_climate.daily_transformation import transform_daily_weather
    from extreme_climate.region_config import load_regions

    source = os.environ if environ is None else environ
    start_date, end_date = _date_window(window_start, window_end)
    regions = load_regions(_regions_path(source))
    with _connect(source) as connection:
        summaries = transform_daily_weather(
            connection,
            start_date,
            end_date,
            regions,
        )
    summary_count = len(summaries)
    logger.info(
        "Stored %d daily summary row(s) for [%s, %s)",
        summary_count,
        start_date,
        end_date,
    )
    return summary_count


def detect_anomalies_task(
    window_start: str,
    window_end: str,
    *,
    environ: Optional[Mapping[str, str]] = None,
) -> int:
    """Evaluate persisted summaries and return the number evaluated."""

    from extreme_climate.anomaly_detection import detect_weather_anomalies

    source = os.environ if environ is None else environ
    start_date, end_date = _date_window(window_start, window_end)
    with _connect(source) as connection:
        evaluations = detect_weather_anomalies(
            connection,
            start_date,
            end_date,
        )
    evaluation_count = len(evaluations)
    logger.info(
        "Evaluated %d daily summary row(s) for anomalies in [%s, %s)",
        evaluation_count,
        start_date,
        end_date,
    )
    return evaluation_count


def generate_report_task(
    window_start: str,
    window_end: str,
    *,
    environ: Optional[Mapping[str, str]] = None,
) -> Mapping[str, Any]:
    """Generate the run's Excel report and return XCom-safe result metadata."""

    from extreme_climate.excel_report import generate_weather_report

    source = os.environ if environ is None else environ
    start_date, end_date = _date_window(window_start, window_end)
    output_path = _report_path(source, start_date)
    with _connect(source) as connection:
        result = generate_weather_report(
            connection,
            start_date,
            end_date,
            output_path,
        )
    logger.info(
        "Wrote %d report row(s) to %s",
        result.row_count,
        result.output_path,
    )
    return {
        "output_path": str(result.output_path),
        "row_count": result.row_count,
    }
