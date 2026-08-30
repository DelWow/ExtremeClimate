"""Aggregate valid raw weather observations into region-local daily summaries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import (
    Any,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Union,
)
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import psycopg

from extreme_climate.region_config import Region
from extreme_climate.weather_validation import (
    MAX_HUMIDITY_PERCENT,
    MAX_OBSERVED_AT,
    MAX_PRECIPITATION_MM,
    MAX_TEMPERATURE_C,
    MAX_WIND_SPEED_MPS,
    MIN_HUMIDITY_PERCENT,
    MIN_OBSERVED_AT,
    MIN_PRECIPITATION_MM,
    MIN_TEMPERATURE_C,
    MIN_WIND_SPEED_MPS,
)


SUMMARY_QUANTUM = Decimal("0.01")

_SELECT_RAW_WEATHER_SQL = """
    SELECT
        region_id,
        observed_at,
        temperature_c,
        humidity_percent,
        precipitation_mm,
        wind_speed_mps
    FROM raw_weather
    WHERE region_id = ANY(%s)
      AND observed_at >= %s
      AND observed_at < %s
    ORDER BY region_id, observed_at, raw_weather_id
"""

_UPSERT_DAILY_SUMMARY_SQL = """
    INSERT INTO weather_daily_summary AS existing (
        region_id,
        summary_date,
        observation_count,
        mean_temperature_c,
        min_temperature_c,
        max_temperature_c,
        mean_humidity_percent,
        total_precipitation_mm,
        max_wind_speed_mps
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (region_id, summary_date) DO UPDATE
    SET observation_count = EXCLUDED.observation_count,
        mean_temperature_c = EXCLUDED.mean_temperature_c,
        min_temperature_c = EXCLUDED.min_temperature_c,
        max_temperature_c = EXCLUDED.max_temperature_c,
        mean_humidity_percent = EXCLUDED.mean_humidity_percent,
        total_precipitation_mm = EXCLUDED.total_precipitation_mm,
        max_wind_speed_mps = EXCLUDED.max_wind_speed_mps,
        is_anomaly = NULL,
        anomaly_details = NULL,
        updated_at = CURRENT_TIMESTAMP
    WHERE (
        existing.observation_count,
        existing.mean_temperature_c,
        existing.min_temperature_c,
        existing.max_temperature_c,
        existing.mean_humidity_percent,
        existing.total_precipitation_mm,
        existing.max_wind_speed_mps
    ) IS DISTINCT FROM (
        EXCLUDED.observation_count,
        EXCLUDED.mean_temperature_c,
        EXCLUDED.min_temperature_c,
        EXCLUDED.max_temperature_c,
        EXCLUDED.mean_humidity_percent,
        EXCLUDED.total_precipitation_mm,
        EXCLUDED.max_wind_speed_mps
    )
"""


class DailyAggregationError(ValueError):
    """Raised when an observation cannot be grouped or aggregated safely."""


class DailyTransformationError(RuntimeError):
    """Raised when raw rows cannot be read or summaries cannot be written."""


@dataclass(frozen=True)
class RawWeatherObservation:
    """The raw_weather fields required for daily aggregation."""

    region_id: str
    observed_at: datetime
    temperature_c: Decimal
    humidity_percent: Optional[Decimal]
    precipitation_mm: Optional[Decimal]
    wind_speed_mps: Optional[Decimal]


@dataclass(frozen=True)
class DailyWeatherSummary:
    """One two-decimal aggregate matching weather_daily_summary units."""

    region_id: str
    summary_date: date
    observation_count: int
    mean_temperature_c: Decimal
    min_temperature_c: Decimal
    max_temperature_c: Decimal
    mean_humidity_percent: Optional[Decimal]
    total_precipitation_mm: Optional[Decimal]
    max_wind_speed_mps: Optional[Decimal]


class CursorProtocol(Protocol):
    def __enter__(self) -> "CursorProtocol":
        ...

    def __exit__(self, *args: Any) -> None:
        ...

    def executemany(
        self,
        query: str,
        params_seq: Iterable[Sequence[Any]],
    ) -> None:
        ...

    def fetchall(self) -> Sequence[Sequence[Any]]:
        ...


class ConnectionProtocol(Protocol):
    def transaction(self) -> Any:
        ...

    def cursor(self) -> CursorProtocol:
        ...

    def execute(self, query: str, params: Sequence[Any]) -> CursorProtocol:
        ...


def _decimal(
    value: Any,
    field_name: str,
    *,
    required: bool,
    minimum: Decimal,
    maximum: Decimal,
) -> Optional[Decimal]:
    if value is None:
        if required:
            raise DailyAggregationError(f"{field_name} must not be null")
        return None
    if isinstance(value, bool):
        raise DailyAggregationError(f"{field_name} must be a number")
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise DailyAggregationError(f"{field_name} must be a number") from exc
    if not number.is_finite():
        raise DailyAggregationError(f"{field_name} must be finite")
    if not minimum <= number <= maximum:
        raise DailyAggregationError(
            f"{field_name} must be between {minimum} and {maximum}"
        )
    return number


def _validated_observation(
    observation: RawWeatherObservation,
    region_timezones: Mapping[str, ZoneInfo],
) -> RawWeatherObservation:
    if observation.region_id not in region_timezones:
        raise DailyAggregationError(
            f"region_id {observation.region_id!r} is not configured"
        )
    observed_at = observation.observed_at
    if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
        raise DailyAggregationError("observed_at must be timezone-aware")
    observed_at_utc = observed_at.astimezone(timezone.utc)
    if not MIN_OBSERVED_AT <= observed_at_utc <= MAX_OBSERVED_AT:
        raise DailyAggregationError("observed_at is outside the supported range")

    temperature = _decimal(
        observation.temperature_c,
        "temperature_c",
        required=True,
        minimum=Decimal(str(MIN_TEMPERATURE_C)),
        maximum=Decimal(str(MAX_TEMPERATURE_C)),
    )
    assert temperature is not None
    return RawWeatherObservation(
        region_id=observation.region_id,
        observed_at=observed_at_utc,
        temperature_c=temperature,
        humidity_percent=_decimal(
            observation.humidity_percent,
            "humidity_percent",
            required=False,
            minimum=Decimal(str(MIN_HUMIDITY_PERCENT)),
            maximum=Decimal(str(MAX_HUMIDITY_PERCENT)),
        ),
        precipitation_mm=_decimal(
            observation.precipitation_mm,
            "precipitation_mm",
            required=False,
            minimum=Decimal(str(MIN_PRECIPITATION_MM)),
            maximum=Decimal(str(MAX_PRECIPITATION_MM)),
        ),
        wind_speed_mps=_decimal(
            observation.wind_speed_mps,
            "wind_speed_mps",
            required=False,
            minimum=Decimal(str(MIN_WIND_SPEED_MPS)),
            maximum=Decimal(str(MAX_WIND_SPEED_MPS)),
        ),
    )


def _timezone_map(
    region_timezones: Mapping[str, Union[str, ZoneInfo]],
) -> Dict[str, ZoneInfo]:
    timezones: Dict[str, ZoneInfo] = {}
    for region_id, value in region_timezones.items():
        if not isinstance(region_id, str) or not region_id:
            raise DailyAggregationError("region timezone keys must be region IDs")
        try:
            zone = value if isinstance(value, ZoneInfo) else ZoneInfo(value)
        except (TypeError, ValueError, ZoneInfoNotFoundError) as exc:
            raise DailyAggregationError(
                f"region {region_id!r} has invalid timezone {value!r}"
            ) from exc
        timezones[region_id] = zone
    if not timezones:
        raise DailyAggregationError("at least one region timezone is required")
    return timezones


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(SUMMARY_QUANTUM, rounding=ROUND_HALF_UP)


def _mean(values: Sequence[Decimal]) -> Decimal:
    return _quantize(sum(values, Decimal("0")) / len(values))


def _optional_mean(values: Sequence[Optional[Decimal]]) -> Optional[Decimal]:
    available = [value for value in values if value is not None]
    return _mean(available) if available else None


def _optional_sum(values: Sequence[Optional[Decimal]]) -> Optional[Decimal]:
    available = [value for value in values if value is not None]
    return _quantize(sum(available, Decimal("0"))) if available else None


def _optional_max(values: Sequence[Optional[Decimal]]) -> Optional[Decimal]:
    available = [value for value in values if value is not None]
    return _quantize(max(available)) if available else None


def aggregate_daily_weather(
    observations: Iterable[RawWeatherObservation],
    region_timezones: Mapping[str, Union[str, ZoneInfo]],
) -> Tuple[DailyWeatherSummary, ...]:
    """Group observations by region-local date and calculate daily metrics.

    Temperature and humidity are arithmetic means of available samples;
    precipitation is the sum of available provider-interval amounts; wind is
    the maximum available speed. Optional outputs are ``None`` only when every
    observation in that group lacks that metric. ``observation_count`` includes
    every valid raw row regardless of optional measurement availability. All
    units remain Celsius, percent, millimetres, and metres per second, rounded
    to two decimal places.
    """

    timezones = _timezone_map(region_timezones)
    grouped: Dict[Tuple[str, date], list[RawWeatherObservation]] = {}
    for raw_observation in observations:
        observation = _validated_observation(raw_observation, timezones)
        local_date = observation.observed_at.astimezone(
            timezones[observation.region_id]
        ).date()
        grouped.setdefault((observation.region_id, local_date), []).append(
            observation
        )

    summaries = []
    for (region_id, summary_date), group in sorted(grouped.items()):
        temperatures = [observation.temperature_c for observation in group]
        summaries.append(
            DailyWeatherSummary(
                region_id=region_id,
                summary_date=summary_date,
                observation_count=len(group),
                mean_temperature_c=_mean(temperatures),
                min_temperature_c=_quantize(min(temperatures)),
                max_temperature_c=_quantize(max(temperatures)),
                mean_humidity_percent=_optional_mean(
                    [observation.humidity_percent for observation in group]
                ),
                total_precipitation_mm=_optional_sum(
                    [observation.precipitation_mm for observation in group]
                ),
                max_wind_speed_mps=_optional_max(
                    [observation.wind_speed_mps for observation in group]
                ),
            )
        )
    return tuple(summaries)


class DailySummaryStore:
    """Idempotently persist daily summaries by their region/date key."""

    def __init__(self, connection: ConnectionProtocol) -> None:
        self._connection = connection

    def upsert(self, summaries: Iterable[DailyWeatherSummary]) -> int:
        """Submit unique summaries atomically and return their input count.

        An identical replay leaves the stored row and ``updated_at`` unchanged.
        Changed aggregate values replace the prior values and clear anomaly
        fields so Step 12 can evaluate the new summary.
        """

        rows = tuple(summaries)
        keys = [(row.region_id, row.summary_date) for row in rows]
        if len(keys) != len(set(keys)):
            raise DailyAggregationError("daily summaries contain duplicate keys")
        if not rows:
            return 0

        parameters = tuple(
            (
                row.region_id,
                row.summary_date,
                row.observation_count,
                row.mean_temperature_c,
                row.min_temperature_c,
                row.max_temperature_c,
                row.mean_humidity_percent,
                row.total_precipitation_mm,
                row.max_wind_speed_mps,
            )
            for row in rows
        )
        try:
            with self._connection.transaction():
                with self._connection.cursor() as cursor:
                    cursor.executemany(_UPSERT_DAILY_SUMMARY_SQL, parameters)
        except psycopg.Error as exc:
            raise DailyTransformationError(
                "PostgreSQL could not persist daily weather summaries"
            ) from exc
        return len(rows)


def _query_bounds(
    start_date: date,
    end_date: date,
    timezones: Mapping[str, ZoneInfo],
) -> Tuple[datetime, datetime]:
    starts = [
        datetime.combine(start_date, time.min, tzinfo=zone).astimezone(timezone.utc)
        for zone in timezones.values()
    ]
    ends = [
        datetime.combine(end_date, time.min, tzinfo=zone).astimezone(timezone.utc)
        for zone in timezones.values()
    ]
    return min(starts), max(ends)


def _observation_from_row(row: Sequence[Any]) -> RawWeatherObservation:
    if len(row) != 6:
        raise DailyAggregationError("raw_weather query returned an invalid row")
    return RawWeatherObservation(
        region_id=row[0],
        observed_at=row[1],
        temperature_c=row[2],
        humidity_percent=row[3],
        precipitation_mm=row[4],
        wind_speed_mps=row[5],
    )


def transform_daily_weather(
    connection: ConnectionProtocol,
    start_date: date,
    end_date: date,
    regions: Sequence[Region],
) -> Tuple[DailyWeatherSummary, ...]:
    """Read, aggregate, and upsert summaries in ``[start_date, end_date)``.

    The raw UTC query uses the widest local-midnight bounds across configured
    regions. Results are then filtered by actual region-local date so timezone
    and daylight-saving boundaries remain correct.
    """

    if not isinstance(start_date, date) or not isinstance(end_date, date):
        raise DailyAggregationError("start_date and end_date must be dates")
    if start_date >= end_date:
        raise DailyAggregationError("start_date must be before end_date")
    region_timezones = {region.id: region.timezone for region in regions}
    if len(region_timezones) != len(regions):
        raise DailyAggregationError("configured regions contain duplicate IDs")
    timezones = _timezone_map(region_timezones)
    query_start, query_end = _query_bounds(start_date, end_date, timezones)

    try:
        cursor = connection.execute(
            _SELECT_RAW_WEATHER_SQL,
            (list(region_timezones), query_start, query_end),
        )
        rows = cursor.fetchall()
    except psycopg.Error as exc:
        raise DailyTransformationError(
            "PostgreSQL could not read raw weather observations"
        ) from exc

    observations = tuple(_observation_from_row(row) for row in rows)
    summaries = tuple(
        summary
        for summary in aggregate_daily_weather(observations, timezones)
        if start_date <= summary.summary_date < end_date
    )
    DailySummaryStore(connection).upsert(summaries)
    return summaries
