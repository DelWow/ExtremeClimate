from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Optional

import psycopg
import pytest

from extreme_climate.daily_transformation import (
    DailyAggregationError,
    DailySummaryStore,
    DailyTransformationError,
    DailyWeatherSummary,
    RawWeatherObservation,
    aggregate_daily_weather,
    transform_daily_weather,
)
from extreme_climate.region_config import Region


TORONTO = Region(
    id="toronto",
    latitude=43.6532,
    longitude=-79.3832,
    timezone="America/Toronto",
)
VANCOUVER = Region(
    id="vancouver",
    latitude=49.2827,
    longitude=-123.1207,
    timezone="America/Vancouver",
)


def _observation(
    observed_at: str = "2026-08-30T12:00:00+00:00",
    *,
    region_id: str = "toronto",
    temperature: str = "20.00",
    humidity: Optional[str] = "50.00",
    precipitation: Optional[str] = "1.00",
    wind: Optional[str] = "3.00",
) -> RawWeatherObservation:
    return RawWeatherObservation(
        region_id=region_id,
        observed_at=datetime.fromisoformat(observed_at),
        temperature_c=Decimal(temperature),
        humidity_percent=None if humidity is None else Decimal(humidity),
        precipitation_mm=(
            None if precipitation is None else Decimal(precipitation)
        ),
        wind_speed_mps=None if wind is None else Decimal(wind),
    )


def _summary(
    summary_date: date = date(2026, 8, 30),
    *,
    region_id: str = "toronto",
) -> DailyWeatherSummary:
    return DailyWeatherSummary(
        region_id=region_id,
        summary_date=summary_date,
        observation_count=3,
        mean_temperature_c=Decimal("20.00"),
        min_temperature_c=Decimal("10.00"),
        max_temperature_c=Decimal("30.00"),
        mean_humidity_percent=Decimal("60.00"),
        total_precipitation_mm=Decimal("3.75"),
        max_wind_speed_mps=Decimal("4.00"),
    )


class FakeCursor:
    def __init__(self, *, rows=(), write_error=None):
        self.rows = tuple(rows)
        self.write_error = write_error
        self.executemany_calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def fetchall(self):
        return self.rows

    def executemany(self, query, parameters):
        self.executemany_calls.append((query, tuple(parameters)))
        if self.write_error is not None:
            raise self.write_error


class FakeTransaction:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        self.log.append("begin")
        return self

    def __exit__(self, exception_type, _exception, _traceback):
        self.log.append("commit" if exception_type is None else "rollback")
        return False


class FakeConnection:
    def __init__(self, *, rows=(), read_error=None, write_error=None):
        self.log = []
        self.read_error = read_error
        self.read_cursor = FakeCursor(rows=rows)
        self.write_cursor = FakeCursor(write_error=write_error)
        self.execute_calls = []

    def execute(self, query, parameters):
        self.execute_calls.append((query, parameters))
        if self.read_error is not None:
            raise self.read_error
        return self.read_cursor

    def transaction(self):
        return FakeTransaction(self.log)

    def cursor(self):
        return self.write_cursor


def test_groups_observations_by_region_local_calendar_date() -> None:
    observations = [
        _observation("2026-08-30T03:30:00+00:00", temperature="10"),
        _observation("2026-08-30T04:30:00+00:00", temperature="20"),
        _observation(
            "2026-08-30T06:30:00+00:00",
            region_id="vancouver",
            temperature="15",
        ),
    ]

    summaries = aggregate_daily_weather(
        observations,
        {
            "toronto": "America/Toronto",
            "vancouver": "America/Vancouver",
        },
    )

    assert [(row.region_id, row.summary_date) for row in summaries] == [
        ("toronto", date(2026, 8, 29)),
        ("toronto", date(2026, 8, 30)),
        ("vancouver", date(2026, 8, 29)),
    ]


def test_calculates_defined_daily_metrics_from_available_samples() -> None:
    observations = [
        _observation(
            temperature="10",
            humidity=None,
            precipitation=None,
            wind="4",
        ),
        _observation(
            "2026-08-30T13:00:00+00:00",
            temperature="20",
            humidity="40",
            precipitation="1.25",
            wind=None,
        ),
        _observation(
            "2026-08-30T14:00:00+00:00",
            temperature="30",
            humidity="80",
            precipitation="2.50",
            wind="2",
        ),
    ]

    summary = aggregate_daily_weather(
        observations,
        {"toronto": "America/Toronto"},
    )[0]

    assert summary == _summary()


def test_keeps_optional_aggregate_null_when_all_samples_are_missing() -> None:
    observations = [
        _observation(humidity=None, precipitation=None, wind=None),
        _observation(
            "2026-08-30T13:00:00+00:00",
            temperature="22",
            humidity=None,
            precipitation=None,
            wind=None,
        ),
    ]

    summary = aggregate_daily_weather(
        observations,
        {"toronto": "America/Toronto"},
    )[0]

    assert summary.observation_count == 2
    assert summary.mean_temperature_c == Decimal("21.00")
    assert summary.mean_humidity_percent is None
    assert summary.total_precipitation_mm is None
    assert summary.max_wind_speed_mps is None


def test_preserves_available_zero_values_as_measurements() -> None:
    summary = aggregate_daily_weather(
        [
            _observation(
                humidity="0",
                precipitation="0",
                wind="0",
            )
        ],
        {"toronto": "America/Toronto"},
    )[0]

    assert summary.mean_humidity_percent == Decimal("0.00")
    assert summary.total_precipitation_mm == Decimal("0.00")
    assert summary.max_wind_speed_mps == Decimal("0.00")


def test_rounds_summary_values_to_two_decimals_half_up() -> None:
    observations = [
        _observation(temperature="0.00", humidity="0.00"),
        _observation(
            "2026-08-30T13:00:00+00:00",
            temperature="0.01",
            humidity="0.01",
        ),
    ]

    summary = aggregate_daily_weather(
        observations,
        {"toronto": "America/Toronto"},
    )[0]

    assert summary.mean_temperature_c == Decimal("0.01")
    assert summary.mean_humidity_percent == Decimal("0.01")


def test_empty_observation_input_produces_no_summaries() -> None:
    assert aggregate_daily_weather([], {"toronto": "UTC"}) == ()


@pytest.mark.parametrize(
    ("observations", "timezones", "expected_error"),
    [
        ([_observation(region_id="halifax")], {"toronto": "UTC"}, "not configured"),
        ([_observation()], {"toronto": "Not/AZone"}, "invalid timezone"),
        ([_observation()], {}, "at least one region timezone"),
        (
            [
                RawWeatherObservation(
                    region_id="toronto",
                    observed_at=datetime(2026, 8, 30, 12),
                    temperature_c=Decimal("20"),
                    humidity_percent=None,
                    precipitation_mm=None,
                    wind_speed_mps=None,
                )
            ],
            {"toronto": "UTC"},
            "timezone-aware",
        ),
        (
            [_observation(temperature="66")],
            {"toronto": "UTC"},
            "temperature_c must be between",
        ),
    ],
)
def test_rejects_invalid_aggregation_inputs(
    observations,
    timezones,
    expected_error: str,
) -> None:
    with pytest.raises(DailyAggregationError, match=expected_error):
        aggregate_daily_weather(observations, timezones)


def test_store_uses_atomic_change_aware_upsert() -> None:
    connection = FakeConnection()
    summaries = [_summary(), _summary(date(2026, 8, 31))]

    submitted = DailySummaryStore(connection).upsert(summaries)

    assert submitted == 2
    query, parameters = connection.write_cursor.executemany_calls[0]
    assert "ON CONFLICT (region_id, summary_date) DO UPDATE" in query
    assert "IS DISTINCT FROM" in query
    assert "is_anomaly = NULL" in query
    assert "anomaly_details = NULL" in query
    assert parameters == (
        (
            "toronto",
            date(2026, 8, 30),
            3,
            Decimal("20.00"),
            Decimal("10.00"),
            Decimal("30.00"),
            Decimal("60.00"),
            Decimal("3.75"),
            Decimal("4.00"),
        ),
        (
            "toronto",
            date(2026, 8, 31),
            3,
            Decimal("20.00"),
            Decimal("10.00"),
            Decimal("30.00"),
            Decimal("60.00"),
            Decimal("3.75"),
            Decimal("4.00"),
        ),
    )
    assert connection.log == ["begin", "commit"]


def test_store_empty_input_is_a_no_op() -> None:
    connection = FakeConnection()

    assert DailySummaryStore(connection).upsert([]) == 0
    assert connection.write_cursor.executemany_calls == []
    assert connection.log == []


def test_store_rejects_duplicate_summary_keys() -> None:
    summary = _summary()

    with pytest.raises(DailyAggregationError, match="duplicate keys"):
        DailySummaryStore(FakeConnection()).upsert([summary, summary])


def test_store_wraps_database_failure_and_rolls_back() -> None:
    connection = FakeConnection(
        write_error=psycopg.OperationalError("database unavailable")
    )

    with pytest.raises(DailyTransformationError, match="could not persist"):
        DailySummaryStore(connection).upsert([_summary()])

    assert connection.log == ["begin", "rollback"]


def test_transformation_reads_wide_utc_window_and_filters_local_dates() -> None:
    connection = FakeConnection(
        rows=[
            (
                "toronto",
                datetime(2026, 8, 30, 4, tzinfo=timezone.utc),
                Decimal("20"),
                Decimal("50"),
                Decimal("1"),
                Decimal("3"),
            ),
            (
                "toronto",
                datetime(2026, 8, 31, 4, 30, tzinfo=timezone.utc),
                Decimal("21"),
                Decimal("51"),
                Decimal("0"),
                Decimal("4"),
            ),
            (
                "vancouver",
                datetime(2026, 8, 31, 6, 30, tzinfo=timezone.utc),
                Decimal("18"),
                None,
                None,
                None,
            ),
        ]
    )

    summaries = transform_daily_weather(
        connection,
        date(2026, 8, 30),
        date(2026, 8, 31),
        [TORONTO, VANCOUVER],
    )

    query, parameters = connection.execute_calls[0]
    assert "FROM raw_weather" in query
    assert parameters == (
        ["toronto", "vancouver"],
        datetime(2026, 8, 30, 4, tzinfo=timezone.utc),
        datetime(2026, 8, 31, 7, tzinfo=timezone.utc),
    )
    assert [(row.region_id, row.summary_date) for row in summaries] == [
        ("toronto", date(2026, 8, 30)),
        ("vancouver", date(2026, 8, 30)),
    ]
    written = connection.write_cursor.executemany_calls[0][1]
    assert [(row[0], row[1]) for row in written] == [
        ("toronto", date(2026, 8, 30)),
        ("vancouver", date(2026, 8, 30)),
    ]


def test_transformation_query_bounds_follow_daylight_saving_transition() -> None:
    connection = FakeConnection()

    summaries = transform_daily_weather(
        connection,
        date(2026, 11, 1),
        date(2026, 11, 2),
        [TORONTO],
    )

    assert summaries == ()
    assert connection.execute_calls[0][1] == (
        ["toronto"],
        datetime(2026, 11, 1, 4, tzinfo=timezone.utc),
        datetime(2026, 11, 2, 5, tzinfo=timezone.utc),
    )
    assert connection.log == []


@pytest.mark.parametrize(
    ("start_date", "end_date"),
    [
        (date(2026, 8, 30), date(2026, 8, 30)),
        (date(2026, 8, 31), date(2026, 8, 30)),
    ],
)
def test_transformation_rejects_empty_or_reversed_date_range(
    start_date: date,
    end_date: date,
) -> None:
    with pytest.raises(DailyAggregationError, match="must be before"):
        transform_daily_weather(FakeConnection(), start_date, end_date, [TORONTO])


def test_transformation_wraps_raw_weather_read_failure() -> None:
    connection = FakeConnection(
        read_error=psycopg.OperationalError("database unavailable")
    )

    with pytest.raises(DailyTransformationError, match="could not read"):
        transform_daily_weather(
            connection,
            date(2026, 8, 30),
            date(2026, 8, 31),
            [TORONTO],
        )
