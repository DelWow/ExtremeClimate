from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from extreme_climate.pipeline_tasks import (
    PipelineTaskConfigError,
    _date_window,
    _report_path,
    validate_raw_weather_window,
)
from extreme_climate.region_config import Region

TORONTO = Region(
    id="toronto",
    latitude=43.6532,
    longitude=-79.3832,
    timezone="America/Toronto",
)


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeConnection:
    def __init__(self, rows):
        self._rows = rows
        self.execute_calls = []

    def execute(self, query, parameters):
        self.execute_calls.append((query, parameters))
        return FakeCursor(self._rows)


def _raw_row(observed_at: str = "2026-08-30T16:00:00+00:00"):
    return (
        "toronto",
        datetime.fromisoformat(observed_at),
        Decimal("22.50"),
        Decimal("60.00"),
        Decimal("0.20"),
        Decimal("4.00"),
        {"provider": "fixture"},
    )


def test_validates_only_rows_in_the_region_local_date_window() -> None:
    connection = FakeConnection(
        [
            _raw_row("2026-08-30T03:30:00+00:00"),
            _raw_row("2026-08-30T04:30:00+00:00"),
        ]
    )

    count = validate_raw_weather_window(
        connection,
        date(2026, 8, 30),
        date(2026, 8, 31),
        [TORONTO],
    )

    assert count == 1
    parameters = connection.execute_calls[0][1]
    assert parameters[0] == ["toronto"]
    assert parameters[1] == datetime(2026, 8, 30, 4, tzinfo=timezone.utc)
    assert parameters[2] == datetime(2026, 8, 31, 4, tzinfo=timezone.utc)


def test_raw_validation_reuses_weather_event_contract() -> None:
    invalid_row = list(_raw_row())
    invalid_row[2] = Decimal("100.00")

    with pytest.raises(ValueError, match="temperature_c must be between"):
        validate_raw_weather_window(
            FakeConnection([tuple(invalid_row)]),
            date(2026, 8, 30),
            date(2026, 8, 31),
            [TORONTO],
        )


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        ("not-a-date", "2026-08-31", "must be ISO dates"),
        ("2026-08-31", "2026-08-31", "must be before"),
        ("2026-09-01", "2026-08-31", "must be before"),
    ],
)
def test_rejects_invalid_runtime_date_windows(start, end, message) -> None:
    with pytest.raises(PipelineTaskConfigError, match=message):
        _date_window(start, end)


def test_report_path_is_deterministic_for_the_run_date() -> None:
    path = _report_path(
        {"REPORT_OUTPUT_DIR": "/tmp/reports"},
        date(2026, 8, 30),
    )

    assert path == Path("/tmp/reports/extreme_climate_daily_2026-08-30.xlsx")
