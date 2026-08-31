from decimal import Decimal
from pathlib import Path

import psycopg
import pytest

from extreme_climate import baseline_seed
from extreme_climate.baseline_seed import (
    DEFAULT_BASELINE_FIXTURE_PATH,
    BaselineFixtureError,
    BaselineSeedError,
    DailyBaseline,
    HistoricalBaselineStore,
    SeedResult,
    expand_daily_baselines,
    load_monthly_baselines,
)
from extreme_climate.region_config import DEFAULT_REGIONS_PATH

_HEADER = (
    "region_id,month,mean_temperature_c,mean_humidity_percent,mean_precipitation_mm\n"
)


def _write_regions(tmp_path: Path, region_id: str = "test-region") -> Path:
    path = tmp_path / "regions.yaml"
    path.write_text(
        f"""
regions:
  - id: {region_id}
    latitude: 1
    longitude: 2
    timezone: UTC
""",
        encoding="utf-8",
    )
    return path


def _monthly_fixture_lines(region_id: str = "test-region") -> list[str]:
    return [f"{region_id},{month},{month}.00,50.00,1.00\n" for month in range(1, 13)]


def _write_fixture(tmp_path: Path, lines: list[str]) -> Path:
    path = tmp_path / "baselines.csv"
    path.write_text(_HEADER + "".join(lines), encoding="utf-8")
    return path


class FakeCursor:
    def __init__(self, *, stored_rows: int, error=None):
        self.stored_rows = stored_rows
        self.error = error
        self.executemany_calls = []
        self.execute_calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def executemany(self, query, parameters):
        self.executemany_calls.append((query, tuple(parameters)))
        if self.error is not None:
            raise self.error

    def execute(self, query, parameters):
        self.execute_calls.append((query, parameters))
        return self

    def fetchone(self):
        return (self.stored_rows,)


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
    def __init__(self, *, stored_rows: int, error=None):
        self.log = []
        self.cursor_instance = FakeCursor(stored_rows=stored_rows, error=error)
        self.closed = False

    def transaction(self):
        return FakeTransaction(self.log)

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


def _daily(day: int = 1) -> DailyBaseline:
    return DailyBaseline(
        region_id="toronto",
        month=7,
        day=day,
        mean_temperature_c=Decimal("22.20"),
        mean_humidity_percent=Decimal("69.00"),
        mean_precipitation_mm=Decimal("2.20"),
    )


def test_checked_in_fixture_has_complete_leap_year_coverage() -> None:
    monthly = load_monthly_baselines(
        DEFAULT_BASELINE_FIXTURE_PATH,
        DEFAULT_REGIONS_PATH,
    )
    daily = expand_daily_baselines(monthly)

    assert len(monthly) == 60
    assert len(daily) == 1830
    assert {row.region_id for row in monthly} == {
        "vancouver",
        "calgary",
        "toronto",
        "montreal",
        "halifax",
    }
    assert all(
        sum(row.region_id == region_id for row in daily) == 366
        for region_id in {row.region_id for row in monthly}
    )
    toronto_july = next(
        row
        for row in daily
        if row.region_id == "toronto" and row.month == 7 and row.day == 15
    )
    assert toronto_july.mean_temperature_c == Decimal("22.20")
    assert toronto_july.mean_humidity_percent == Decimal("69.00")
    assert toronto_july.mean_precipitation_mm == Decimal("2.20")
    assert any(row.month == 2 and row.day == 29 for row in daily)


@pytest.mark.parametrize(
    ("mutate", "expected_error"),
    [
        (
            lambda lines: lines.append(lines[0]),
            "duplicates region/month test-region/1",
        ),
        (lambda lines: lines.pop(), "missing 1 region/month row"),
        (
            lambda lines: lines.__setitem__(0, "unknown,1,1,50,1\n"),
            "is not a configured region",
        ),
        (
            lambda lines: lines.__setitem__(0, "test-region,13,1,50,1\n"),
            "month must be an integer from 1 to 12",
        ),
        (
            lambda lines: lines.__setitem__(0, "test-region,1,1,101,1\n"),
            "mean_humidity_percent must be between 0 and 100",
        ),
        (
            lambda lines: lines.__setitem__(0, "test-region,1,1,50,-1\n"),
            "mean_precipitation_mm must be non-negative",
        ),
        (
            lambda lines: lines.__setitem__(0, "test-region,1,nan,50,1\n"),
            "mean_temperature_c must be finite",
        ),
    ],
)
def test_rejects_invalid_fixture(tmp_path: Path, mutate, expected_error: str) -> None:
    lines = _monthly_fixture_lines()
    mutate(lines)
    fixture_path = _write_fixture(tmp_path, lines)

    with pytest.raises(BaselineFixtureError) as exc_info:
        load_monthly_baselines(fixture_path, _write_regions(tmp_path))

    assert expected_error in str(exc_info.value)


def test_rejects_wrong_fixture_header(tmp_path: Path) -> None:
    fixture_path = tmp_path / "baselines.csv"
    fixture_path.write_text("region_id,month\ntest-region,1\n", encoding="utf-8")

    with pytest.raises(BaselineFixtureError, match="header must be exactly"):
        load_monthly_baselines(fixture_path, _write_regions(tmp_path))


def test_rejects_invalid_utf8_fixture(tmp_path: Path) -> None:
    fixture_path = tmp_path / "baselines.csv"
    fixture_path.write_bytes(_HEADER.encode("utf-8") + b"test-region,1,\xff")

    with pytest.raises(BaselineFixtureError, match="could not parse"):
        load_monthly_baselines(fixture_path, _write_regions(tmp_path))


def test_store_upserts_atomically_and_verifies_count() -> None:
    connection = FakeConnection(stored_rows=2)

    result = HistoricalBaselineStore(connection).seed([_daily(1), _daily(2)])

    assert result == SeedResult(input_rows=2, stored_rows=2)
    query, parameters = connection.cursor_instance.executemany_calls[0]
    assert "ON CONFLICT (region_id, baseline_month, baseline_day)" in query
    assert "IS DISTINCT FROM" in query
    assert parameters == (
        ("toronto", 7, 1, Decimal("22.20"), Decimal("69.00"), Decimal("2.20")),
        ("toronto", 7, 2, Decimal("22.20"), Decimal("69.00"), Decimal("2.20")),
    )
    count_query, count_parameters = connection.cursor_instance.execute_calls[0]
    assert "SELECT COUNT(*)" in count_query
    assert count_parameters == (["toronto"],)
    assert connection.log == ["begin", "commit"]


def test_store_rolls_back_when_verified_count_is_wrong() -> None:
    connection = FakeConnection(stored_rows=1)

    with pytest.raises(BaselineSeedError, match="expected 2.*found 1"):
        HistoricalBaselineStore(connection).seed([_daily(1), _daily(2)])

    assert connection.log == ["begin", "rollback"]


def test_store_wraps_database_failure_and_rolls_back() -> None:
    connection = FakeConnection(
        stored_rows=0,
        error=psycopg.OperationalError("database unavailable"),
    )

    with pytest.raises(BaselineSeedError, match="could not seed"):
        HistoricalBaselineStore(connection).seed([_daily()])

    assert connection.log == ["begin", "rollback"]


def test_store_rejects_empty_input() -> None:
    with pytest.raises(BaselineSeedError, match="must not be empty"):
        HistoricalBaselineStore(FakeConnection(stored_rows=0)).seed([])


def test_main_reports_seed_result(monkeypatch, capsys) -> None:
    settings = object()
    calls = []
    monkeypatch.setattr(baseline_seed, "load_postgres_settings", lambda: settings)

    def seed(fixture_path, regions_path, postgres):
        calls.append((fixture_path, regions_path, postgres))
        return SeedResult(input_rows=1830, stored_rows=1830)

    monkeypatch.setattr(baseline_seed, "seed_historical_baselines", seed)

    result = baseline_seed.main([])

    captured = capsys.readouterr()
    assert result == 0
    assert calls == [(DEFAULT_BASELINE_FIXTURE_PATH, DEFAULT_REGIONS_PATH, settings)]
    assert captured.out == (
        "Seeded 1830 historical baseline row(s) "
        "from data/historical_baseline_monthly.csv\n"
    )
    assert captured.err == ""
