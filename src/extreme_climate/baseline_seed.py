"""Validate, expand, and seed the historical baseline development fixture."""

from __future__ import annotations

import argparse
import calendar
import csv
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

import psycopg

from extreme_climate.postgres_config import (
    PostgresConfigError,
    PostgresSettings,
    load_postgres_settings,
)
from extreme_climate.region_config import (
    DEFAULT_REGIONS_PATH,
    RegionConfigError,
    load_regions,
)

DEFAULT_BASELINE_FIXTURE_PATH = Path("data/historical_baseline_monthly.csv")

_FIXTURE_FIELDS = (
    "region_id",
    "month",
    "mean_temperature_c",
    "mean_humidity_percent",
    "mean_precipitation_mm",
)

_UPSERT_BASELINE_SQL = """
    INSERT INTO historical_baselines AS existing (
        region_id,
        baseline_month,
        baseline_day,
        mean_temperature_c,
        mean_humidity_percent,
        mean_precipitation_mm
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (region_id, baseline_month, baseline_day) DO UPDATE
    SET mean_temperature_c = EXCLUDED.mean_temperature_c,
        mean_humidity_percent = EXCLUDED.mean_humidity_percent,
        mean_precipitation_mm = EXCLUDED.mean_precipitation_mm,
        updated_at = CURRENT_TIMESTAMP
    WHERE (
        existing.mean_temperature_c,
        existing.mean_humidity_percent,
        existing.mean_precipitation_mm
    ) IS DISTINCT FROM (
        EXCLUDED.mean_temperature_c,
        EXCLUDED.mean_humidity_percent,
        EXCLUDED.mean_precipitation_mm
    )
"""

_COUNT_BASELINES_SQL = """
    SELECT COUNT(*)
    FROM historical_baselines
    WHERE region_id = ANY(%s)
"""


class BaselineFixtureError(ValueError):
    """Raised when the checked-in baseline fixture is malformed."""


class BaselineSeedError(RuntimeError):
    """Raised when baseline rows cannot be seeded atomically."""


@dataclass(frozen=True)
class MonthlyBaseline:
    """One validated source row shared by every day in its month."""

    region_id: str
    month: int
    mean_temperature_c: Decimal
    mean_humidity_percent: Decimal
    mean_precipitation_mm: Decimal


@dataclass(frozen=True)
class DailyBaseline:
    """One row matching the historical_baselines database key."""

    region_id: str
    month: int
    day: int
    mean_temperature_c: Decimal
    mean_humidity_percent: Decimal
    mean_precipitation_mm: Decimal


@dataclass(frozen=True)
class SeedResult:
    """Verified outcome of an atomic baseline seed transaction."""

    input_rows: int
    stored_rows: int


class CursorProtocol(Protocol):
    """Database cursor operations used by baseline seeding."""

    def __enter__(self) -> "CursorProtocol": ...

    def __exit__(self, *args: Any) -> None: ...

    def executemany(self, query: str, params_seq: Iterable[Sequence[Any]]) -> None: ...

    def execute(self, query: str, params: Sequence[Any]) -> Any: ...

    def fetchone(self) -> Optional[Tuple[Any, ...]]: ...


class ConnectionProtocol(Protocol):
    """Database connection operations required by baseline seeding."""

    def transaction(self) -> Any: ...

    def cursor(self) -> CursorProtocol: ...

    def close(self) -> None: ...


ConnectionFactory = Callable[..., ConnectionProtocol]


def _fixture_error(path: Path, line: int, detail: str) -> BaselineFixtureError:
    return BaselineFixtureError(f"{path}: line {line}: {detail}")


def _decimal(
    raw_value: Optional[str],
    *,
    field_name: str,
    path: Path,
    line: int,
) -> Decimal:
    if raw_value is None or not raw_value or raw_value != raw_value.strip():
        raise _fixture_error(path, line, f"{field_name} must be a number")
    try:
        value = Decimal(raw_value)
    except InvalidOperation as exc:
        raise _fixture_error(path, line, f"{field_name} must be a number") from exc
    if not value.is_finite():
        raise _fixture_error(path, line, f"{field_name} must be finite")
    return value


def _monthly_baseline(
    raw: Mapping[Optional[str], Optional[str]],
    *,
    configured_region_ids: frozenset[str],
    path: Path,
    line: int,
) -> MonthlyBaseline:
    if None in raw or any(raw.get(field) is None for field in _FIXTURE_FIELDS):
        raise _fixture_error(path, line, "must contain exactly five columns")

    region_id = raw["region_id"]
    assert region_id is not None
    if region_id != region_id.strip() or region_id not in configured_region_ids:
        raise _fixture_error(
            path,
            line,
            f"region_id {region_id!r} is not a configured region",
        )

    raw_month = raw["month"]
    assert raw_month is not None
    try:
        month = int(raw_month)
    except ValueError as exc:
        raise _fixture_error(path, line, "month must be an integer") from exc
    if raw_month != str(month) or not 1 <= month <= 12:
        raise _fixture_error(path, line, "month must be an integer from 1 to 12")

    temperature = _decimal(
        raw["mean_temperature_c"],
        field_name="mean_temperature_c",
        path=path,
        line=line,
    )
    humidity = _decimal(
        raw["mean_humidity_percent"],
        field_name="mean_humidity_percent",
        path=path,
        line=line,
    )
    precipitation = _decimal(
        raw["mean_precipitation_mm"],
        field_name="mean_precipitation_mm",
        path=path,
        line=line,
    )
    if temperature < Decimal("-273.15"):
        raise _fixture_error(path, line, "mean_temperature_c must be at least -273.15")
    if not Decimal("0") <= humidity <= Decimal("100"):
        raise _fixture_error(
            path, line, "mean_humidity_percent must be between 0 and 100"
        )
    if precipitation < Decimal("0"):
        raise _fixture_error(path, line, "mean_precipitation_mm must be non-negative")
    return MonthlyBaseline(
        region_id=region_id,
        month=month,
        mean_temperature_c=temperature,
        mean_humidity_percent=humidity,
        mean_precipitation_mm=precipitation,
    )


def load_monthly_baselines(
    fixture_path: Path = DEFAULT_BASELINE_FIXTURE_PATH,
    regions_path: Path = DEFAULT_REGIONS_PATH,
) -> Tuple[MonthlyBaseline, ...]:
    """Load a complete 12-month baseline fixture for configured regions."""

    regions = load_regions(regions_path)
    configured_ids = tuple(region.id for region in regions)
    configured_id_set = frozenset(configured_ids)
    path = Path(fixture_path)
    try:
        fixture = path.open("r", encoding="utf-8", newline="")
    except (OSError, UnicodeError) as exc:
        detail = getattr(exc, "strerror", None) or str(exc)
        raise BaselineFixtureError(
            f"{path}: could not read baseline fixture: {detail}"
        ) from exc

    by_key: Dict[Tuple[str, int], MonthlyBaseline] = {}
    try:
        with fixture:
            reader = csv.DictReader(fixture)
            if reader.fieldnames != list(_FIXTURE_FIELDS):
                raise BaselineFixtureError(
                    f"{path}: header must be exactly {', '.join(_FIXTURE_FIELDS)}"
                )
            for line, raw in enumerate(reader, start=2):
                baseline = _monthly_baseline(
                    raw,
                    configured_region_ids=configured_id_set,
                    path=path,
                    line=line,
                )
                key = (baseline.region_id, baseline.month)
                if key in by_key:
                    raise _fixture_error(
                        path,
                        line,
                        f"duplicates region/month "
                        f"{baseline.region_id}/{baseline.month}",
                    )
                by_key[key] = baseline
    except (UnicodeError, csv.Error) as exc:
        raise BaselineFixtureError(
            f"{path}: could not parse baseline fixture: {exc}"
        ) from exc

    expected_keys = {
        (region_id, month) for region_id in configured_ids for month in range(1, 13)
    }
    missing = expected_keys - set(by_key)
    if missing:
        first_region, first_month = sorted(missing)[0]
        raise BaselineFixtureError(
            f"{path}: missing {len(missing)} region/month row(s); "
            f"first missing key is {first_region}/{first_month}"
        )
    return tuple(
        by_key[(region_id, month)]
        for region_id in configured_ids
        for month in range(1, 13)
    )


def expand_daily_baselines(
    monthly: Iterable[MonthlyBaseline],
) -> Tuple[DailyBaseline, ...]:
    """Expand monthly means to every valid day using leap year 2000."""

    daily = []
    for baseline in monthly:
        days_in_month = calendar.monthrange(2000, baseline.month)[1]
        for day in range(1, days_in_month + 1):
            daily.append(
                DailyBaseline(
                    region_id=baseline.region_id,
                    month=baseline.month,
                    day=day,
                    mean_temperature_c=baseline.mean_temperature_c,
                    mean_humidity_percent=baseline.mean_humidity_percent,
                    mean_precipitation_mm=baseline.mean_precipitation_mm,
                )
            )
    return tuple(daily)


class HistoricalBaselineStore:
    """Atomically upsert a complete fixture without touching unchanged rows."""

    def __init__(self, connection: ConnectionProtocol) -> None:
        self._connection = connection

    def seed(self, baselines: Iterable[DailyBaseline]) -> SeedResult:
        """Upsert baseline rows and verify the resulting configured row count."""

        rows = tuple(baselines)
        if not rows:
            raise BaselineSeedError("baseline seed input must not be empty")
        region_ids = sorted({row.region_id for row in rows})
        parameters = tuple(
            (
                row.region_id,
                row.month,
                row.day,
                row.mean_temperature_c,
                row.mean_humidity_percent,
                row.mean_precipitation_mm,
            )
            for row in rows
        )
        try:
            with self._connection.transaction():
                with self._connection.cursor() as cursor:
                    cursor.executemany(_UPSERT_BASELINE_SQL, parameters)
                    cursor.execute(_COUNT_BASELINES_SQL, (region_ids,))
                    result = cursor.fetchone()
                    stored_rows = int(result[0]) if result is not None else -1
                    if stored_rows != len(rows):
                        raise BaselineSeedError(
                            f"expected {len(rows)} stored baseline rows, "
                            f"found {stored_rows}"
                        )
        except psycopg.Error as exc:
            raise BaselineSeedError(
                "PostgreSQL could not seed historical baselines"
            ) from exc
        return SeedResult(input_rows=len(rows), stored_rows=stored_rows)


def seed_historical_baselines(
    fixture_path: Path,
    regions_path: Path,
    postgres: PostgresSettings,
    *,
    connection_factory: ConnectionFactory = psycopg.connect,
) -> SeedResult:
    """Load the fixture and seed it through one PostgreSQL transaction."""

    monthly = load_monthly_baselines(fixture_path, regions_path)
    daily = expand_daily_baselines(monthly)
    try:
        connection = connection_factory(**postgres.connection_kwargs())
    except psycopg.Error as exc:
        raise BaselineSeedError("could not connect to PostgreSQL") from exc
    try:
        return HistoricalBaselineStore(connection).seed(daily)
    finally:
        connection.close()


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the historical baseline seed command."""

    parser = argparse.ArgumentParser(
        description="Seed deterministic development historical baselines."
    )
    parser.add_argument(
        "--fixture",
        type=Path,
        default=DEFAULT_BASELINE_FIXTURE_PATH,
        help=f"monthly CSV fixture (default: {DEFAULT_BASELINE_FIXTURE_PATH})",
    )
    parser.add_argument(
        "--regions",
        type=Path,
        default=DEFAULT_REGIONS_PATH,
        help=f"region configuration (default: {DEFAULT_REGIONS_PATH})",
    )
    args = parser.parse_args(argv)

    try:
        result = seed_historical_baselines(
            args.fixture,
            args.regions,
            load_postgres_settings(),
        )
    except (
        BaselineFixtureError,
        PostgresConfigError,
        RegionConfigError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except BaselineSeedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"Seeded {result.stored_rows} historical baseline row(s) from {args.fixture}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
