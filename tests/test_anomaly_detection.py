import json
from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import psycopg
import pytest

from extreme_climate.anomaly_detection import (
    AnomalyDetectionError,
    AnomalyEvaluationError,
    AnomalyThresholds,
    HistoricalWeatherBaseline,
    detect_weather_anomalies,
    evaluate_weather_anomaly,
)
from extreme_climate.daily_transformation import DailyWeatherSummary

SUMMARY_DATE = date(2026, 8, 30)


def _summary(**changes) -> DailyWeatherSummary:
    summary = DailyWeatherSummary(
        region_id="toronto",
        summary_date=SUMMARY_DATE,
        observation_count=24,
        mean_temperature_c=Decimal("21.00"),
        min_temperature_c=Decimal("16.00"),
        max_temperature_c=Decimal("26.00"),
        mean_humidity_percent=Decimal("60.00"),
        total_precipitation_mm=Decimal("2.00"),
        max_wind_speed_mps=Decimal("8.00"),
    )
    return replace(summary, **changes)


def _baseline(**changes) -> HistoricalWeatherBaseline:
    baseline = HistoricalWeatherBaseline(
        region_id="toronto",
        month=8,
        day=30,
        mean_temperature_c=Decimal("20.00"),
        mean_humidity_percent=Decimal("55.00"),
        mean_precipitation_mm=Decimal("2.00"),
    )
    return replace(baseline, **changes)


def _database_row(
    *,
    summary: Optional[DailyWeatherSummary] = None,
    baseline: Optional[HistoricalWeatherBaseline] = None,
    missing_baseline: bool = False,
):
    summary = _summary() if summary is None else summary
    baseline = _baseline() if baseline is None else baseline
    return (
        summary.region_id,
        summary.summary_date,
        summary.observation_count,
        summary.mean_temperature_c,
        summary.min_temperature_c,
        summary.max_temperature_c,
        summary.mean_humidity_percent,
        summary.total_precipitation_mm,
        summary.max_wind_speed_mps,
        None if missing_baseline else baseline.region_id,
        None if missing_baseline else baseline.mean_temperature_c,
        None if missing_baseline else baseline.mean_humidity_percent,
        None if missing_baseline else baseline.mean_precipitation_mm,
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

    def transaction(self):
        return FakeTransaction(self.log)

    def execute(self, query, parameters):
        self.execute_calls.append((query, parameters))
        if self.read_error is not None:
            raise self.read_error
        return self.read_cursor

    def cursor(self):
        return self.write_cursor


def test_labels_values_inside_thresholds_as_normal() -> None:
    evaluation = evaluate_weather_anomaly(_summary(), _baseline())

    assert evaluation.region_id == "toronto"
    assert evaluation.summary_date == SUMMARY_DATE
    assert evaluation.is_anomaly is False
    assert evaluation.details == {
        "status": "normal",
        "anomalies": {},
        "evaluated_metrics": [
            "mean_temperature_c",
            "mean_humidity_percent",
            "total_precipitation_mm",
        ],
        "unavailable_metrics": [],
        "threshold_rule": "absolute_deviation_greater_than_or_equal",
        "thresholds": {
            "mean_temperature_c": 5.0,
            "mean_humidity_percent": 15.0,
            "total_precipitation_mm": 10.0,
        },
    }


@pytest.mark.parametrize(
    ("summary_changes", "expected_metric", "expected_threshold"),
    [
        (
            {"mean_temperature_c": Decimal("25.00")},
            "mean_temperature_c",
            5.0,
        ),
        (
            {"mean_humidity_percent": Decimal("70.00")},
            "mean_humidity_percent",
            15.0,
        ),
        (
            {"total_precipitation_mm": Decimal("12.00")},
            "total_precipitation_mm",
            10.0,
        ),
    ],
)
def test_exact_threshold_is_anomalous(
    summary_changes,
    expected_metric: str,
    expected_threshold: float,
) -> None:
    evaluation = evaluate_weather_anomaly(
        _summary(**summary_changes),
        _baseline(),
    )

    assert evaluation.is_anomaly is True
    assert evaluation.details["status"] == "anomaly"
    anomaly = evaluation.details["anomalies"][expected_metric]
    assert anomaly["absolute_deviation"] == expected_threshold
    assert anomaly["threshold"] == expected_threshold


@pytest.mark.parametrize(
    "summary_changes",
    [
        {"mean_temperature_c": Decimal("24.99")},
        {"mean_humidity_percent": Decimal("69.99")},
        {"total_precipitation_mm": Decimal("11.99")},
    ],
)
def test_value_just_inside_threshold_is_normal(summary_changes) -> None:
    assert (
        evaluate_weather_anomaly(
            _summary(**summary_changes),
            _baseline(),
        ).is_anomaly
        is False
    )


def test_records_each_anomalous_metric_with_direction_and_units() -> None:
    evaluation = evaluate_weather_anomaly(
        _summary(
            mean_temperature_c=Decimal("14.00"),
            mean_humidity_percent=Decimal("75.00"),
            total_precipitation_mm=Decimal("13.00"),
        ),
        _baseline(),
    )

    assert evaluation.is_anomaly is True
    anomalies = evaluation.details["anomalies"]
    assert anomalies["mean_temperature_c"] == {
        "observed": 14.0,
        "baseline": 20.0,
        "deviation": -6.0,
        "absolute_deviation": 6.0,
        "threshold": 5.0,
        "unit": "degC",
        "direction": "below",
    }
    assert anomalies["mean_humidity_percent"]["direction"] == "above"
    assert anomalies["mean_humidity_percent"]["unit"] == "percentage_points"
    assert anomalies["total_precipitation_mm"]["direction"] == "above"
    assert anomalies["total_precipitation_mm"]["unit"] == "mm"
    assert "max_wind_speed_mps" not in anomalies


def test_missing_baseline_remains_unevaluated() -> None:
    evaluation = evaluate_weather_anomaly(_summary(), None)

    assert evaluation.is_anomaly is None
    assert evaluation.details == {
        "status": "missing_baseline",
        "anomalies": {},
        "evaluated_metrics": [],
        "unavailable_metrics": [
            "mean_temperature_c",
            "mean_humidity_percent",
            "total_precipitation_mm",
        ],
        "threshold_rule": "absolute_deviation_greater_than_or_equal",
        "thresholds": {
            "mean_temperature_c": 5.0,
            "mean_humidity_percent": 15.0,
            "total_precipitation_mm": 10.0,
        },
    }


def test_missing_optional_pair_is_skipped_without_hiding_normal_temperature() -> None:
    evaluation = evaluate_weather_anomaly(
        _summary(
            mean_humidity_percent=None,
            total_precipitation_mm=None,
        ),
        _baseline(),
    )

    assert evaluation.is_anomaly is False
    assert evaluation.details["evaluated_metrics"] == ["mean_temperature_c"]
    assert evaluation.details["unavailable_metrics"] == [
        "mean_humidity_percent",
        "total_precipitation_mm",
    ]


def test_no_comparable_pairs_remains_unevaluated() -> None:
    evaluation = evaluate_weather_anomaly(
        _summary(
            mean_humidity_percent=None,
            total_precipitation_mm=None,
        ),
        _baseline(
            mean_temperature_c=None,
            mean_humidity_percent=Decimal("55"),
            mean_precipitation_mm=None,
        ),
    )

    assert evaluation.is_anomaly is None
    assert evaluation.details["status"] == "insufficient_data"
    assert evaluation.details["evaluated_metrics"] == []


@pytest.mark.parametrize(
    "baseline",
    [
        _baseline(region_id="halifax"),
        _baseline(month=7),
        _baseline(day=29),
    ],
)
def test_rejects_mismatched_baseline_key(baseline) -> None:
    with pytest.raises(AnomalyEvaluationError, match="baseline key does not match"):
        evaluate_weather_anomaly(_summary(), baseline)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("temperature_c", 0),
        ("temperature_c", Decimal("NaN")),
        ("humidity_percentage_points", -1),
        ("precipitation_mm", "not-a-number"),
    ],
)
def test_rejects_invalid_thresholds(field_name: str, value) -> None:
    with pytest.raises(AnomalyEvaluationError, match="positive number"):
        AnomalyThresholds(**{field_name: value})


def test_custom_thresholds_are_applied() -> None:
    thresholds = AnomalyThresholds(
        temperature_c=Decimal("2"),
        humidity_percentage_points=Decimal("30"),
        precipitation_mm=Decimal("20"),
    )

    evaluation = evaluate_weather_anomaly(_summary(), _baseline(), thresholds)

    assert evaluation.is_anomaly is False
    assert thresholds.temperature_c == Decimal("2")
    assert evaluation.details["thresholds"]["mean_temperature_c"] == 2.0


def test_detection_reads_locks_and_idempotently_labels_each_summary() -> None:
    anomalous_summary = _summary(
        summary_date=date(2026, 8, 31),
        mean_temperature_c=Decimal("27"),
    )
    anomalous_baseline = _baseline(day=31)
    connection = FakeConnection(
        rows=[
            _database_row(),
            _database_row(
                summary=anomalous_summary,
                baseline=anomalous_baseline,
            ),
            _database_row(
                summary=_summary(summary_date=date(2026, 9, 1)),
                missing_baseline=True,
            ),
        ]
    )

    evaluations = detect_weather_anomalies(
        connection,
        date(2026, 8, 30),
        date(2026, 9, 2),
    )

    select_query, select_parameters = connection.execute_calls[0]
    assert "LEFT JOIN historical_baselines" in select_query
    assert "FOR UPDATE OF summary" in select_query
    assert select_parameters == (date(2026, 8, 30), date(2026, 9, 2))
    assert [evaluation.is_anomaly for evaluation in evaluations] == [
        False,
        True,
        None,
    ]

    update_query, update_parameters = connection.write_cursor.executemany_calls[0]
    assert "IS DISTINCT FROM" in update_query
    assert "updated_at = CURRENT_TIMESTAMP" in update_query
    assert len(update_parameters) == 3
    normal = update_parameters[0]
    assert normal[0] is False
    assert normal[2:4] == ("toronto", date(2026, 8, 30))
    assert normal[0] == normal[4]
    assert normal[1] == normal[5]
    assert json.loads(normal[1])["status"] == "normal"
    assert json.loads(update_parameters[1][1])["status"] == "anomaly"
    assert json.loads(update_parameters[2][1])["status"] == "missing_baseline"
    assert connection.log == ["begin", "commit"]


def test_detection_with_no_summaries_commits_without_writes() -> None:
    connection = FakeConnection()

    evaluations = detect_weather_anomalies(
        connection,
        date(2026, 8, 30),
        date(2026, 8, 31),
    )

    assert evaluations == ()
    assert connection.write_cursor.executemany_calls == []
    assert connection.log == ["begin", "commit"]


@pytest.mark.parametrize("failure_stage", ["read", "write"])
def test_detection_wraps_database_failures_and_rolls_back(
    failure_stage: str,
) -> None:
    error = psycopg.OperationalError("database unavailable")
    connection = FakeConnection(
        rows=[_database_row()],
        read_error=error if failure_stage == "read" else None,
        write_error=error if failure_stage == "write" else None,
    )

    with pytest.raises(AnomalyDetectionError, match="could not evaluate"):
        detect_weather_anomalies(
            connection,
            date(2026, 8, 30),
            date(2026, 8, 31),
        )

    assert connection.log == ["begin", "rollback"]


@pytest.mark.parametrize(
    ("start_date", "end_date", "expected_error"),
    [
        (date(2026, 8, 30), date(2026, 8, 30), "must be before"),
        (date(2026, 8, 31), date(2026, 8, 30), "must be before"),
        (datetime(2026, 8, 30), date(2026, 8, 31), "must be dates"),
    ],
)
def test_detection_rejects_invalid_date_range(
    start_date,
    end_date,
    expected_error: str,
) -> None:
    with pytest.raises(AnomalyEvaluationError, match=expected_error):
        detect_weather_anomalies(FakeConnection(), start_date, end_date)
