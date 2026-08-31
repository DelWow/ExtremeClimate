"""Compare daily weather summaries with calendar-day historical baselines."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, Mapping, Optional, Protocol, Sequence, Tuple

import psycopg

from extreme_climate.daily_transformation import DailyWeatherSummary

DEFAULT_TEMPERATURE_DEVIATION_C = Decimal("5.00")
DEFAULT_HUMIDITY_DEVIATION_PERCENTAGE_POINTS = Decimal("15.00")
DEFAULT_PRECIPITATION_DEVIATION_MM = Decimal("10.00")

_COMPARABLE_METRICS = (
    "mean_temperature_c",
    "mean_humidity_percent",
    "total_precipitation_mm",
)

_SELECT_SUMMARIES_WITH_BASELINES_SQL = """
    SELECT
        summary.region_id,
        summary.summary_date,
        summary.observation_count,
        summary.mean_temperature_c,
        summary.min_temperature_c,
        summary.max_temperature_c,
        summary.mean_humidity_percent,
        summary.total_precipitation_mm,
        summary.max_wind_speed_mps,
        baseline.region_id,
        baseline.mean_temperature_c,
        baseline.mean_humidity_percent,
        baseline.mean_precipitation_mm
    FROM weather_daily_summary AS summary
    LEFT JOIN historical_baselines AS baseline
      ON baseline.region_id = summary.region_id
     AND baseline.baseline_month = EXTRACT(MONTH FROM summary.summary_date)
     AND baseline.baseline_day = EXTRACT(DAY FROM summary.summary_date)
    WHERE summary.summary_date >= %s
      AND summary.summary_date < %s
    ORDER BY summary.region_id, summary.summary_date
    FOR UPDATE OF summary
"""

_UPDATE_ANOMALY_SQL = """
    UPDATE weather_daily_summary AS existing
    SET is_anomaly = %s,
        anomaly_details = %s::jsonb,
        updated_at = CURRENT_TIMESTAMP
    WHERE existing.region_id = %s
      AND existing.summary_date = %s
      AND (
          existing.is_anomaly,
          existing.anomaly_details
      ) IS DISTINCT FROM (
          %s,
          %s::jsonb
      )
"""


class AnomalyEvaluationError(ValueError):
    """Raised when thresholds or evaluation inputs are inconsistent."""


class AnomalyDetectionError(RuntimeError):
    """Raised when summaries cannot be read or labeled atomically."""


def _positive_decimal(value: Any, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise AnomalyEvaluationError(f"{field_name} must be a positive number")
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise AnomalyEvaluationError(f"{field_name} must be a positive number") from exc
    if not number.is_finite() or number <= 0:
        raise AnomalyEvaluationError(f"{field_name} must be a positive number")
    return number


@dataclass(frozen=True)
class AnomalyThresholds:
    """Inclusive absolute-deviation thresholds for comparable daily metrics."""

    temperature_c: Decimal = DEFAULT_TEMPERATURE_DEVIATION_C
    humidity_percentage_points: Decimal = DEFAULT_HUMIDITY_DEVIATION_PERCENTAGE_POINTS
    precipitation_mm: Decimal = DEFAULT_PRECIPITATION_DEVIATION_MM

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "temperature_c",
            _positive_decimal(self.temperature_c, "temperature_c"),
        )
        object.__setattr__(
            self,
            "humidity_percentage_points",
            _positive_decimal(
                self.humidity_percentage_points,
                "humidity_percentage_points",
            ),
        )
        object.__setattr__(
            self,
            "precipitation_mm",
            _positive_decimal(self.precipitation_mm, "precipitation_mm"),
        )


DEFAULT_ANOMALY_THRESHOLDS = AnomalyThresholds()


@dataclass(frozen=True)
class HistoricalWeatherBaseline:
    """Baseline values for one region and calendar month/day."""

    region_id: str
    month: int
    day: int
    mean_temperature_c: Optional[Decimal]
    mean_humidity_percent: Optional[Decimal]
    mean_precipitation_mm: Optional[Decimal]


@dataclass(frozen=True)
class AnomalyEvaluation:
    """One persisted anomaly label and its JSON-compatible explanation."""

    region_id: str
    summary_date: date
    is_anomaly: Optional[bool]
    details: Mapping[str, Any]


class CursorProtocol(Protocol):
    """Database cursor operations used by anomaly detection."""

    def __enter__(self) -> "CursorProtocol": ...

    def __exit__(self, *args: Any) -> None: ...

    def fetchall(self) -> Sequence[Sequence[Any]]: ...

    def executemany(
        self,
        query: str,
        params_seq: Iterable[Sequence[Any]],
    ) -> None: ...


class ConnectionProtocol(Protocol):
    """Database connection operations required by anomaly detection."""

    def transaction(self) -> Any: ...

    def execute(self, query: str, params: Sequence[Any]) -> CursorProtocol: ...

    def cursor(self) -> CursorProtocol: ...


def _policy_details(thresholds: AnomalyThresholds) -> Dict[str, Any]:
    return {
        "threshold_rule": "absolute_deviation_greater_than_or_equal",
        "thresholds": {
            "mean_temperature_c": float(thresholds.temperature_c),
            "mean_humidity_percent": float(thresholds.humidity_percentage_points),
            "total_precipitation_mm": float(thresholds.precipitation_mm),
        },
    }


def _finite_decimal(value: Any, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise AnomalyEvaluationError(f"{field_name} must be finite")
    try:
        number = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise AnomalyEvaluationError(f"{field_name} must be finite") from exc
    if not number.is_finite():
        raise AnomalyEvaluationError(f"{field_name} must be finite")
    return number


def _metric_result(
    observed: Any,
    baseline: Any,
    threshold: Decimal,
    *,
    metric_name: str,
    unit: str,
) -> Optional[Dict[str, Any]]:
    if observed is None or baseline is None:
        return None
    observed_value = _finite_decimal(observed, metric_name)
    baseline_value = _finite_decimal(baseline, f"baseline {metric_name}")
    deviation = observed_value - baseline_value
    if abs(deviation) < threshold:
        return {}
    return {
        "observed": float(observed_value),
        "baseline": float(baseline_value),
        "deviation": float(deviation),
        "absolute_deviation": float(abs(deviation)),
        "threshold": float(threshold),
        "unit": unit,
        "direction": "above" if deviation > 0 else "below",
    }


def evaluate_weather_anomaly(
    summary: DailyWeatherSummary,
    baseline: Optional[HistoricalWeatherBaseline],
    thresholds: AnomalyThresholds = DEFAULT_ANOMALY_THRESHOLDS,
) -> AnomalyEvaluation:
    """Evaluate one daily summary with inclusive deviation thresholds.

    Mean temperature, mean humidity, and total daily precipitation are compared
    with their matching historical means. A deviation equal to its threshold is
    anomalous. A missing baseline or no comparable value pairs produces an
    unevaluated ``None`` label rather than a false normal result. Wind is not
    evaluated because the historical baseline schema has no wind metric.
    """

    if not isinstance(summary.summary_date, date) or isinstance(
        summary.summary_date, datetime
    ):
        raise AnomalyEvaluationError("summary_date must be a date")
    policy = _policy_details(thresholds)
    if baseline is None:
        return AnomalyEvaluation(
            region_id=summary.region_id,
            summary_date=summary.summary_date,
            is_anomaly=None,
            details={
                "status": "missing_baseline",
                "anomalies": {},
                "evaluated_metrics": [],
                "unavailable_metrics": list(_COMPARABLE_METRICS),
                **policy,
            },
        )
    if (
        baseline.region_id != summary.region_id
        or baseline.month != summary.summary_date.month
        or baseline.day != summary.summary_date.day
    ):
        raise AnomalyEvaluationError("baseline key does not match the daily summary")

    metric_inputs = (
        (
            "mean_temperature_c",
            summary.mean_temperature_c,
            baseline.mean_temperature_c,
            thresholds.temperature_c,
            "degC",
        ),
        (
            "mean_humidity_percent",
            summary.mean_humidity_percent,
            baseline.mean_humidity_percent,
            thresholds.humidity_percentage_points,
            "percentage_points",
        ),
        (
            "total_precipitation_mm",
            summary.total_precipitation_mm,
            baseline.mean_precipitation_mm,
            thresholds.precipitation_mm,
            "mm",
        ),
    )
    anomalies: Dict[str, Any] = {}
    evaluated_metrics = []
    unavailable_metrics = []
    for metric_name, observed, expected, threshold, unit in metric_inputs:
        result = _metric_result(
            observed,
            expected,
            threshold,
            metric_name=metric_name,
            unit=unit,
        )
        if result is None:
            unavailable_metrics.append(metric_name)
            continue
        evaluated_metrics.append(metric_name)
        if result:
            anomalies[metric_name] = result

    if not evaluated_metrics:
        status = "insufficient_data"
        is_anomaly: Optional[bool] = None
    else:
        is_anomaly = bool(anomalies)
        status = "anomaly" if is_anomaly else "normal"
    return AnomalyEvaluation(
        region_id=summary.region_id,
        summary_date=summary.summary_date,
        is_anomaly=is_anomaly,
        details={
            "status": status,
            "anomalies": anomalies,
            "evaluated_metrics": evaluated_metrics,
            "unavailable_metrics": unavailable_metrics,
            **policy,
        },
    )


def _summary_and_baseline_from_row(
    row: Sequence[Any],
) -> Tuple[DailyWeatherSummary, Optional[HistoricalWeatherBaseline]]:
    if len(row) != 13:
        raise AnomalyEvaluationError("summary/baseline query returned an invalid row")
    summary = DailyWeatherSummary(
        region_id=row[0],
        summary_date=row[1],
        observation_count=row[2],
        mean_temperature_c=row[3],
        min_temperature_c=row[4],
        max_temperature_c=row[5],
        mean_humidity_percent=row[6],
        total_precipitation_mm=row[7],
        max_wind_speed_mps=row[8],
    )
    if row[9] is None:
        return summary, None
    return summary, HistoricalWeatherBaseline(
        region_id=row[9],
        month=summary.summary_date.month,
        day=summary.summary_date.day,
        mean_temperature_c=row[10],
        mean_humidity_percent=row[11],
        mean_precipitation_mm=row[12],
    )


def _update_parameters(
    evaluations: Iterable[AnomalyEvaluation],
) -> Tuple[Tuple[Any, ...], ...]:
    parameters = []
    for evaluation in evaluations:
        details_json = json.dumps(
            evaluation.details,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        parameters.append(
            (
                evaluation.is_anomaly,
                details_json,
                evaluation.region_id,
                evaluation.summary_date,
                evaluation.is_anomaly,
                details_json,
            )
        )
    return tuple(parameters)


def detect_weather_anomalies(
    connection: ConnectionProtocol,
    start_date: date,
    end_date: date,
    thresholds: AnomalyThresholds = DEFAULT_ANOMALY_THRESHOLDS,
) -> Tuple[AnomalyEvaluation, ...]:
    """Atomically evaluate and label summaries in ``[start_date, end_date)``.

    Summary rows are locked while their baselines are read and labels updated,
    preventing a concurrent aggregation change from receiving a stale result.
    Identical re-evaluations leave ``updated_at`` unchanged.
    """

    if (
        not isinstance(start_date, date)
        or isinstance(start_date, datetime)
        or not isinstance(end_date, date)
        or isinstance(end_date, datetime)
    ):
        raise AnomalyEvaluationError("start_date and end_date must be dates")
    if start_date >= end_date:
        raise AnomalyEvaluationError("start_date must be before end_date")

    try:
        with connection.transaction():
            read_cursor = connection.execute(
                _SELECT_SUMMARIES_WITH_BASELINES_SQL,
                (start_date, end_date),
            )
            evaluations = tuple(
                evaluate_weather_anomaly(summary, baseline, thresholds)
                for summary, baseline in (
                    _summary_and_baseline_from_row(row)
                    for row in read_cursor.fetchall()
                )
            )
            parameters = _update_parameters(evaluations)
            if parameters:
                with connection.cursor() as write_cursor:
                    write_cursor.executemany(_UPDATE_ANOMALY_SQL, parameters)
    except psycopg.Error as exc:
        raise AnomalyDetectionError(
            "PostgreSQL could not evaluate daily weather anomalies"
        ) from exc
    return evaluations
