"""Reusable validation for normalized weather-event records."""

from __future__ import annotations

import copy
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Collection, Mapping, Optional

WEATHER_EVENT_FIELDS = frozenset(
    {
        "region_id",
        "observed_at",
        "temperature_c",
        "humidity_percent",
        "precipitation_mm",
        "wind_speed_mps",
        "source_payload",
    }
)

# Fixed bounds keep validation deterministic for replays and historical backfills.
MIN_OBSERVED_AT = datetime(1900, 1, 1, tzinfo=timezone.utc)
MAX_OBSERVED_AT = datetime(2100, 1, 1, tzinfo=timezone.utc)

# Generous terrestrial envelopes catch unit mistakes and corrupt readings without
# rejecting credible extremes. Precipitation is the provider interval's amount.
MIN_TEMPERATURE_C = -100.0
MAX_TEMPERATURE_C = 65.0
MIN_HUMIDITY_PERCENT = 0.0
MAX_HUMIDITY_PERCENT = 100.0
MIN_PRECIPITATION_MM = 0.0
MAX_PRECIPITATION_MM = 1000.0
MIN_WIND_SPEED_MPS = 0.0
MAX_WIND_SPEED_MPS = 150.0

_REGION_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*$")
_UTC_TIMESTAMP_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$"
)


class WeatherValidationError(ValueError):
    """Raised when a normalized weather record violates its contract."""


@dataclass(frozen=True)
class ValidatedWeatherEvent:
    """Typed, detached values returned by weather-event validation."""

    region_id: str
    observed_at: datetime
    temperature_c: float
    humidity_percent: Optional[float]
    precipitation_mm: Optional[float]
    wind_speed_mps: Optional[float]
    source_payload: Mapping[str, Any]


def _validate_fields(payload: Mapping[str, Any]) -> None:
    actual_fields = set(payload)
    missing = WEATHER_EVENT_FIELDS - actual_fields
    unknown = actual_fields - WEATHER_EVENT_FIELDS
    if missing:
        raise WeatherValidationError(
            "weather event is missing field(s): " + ", ".join(sorted(missing))
        )
    if unknown:
        raise WeatherValidationError(
            "weather event has unknown field(s): "
            + ", ".join(sorted(str(field) for field in unknown))
        )


def _validate_region_id(
    value: Any,
    allowed_region_ids: Optional[Collection[str]],
) -> str:
    if not isinstance(value, str):
        raise WeatherValidationError("region_id must be a string")
    if not value or value != value.strip() or len(value) > 64:
        raise WeatherValidationError(
            "region_id must be a non-empty trimmed string of at most 64 characters"
        )
    if _REGION_ID_PATTERN.fullmatch(value) is None:
        raise WeatherValidationError(
            "region_id must start with a lowercase letter and contain only "
            "lowercase letters, digits, underscores, or hyphens"
        )
    if allowed_region_ids is not None and value not in allowed_region_ids:
        raise WeatherValidationError(f"region_id {value!r} is not configured")
    return value


def _validate_observed_at(value: Any) -> datetime:
    if not isinstance(value, str) or _UTC_TIMESTAMP_PATTERN.fullmatch(value) is None:
        raise WeatherValidationError(
            "observed_at must be an RFC 3339 UTC timestamp ending in Z"
        )
    try:
        observed_at = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise WeatherValidationError(
            "observed_at must be a valid RFC 3339 UTC timestamp"
        ) from exc
    if not MIN_OBSERVED_AT <= observed_at <= MAX_OBSERVED_AT:
        raise WeatherValidationError(
            "observed_at must be between 1900-01-01T00:00:00Z and 2100-01-01T00:00:00Z"
        )
    return observed_at


def _validate_number(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    required: bool,
    minimum: float,
    maximum: float,
) -> Optional[float]:
    value = payload[field_name]
    if value is None:
        if required:
            raise WeatherValidationError(f"{field_name} must not be null")
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        suffix = "" if required else " or null"
        raise WeatherValidationError(f"{field_name} must be a number{suffix}")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise WeatherValidationError(f"{field_name} must be finite") from exc
    if not math.isfinite(number):
        raise WeatherValidationError(f"{field_name} must be finite")
    if not minimum <= number <= maximum:
        raise WeatherValidationError(
            f"{field_name} must be between {minimum:g} and {maximum:g}"
        )
    return number


def _validate_source_payload(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise WeatherValidationError("source_payload must be a JSON object")
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise WeatherValidationError(
            "source_payload must be JSON-compatible and contain only finite numbers"
        ) from exc
    return copy.deepcopy(value)


def validate_weather_event(
    payload: Mapping[str, Any],
    *,
    allowed_region_ids: Optional[Collection[str]] = None,
) -> ValidatedWeatherEvent:
    """Validate and normalize one complete weather-event mapping.

    Supplying ``allowed_region_ids`` additionally checks that the syntactically
    valid region identifier belongs to the pipeline's configured region set.
    """

    if not isinstance(payload, Mapping):
        raise WeatherValidationError("weather event must be a JSON object")
    _validate_fields(payload)

    temperature_c = _validate_number(
        payload,
        "temperature_c",
        required=True,
        minimum=MIN_TEMPERATURE_C,
        maximum=MAX_TEMPERATURE_C,
    )
    assert temperature_c is not None
    return ValidatedWeatherEvent(
        region_id=_validate_region_id(payload["region_id"], allowed_region_ids),
        observed_at=_validate_observed_at(payload["observed_at"]),
        temperature_c=temperature_c,
        humidity_percent=_validate_number(
            payload,
            "humidity_percent",
            required=False,
            minimum=MIN_HUMIDITY_PERCENT,
            maximum=MAX_HUMIDITY_PERCENT,
        ),
        precipitation_mm=_validate_number(
            payload,
            "precipitation_mm",
            required=False,
            minimum=MIN_PRECIPITATION_MM,
            maximum=MAX_PRECIPITATION_MM,
        ),
        wind_speed_mps=_validate_number(
            payload,
            "wind_speed_mps",
            required=False,
            minimum=MIN_WIND_SPEED_MPS,
            maximum=MAX_WIND_SPEED_MPS,
        ),
        source_payload=_validate_source_payload(payload["source_payload"]),
    )


def format_utc_timestamp(value: datetime) -> str:
    """Format a validated UTC datetime in the event's canonical representation."""

    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
