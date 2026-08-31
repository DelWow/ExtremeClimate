from datetime import datetime, timezone

import pytest

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
    WEATHER_EVENT_FIELDS,
    WeatherValidationError,
    validate_weather_event,
)


def _record() -> dict:
    return {
        "region_id": "toronto",
        "observed_at": "2026-08-28T12:00:00Z",
        "temperature_c": 20.5,
        "humidity_percent": 55,
        "precipitation_mm": 0,
        "wind_speed_mps": 3.5,
        "source_payload": {"provider": "stub", "samples": [1, 2]},
    }


def test_accepts_and_normalizes_complete_record() -> None:
    record = _record()

    validated = validate_weather_event(
        record,
        allowed_region_ids={"toronto", "halifax"},
    )
    record["source_payload"]["samples"].append(3)

    assert validated.region_id == "toronto"
    assert validated.observed_at == datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)
    assert validated.temperature_c == 20.5
    assert validated.humidity_percent == 55.0
    assert validated.precipitation_mm == 0.0
    assert validated.wind_speed_mps == 3.5
    assert validated.source_payload == {"provider": "stub", "samples": [1, 2]}


def test_accepts_null_optional_measurements_and_fractional_timestamp() -> None:
    record = _record()
    record.update(
        {
            "observed_at": "2026-08-28T12:00:00.123456Z",
            "humidity_percent": None,
            "precipitation_mm": None,
            "wind_speed_mps": None,
        }
    )

    validated = validate_weather_event(record)

    assert validated.observed_at.microsecond == 123456
    assert validated.humidity_percent is None
    assert validated.precipitation_mm is None
    assert validated.wind_speed_mps is None


@pytest.mark.parametrize(
    ("observed_at", "temperature", "humidity", "precipitation", "wind"),
    [
        (
            MIN_OBSERVED_AT.isoformat().replace("+00:00", "Z"),
            MIN_TEMPERATURE_C,
            MIN_HUMIDITY_PERCENT,
            MIN_PRECIPITATION_MM,
            MIN_WIND_SPEED_MPS,
        ),
        (
            MAX_OBSERVED_AT.isoformat().replace("+00:00", "Z"),
            MAX_TEMPERATURE_C,
            MAX_HUMIDITY_PERCENT,
            MAX_PRECIPITATION_MM,
            MAX_WIND_SPEED_MPS,
        ),
    ],
)
def test_accepts_inclusive_contract_boundaries(
    observed_at: str,
    temperature: float,
    humidity: float,
    precipitation: float,
    wind: float,
) -> None:
    record = _record()
    record.update(
        {
            "observed_at": observed_at,
            "temperature_c": temperature,
            "humidity_percent": humidity,
            "precipitation_mm": precipitation,
            "wind_speed_mps": wind,
        }
    )

    validate_weather_event(record)


@pytest.mark.parametrize("field_name", sorted(WEATHER_EVENT_FIELDS))
def test_rejects_each_missing_required_field(field_name: str) -> None:
    record = _record()
    record.pop(field_name)

    with pytest.raises(WeatherValidationError, match="missing field") as exc_info:
        validate_weather_event(record)

    assert field_name in str(exc_info.value)


def test_rejects_unknown_fields() -> None:
    record = _record()
    record["unexpected"] = True

    with pytest.raises(WeatherValidationError, match="unknown field.*unexpected"):
        validate_weather_event(record)


@pytest.mark.parametrize(
    "observed_at",
    [
        None,
        "2026-08-28",
        "2026-08-28T12:00:00",
        "2026-08-28T12:00:00+00:00",
        "2026-08-28T12:00:00.1234567Z",
        "2026-02-30T12:00:00Z",
        "1899-12-31T23:59:59Z",
        "2100-01-01T00:00:01Z",
    ],
)
def test_rejects_invalid_or_unsupported_timestamps(observed_at) -> None:
    record = _record()
    record["observed_at"] = observed_at

    with pytest.raises(WeatherValidationError, match="observed_at"):
        validate_weather_event(record)


@pytest.mark.parametrize(
    "region_id",
    [
        None,
        "",
        " toronto",
        "toronto ",
        "Toronto",
        "1toronto",
        "toronto.ca",
        "a" * 65,
    ],
)
def test_rejects_invalid_region_identifiers(region_id) -> None:
    record = _record()
    record["region_id"] = region_id

    with pytest.raises(WeatherValidationError, match="region_id"):
        validate_weather_event(record)


def test_rejects_region_not_present_in_configuration() -> None:
    with pytest.raises(WeatherValidationError, match="'toronto' is not configured"):
        validate_weather_event(_record(), allowed_region_ids={"halifax"})


@pytest.mark.parametrize(
    ("field_name", "value", "expected_range"),
    [
        ("temperature_c", -100.01, "-100 and 65"),
        ("temperature_c", 65.01, "-100 and 65"),
        ("humidity_percent", -0.01, "0 and 100"),
        ("humidity_percent", 100.01, "0 and 100"),
        ("precipitation_mm", -0.01, "0 and 1000"),
        ("precipitation_mm", 1000.01, "0 and 1000"),
        ("wind_speed_mps", -0.01, "0 and 150"),
        ("wind_speed_mps", 150.01, "0 and 150"),
    ],
)
def test_rejects_weather_values_outside_plausible_ranges(
    field_name: str,
    value: float,
    expected_range: str,
) -> None:
    record = _record()
    record[field_name] = value

    with pytest.raises(WeatherValidationError) as exc_info:
        validate_weather_event(record)

    assert field_name in str(exc_info.value)
    assert expected_range in str(exc_info.value)


def test_rejects_null_required_temperature() -> None:
    record = _record()
    record["temperature_c"] = None

    with pytest.raises(WeatherValidationError, match="temperature_c must not be null"):
        validate_weather_event(record)


@pytest.mark.parametrize(
    "field_name",
    [
        "temperature_c",
        "humidity_percent",
        "precipitation_mm",
        "wind_speed_mps",
    ],
)
@pytest.mark.parametrize("value", [True, "12", float("nan"), float("inf")])
def test_rejects_non_numeric_or_non_finite_weather_values(
    field_name: str,
    value,
) -> None:
    record = _record()
    record[field_name] = value

    with pytest.raises(WeatherValidationError, match=field_name):
        validate_weather_event(record)


@pytest.mark.parametrize(
    "source_payload",
    [
        None,
        [],
        {"reading": float("nan")},
        {"reading": {1, 2}},
    ],
)
def test_rejects_invalid_source_payload(source_payload) -> None:
    record = _record()
    record["source_payload"] = source_payload

    with pytest.raises(WeatherValidationError, match="source_payload"):
        validate_weather_event(record)
