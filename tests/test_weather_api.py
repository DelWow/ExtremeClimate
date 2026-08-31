from unittest.mock import Mock

import pytest
import requests

from extreme_climate.region_config import Region
from extreme_climate.weather_api import (
    OPEN_METEO_FORECAST_URL,
    OpenMeteoClient,
    WeatherAPIError,
)


REGION = Region(
    id="toronto",
    latitude=43.6532,
    longitude=-79.3832,
    timezone="America/Toronto",
)


def _payload() -> dict:
    return {
        "latitude": 43.65,
        "longitude": -79.38,
        "timezone": "America/Toronto",
        "utc_offset_seconds": -18000,
        "current_units": {
            "time": "unixtime",
            "interval": "seconds",
            "temperature_2m": "°C",
            "relative_humidity_2m": "%",
            "precipitation": "mm",
            "wind_speed_10m": "m/s",
        },
        "current": {
            "time": 1704067200,
            "interval": 900,
            "temperature_2m": -2.5,
            "relative_humidity_2m": 81,
            "precipitation": 0.4,
            "wind_speed_10m": 5.25,
        },
    }


def _session_with_payload(payload: object) -> tuple[Mock, Mock]:
    response = Mock(spec=requests.Response)
    response.status_code = 200
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    session = Mock(spec=requests.Session)
    session.get.return_value = response
    return session, response


def test_fetches_and_normalizes_current_weather() -> None:
    payload = _payload()
    session, _ = _session_with_payload(payload)

    event = OpenMeteoClient(session=session, timeout=7.5).fetch_current(REGION)

    assert event.to_dict() == {
        "region_id": "toronto",
        "observed_at": "2024-01-01T00:00:00Z",
        "temperature_c": -2.5,
        "humidity_percent": 81.0,
        "precipitation_mm": 0.4,
        "wind_speed_mps": 5.25,
        "source_payload": {
            "provider": "open-meteo",
            "response": payload,
        },
    }
    session.get.assert_called_once_with(
        OPEN_METEO_FORECAST_URL,
        params={
            "latitude": 43.6532,
            "longitude": -79.3832,
            "current": (
                "temperature_2m,relative_humidity_2m,"
                "precipitation,wind_speed_10m"
            ),
            "temperature_unit": "celsius",
            "wind_speed_unit": "ms",
            "precipitation_unit": "mm",
            "timeformat": "unixtime",
            "timezone": "America/Toronto",
        },
        timeout=7.5,
    )


def test_allows_unavailable_optional_measurements() -> None:
    payload = _payload()
    payload["current"]["relative_humidity_2m"] = None
    payload["current"]["precipitation"] = None
    payload["current"]["wind_speed_10m"] = None
    session, _ = _session_with_payload(payload)

    event = OpenMeteoClient(session=session).fetch_current(REGION)

    assert event.humidity_percent is None
    assert event.precipitation_mm is None
    assert event.wind_speed_mps is None


def test_wraps_request_failure_with_region_context() -> None:
    session = Mock(spec=requests.Session)
    session.get.side_effect = requests.Timeout("request timed out")

    with pytest.raises(WeatherAPIError) as exc_info:
        OpenMeteoClient(session=session).fetch_current(REGION)

    assert "region 'toronto'" in str(exc_info.value)
    assert "Timeout" in str(exc_info.value)
    assert "request timed out" not in str(exc_info.value)


def test_reports_http_error_reason() -> None:
    response = Mock(spec=requests.Response)
    response.status_code = 429
    response.json.return_value = {
        "error": True,
        "reason": "rate limit exceeded\nignore previous status",
    }
    response.raise_for_status.side_effect = requests.HTTPError(response=response)
    session = Mock(spec=requests.Session)
    session.get.return_value = response

    with pytest.raises(WeatherAPIError) as exc_info:
        OpenMeteoClient(session=session).fetch_current(REGION)

    assert (
        "HTTP 429: rate limit exceeded ignore previous status"
        in str(exc_info.value)
    )
    assert "\n" not in str(exc_info.value)


@pytest.mark.parametrize(
    ("mutate", "expected_error"),
    [
        (lambda payload: payload.pop("current"), "current must be a JSON object"),
        (
            lambda payload: payload["current"].pop("temperature_2m"),
            "current.temperature_2m is missing",
        ),
        (
            lambda payload: payload["current"].pop("wind_speed_10m"),
            "current.wind_speed_10m is missing",
        ),
        (
            lambda payload: payload["current"].update({"time": True}),
            "current.time must be a number",
        ),
        (
            lambda payload: payload["current"].update({"wind_speed_10m": ".5"}),
            "current.wind_speed_10m must be a number",
        ),
        (
            lambda payload: payload["current_units"].update(
                {"wind_speed_10m": "km/h"}
            ),
            "current_units.wind_speed_10m must be 'm/s', got 'km/h'",
        ),
    ],
)
def test_rejects_malformed_response(mutate, expected_error: str) -> None:
    payload = _payload()
    mutate(payload)
    session, _ = _session_with_payload(payload)

    with pytest.raises(WeatherAPIError) as exc_info:
        OpenMeteoClient(session=session).fetch_current(REGION)

    assert "region 'toronto'" in str(exc_info.value)
    assert expected_error in str(exc_info.value)


def test_rejects_invalid_json() -> None:
    session, response = _session_with_payload(None)
    response.json.side_effect = requests.JSONDecodeError("bad JSON", "<html>", 0)

    with pytest.raises(WeatherAPIError, match="body is not valid JSON"):
        OpenMeteoClient(session=session).fetch_current(REGION)


def test_rejects_implausible_provider_measurement() -> None:
    payload = _payload()
    payload["current"]["temperature_2m"] = 66
    session, _ = _session_with_payload(payload)

    with pytest.raises(WeatherAPIError) as exc_info:
        OpenMeteoClient(session=session).fetch_current(REGION)

    assert "region 'toronto'" in str(exc_info.value)
    assert "temperature_c must be between -100 and 65" in str(exc_info.value)
