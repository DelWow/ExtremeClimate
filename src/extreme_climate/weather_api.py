"""Fetch and normalize current weather observations from Open-Meteo."""

from __future__ import annotations

import copy
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

import requests

from extreme_climate.region_config import Region


OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 10.0

_CURRENT_FIELDS = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
)
_EXPECTED_UNITS = {
    "time": "unixtime",
    "temperature_2m": "°C",
    "relative_humidity_2m": "%",
    "precipitation": "mm",
    "wind_speed_10m": "m/s",
}


class WeatherAPIError(RuntimeError):
    """Raised when weather data cannot be fetched or normalized."""


@dataclass(frozen=True)
class WeatherEvent:
    """A provider-neutral weather observation ready for pipeline transport.

    ``precipitation_mm`` is the amount reported for the provider's observation
    interval, not an instantaneous rate. The provider-specific interval and
    other provenance remain available in ``source_payload``.
    """

    region_id: str
    observed_at: str
    temperature_c: float
    humidity_percent: Optional[float]
    precipitation_mm: Optional[float]
    wind_speed_mps: Optional[float]
    source_payload: Mapping[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Return a detached, JSON-compatible event mapping."""

        return {
            "region_id": self.region_id,
            "observed_at": self.observed_at,
            "temperature_c": self.temperature_c,
            "humidity_percent": self.humidity_percent,
            "precipitation_mm": self.precipitation_mm,
            "wind_speed_mps": self.wind_speed_mps,
            "source_payload": copy.deepcopy(self.source_payload),
        }


def _response_error(region: Region, detail: str) -> WeatherAPIError:
    return WeatherAPIError(f"Open-Meteo response for region {region.id!r}: {detail}")


def _number(
    current: Mapping[str, Any],
    field: str,
    region: Region,
    *,
    required: bool,
) -> Optional[float]:
    if field not in current:
        raise _response_error(region, f"current.{field} is missing")
    value = current[field]
    if value is None:
        if required:
            raise _response_error(region, f"current.{field} is required")
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _response_error(region, f"current.{field} must be a number")

    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise _response_error(region, f"current.{field} must be finite") from exc
    if not math.isfinite(number):
        raise _response_error(region, f"current.{field} must be finite")
    return number


def _require_unit(
    units: Mapping[str, Any], field: str, expected: str, region: Region
) -> None:
    actual = units.get(field)
    if actual != expected:
        raise _response_error(
            region,
            f"current_units.{field} must be {expected!r}, got {actual!r}",
        )


def _utc_timestamp(unix_seconds: float, region: Region) -> str:
    try:
        observed_at = datetime.fromtimestamp(unix_seconds, tz=timezone.utc)
    except (OSError, OverflowError, ValueError) as exc:
        raise _response_error(region, "current.time is outside the supported range") from exc
    return observed_at.isoformat(timespec="seconds").replace("+00:00", "Z")


def _error_reason(response: requests.Response) -> Optional[str]:
    try:
        payload = response.json()
    except ValueError:
        return None
    if isinstance(payload, Mapping):
        reason = payload.get("reason")
        if isinstance(reason, str) and reason.strip():
            return " ".join(reason.split())[:500]
    return None


class OpenMeteoClient:
    """Client for Open-Meteo's current conditions endpoint."""

    def __init__(
        self,
        *,
        session: Optional[requests.Session] = None,
        endpoint: str = OPEN_METEO_FORECAST_URL,
        timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        if not isinstance(endpoint, str) or not endpoint.strip():
            raise ValueError("endpoint must not be empty")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise ValueError("timeout must be a positive finite number")
        if not math.isfinite(float(timeout)) or timeout <= 0:
            raise ValueError("timeout must be a positive finite number")

        self._owns_session = session is None
        self._session = session or requests.Session()
        self._endpoint = endpoint
        self._timeout = float(timeout)

    def close(self) -> None:
        """Close the internally created HTTP session, if any."""

        if self._owns_session:
            self._session.close()

    def __enter__(self) -> "OpenMeteoClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def fetch_current(self, region: Region) -> WeatherEvent:
        """Fetch one region and normalize its current observation."""

        params = {
            "latitude": region.latitude,
            "longitude": region.longitude,
            "current": ",".join(_CURRENT_FIELDS),
            "temperature_unit": "celsius",
            "wind_speed_unit": "ms",
            "precipitation_unit": "mm",
            "timeformat": "unixtime",
            "timezone": region.timezone,
        }
        try:
            response = self._session.get(
                self._endpoint,
                params=params,
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            raise WeatherAPIError(
                f"Open-Meteo request for region {region.id!r} failed "
                f"({type(exc).__name__})"
            ) from exc

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            reason = _error_reason(response)
            detail = f"HTTP {response.status_code}"
            if reason is not None:
                detail += f": {reason}"
            raise WeatherAPIError(
                f"Open-Meteo request for region {region.id!r} failed with {detail}"
            ) from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise _response_error(region, "body is not valid JSON") from exc

        if not isinstance(payload, Mapping):
            raise _response_error(region, "body must be a JSON object")
        if payload.get("error") is True:
            reason = payload.get("reason")
            detail = (
                " ".join(reason.split())[:500]
                if isinstance(reason, str) and reason.strip()
                else "unknown API error"
            )
            raise _response_error(region, detail)

        current = payload.get("current")
        current_units = payload.get("current_units")
        if not isinstance(current, Mapping):
            raise _response_error(region, "current must be a JSON object")
        if not isinstance(current_units, Mapping):
            raise _response_error(region, "current_units must be a JSON object")

        observed_seconds = _number(current, "time", region, required=True)
        temperature_c = _number(current, "temperature_2m", region, required=True)
        humidity_percent = _number(
            current, "relative_humidity_2m", region, required=False
        )
        precipitation_mm = _number(current, "precipitation", region, required=False)
        wind_speed_mps = _number(current, "wind_speed_10m", region, required=False)

        _require_unit(current_units, "time", _EXPECTED_UNITS["time"], region)
        _require_unit(
            current_units,
            "temperature_2m",
            _EXPECTED_UNITS["temperature_2m"],
            region,
        )
        for field, value in (
            ("relative_humidity_2m", humidity_percent),
            ("precipitation", precipitation_mm),
            ("wind_speed_10m", wind_speed_mps),
        ):
            if value is not None:
                _require_unit(current_units, field, _EXPECTED_UNITS[field], region)

        try:
            json.dumps(payload, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise _response_error(region, "body is not JSON-compatible") from exc

        assert observed_seconds is not None
        assert temperature_c is not None
        return WeatherEvent(
            region_id=region.id,
            observed_at=_utc_timestamp(observed_seconds, region),
            temperature_c=temperature_c,
            humidity_percent=humidity_percent,
            precipitation_mm=precipitation_mm,
            wind_speed_mps=wind_speed_mps,
            source_payload={
                "provider": "open-meteo",
                "response": copy.deepcopy(payload),
            },
        )
