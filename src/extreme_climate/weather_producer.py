"""Fetch current weather observations and publish normalized JSON events."""

from __future__ import annotations

import argparse
import math
import os
import sys
from pathlib import Path
from typing import Iterable, Mapping, Optional, Sequence, TextIO, Tuple

from extreme_climate.kafka_publisher import (
    KafkaConfigError,
    KafkaPublishError,
    KafkaSettings,
    load_kafka_settings,
    publish_weather_events,
)

from extreme_climate.region_config import (
    DEFAULT_REGIONS_PATH,
    Region,
    RegionConfigError,
    load_regions,
)
from extreme_climate.weather_api import (
    DEFAULT_REQUEST_TIMEOUT_SECONDS,
    OPEN_METEO_FORECAST_URL,
    OpenMeteoClient,
    WeatherAPIError,
    WeatherEvent,
)


class WeatherProducerConfigError(ValueError):
    """Raised when producer-specific environment configuration is invalid."""


def fetch_weather_events(
    regions: Iterable[Region], client: OpenMeteoClient
) -> Tuple[WeatherEvent, ...]:
    """Fetch all configured regions in their declared order."""

    return tuple(client.fetch_current(region) for region in regions)


def fetch_and_print(
    config_path: Path, client: OpenMeteoClient, output: TextIO
) -> Tuple[WeatherEvent, ...]:
    """Load regions, fetch every observation, and print deterministic JSONL."""

    regions = load_regions(config_path)
    events = fetch_weather_events(regions, client)
    print_weather_events(events, output)
    return events


def print_weather_events(events: Iterable[WeatherEvent], output: TextIO) -> None:
    """Print deterministic JSONL for already-normalized events."""

    lines = tuple(event.to_json() for event in events)
    for line in lines:
        print(line, file=output)


def _weather_api_endpoint(
    environ: Optional[Mapping[str, str]] = None,
) -> str:
    source = os.environ if environ is None else environ
    endpoint = source.get("WEATHER_API_URL", OPEN_METEO_FORECAST_URL).strip()
    if not endpoint:
        raise WeatherProducerConfigError("WEATHER_API_URL must not be empty")
    return endpoint


def _positive_timeout(value: str) -> float:
    try:
        timeout = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(timeout) or timeout <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return timeout


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the weather producer command."""

    parser = argparse.ArgumentParser(
        description="Fetch current weather, publish it to Kafka, and print JSON events."
    )
    parser.add_argument(
        "--regions",
        type=Path,
        default=DEFAULT_REGIONS_PATH,
        help=f"region configuration (default: {DEFAULT_REGIONS_PATH})",
    )
    parser.add_argument(
        "--timeout",
        type=_positive_timeout,
        default=DEFAULT_REQUEST_TIMEOUT_SECONDS,
        help=f"HTTP timeout in seconds (default: {DEFAULT_REQUEST_TIMEOUT_SECONDS:g})",
    )
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="print events without connecting to Kafka",
    )
    args = parser.parse_args(argv)

    try:
        endpoint = _weather_api_endpoint()
        kafka_settings: Optional[KafkaSettings] = None
        if not args.print_only:
            kafka_settings = load_kafka_settings()

        with OpenMeteoClient(endpoint=endpoint, timeout=args.timeout) as client:
            regions = load_regions(args.regions)
            events = fetch_weather_events(regions, client)

        if kafka_settings is not None:
            publish_weather_events(events, kafka_settings)
        print_weather_events(events, sys.stdout)
    except (
        RegionConfigError,
        KafkaConfigError,
        WeatherProducerConfigError,
    ) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except WeatherAPIError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KafkaPublishError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
