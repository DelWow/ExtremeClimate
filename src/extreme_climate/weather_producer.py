"""Fetch current weather observations and print normalized JSON events."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Iterable, Optional, Sequence, TextIO, Tuple

from extreme_climate.region_config import (
    DEFAULT_REGIONS_PATH,
    Region,
    RegionConfigError,
    load_regions,
)
from extreme_climate.weather_api import (
    DEFAULT_REQUEST_TIMEOUT_SECONDS,
    OpenMeteoClient,
    WeatherAPIError,
    WeatherEvent,
)


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
    lines = tuple(
        json.dumps(
            event.to_dict(),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        for event in events
    )
    for line in lines:
        print(line, file=output)
    return events


def _positive_timeout(value: str) -> float:
    try:
        timeout = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if not math.isfinite(timeout) or timeout <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return timeout


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the fetch-and-print command."""

    parser = argparse.ArgumentParser(
        description="Fetch current weather and print normalized JSON events."
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
    args = parser.parse_args(argv)

    try:
        with OpenMeteoClient(timeout=args.timeout) as client:
            fetch_and_print(args.regions, client, sys.stdout)
    except RegionConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except WeatherAPIError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
