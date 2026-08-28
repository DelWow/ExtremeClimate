"""Load and validate the pipeline's region configuration."""

from __future__ import annotations

import argparse
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence, Tuple, Union
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from yaml.constructor import ConstructorError
from yaml.nodes import MappingNode


DEFAULT_REGIONS_PATH = Path("config/regions.yaml")

_ROOT_FIELDS = frozenset({"regions"})
_REGION_FIELDS = frozenset({"id", "latitude", "longitude", "timezone"})
_REGION_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_-]*$")


class RegionConfigError(ValueError):
    """Raised when a region configuration cannot be read or validated."""


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """A safe YAML loader that rejects duplicate mapping keys."""

    def construct_mapping(self, node: MappingNode, deep: bool = False) -> dict:
        if not isinstance(node, MappingNode):
            raise ConstructorError(
                None,
                None,
                f"expected a mapping node, but found {node.id}",
                node.start_mark,
            )

        self.flatten_mapping(node)
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                duplicate = key in mapping
            except TypeError as exc:
                raise ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    "found an unhashable mapping key",
                    key_node.start_mark,
                ) from exc
            if duplicate:
                raise ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found duplicate mapping key {key!r}",
                    key_node.start_mark,
                )
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


@dataclass(frozen=True)
class Region:
    """A configured region and the metadata needed for a weather query."""

    id: str
    latitude: float
    longitude: float
    timezone: str


def _fail(path: Path, location: str, message: str) -> RegionConfigError:
    return RegionConfigError(f"{path}: {location}: {message}")


def _validate_fields(
    value: Mapping[Any, Any],
    expected: frozenset[str],
    path: Path,
    location: str,
) -> None:
    actual = set(value)
    missing = expected - actual
    unknown = actual - expected

    problems = []
    if missing:
        problems.append(
            "missing required field(s): "
            + ", ".join(sorted(repr(field) for field in missing))
        )
    if unknown:
        problems.append(
            "unknown field(s): "
            + ", ".join(sorted(repr(field) for field in unknown))
        )

    if problems:
        raise _fail(path, location, "; ".join(problems))


def _validate_region_id(value: Any, path: Path, location: str) -> str:
    if not isinstance(value, str):
        raise _fail(path, location, "must be a string")
    if value != value.strip():
        raise _fail(path, location, "must not have leading or trailing whitespace")
    if not value:
        raise _fail(path, location, "must not be empty")
    if len(value) > 64:
        raise _fail(path, location, "must be at most 64 characters")
    if _REGION_ID_PATTERN.fullmatch(value) is None:
        raise _fail(
            path,
            location,
            "must start with a lowercase letter and contain only lowercase "
            "letters, digits, underscores, or hyphens",
        )
    return value


def _validate_coordinate(
    value: Any,
    minimum: float,
    maximum: float,
    path: Path,
    location: str,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _fail(path, location, "must be a number")

    try:
        coordinate = float(value)
    except (OverflowError, ValueError) as exc:
        raise _fail(path, location, "must be finite") from exc
    if not math.isfinite(coordinate):
        raise _fail(path, location, "must be finite")
    if not minimum <= coordinate <= maximum:
        raise _fail(path, location, f"must be between {minimum:g} and {maximum:g}")
    return coordinate


def _validate_timezone(value: Any, path: Path, location: str) -> str:
    if not isinstance(value, str):
        raise _fail(path, location, "must be a string")
    if value != value.strip() or not value:
        raise _fail(path, location, "must be a non-empty IANA timezone")

    try:
        ZoneInfo(value)
    except (ValueError, ZoneInfoNotFoundError) as exc:
        raise _fail(path, location, f"unknown IANA timezone {value!r}") from exc
    return value


def load_regions(
    config_path: Union[str, Path],
) -> Tuple[Region, ...]:
    """Load a YAML file and return its validated regions in declared order."""

    path = Path(config_path)
    try:
        contents = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        detail = getattr(exc, "strerror", None) or str(exc)
        raise RegionConfigError(f"{path}: could not read configuration: {detail}") from exc

    try:
        document = yaml.load(contents, Loader=_UniqueKeySafeLoader)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        position = ""
        if mark is not None:
            position = f" at line {mark.line + 1}, column {mark.column + 1}"
        raise RegionConfigError(f"{path}: invalid YAML{position}: {exc}") from exc

    if not isinstance(document, Mapping):
        raise _fail(path, "root", "must be a mapping")
    _validate_fields(document, _ROOT_FIELDS, path, "root")

    raw_regions = document["regions"]
    if not isinstance(raw_regions, list):
        raise _fail(path, "regions", "must be a list")
    if not raw_regions:
        raise _fail(path, "regions", "must contain at least one region")

    regions = []
    seen_ids = {}
    for index, raw_region in enumerate(raw_regions):
        location = f"regions[{index}]"
        if not isinstance(raw_region, Mapping):
            raise _fail(path, location, "must be a mapping")
        _validate_fields(raw_region, _REGION_FIELDS, path, location)

        region_id = _validate_region_id(raw_region["id"], path, f"{location}.id")
        if region_id in seen_ids:
            first_index = seen_ids[region_id]
            raise _fail(
                path,
                f"{location}.id",
                f"duplicates regions[{first_index}].id ({region_id!r})",
            )
        seen_ids[region_id] = index

        regions.append(
            Region(
                id=region_id,
                latitude=_validate_coordinate(
                    raw_region["latitude"],
                    -90,
                    90,
                    path,
                    f"{location}.latitude",
                ),
                longitude=_validate_coordinate(
                    raw_region["longitude"],
                    -180,
                    180,
                    path,
                    f"{location}.longitude",
                ),
                timezone=_validate_timezone(
                    raw_region["timezone"], path, f"{location}.timezone"
                ),
            )
        )

    return tuple(regions)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Validate a region file from the command line."""

    parser = argparse.ArgumentParser(description="Validate region configuration.")
    parser.add_argument(
        "config",
        nargs="?",
        type=Path,
        default=DEFAULT_REGIONS_PATH,
        help=f"YAML file to validate (default: {DEFAULT_REGIONS_PATH})",
    )
    args = parser.parse_args(argv)

    try:
        regions = load_regions(args.config)
    except RegionConfigError as exc:
        parser.exit(2, f"error: {exc}\n")

    print(f"Validated {len(regions)} region(s) from {args.config}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
