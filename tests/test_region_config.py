from pathlib import Path

import pytest

from extreme_climate.region_config import (
    DEFAULT_REGIONS_PATH,
    Region,
    RegionConfigError,
    load_regions,
)


def _write_config(tmp_path: Path, contents: str) -> Path:
    path = tmp_path / "regions.yaml"
    path.write_text(contents, encoding="utf-8")
    return path


def test_checked_in_configuration_loads_in_declared_order() -> None:
    regions = load_regions(DEFAULT_REGIONS_PATH)

    assert tuple(region.id for region in regions) == (
        "vancouver",
        "calgary",
        "toronto",
        "montreal",
        "halifax",
    )
    assert regions[2] == Region(
        id="toronto",
        latitude=43.6532,
        longitude=-79.3832,
        timezone="America/Toronto",
    )


def test_loads_boundary_coordinates(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path,
        """
regions:
  - id: boundary
    latitude: 90
    longitude: -180
    timezone: UTC
""",
    )

    assert load_regions(path) == (
        Region(
            id="boundary",
            latitude=90.0,
            longitude=-180.0,
            timezone="UTC",
        ),
    )


@pytest.mark.parametrize(
    ("contents", "expected_error"),
    [
        ("", "root: must be a mapping"),
        ("regions: [", "invalid YAML"),
        (
            "regions: []\nregions: []",
            "found duplicate mapping key 'regions'",
        ),
        ("- id: toronto", "root: must be a mapping"),
        ("locations: []", "root: missing required field(s): 'regions'"),
        ("regions: {}", "regions: must be a list"),
        ("regions: []", "regions: must contain at least one region"),
        ("regions:\n  - toronto", "regions[0]: must be a mapping"),
        (
            """
regions:
  - id: toronto
    latitude: 43.6532
    longitude: -79.3832
""",
            "regions[0]: missing required field(s): 'timezone'",
        ),
        (
            """
regions:
  - id: toronto
    latitude: 43.6532
    longitude: -79.3832
    timezone: America/Toronto
    label: Toronto
""",
            "regions[0]: unknown field(s): 'label'",
        ),
        (
            """
regions:
  - id: toronto
    latitude: 43.6532
    latitude: 44
    longitude: -79.3832
    timezone: America/Toronto
""",
            "found duplicate mapping key 'latitude'",
        ),
        (
            """
regions:
  - id: Toronto
    latitude: 43.6532
    longitude: -79.3832
    timezone: America/Toronto
""",
            "regions[0].id: must start with a lowercase letter",
        ),
        (
            """
regions:
  - id: toronto
    latitude: true
    longitude: -79.3832
    timezone: America/Toronto
""",
            "regions[0].latitude: must be a number",
        ),
        (
            """
regions:
  - id: toronto
    latitude: .nan
    longitude: -79.3832
    timezone: America/Toronto
""",
            "regions[0].latitude: must be finite",
        ),
        (
            """
regions:
  - id: toronto
    latitude: 91
    longitude: -79.3832
    timezone: America/Toronto
""",
            "regions[0].latitude: must be between -90 and 90",
        ),
        (
            """
regions:
  - id: toronto
    latitude: 43.6532
    longitude: -181
    timezone: America/Toronto
""",
            "regions[0].longitude: must be between -180 and 180",
        ),
        (
            """
regions:
  - id: toronto
    latitude: 43.6532
    longitude: -79.3832
    timezone: Mars/Olympus_Mons
""",
            "regions[0].timezone: unknown IANA timezone 'Mars/Olympus_Mons'",
        ),
        (
            """
regions:
  - id: toronto
    latitude: 43.6532
    longitude: -79.3832
    timezone: America/Toronto
  - id: toronto
    latitude: 43.7
    longitude: -79.4
    timezone: America/Toronto
""",
            "regions[1].id: duplicates regions[0].id ('toronto')",
        ),
    ],
)
def test_rejects_malformed_configuration(
    tmp_path: Path, contents: str, expected_error: str
) -> None:
    path = _write_config(tmp_path, contents)

    with pytest.raises(RegionConfigError) as exc_info:
        load_regions(path)

    assert str(path) in str(exc_info.value)
    assert expected_error in str(exc_info.value)


def test_reports_missing_file(tmp_path: Path) -> None:
    path = tmp_path / "missing.yaml"

    with pytest.raises(RegionConfigError) as exc_info:
        load_regions(path)

    assert str(path) in str(exc_info.value)
    assert "could not read configuration" in str(exc_info.value)


def test_reports_unrepresentable_coordinate(tmp_path: Path) -> None:
    huge_number = "1" + ("0" * 400)
    path = _write_config(
        tmp_path,
        f"""
regions:
  - id: toronto
    latitude: {huge_number}
    longitude: -79.3832
    timezone: America/Toronto
""",
    )

    with pytest.raises(RegionConfigError) as exc_info:
        load_regions(path)

    assert "regions[0].latitude: must be finite" in str(exc_info.value)


def test_reports_invalid_utf8(tmp_path: Path) -> None:
    path = tmp_path / "regions.yaml"
    path.write_bytes(b"regions:\n  - \xff")

    with pytest.raises(RegionConfigError) as exc_info:
        load_regions(path)

    assert str(path) in str(exc_info.value)
    assert "could not read configuration" in str(exc_info.value)
