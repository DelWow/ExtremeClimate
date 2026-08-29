import json
from io import StringIO
from pathlib import Path

import pytest

from extreme_climate import weather_producer
from extreme_climate.weather_api import WeatherAPIError, WeatherEvent
from extreme_climate.weather_producer import fetch_and_print


class StubWeatherClient:
    def __init__(self) -> None:
        self.requested_regions = []

    def fetch_current(self, region):
        self.requested_regions.append(region.id)
        return WeatherEvent(
            region_id=region.id,
            observed_at="2026-08-28T12:00:00Z",
            temperature_c=20.0,
            humidity_percent=None,
            precipitation_mm=0.0,
            wind_speed_mps=3.5,
            source_payload={"provider": "stub", "response": {"region": region.id}},
        )


class SecondRegionFailureClient(StubWeatherClient):
    def fetch_current(self, region):
        if region.id == "second":
            raise WeatherAPIError("second region failed")
        return super().fetch_current(region)


class StubClientContext:
    last_timeout = None
    last_endpoint = None

    def __init__(self, *, endpoint, timeout):
        type(self).last_timeout = timeout
        type(self).last_endpoint = endpoint
        self.client = StubWeatherClient()

    def __enter__(self):
        return self.client

    def __exit__(self, *args):
        return None


class FailureClientContext(StubClientContext):
    class FailureClient:
        def fetch_current(self, region):
            raise WeatherAPIError(f"upstream failed for {region.id}")

    def __init__(self, *, endpoint, timeout):
        type(self).last_timeout = timeout
        type(self).last_endpoint = endpoint
        self.client = self.FailureClient()


def test_fetch_and_print_emits_ordered_json_lines(tmp_path: Path) -> None:
    config_path = tmp_path / "regions.yaml"
    config_path.write_text(
        """
regions:
  - id: first
    latitude: 1
    longitude: 2
    timezone: UTC
  - id: second
    latitude: 3
    longitude: 4
    timezone: UTC
""",
        encoding="utf-8",
    )
    client = StubWeatherClient()
    output = StringIO()

    events = fetch_and_print(config_path, client, output)

    lines = output.getvalue().splitlines()
    assert client.requested_regions == ["first", "second"]
    assert tuple(event.region_id for event in events) == ("first", "second")
    assert len(lines) == 2
    assert [json.loads(line)["region_id"] for line in lines] == ["first", "second"]
    assert json.loads(lines[0])["humidity_percent"] is None
    assert lines[0].startswith('{"humidity_percent":null,"observed_at"')


def test_fetch_failure_does_not_emit_partial_output(tmp_path: Path) -> None:
    config_path = tmp_path / "regions.yaml"
    config_path.write_text(
        """
regions:
  - id: first
    latitude: 1
    longitude: 2
    timezone: UTC
  - id: second
    latitude: 3
    longitude: 4
    timezone: UTC
""",
        encoding="utf-8",
    )
    output = StringIO()

    with pytest.raises(WeatherAPIError, match="second region failed"):
        fetch_and_print(config_path, SecondRegionFailureClient(), output)

    assert output.getvalue() == ""


def test_main_prints_events_and_passes_timeout(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config_path = tmp_path / "regions.yaml"
    config_path.write_text(
        """
regions:
  - id: first
    latitude: 1
    longitude: 2
    timezone: UTC
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(weather_producer, "OpenMeteoClient", StubClientContext)

    result = weather_producer.main(
        [
            "--regions",
            str(config_path),
            "--timeout",
            "3.25",
            "--print-only",
        ]
    )

    captured = capsys.readouterr()
    assert result == 0
    assert StubClientContext.last_timeout == 3.25
    assert json.loads(captured.out)["region_id"] == "first"
    assert captured.err == ""


def test_main_reports_api_failure(tmp_path: Path, monkeypatch, capsys) -> None:
    config_path = tmp_path / "regions.yaml"
    config_path.write_text(
        """
regions:
  - id: first
    latitude: 1
    longitude: 2
    timezone: UTC
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(weather_producer, "OpenMeteoClient", FailureClientContext)

    result = weather_producer.main(
        ["--regions", str(config_path), "--print-only"]
    )

    captured = capsys.readouterr()
    assert result == 1
    assert captured.out == ""
    assert captured.err == "error: upstream failed for first\n"


def test_main_reports_region_config_failure(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    monkeypatch.setattr(weather_producer, "OpenMeteoClient", StubClientContext)

    result = weather_producer.main(
        [
            "--regions",
            str(tmp_path / "missing-regions.yaml"),
            "--print-only",
        ]
    )

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert "could not read configuration" in captured.err


def test_main_rejects_empty_weather_api_endpoint(monkeypatch, capsys) -> None:
    monkeypatch.setenv("WEATHER_API_URL", " ")

    result = weather_producer.main(["--print-only"])

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: WEATHER_API_URL must not be empty\n"


def test_main_publishes_before_printing(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config_path = tmp_path / "regions.yaml"
    config_path.write_text(
        """
regions:
  - id: first
    latitude: 1
    longitude: 2
    timezone: UTC
""",
        encoding="utf-8",
    )
    settings = object()
    published = []
    monkeypatch.setenv("WEATHER_API_URL", "https://weather.example/forecast")
    monkeypatch.setattr(weather_producer, "OpenMeteoClient", StubClientContext)
    monkeypatch.setattr(
        weather_producer,
        "load_kafka_settings",
        lambda: settings,
    )

    def record_publish(events, actual_settings):
        published.append((tuple(events), actual_settings))
        return 1

    monkeypatch.setattr(
        weather_producer,
        "publish_weather_events",
        record_publish,
    )

    result = weather_producer.main(["--regions", str(config_path)])

    captured = capsys.readouterr()
    assert result == 0
    assert StubClientContext.last_endpoint == "https://weather.example/forecast"
    assert published[0][1] is settings
    assert tuple(event.region_id for event in published[0][0]) == ("first",)
    assert json.loads(captured.out)["region_id"] == "first"
    assert captured.err == ""


def test_main_reports_kafka_failure_without_printing(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    config_path = tmp_path / "regions.yaml"
    config_path.write_text(
        """
regions:
  - id: first
    latitude: 1
    longitude: 2
    timezone: UTC
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(weather_producer, "OpenMeteoClient", StubClientContext)
    monkeypatch.setattr(
        weather_producer,
        "load_kafka_settings",
        lambda: object(),
    )

    def fail_publish(_events, _settings):
        raise weather_producer.KafkaPublishError("broker unavailable")

    monkeypatch.setattr(
        weather_producer,
        "publish_weather_events",
        fail_publish,
    )

    result = weather_producer.main(["--regions", str(config_path)])

    captured = capsys.readouterr()
    assert result == 3
    assert captured.out == ""
    assert captured.err == "error: broker unavailable\n"
