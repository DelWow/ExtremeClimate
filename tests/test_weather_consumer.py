import json
from datetime import datetime, timezone
from io import StringIO

import psycopg
import pytest
from confluent_kafka import KafkaException

from extreme_climate import weather_consumer
from extreme_climate.postgres_config import PostgresConfigError
from extreme_climate.weather_api import WeatherEvent
from extreme_climate.weather_consumer import (
    ConsumerConfigError,
    EventDeserializationError,
    KafkaPosition,
    RawWeatherStore,
    WeatherConsumerError,
    WeatherPersistenceError,
    consume_weather_events,
    deserialize_weather_event,
    load_weather_consumer_settings,
    process_message,
)


def _payload() -> dict:
    return {
        "region_id": "toronto",
        "observed_at": "2026-08-28T12:00:00Z",
        "temperature_c": 20.5,
        "humidity_percent": 55.0,
        "precipitation_mm": 0.0,
        "wind_speed_mps": 3.5,
        "source_payload": {
            "provider": "stub",
            "response": {"interval": 900},
        },
    }


def _event_bytes() -> bytes:
    payload = _payload()
    event = WeatherEvent(
        region_id=payload["region_id"],
        observed_at=payload["observed_at"],
        temperature_c=payload["temperature_c"],
        humidity_percent=payload["humidity_percent"],
        precipitation_mm=payload["precipitation_mm"],
        wind_speed_mps=payload["wind_speed_mps"],
        source_payload=payload["source_payload"],
    )
    return event.to_json_bytes()


class FakeCursor:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class FakeTransaction:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        self.log.append("db_begin")
        return self

    def __exit__(self, exception_type, _exception, _traceback):
        self.log.append("db_commit" if exception_type is None else "db_rollback")
        return False


class FakeConnection:
    def __init__(self, *, row=(1,), error=None, log=None):
        self.row = row
        self.error = error
        self.log = [] if log is None else log
        self.executions = []
        self.closed = False

    def transaction(self):
        return FakeTransaction(self.log)

    def execute(self, query, parameters):
        self.log.append("db_execute")
        self.executions.append((query, parameters))
        if self.error is not None:
            raise self.error
        return FakeCursor(self.row)

    def close(self):
        self.closed = True


class FakeMessage:
    def __init__(
        self,
        *,
        value=_event_bytes(),
        key=b"toronto",
        topic="weather",
        partition=0,
        offset=12,
        error=None,
    ):
        self._value = value
        self._key = key
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._error = error

    def value(self):
        return self._value

    def key(self):
        return self._key

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return self._error


class FakeConsumer:
    def __init__(self, config=None, *, messages=None, log=None):
        self.config = config
        self.messages = [] if messages is None else list(messages)
        self.log = [] if log is None else log
        self.subscriptions = []
        self.commits = []
        self.commit_error = None
        self.commit_result = []
        self.closed = False

    def subscribe(self, topics):
        self.subscriptions.append(list(topics))

    def poll(self, timeout):
        self.log.append(("poll", timeout))
        return self.messages.pop(0) if self.messages else None

    def commit(self, *, message, asynchronous):
        self.log.append("kafka_commit")
        self.commits.append((message, asynchronous))
        if self.commit_error is not None:
            raise self.commit_error
        return self.commit_result

    def close(self):
        self.closed = True


def test_deserializes_producer_event_contract() -> None:
    event = deserialize_weather_event(_event_bytes())

    assert event.region_id == "toronto"
    assert event.observed_at == datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)
    assert event.temperature_c == 20.5
    assert event.humidity_percent == 55.0
    assert event.precipitation_mm == 0.0
    assert event.wind_speed_mps == 3.5
    assert event.source_payload["response"]["interval"] == 900


@pytest.mark.parametrize(
    ("value", "expected_error"),
    [
        (None, "must not be null"),
        (b"\xff", "valid finite JSON"),
        (b"[]", "must be a JSON object"),
        (b'{"temperature_c":NaN}', "valid finite JSON"),
        (
            json.dumps({**_payload(), "unexpected": True}).encode(),
            "unknown field(s): unexpected",
        ),
        (
            json.dumps(
                {key: value for key, value in _payload().items() if key != "region_id"}
            ).encode(),
            "missing field(s): region_id",
        ),
        (
            json.dumps({**_payload(), "temperature_c": True}).encode(),
            "temperature_c must be a number",
        ),
        (
            json.dumps({**_payload(), "temperature_c": 10**400}).encode(),
            "temperature_c must be finite",
        ),
        (
            json.dumps({**_payload(), "observed_at": "2026-08-28"}).encode(),
            "RFC 3339 UTC timestamp",
        ),
        (
            json.dumps({**_payload(), "source_payload": None}).encode(),
            "source_payload must be a JSON object",
        ),
    ],
)
def test_rejects_invalid_event_payload(value, expected_error: str) -> None:
    with pytest.raises(EventDeserializationError) as exc_info:
        deserialize_weather_event(value)

    assert expected_error in str(exc_info.value)


def test_store_uses_parameterized_idempotent_insert() -> None:
    connection = FakeConnection()
    store = RawWeatherStore(connection)
    event = deserialize_weather_event(_event_bytes())
    position = KafkaPosition(topic="weather", partition=2, offset=41)

    inserted = store.insert(event, position)

    assert inserted is True
    query, parameters = connection.executions[0]
    assert "ON CONFLICT (kafka_topic, kafka_partition, kafka_offset)" in query
    assert "DO NOTHING" in query
    assert parameters[:6] == (
        "toronto",
        datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc),
        20.5,
        55.0,
        0.0,
        3.5,
    )
    assert json.loads(parameters[6]) == _payload()["source_payload"]
    assert parameters[7:] == ("weather", 2, 41)
    assert connection.log == ["db_begin", "db_execute", "db_commit"]


def test_store_returns_false_for_duplicate_position() -> None:
    connection = FakeConnection(row=None)

    inserted = RawWeatherStore(connection).insert(
        deserialize_weather_event(_event_bytes()),
        KafkaPosition(topic="weather", partition=0, offset=12),
    )

    assert inserted is False
    assert connection.log[-1] == "db_commit"


def test_process_commits_only_after_database_commit() -> None:
    log = []
    connection = FakeConnection(log=log)
    consumer = FakeConsumer(log=log)
    message = FakeMessage()

    result = process_message(consumer, RawWeatherStore(connection), message)

    assert result.inserted is True
    assert result.position == KafkaPosition("weather", 0, 12)
    assert log == ["db_begin", "db_execute", "db_commit", "kafka_commit"]
    assert consumer.commits == [(message, False)]


def test_duplicate_replay_still_commits_offset() -> None:
    connection = FakeConnection(row=None)
    consumer = FakeConsumer()
    message = FakeMessage()

    result = process_message(consumer, RawWeatherStore(connection), message)

    assert result.inserted is False
    assert consumer.commits == [(message, False)]


def test_database_failure_rolls_back_without_committing_offset() -> None:
    connection = FakeConnection(error=psycopg.OperationalError("db unavailable"))
    consumer = FakeConsumer()

    with pytest.raises(WeatherPersistenceError):
        process_message(
            consumer,
            RawWeatherStore(connection),
            FakeMessage(),
        )

    assert connection.log[-1] == "db_rollback"
    assert consumer.commits == []


def test_invalid_message_does_not_touch_database_or_commit() -> None:
    connection = FakeConnection()
    consumer = FakeConsumer()

    with pytest.raises(WeatherConsumerError, match="key does not match"):
        process_message(
            consumer,
            RawWeatherStore(connection),
            FakeMessage(key=b"halifax"),
        )

    assert connection.executions == []
    assert consumer.commits == []


def test_unconfigured_region_does_not_touch_database_or_commit() -> None:
    connection = FakeConnection()
    consumer = FakeConsumer()

    with pytest.raises(WeatherConsumerError, match="'toronto' is not configured"):
        process_message(
            consumer,
            RawWeatherStore(connection),
            FakeMessage(),
            allowed_region_ids={"halifax"},
        )

    assert connection.executions == []
    assert consumer.commits == []


def test_commit_failure_occurs_after_database_commit() -> None:
    log = []
    connection = FakeConnection(log=log)
    consumer = FakeConsumer(log=log)
    consumer.commit_error = KafkaException("commit failed")

    with pytest.raises(WeatherConsumerError, match="offset commit failed"):
        process_message(
            consumer,
            RawWeatherStore(connection),
            FakeMessage(),
        )

    assert log == ["db_begin", "db_execute", "db_commit", "kafka_commit"]


def test_loads_manual_commit_consumer_and_database_settings() -> None:
    settings = load_weather_consumer_settings(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:9092",
            "KAFKA_TOPIC": "weather-secure",
            "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_SASL_USERNAME": "consumer-user",
            "KAFKA_SASL_PASSWORD": "consumer-secret",
            "KAFKA_CONSUMER_GROUP_ID": "raw-weather-test",
            "KAFKA_AUTO_OFFSET_RESET": "latest",
            "KAFKA_POLL_TIMEOUT_SECONDS": "2.5",
            "POSTGRES_HOST": "database",
            "POSTGRES_PORT": "5434",
            "POSTGRES_DB": "climate",
            "POSTGRES_USER": "pipeline",
            "POSTGRES_PASSWORD": "database-secret",
            "POSTGRES_CONNECT_TIMEOUT_SECONDS": "7",
        }
    )

    assert settings.consumer_config() == {
        "bootstrap.servers": "broker:9092",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "consumer-user",
        "sasl.password": "consumer-secret",
        "group.id": "raw-weather-test",
        "client.id": "extreme-climate-weather-consumer",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }
    assert settings.postgres.connection_kwargs() == {
        "host": "database",
        "port": 5434,
        "dbname": "climate",
        "user": "pipeline",
        "password": "database-secret",
        "connect_timeout": 7,
    }
    assert settings.region_ids == {
        "vancouver",
        "calgary",
        "toronto",
        "montreal",
        "halifax",
    }
    assert "consumer-secret" not in repr(settings)
    assert "database-secret" not in repr(settings)


@pytest.mark.parametrize(
    ("environ", "error_type", "expected_error"),
    [
        (
            {"KAFKA_CONSUMER_GROUP_ID": " "},
            ConsumerConfigError,
            "must not be empty",
        ),
        (
            {"KAFKA_AUTO_OFFSET_RESET": "middle"},
            ConsumerConfigError,
            "must be earliest",
        ),
        (
            {"KAFKA_POLL_TIMEOUT_SECONDS": "0"},
            ConsumerConfigError,
            "positive finite number",
        ),
        (
            {"POSTGRES_PORT": "70000"},
            PostgresConfigError,
            "must be at most 65535",
        ),
        (
            {"POSTGRES_CONNECT_TIMEOUT_SECONDS": "1.5"},
            PostgresConfigError,
            "must be an integer",
        ),
    ],
)
def test_rejects_invalid_consumer_settings(
    environ, error_type, expected_error: str
) -> None:
    with pytest.raises(error_type) as exc_info:
        load_weather_consumer_settings(environ)

    assert expected_error in str(exc_info.value)


def test_consume_loop_closes_resources_after_message_limit() -> None:
    consumer = FakeConsumer(messages=[FakeMessage()])
    connection = FakeConnection()
    output = StringIO()
    settings = load_weather_consumer_settings({})

    def consumer_factory(config):
        consumer.config = config
        return consumer

    processed = consume_weather_events(
        settings,
        max_messages=1,
        output=output,
        consumer_factory=consumer_factory,
        connection_factory=lambda **_kwargs: connection,
    )

    assert processed == 1
    assert consumer.subscriptions == [["weather"]]
    assert consumer.config == settings.consumer_config()
    assert consumer.closed is True
    assert connection.closed is True
    assert output.getvalue() == "inserted weather[0]@12\n"


def test_main_passes_message_limit(monkeypatch) -> None:
    settings = load_weather_consumer_settings({})
    calls = []
    monkeypatch.setattr(
        weather_consumer,
        "load_weather_consumer_settings",
        lambda: settings,
    )

    def consume(actual_settings, *, max_messages):
        calls.append((actual_settings, max_messages))
        return max_messages

    monkeypatch.setattr(weather_consumer, "consume_weather_events", consume)

    result = weather_consumer.main(["--max-messages", "3"])

    assert result == 0
    assert calls == [(settings, 3)]


def test_main_reports_configuration_failure(monkeypatch, capsys) -> None:
    def fail_settings():
        raise ConsumerConfigError("bad consumer configuration")

    monkeypatch.setattr(
        weather_consumer,
        "load_weather_consumer_settings",
        fail_settings,
    )

    result = weather_consumer.main([])

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert captured.err == "error: bad consumer configuration\n"


def test_main_reports_consumer_failure(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        weather_consumer,
        "load_weather_consumer_settings",
        lambda: load_weather_consumer_settings({}),
    )

    def fail_consumer(_settings, *, max_messages):
        raise WeatherConsumerError("poll failed")

    monkeypatch.setattr(
        weather_consumer,
        "consume_weather_events",
        fail_consumer,
    )

    result = weather_consumer.main([])

    captured = capsys.readouterr()
    assert result == 1
    assert captured.out == ""
    assert captured.err == "error: poll failed\n"
