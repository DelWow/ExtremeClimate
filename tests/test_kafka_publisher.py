import json

import pytest

from extreme_climate.kafka_publisher import (
    DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    DEFAULT_KAFKA_CLIENT_ID,
    DEFAULT_KAFKA_TOPIC,
    KafkaConfigError,
    KafkaPublishError,
    KafkaSettings,
    load_kafka_settings,
    publish_weather_events,
)
from extreme_climate.weather_api import WeatherEvent


def _event(region_id: str = "toronto") -> WeatherEvent:
    return WeatherEvent(
        region_id=region_id,
        observed_at="2026-08-28T12:00:00Z",
        temperature_c=20.5,
        humidity_percent=55.0,
        precipitation_mm=0.0,
        wind_speed_mps=3.5,
        source_payload={"provider": "stub", "response": {"interval": 900}},
    )


def _settings() -> KafkaSettings:
    return KafkaSettings(
        bootstrap_servers="broker:9092",
        topic="weather-events",
        client_id="weather-test",
        delivery_timeout_seconds=4.5,
        security_protocol="PLAINTEXT",
    )


class FakeProducer:
    def __init__(self, config):
        self.config = config
        self.messages = []
        self.poll_timeouts = []
        self.flush_timeout = None
        self.flush_remaining = 0
        self.delivery_error = None
        self.produce_error = None

    def produce(self, topic, *, value, key, on_delivery):
        if self.produce_error is not None:
            raise self.produce_error
        self.messages.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
                "on_delivery": on_delivery,
            }
        )

    def poll(self, timeout):
        self.poll_timeouts.append(timeout)
        return 0

    def flush(self, timeout):
        self.flush_timeout = timeout
        if not self.flush_remaining:
            for message in self.messages:
                message["on_delivery"](self.delivery_error, object())
        return self.flush_remaining


def test_loads_default_kafka_settings() -> None:
    settings = load_kafka_settings({})

    assert settings.bootstrap_servers == DEFAULT_KAFKA_BOOTSTRAP_SERVERS
    assert settings.topic == DEFAULT_KAFKA_TOPIC
    assert settings.client_id == DEFAULT_KAFKA_CLIENT_ID
    assert settings.delivery_timeout_seconds == 30.0
    assert settings.producer_config() == {
        "bootstrap.servers": "127.0.0.1:29092",
        "client.id": "extreme-climate-weather-producer",
        "security.protocol": "PLAINTEXT",
        "enable.idempotence": True,
        "acks": "all",
        "delivery.timeout.ms": 30000,
    }


def test_loads_authenticated_kafka_settings_without_exposing_password() -> None:
    settings = load_kafka_settings(
        {
            "KAFKA_BOOTSTRAP_SERVERS": "cloud.example:9092",
            "KAFKA_TOPIC": "weather.secure",
            "KAFKA_CLIENT_ID": "secure-producer",
            "KAFKA_DELIVERY_TIMEOUT_SECONDS": "12.5",
            "KAFKA_SECURITY_PROTOCOL": "sasl_ssl",
            "KAFKA_SASL_MECHANISM": "scram-sha-512",
            "KAFKA_SASL_USERNAME": "pipeline-user",
            "KAFKA_SASL_PASSWORD": "super-secret",
            "KAFKA_SSL_CA_LOCATION": "/certs/ca.pem",
        }
    )

    assert settings.producer_config() == {
        "bootstrap.servers": "cloud.example:9092",
        "client.id": "secure-producer",
        "security.protocol": "SASL_SSL",
        "enable.idempotence": True,
        "acks": "all",
        "delivery.timeout.ms": 12500,
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "pipeline-user",
        "sasl.password": "super-secret",
        "ssl.ca.location": "/certs/ca.pem",
    }
    assert "super-secret" not in repr(settings)


@pytest.mark.parametrize(
    ("environ", "expected_error"),
    [
        ({"KAFKA_BOOTSTRAP_SERVERS": " "}, "must not be empty"),
        ({"KAFKA_TOPIC": "weather events"}, "KAFKA_TOPIC must be"),
        ({"KAFKA_TOPIC": ".."}, "KAFKA_TOPIC must be"),
        (
            {"KAFKA_DELIVERY_TIMEOUT_SECONDS": "nan"},
            "must be a positive finite number",
        ),
        (
            {"KAFKA_SECURITY_PROTOCOL": "SASL_SSL"},
            "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required",
        ),
        (
            {"KAFKA_SASL_USERNAME": "unexpected"},
            "KAFKA_SASL_* settings require a SASL security protocol",
        ),
    ],
)
def test_rejects_invalid_kafka_settings(environ, expected_error: str) -> None:
    with pytest.raises(KafkaConfigError) as exc_info:
        load_kafka_settings(environ)

    assert expected_error in str(exc_info.value)


def test_serializes_and_delivers_keyed_events() -> None:
    producer = FakeProducer({})

    def producer_factory(config):
        producer.config.update(config)
        return producer

    delivered = publish_weather_events(
        [_event("toronto"), _event("halifax")],
        _settings(),
        producer_factory=producer_factory,
    )

    assert delivered == 2
    assert producer.config == _settings().producer_config()
    assert [message["topic"] for message in producer.messages] == [
        "weather-events",
        "weather-events",
    ]
    assert [message["key"] for message in producer.messages] == [
        b"toronto",
        b"halifax",
    ]
    assert json.loads(producer.messages[0]["value"])["region_id"] == "toronto"
    assert producer.messages[0]["value"] == _event("toronto").to_json_bytes()
    assert producer.poll_timeouts == [0, 0]
    assert producer.flush_timeout == 4.5


def test_reports_delivery_callback_failure() -> None:
    producer = FakeProducer({})
    producer.delivery_error = RuntimeError("authorization failed\nforged status")

    with pytest.raises(KafkaPublishError) as exc_info:
        publish_weather_events(
            [_event()],
            _settings(),
            producer_factory=lambda _config: producer,
        )

    assert "authorization failed forged status" in str(exc_info.value)
    assert "\n" not in str(exc_info.value)


def test_reports_delivery_timeout() -> None:
    producer = FakeProducer({})
    producer.flush_remaining = 1

    with pytest.raises(KafkaPublishError, match=r"1 event\(s\) still pending"):
        publish_weather_events(
            [_event()],
            _settings(),
            producer_factory=lambda _config: producer,
        )


def test_reports_enqueue_failure_with_event_position() -> None:
    producer = FakeProducer({})
    producer.produce_error = BufferError("local queue full")

    with pytest.raises(KafkaPublishError, match="event 1: local queue full"):
        publish_weather_events(
            [_event()],
            _settings(),
            producer_factory=lambda _config: producer,
        )
