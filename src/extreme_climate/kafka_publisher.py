"""Publish normalized weather events to Kafka."""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Protocol,
)

from confluent_kafka import KafkaException, Producer

if TYPE_CHECKING:
    from extreme_climate.weather_api import WeatherEvent


DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:29092"
DEFAULT_KAFKA_TOPIC = "weather"
DEFAULT_KAFKA_CLIENT_ID = "extreme-climate-weather-producer"
DEFAULT_KAFKA_DELIVERY_TIMEOUT_SECONDS = 30.0

_TOPIC_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")
_SECURITY_PROTOCOLS = frozenset({"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"})


class KafkaConfigError(ValueError):
    """Raised when Kafka environment configuration is invalid."""


class KafkaPublishError(RuntimeError):
    """Raised when weather events cannot be delivered to Kafka."""


class ProducerProtocol(Protocol):
    """The subset of ``confluent_kafka.Producer`` used by this module."""

    def produce(
        self,
        topic: str,
        *,
        value: bytes,
        key: bytes,
        on_delivery: Callable[[Any, Any], None],
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


ProducerFactory = Callable[[Mapping[str, Any]], ProducerProtocol]


def _required_text(environ: Mapping[str, str], name: str, default: str) -> str:
    value = environ.get(name, default).strip()
    if not value:
        raise KafkaConfigError(f"{name} must not be empty")
    return value


def _optional_text(environ: Mapping[str, str], name: str) -> Optional[str]:
    value = environ.get(name)
    if value is None or not value.strip():
        return None
    return value.strip()


def _delivery_timeout(environ: Mapping[str, str]) -> float:
    raw_value = environ.get(
        "KAFKA_DELIVERY_TIMEOUT_SECONDS",
        str(DEFAULT_KAFKA_DELIVERY_TIMEOUT_SECONDS),
    )
    try:
        timeout = float(raw_value)
    except ValueError as exc:
        raise KafkaConfigError(
            "KAFKA_DELIVERY_TIMEOUT_SECONDS must be a number"
        ) from exc
    if not math.isfinite(timeout) or timeout <= 0:
        raise KafkaConfigError(
            "KAFKA_DELIVERY_TIMEOUT_SECONDS must be a positive finite number"
        )
    return timeout


@dataclass(frozen=True)
class KafkaSettings:
    """Validated producer settings sourced from the environment."""

    bootstrap_servers: str
    topic: str
    client_id: str
    delivery_timeout_seconds: float
    security_protocol: str
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = field(default=None, repr=False)
    ssl_ca_location: Optional[str] = None

    def connection_config(self) -> Dict[str, Any]:
        """Return shared broker configuration; callers must not log it."""

        config: Dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
        }
        if self.sasl_mechanism is not None:
            config["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username is not None:
            config["sasl.username"] = self.sasl_username
        if self.sasl_password is not None:
            config["sasl.password"] = self.sasl_password
        if self.ssl_ca_location is not None:
            config["ssl.ca.location"] = self.ssl_ca_location
        return config

    def producer_config(self) -> Dict[str, Any]:
        """Return librdkafka producer configuration; callers must not log it."""

        config = self.connection_config()
        config.update(
            {
                "client.id": self.client_id,
                "enable.idempotence": True,
                "acks": "all",
                "delivery.timeout.ms": max(
                    1, round(self.delivery_timeout_seconds * 1000)
                ),
            }
        )
        return config


def load_kafka_settings(
    environ: Optional[Mapping[str, str]] = None,
) -> KafkaSettings:
    """Load and validate Kafka producer settings from environment variables."""

    source = os.environ if environ is None else environ
    bootstrap_servers = _required_text(
        source,
        "KAFKA_BOOTSTRAP_SERVERS",
        DEFAULT_KAFKA_BOOTSTRAP_SERVERS,
    )
    topic = _required_text(source, "KAFKA_TOPIC", DEFAULT_KAFKA_TOPIC)
    if (
        topic in {".", ".."}
        or len(topic) > 249
        or _TOPIC_PATTERN.fullmatch(topic) is None
    ):
        raise KafkaConfigError(
            "KAFKA_TOPIC must be at most 249 characters and contain only "
            "letters, digits, periods, underscores, or hyphens"
        )

    client_id = _required_text(
        source,
        "KAFKA_CLIENT_ID",
        DEFAULT_KAFKA_CLIENT_ID,
    )
    security_protocol = _required_text(
        source,
        "KAFKA_SECURITY_PROTOCOL",
        "PLAINTEXT",
    ).upper()
    if security_protocol not in _SECURITY_PROTOCOLS:
        raise KafkaConfigError(
            "KAFKA_SECURITY_PROTOCOL must be one of "
            + ", ".join(sorted(_SECURITY_PROTOCOLS))
        )

    sasl_mechanism = _optional_text(source, "KAFKA_SASL_MECHANISM")
    sasl_username = _optional_text(source, "KAFKA_SASL_USERNAME")
    raw_password = source.get("KAFKA_SASL_PASSWORD")
    sasl_password = raw_password if raw_password else None
    uses_sasl = security_protocol.startswith("SASL_")
    if uses_sasl:
        sasl_mechanism = (sasl_mechanism or "PLAIN").upper()
        if sasl_username is None or sasl_password is None:
            raise KafkaConfigError(
                "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required "
                f"when KAFKA_SECURITY_PROTOCOL is {security_protocol}"
            )
    elif any((sasl_mechanism, sasl_username, sasl_password)):
        raise KafkaConfigError("KAFKA_SASL_* settings require a SASL security protocol")

    ssl_ca_location = _optional_text(source, "KAFKA_SSL_CA_LOCATION")
    if ssl_ca_location is not None and security_protocol not in {"SSL", "SASL_SSL"}:
        raise KafkaConfigError("KAFKA_SSL_CA_LOCATION requires SSL or SASL_SSL")

    return KafkaSettings(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        client_id=client_id,
        delivery_timeout_seconds=_delivery_timeout(source),
        security_protocol=security_protocol,
        sasl_mechanism=sasl_mechanism,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
        ssl_ca_location=ssl_ca_location,
    )


def _safe_error_detail(error: Any) -> str:
    detail = " ".join(str(error).split())[:500]
    return detail or type(error).__name__


class KafkaWeatherPublisher:
    """Synchronous delivery boundary around Kafka's asynchronous producer."""

    def __init__(
        self,
        settings: KafkaSettings,
        *,
        producer_factory: ProducerFactory = Producer,
    ) -> None:
        self._settings = settings
        try:
            self._producer = producer_factory(settings.producer_config())
        except KafkaException as exc:
            raise KafkaPublishError("could not initialize the Kafka producer") from exc

    def publish(self, events: Iterable[WeatherEvent]) -> int:
        """Publish all events and wait for broker delivery acknowledgements."""

        delivery_errors = []

        def on_delivery(error: Any, _message: Any) -> None:
            if error is not None:
                delivery_errors.append(_safe_error_detail(error))

        count = 0
        try:
            for event in events:
                self._producer.produce(
                    self._settings.topic,
                    key=event.region_id.encode("utf-8"),
                    value=event.to_json_bytes(),
                    on_delivery=on_delivery,
                )
                count += 1
                self._producer.poll(0)
        except (BufferError, KafkaException) as exc:
            raise KafkaPublishError(
                f"could not enqueue weather event {count + 1}: "
                f"{_safe_error_detail(exc)}"
            ) from exc

        try:
            remaining = self._producer.flush(self._settings.delivery_timeout_seconds)
        except KafkaException as exc:
            raise KafkaPublishError(
                f"Kafka flush failed: {_safe_error_detail(exc)}"
            ) from exc

        if remaining:
            raise KafkaPublishError(
                f"Kafka delivery timed out with {remaining} event(s) still pending"
            )
        if delivery_errors:
            raise KafkaPublishError(
                f"Kafka rejected {len(delivery_errors)} event(s): {delivery_errors[0]}"
            )
        return count


def publish_weather_events(
    events: Iterable[WeatherEvent],
    settings: KafkaSettings,
    *,
    producer_factory: ProducerFactory = Producer,
) -> int:
    """Create a producer, publish events, and return the delivered count."""

    return KafkaWeatherPublisher(
        settings,
        producer_factory=producer_factory,
    ).publish(events)
