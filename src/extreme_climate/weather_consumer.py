"""Consume normalized weather events from Kafka and persist them safely."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    Dict,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    TextIO,
    Tuple,
)

import psycopg
from confluent_kafka import Consumer, KafkaException

from extreme_climate.kafka_publisher import (
    KafkaConfigError,
    KafkaSettings,
    load_kafka_settings,
)


DEFAULT_CONSUMER_GROUP_ID = "extreme-climate-raw-weather"
DEFAULT_CONSUMER_CLIENT_ID = "extreme-climate-weather-consumer"
DEFAULT_POLL_TIMEOUT_SECONDS = 1.0

_EVENT_FIELDS = frozenset(
    {
        "region_id",
        "observed_at",
        "temperature_c",
        "humidity_percent",
        "precipitation_mm",
        "wind_speed_mps",
        "source_payload",
    }
)

_INSERT_RAW_WEATHER_SQL = """
    INSERT INTO raw_weather (
        region_id,
        observed_at,
        temperature_c,
        humidity_percent,
        precipitation_mm,
        wind_speed_mps,
        source_payload,
        kafka_topic,
        kafka_partition,
        kafka_offset
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
    ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
    RETURNING raw_weather_id
"""


class ConsumerConfigError(ValueError):
    """Raised when consumer or PostgreSQL configuration is invalid."""


class EventDeserializationError(ValueError):
    """Raised when a Kafka value does not match the weather event contract."""


class WeatherPersistenceError(RuntimeError):
    """Raised when PostgreSQL cannot persist a weather event."""


class WeatherConsumerError(RuntimeError):
    """Raised when polling, processing, or committing a Kafka record fails."""


@dataclass(frozen=True)
class RawWeatherEvent:
    """A deserialized weather event ready for a parameterized SQL insert."""

    region_id: str
    observed_at: datetime
    temperature_c: float
    humidity_percent: Optional[float]
    precipitation_mm: Optional[float]
    wind_speed_mps: Optional[float]
    source_payload: Mapping[str, Any]


@dataclass(frozen=True)
class KafkaPosition:
    """The immutable source position used as the database deduplication key."""

    topic: str
    partition: int
    offset: int


@dataclass(frozen=True)
class PostgresSettings:
    """PostgreSQL connection settings sourced from the environment."""

    host: str
    port: int
    dbname: str
    user: str
    password: str = field(repr=False)
    connect_timeout_seconds: int = 10

    def connection_kwargs(self) -> Dict[str, Any]:
        """Return psycopg connection parameters; callers must not log them."""

        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "connect_timeout": self.connect_timeout_seconds,
        }


@dataclass(frozen=True)
class WeatherConsumerSettings:
    """Validated settings for one consumer process."""

    kafka: KafkaSettings
    postgres: PostgresSettings
    group_id: str
    client_id: str
    auto_offset_reset: str
    poll_timeout_seconds: float

    def consumer_config(self) -> Dict[str, Any]:
        """Return a manual-commit, at-least-once consumer configuration."""

        config = self.kafka.connection_config()
        config.update(
            {
                "group.id": self.group_id,
                "client.id": self.client_id,
                "auto.offset.reset": self.auto_offset_reset,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
            }
        )
        return config


class MessageProtocol(Protocol):
    def topic(self) -> str:
        ...

    def partition(self) -> int:
        ...

    def offset(self) -> int:
        ...

    def key(self) -> Optional[bytes]:
        ...

    def value(self) -> Optional[bytes]:
        ...

    def error(self) -> Any:
        ...


class ConsumerProtocol(Protocol):
    def subscribe(self, topics: Sequence[str]) -> None:
        ...

    def poll(self, timeout: float) -> Optional[MessageProtocol]:
        ...

    def commit(
        self, *, message: MessageProtocol, asynchronous: bool
    ) -> Any:
        ...

    def close(self) -> None:
        ...


class CursorProtocol(Protocol):
    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        ...


class ConnectionProtocol(Protocol):
    def transaction(self) -> Any:
        ...

    def execute(self, query: str, params: Sequence[Any]) -> CursorProtocol:
        ...

    def close(self) -> None:
        ...


ConsumerFactory = Callable[[Mapping[str, Any]], ConsumerProtocol]
ConnectionFactory = Callable[..., ConnectionProtocol]


def _required_environment_text(
    environ: Mapping[str, str], name: str, default: str
) -> str:
    value = environ.get(name, default).strip()
    if not value:
        raise ConsumerConfigError(f"{name} must not be empty")
    return value


def _positive_float(environ: Mapping[str, str], name: str, default: float) -> float:
    raw_value = environ.get(name, str(default))
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ConsumerConfigError(f"{name} must be a number") from exc
    if not math.isfinite(value) or value <= 0:
        raise ConsumerConfigError(f"{name} must be a positive finite number")
    return value


def _positive_integer(environ: Mapping[str, str], name: str, default: int) -> int:
    raw_value = environ.get(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ConsumerConfigError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ConsumerConfigError(f"{name} must be positive")
    return value


def load_weather_consumer_settings(
    environ: Optional[Mapping[str, str]] = None,
) -> WeatherConsumerSettings:
    """Load Kafka consumer and PostgreSQL settings from the environment."""

    source = os.environ if environ is None else environ
    kafka = load_kafka_settings(source)
    auto_offset_reset = _required_environment_text(
        source,
        "KAFKA_AUTO_OFFSET_RESET",
        "earliest",
    ).lower()
    if auto_offset_reset not in {"earliest", "latest", "error"}:
        raise ConsumerConfigError(
            "KAFKA_AUTO_OFFSET_RESET must be earliest, latest, or error"
        )

    postgres_port = _positive_integer(source, "POSTGRES_PORT", 5433)
    if postgres_port > 65535:
        raise ConsumerConfigError("POSTGRES_PORT must be at most 65535")

    return WeatherConsumerSettings(
        kafka=kafka,
        postgres=PostgresSettings(
            host=_required_environment_text(
                source, "POSTGRES_HOST", "127.0.0.1"
            ),
            port=postgres_port,
            dbname=_required_environment_text(
                source, "POSTGRES_DB", "extreme_climate"
            ),
            user=_required_environment_text(
                source, "POSTGRES_USER", "extreme_climate"
            ),
            password=_required_environment_text(
                source, "POSTGRES_PASSWORD", "extreme_climate_dev"
            ),
            connect_timeout_seconds=_positive_integer(
                source, "POSTGRES_CONNECT_TIMEOUT_SECONDS", 10
            ),
        ),
        group_id=_required_environment_text(
            source, "KAFKA_CONSUMER_GROUP_ID", DEFAULT_CONSUMER_GROUP_ID
        ),
        client_id=_required_environment_text(
            source, "KAFKA_CONSUMER_CLIENT_ID", DEFAULT_CONSUMER_CLIENT_ID
        ),
        auto_offset_reset=auto_offset_reset,
        poll_timeout_seconds=_positive_float(
            source, "KAFKA_POLL_TIMEOUT_SECONDS", DEFAULT_POLL_TIMEOUT_SECONDS
        ),
    )


def _json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number {value}")


def _number(
    payload: Mapping[str, Any], field_name: str, *, required: bool
) -> Optional[float]:
    value = payload[field_name]
    if value is None:
        if required:
            raise EventDeserializationError(f"{field_name} must not be null")
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EventDeserializationError(f"{field_name} must be a number or null")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise EventDeserializationError(f"{field_name} must be finite") from exc
    if not math.isfinite(number):
        raise EventDeserializationError(f"{field_name} must be finite")
    return number


def _observed_at(value: Any) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise EventDeserializationError(
            "observed_at must be an RFC 3339 UTC timestamp ending in Z"
        )
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise EventDeserializationError(
            "observed_at must be a valid RFC 3339 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EventDeserializationError("observed_at must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def deserialize_weather_event(value: Optional[bytes]) -> RawWeatherEvent:
    """Strictly deserialize one UTF-8 JSON weather event."""

    if value is None:
        raise EventDeserializationError("message value must not be null")
    if not isinstance(value, bytes):
        raise EventDeserializationError("message value must be UTF-8 bytes")
    try:
        payload = json.loads(value.decode("utf-8"), parse_constant=_json_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise EventDeserializationError(
            "message value must be valid finite JSON encoded as UTF-8"
        ) from exc
    if not isinstance(payload, Mapping):
        raise EventDeserializationError("weather event must be a JSON object")

    actual_fields = set(payload)
    missing = _EVENT_FIELDS - actual_fields
    unknown = actual_fields - _EVENT_FIELDS
    if missing:
        raise EventDeserializationError(
            "weather event is missing field(s): "
            + ", ".join(sorted(missing))
        )
    if unknown:
        raise EventDeserializationError(
            "weather event has unknown field(s): "
            + ", ".join(sorted(unknown))
        )

    region_id = payload["region_id"]
    if (
        not isinstance(region_id, str)
        or not region_id
        or region_id != region_id.strip()
        or len(region_id) > 64
    ):
        raise EventDeserializationError(
            "region_id must be a non-empty trimmed string of at most 64 characters"
        )
    source_payload = payload["source_payload"]
    if not isinstance(source_payload, Mapping):
        raise EventDeserializationError("source_payload must be a JSON object")

    temperature_c = _number(payload, "temperature_c", required=True)
    assert temperature_c is not None
    return RawWeatherEvent(
        region_id=region_id,
        observed_at=_observed_at(payload["observed_at"]),
        temperature_c=temperature_c,
        humidity_percent=_number(payload, "humidity_percent", required=False),
        precipitation_mm=_number(payload, "precipitation_mm", required=False),
        wind_speed_mps=_number(payload, "wind_speed_mps", required=False),
        source_payload=source_payload,
    )


class RawWeatherStore:
    """Persist raw observations with Kafka-position idempotency.

    Only a replay of the exact topic/partition/offset is a duplicate. An equal
    event published at a different offset remains a distinct stream record.
    """

    def __init__(self, connection: ConnectionProtocol) -> None:
        self._connection = connection

    def insert(self, event: RawWeatherEvent, position: KafkaPosition) -> bool:
        """Insert one row, returning false when its Kafka position exists."""

        parameters = (
            event.region_id,
            event.observed_at,
            event.temperature_c,
            event.humidity_percent,
            event.precipitation_mm,
            event.wind_speed_mps,
            json.dumps(event.source_payload, allow_nan=False, separators=(",", ":")),
            position.topic,
            position.partition,
            position.offset,
        )
        try:
            with self._connection.transaction():
                cursor = self._connection.execute(
                    _INSERT_RAW_WEATHER_SQL,
                    parameters,
                )
                inserted = cursor.fetchone() is not None
        except psycopg.Error as exc:
            raise WeatherPersistenceError(
                "PostgreSQL could not persist the weather event"
            ) from exc
        return inserted


@dataclass(frozen=True)
class ProcessedMessage:
    """Outcome of one database-persisted and offset-committed message."""

    position: KafkaPosition
    inserted: bool


def _position(message: MessageProtocol) -> KafkaPosition:
    topic = message.topic()
    partition = message.partition()
    offset = message.offset()
    if (
        not isinstance(topic, str)
        or not topic
        or not isinstance(partition, int)
        or partition < 0
        or not isinstance(offset, int)
        or offset < 0
    ):
        raise WeatherConsumerError("Kafka message has an invalid source position")
    return KafkaPosition(topic=topic, partition=partition, offset=offset)


def _check_message_key(message: MessageProtocol, region_id: str) -> None:
    key = message.key()
    if key is None:
        raise EventDeserializationError("message key must contain region_id")
    try:
        decoded_key = key.decode("utf-8")
    except (AttributeError, UnicodeDecodeError) as exc:
        raise EventDeserializationError(
            "message key must be region_id encoded as UTF-8"
        ) from exc
    if decoded_key != region_id:
        raise EventDeserializationError(
            "message key does not match the event region_id"
        )


def _commit_message(consumer: ConsumerProtocol, message: MessageProtocol) -> None:
    try:
        committed = consumer.commit(message=message, asynchronous=False)
    except KafkaException as exc:
        raise WeatherConsumerError("Kafka offset commit failed") from exc
    if committed is not None:
        for topic_partition in committed:
            error = getattr(topic_partition, "error", None)
            if error is not None:
                raise WeatherConsumerError("Kafka offset commit was rejected")


def process_message(
    consumer: ConsumerProtocol,
    store: RawWeatherStore,
    message: MessageProtocol,
) -> ProcessedMessage:
    """Persist one message transactionally, then synchronously commit offset.

    A database failure or malformed record leaves the offset uncommitted. A
    crash between the database and Kafka commits replays the same position;
    the unique database index turns that replay into a duplicate no-op.
    """

    message_error = message.error()
    if message_error is not None:
        raise WeatherConsumerError("Kafka polling returned a message error")

    position = _position(message)
    try:
        event = deserialize_weather_event(message.value())
        _check_message_key(message, event.region_id)
    except EventDeserializationError as exc:
        raise WeatherConsumerError(
            f"invalid weather event at {position.topic}[{position.partition}] "
            f"offset {position.offset}: {exc}"
        ) from exc

    inserted = store.insert(event, position)
    _commit_message(consumer, message)
    return ProcessedMessage(position=position, inserted=inserted)


def consume_weather_events(
    settings: WeatherConsumerSettings,
    *,
    max_messages: Optional[int] = None,
    output: Optional[TextIO] = None,
    consumer_factory: ConsumerFactory = Consumer,
    connection_factory: ConnectionFactory = psycopg.connect,
) -> int:
    """Consume until interrupted or until ``max_messages`` have committed."""

    try:
        consumer = consumer_factory(settings.consumer_config())
    except KafkaException as exc:
        raise WeatherConsumerError("could not initialize Kafka consumer") from exc
    try:
        connection = connection_factory(**settings.postgres.connection_kwargs())
    except psycopg.Error as exc:
        consumer.close()
        raise WeatherPersistenceError("could not connect to PostgreSQL") from exc

    destination = sys.stdout if output is None else output
    processed = 0
    try:
        try:
            consumer.subscribe([settings.kafka.topic])
        except KafkaException as exc:
            raise WeatherConsumerError("Kafka subscription failed") from exc
        store = RawWeatherStore(connection)
        while max_messages is None or processed < max_messages:
            try:
                message = consumer.poll(settings.poll_timeout_seconds)
            except KafkaException as exc:
                raise WeatherConsumerError("Kafka poll failed") from exc
            if message is None:
                continue
            result = process_message(consumer, store, message)
            action = "inserted" if result.inserted else "duplicate"
            print(
                f"{action} {result.position.topic}"
                f"[{result.position.partition}]@{result.position.offset}",
                file=destination,
            )
            processed += 1
    finally:
        try:
            connection.close()
        finally:
            consumer.close()
    return processed


def _positive_message_count(value: str) -> int:
    try:
        count = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if count <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return count


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Run the raw-weather Kafka consumer."""

    parser = argparse.ArgumentParser(
        description="Consume weather events from Kafka into PostgreSQL."
    )
    parser.add_argument(
        "--max-messages",
        type=_positive_message_count,
        help="exit after this many committed messages (default: run forever)",
    )
    args = parser.parse_args(argv)

    try:
        settings = load_weather_consumer_settings()
        consume_weather_events(settings, max_messages=args.max_messages)
    except (KafkaConfigError, ConsumerConfigError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except (WeatherConsumerError, WeatherPersistenceError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("stopped", file=sys.stderr)
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
