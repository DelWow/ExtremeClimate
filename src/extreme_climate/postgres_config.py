"""Load shared PostgreSQL connection settings from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional


class PostgresConfigError(ValueError):
    """Raised when PostgreSQL environment configuration is invalid."""


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


def _required_text(
    environ: Mapping[str, str], name: str, default: str
) -> str:
    value = environ.get(name, default).strip()
    if not value:
        raise PostgresConfigError(f"{name} must not be empty")
    return value


def _positive_integer(
    environ: Mapping[str, str], name: str, default: int
) -> int:
    raw_value = environ.get(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise PostgresConfigError(f"{name} must be an integer") from exc
    if value <= 0:
        raise PostgresConfigError(f"{name} must be positive")
    return value


def load_postgres_settings(
    environ: Optional[Mapping[str, str]] = None,
) -> PostgresSettings:
    """Load validated PostgreSQL connection settings."""

    source = os.environ if environ is None else environ
    port = _positive_integer(source, "POSTGRES_PORT", 5433)
    if port > 65535:
        raise PostgresConfigError("POSTGRES_PORT must be at most 65535")

    return PostgresSettings(
        host=_required_text(source, "POSTGRES_HOST", "127.0.0.1"),
        port=port,
        dbname=_required_text(source, "POSTGRES_DB", "extreme_climate"),
        user=_required_text(source, "POSTGRES_USER", "extreme_climate"),
        password=_required_text(
            source,
            "POSTGRES_PASSWORD",
            "extreme_climate_dev",
        ),
        connect_timeout_seconds=_positive_integer(
            source,
            "POSTGRES_CONNECT_TIMEOUT_SECONDS",
            10,
        ),
    )
