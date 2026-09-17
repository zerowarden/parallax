from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AppConfig(ConfigModel):
    database_path: Path = Path("data/parallax.db")
    log_path: Path = Path("data/parallax.log")
    log_level: str = "INFO"

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
            raise ValueError(
                "app.log_level must be CRITICAL, ERROR, WARNING, INFO, or DEBUG"
            )
        return normalized


class HttpConfig(ConfigModel):
    user_agent: str = "parallax/0.1"
    connect_timeout_seconds: float = Field(default=5.0, gt=0)
    read_timeout_seconds: float = Field(default=15.0, gt=0)
    write_timeout_seconds: float = Field(default=10.0, gt=0)
    pool_timeout_seconds: float = Field(default=5.0, gt=0)
    max_connections: int = Field(default=10, ge=1)
    max_keepalive_connections: int = Field(default=5, ge=0)
    max_connections_per_host: int = Field(default=2, ge=1)
    max_response_bytes: int = Field(default=2 * 1024 * 1024, ge=1024)
    follow_redirects: bool = True
    max_attempts: int = Field(default=2, ge=1)
    retry_backoff_seconds: float = Field(default=0.5, ge=0)


class SchedulerConfig(ConfigModel):
    loop_sleep_seconds: float = Field(default=2.0, gt=0)


class IngestionConfig(ConfigModel):
    max_concurrent_sources: int = Field(default=8, ge=1)
    retry_base_seconds: int = Field(default=30, ge=1)
    retry_max_seconds: int = Field(default=1800, ge=1)

    @model_validator(mode="after")
    def validate_retry_bounds(self) -> IngestionConfig:
        if self.retry_max_seconds < self.retry_base_seconds:
            raise ValueError("retry_max_seconds must be >= retry_base_seconds")
        return self


class ValidationConfig(ConfigModel):
    max_title_length: int = Field(default=1000, ge=1)
    allow_empty_batches: bool = False
    max_future_seconds: int = Field(default=7200, ge=0)


class AuthConfig(ConfigModel):
    kind: Literal["none", "header", "bearer", "query"] = "none"
    env_var: str | None = None
    name: str | None = None
    prefix: str = "Bearer"

    @model_validator(mode="after")
    def validate_auth(self) -> AuthConfig:
        if self.kind == "none":
            return self
        if not self.env_var:
            raise ValueError("auth.env_var is required for authenticated sources")
        if self.kind in {"header", "query"} and not self.name:
            raise ValueError(f"auth.name is required for auth kind {self.kind}")
        return self

    def resolve_secret(self) -> str | None:
        if self.kind == "none":
            return None
        assert self.env_var is not None
        value = os.getenv(self.env_var)
        if not value:
            raise RuntimeError(
                f"Required environment variable {self.env_var!r} is not set"
            )
        return value


class RedirectPolicyConfig(ConfigModel):
    allowed_hosts: tuple[str, ...] = ()
    allow_https_downgrade: bool = False

    @field_validator("allowed_hosts")
    @classmethod
    def validate_allowed_hosts(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized: list[str] = []
        for host in value:
            if not isinstance(host, str):
                raise ValueError("redirect.allowed_hosts entries must be hostnames")
            candidate = host.strip()
            if candidate != host or not candidate or "://" in candidate:
                raise ValueError(
                    "redirect.allowed_hosts entries must be exact hostnames"
                )
            if any(character in candidate for character in "*/?#@"):
                raise ValueError(
                    "redirect.allowed_hosts entries must be exact hostnames"
                )
            split = urlsplit(f"//{candidate}")
            try:
                port = split.port
            except ValueError as exc:
                raise ValueError(
                    "redirect.allowed_hosts entries must be exact hostnames"
                ) from exc
            if (
                split.hostname is None
                or split.username is not None
                or split.password is not None
                or port is not None
                or split.path
            ):
                raise ValueError(
                    "redirect.allowed_hosts entries must be exact hostnames"
                )
            normalized.append(split.hostname.lower())
        return tuple(normalized)


class SourceConfig(ConfigModel):
    id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9-]*$")
    name: str = Field(min_length=1)
    region: str = Field(min_length=2)
    language: str = Field(min_length=2)
    adapter: str = Field(min_length=1)
    url: str = Field(min_length=1)
    stream_kind: str = Field(default="latest", min_length=1)
    item_kind: str = Field(default="article", min_length=1)
    retrieval_method: str = Field(default="unknown", min_length=1)
    enabled: bool = True
    schedule_seconds: int = Field(default=900, ge=30)
    headers: dict[str, str] = Field(default_factory=dict)
    options: dict[str, Any] = Field(default_factory=dict)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    redirect: RedirectPolicyConfig = Field(default_factory=RedirectPolicyConfig)

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        try:
            split = urlsplit(value)
            _ = split.port
        except ValueError as exc:
            raise ValueError("source URL contains an invalid port") from exc
        if split.scheme not in {"http", "https"} or not split.hostname:
            raise ValueError("source URL must be an absolute HTTP(S) URL")
        return value


class Settings(ConfigModel):
    app: AppConfig = Field(default_factory=AppConfig)
    http: HttpConfig = Field(default_factory=HttpConfig)
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    sources: list[SourceConfig]

    @model_validator(mode="after")
    def validate_sources(self) -> Settings:
        seen: set[str] = set()
        duplicates: set[str] = set()
        for source in self.sources:
            if source.id in seen:
                duplicates.add(source.id)
            seen.add(source.id)
        if duplicates:
            joined = ", ".join(sorted(duplicates))
            raise ValueError(f"Duplicate source ids: {joined}")
        return self


def load_settings(config_path: Path) -> Settings:
    config_path = config_path.expanduser().resolve()
    with config_path.open("rb") as handle:
        raw = tomllib.load(handle)

    settings = Settings.model_validate(raw)
    base_dir = config_path.parent
    settings.app.database_path = _resolve_path(base_dir, settings.app.database_path)
    settings.app.log_path = _resolve_path(base_dir, settings.app.log_path)
    return settings


def _resolve_path(base_dir: Path, path: Path) -> Path:
    path = path.expanduser()
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()
