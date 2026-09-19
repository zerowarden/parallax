from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Literal
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

from parallax.config.validation import (
    validate_language_tag,
    validate_market,
    validate_topic_id,
)
from parallax.domain import (
    BrowseSurface,
    ChannelRole,
    EntityKind,
    ItemKind,
    ItemVariant,
    ProviderKind,
    StreamKind,
    validate_item_classification,
)

SOURCE_ID_PATTERN = r"^[a-z0-9][a-z0-9-]*$"
type OptionValue = str | int | float | bool


def _freeze_mapping[Key, Value](
    value: Mapping[Key, Value],
) -> Mapping[Key, Value]:
    return MappingProxyType(dict(value))


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


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
    user_agent: str = "parallax/0"
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


def validate_http_url(value: str) -> str:
    try:
        split = urlsplit(value)
        _ = split.port
    except ValueError as exc:
        raise ValueError("URL contains an invalid port") from exc
    if split.scheme not in {"http", "https"} or not split.hostname:
        raise ValueError("URL must be an absolute HTTP(S) URL")
    return value


class Endpoint(ConfigModel):
    adapter: str = Field(min_length=1)
    url: str = Field(min_length=1)
    options: Mapping[str, OptionValue] = Field(default_factory=dict)

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        return validate_http_url(value)

    @field_validator("options")
    @classmethod
    def freeze_options(
        cls, value: Mapping[str, OptionValue]
    ) -> Mapping[str, OptionValue]:
        return _freeze_mapping(value)

    @field_serializer("options")
    def serialize_options(
        self, value: Mapping[str, OptionValue]
    ) -> dict[str, OptionValue]:
        return dict(value)


class SourceDefinition(ConfigModel):
    id: str = Field(min_length=1, pattern=SOURCE_ID_PATTERN)
    provider_id: str = Field(min_length=1, pattern=SOURCE_ID_PATTERN)
    provider_name: str = Field(min_length=1)
    provider_kind: ProviderKind

    channel_id: str = Field(min_length=1)
    channel_label: str = Field(min_length=1)
    channel_role: ChannelRole

    stream_kind: StreamKind

    item_kind: ItemKind
    entity_kind: EntityKind | None = None
    item_variant: ItemVariant | None = None

    topics: tuple[str, ...] = ()
    surfaces: frozenset[BrowseSurface] = Field(min_length=1)

    language: str = Field(min_length=2)
    market: str = Field(min_length=2)

    interval_seconds: int = Field(ge=30)
    max_items: int = Field(ge=1)
    enabled: bool = True

    headers: Mapping[str, str] = Field(default_factory=dict)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    redirect: RedirectPolicyConfig = Field(default_factory=RedirectPolicyConfig)

    endpoint: Endpoint

    @field_validator("headers")
    @classmethod
    def freeze_headers(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        return _freeze_mapping(value)

    @field_serializer("headers")
    def serialize_headers(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @model_validator(mode="after")
    def validate_classification(self) -> SourceDefinition:
        validate_item_classification(
            item_kind=self.item_kind,
            entity_kind=self.entity_kind,
            item_variant=self.item_variant,
        )
        return self

    @field_validator("topics")
    @classmethod
    def validate_topics(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(validate_topic_id(topic) for topic in value)

    @field_serializer("topics")
    def serialize_topics(self, value: tuple[str, ...]) -> list[str]:
        return sorted(value)

    @field_serializer("surfaces")
    def serialize_surfaces(
        self, value: frozenset[BrowseSurface]
    ) -> list[BrowseSurface]:
        return sorted(value)

    @field_validator("language")
    @classmethod
    def validate_source_language(cls, value: str) -> str:
        return validate_language_tag(value)

    @field_validator("market")
    @classmethod
    def validate_source_market(cls, value: str) -> str:
        return validate_market(value)


class Source(SourceDefinition):
    defined_in: str = Field(default="", exclude=True)

    @classmethod
    def from_definition(
        cls,
        definition: SourceDefinition,
        *,
        defined_in: str,
    ) -> Source:
        return cls.model_validate(
            {**definition.model_dump(mode="python"), "defined_in": defined_in}
        )


class CatalogRoot(ConfigModel):
    schema_version: int
    app: AppConfig = Field(default_factory=AppConfig)
    http: HttpConfig = Field(default_factory=HttpConfig)
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)


class SourceFile(ConfigModel):
    sources: tuple[SourceDefinition, ...] = ()
