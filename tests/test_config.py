from __future__ import annotations

import tomllib
from importlib.metadata import version
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

from parallax import __version__
from parallax.adapters import AdapterRegistry
from parallax.adapters.common.options import option_int
from parallax.config import (
    AppConfig,
    AuthConfig,
    CatalogError,
    HttpConfig,
    IngestionConfig,
    RedirectPolicyConfig,
    Source,
    SourceDefinition,
    load_catalog,
)
from source_factory import make_source

CONFIG_ROOT = Path("config/config.toml")
REQUIRED_DOCUMENTS = ("README.md",)


def _catalog():
    return load_catalog(Path(__file__).resolve().parents[1] / CONFIG_ROOT)


def test_required_documents_exist(project_root: Path):
    missing = [
        name for name in REQUIRED_DOCUMENTS if not (project_root / name).is_file()
    ]

    assert not missing, f"required repository documents missing: {missing}"


def test_checked_in_catalog_loads_and_uses_known_adapters() -> None:
    config = _catalog()
    registry = AdapterRegistry()
    for source in config.sources:
        registry.validate_source(source)


def test_unknown_source_kinds_are_rejected() -> None:
    payload = _source_payload(item_kind="article", stream_kind="unknown")

    with pytest.raises(ValidationError, match="stream_kind"):
        SourceDefinition.model_validate(payload)


@pytest.mark.parametrize(
    "overrides",
    [
        {"item_kind": "entity", "entity_kind": None},
        {"item_kind": "article", "entity_kind": "game"},
        {"item_kind": "post", "item_variant": "flash", "entity_kind": None},
        {"item_kind": "article", "entity_kind": None, "item_variant": "unknown"},
    ],
)
def test_conditional_ontology_rules_are_enforced(
    overrides: dict[str, object],
) -> None:
    payload = _source_payload(**overrides)

    with pytest.raises(ValidationError):
        SourceDefinition.model_validate(payload)


def test_entity_sources_require_an_entity_kind() -> None:
    payload = _source_payload(item_kind="entity", entity_kind="security")

    definition = SourceDefinition.model_validate(payload)
    assert definition.entity_kind == "security"


def test_surfaces_are_required() -> None:
    payload = _source_payload(surfaces=[])

    with pytest.raises(ValidationError, match="surfaces"):
        SourceDefinition.model_validate(payload)


def test_topics_must_be_lowercase_slugs() -> None:
    payload = _source_payload(topics=["Business"])

    with pytest.raises(ValidationError, match="lowercase slug"):
        SourceDefinition.model_validate(payload)


def test_unknown_configuration_fields_are_rejected() -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        AppConfig.model_validate({"log_levle": "INFO"})


def test_invalid_log_level_is_rejected() -> None:
    with pytest.raises(ValidationError, match="app.log_level"):
        AppConfig(log_level="verbose")


def test_ingestion_and_transport_defaults() -> None:
    ingestion = IngestionConfig()
    transport = HttpConfig()

    assert ingestion.max_concurrent_sources == 8
    assert ingestion.retry_base_seconds == 30
    assert ingestion.retry_max_seconds == 1800
    assert transport.max_connections_per_host == 2
    assert transport.max_attempts == 2
    assert transport.retry_backoff_seconds == 0.5


@pytest.mark.parametrize(
    "config",
    [
        {"max_concurrent_sources": 0},
        {"retry_base_seconds": 31, "retry_max_seconds": 30},
    ],
)
def test_ingestion_rejects_invalid_bounds(config: dict[str, int]) -> None:
    with pytest.raises(ValidationError):
        IngestionConfig.model_validate(config)


@pytest.mark.parametrize(
    "host",
    [
        "*.example.test",
        "https://example.test",
        "example.test/path",
        "user@example.test",
        "example.test:443",
    ],
)
def test_redirect_policy_rejects_non_hostname_hosts(host: str) -> None:
    with pytest.raises(ValidationError, match="exact hostnames"):
        RedirectPolicyConfig(allowed_hosts=(host,))


def test_redirect_policy_normalizes_exact_hosts() -> None:
    policy = RedirectPolicyConfig(
        allowed_hosts=("WWW.Example.TEST", "publisher.example.test"),
        allow_https_downgrade=True,
    )

    assert policy.allowed_hosts == ("www.example.test", "publisher.example.test")
    assert policy.allow_https_downgrade is True


def test_source_redirect_defaults_to_no_cross_authority_or_downgrade() -> None:
    source = make_source()

    assert source.redirect.allowed_hosts == ()
    assert source.redirect.allow_https_downgrade is False


def test_registry_rejects_unknown_adapter_and_options() -> None:
    registry = AdapterRegistry()
    source = make_source(options={"history_max_page": 10})

    with pytest.raises(ValueError, match="Unknown options.*history_max_page"):
        registry.validate_source(source)

    missing = make_source(adapter="missing")
    with pytest.raises(KeyError, match="Unknown adapter"):
        registry.validate_source(missing)


@pytest.mark.parametrize("value", [True, 1.5, "5", 0, -1])
def test_integer_options_reject_invalid_or_coerced_values(value: object) -> None:
    source = make_source(options={"history_max_pages": value})

    with pytest.raises(ValueError, match="must be"):
        option_int(source, "history_max_pages", default=10)


def test_auth_requires_environment_backed_secret() -> None:
    with pytest.raises(ValidationError, match="auth.env_var"):
        AuthConfig(kind="bearer")

    with pytest.raises(ValidationError, match="auth.name"):
        AuthConfig(kind="header", env_var="PARALLAX_TOKEN")


def test_release_metadata_matches_installed_package(project_root: Path) -> None:
    with (project_root / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)

    expected = pyproject["project"]["version"]

    assert isinstance(expected, str)
    assert __version__ == expected
    assert version("parallax") == expected


def test_root_manifest_contains_only_package_settings() -> None:
    with CONFIG_ROOT.open("rb") as handle:
        raw = tomllib.load(handle)

    assert raw["schema_version"] == 0
    assert set(raw) == {
        "schema_version",
        "app",
        "http",
        "ingestion",
        "scheduler",
        "validation",
    }


def _source_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": "fixture",
        "provider_id": "fixture",
        "provider_name": "Fixture Provider",
        "provider_kind": "publisher",
        "channel_id": "main",
        "channel_label": "Fixture",
        "channel_role": "aggregate",
        "stream_kind": "latest",
        "item_kind": "article",
        "topics": [],
        "surfaces": ["news"],
        "language": "en-GB",
        "market": "GB",
        "interval_seconds": 1800,
        "max_items": 100,
        "endpoint": {
            "adapter": "rss",
            "url": "https://example.test/feed.xml",
        },
    }
    payload.update(overrides)
    return payload


def test_source_definition_rejects_non_http_endpoint_urls() -> None:
    payload = _source_payload(
        endpoint={"adapter": "rss", "url": "ftp://example.test/feed.xml"}
    )

    with pytest.raises(ValidationError, match="absolute HTTP"):
        SourceDefinition.model_validate(payload)


def test_source_definition_rejects_invalid_language_tags() -> None:
    payload = _source_payload(language="english")

    with pytest.raises(ValidationError, match="BCP-47"):
        SourceDefinition.model_validate(payload)


def test_source_definition_rejects_invalid_market_codes() -> None:
    payload = _source_payload(market="gb")

    with pytest.raises(ValidationError, match="uppercase"):
        SourceDefinition.model_validate(payload)


def _write_toml(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _write_root(
    base: Path,
    *,
    schema_version: int = 0,
) -> Path:
    _write_toml(
        base / "config.toml",
        f"schema_version = {schema_version}\n",
    )
    return base / "config.toml"


def _source_toml(
    source_id: str,
    *,
    url: str = "https://example.test/a.xml",
    provider_name: str = "Fixture Provider",
) -> str:
    return (
        "[[sources]]\n"
        f'id = "{source_id}"\n'
        'provider_id = "fixture"\n'
        f'provider_name = "{provider_name}"\n'
        'provider_kind = "publisher"\n'
        'channel_id = "main"\n'
        'channel_label = "Main"\n'
        'channel_role = "aggregate"\n'
        'stream_kind = "latest"\n'
        'item_kind = "article"\n'
        'topics = ["business"]\n'
        'surfaces = ["news"]\n'
        'language = "en-GB"\n'
        'market = "GB"\n'
        "interval_seconds = 1800\n"
        "max_items = 100\n"
        "[sources.endpoint]\n"
        'adapter = "rss"\n'
        f'url = "{url}"\n'
    )


def test_loader_discovers_and_sorts_sources_without_path_semantics(
    tmp_path: Path,
) -> None:
    base = tmp_path / "catalog"
    _write_root(base)
    _write_toml(base / "sources" / "top.toml", _source_toml("beta"))
    _write_toml(
        base / "sources" / "arbitrary" / "nested" / "source.toml",
        _source_toml("alpha"),
    )

    config = load_catalog(base / "config.toml")

    assert [source.id for source in config.sources] == ["alpha", "beta"]
    assert config.source("alpha").defined_in == "arbitrary/nested/source.toml"


def test_loader_rejects_duplicate_ids(tmp_path: Path) -> None:
    base = tmp_path / "catalog"
    _write_root(base)
    _write_toml(base / "sources" / "a.toml", _source_toml("alpha"))
    _write_toml(base / "sources" / "b.toml", _source_toml("alpha"))

    with pytest.raises(CatalogError, match="duplicate source id"):
        load_catalog(base / "config.toml")


def test_loader_rejects_inconsistent_provider_metadata(tmp_path: Path) -> None:
    base = tmp_path / "catalog"
    _write_root(base)
    _write_toml(base / "sources" / "a.toml", _source_toml("alpha"))
    _write_toml(
        base / "sources" / "b.toml",
        _source_toml("beta", provider_name="Other Provider"),
    )

    with pytest.raises(CatalogError, match="declared inconsistently"):
        load_catalog(base / "config.toml")


def test_loader_rejects_wrong_schema_version(tmp_path: Path) -> None:
    base = tmp_path / "catalog"
    _write_root(base, schema_version=1)

    with pytest.raises(CatalogError, match="schema_version"):
        load_catalog(base / "config.toml")


def test_loader_rejects_recursive_includes(tmp_path: Path) -> None:
    base = tmp_path / "catalog"
    _write_root(base)
    _write_toml(
        base / "sources" / "a.toml",
        'include = ["b.toml"]\n' + _source_toml("alpha"),
    )

    with pytest.raises(CatalogError, match="extra_forbidden"):
        load_catalog(base / "config.toml")


def test_loader_requires_a_sources_directory(tmp_path: Path) -> None:
    base = tmp_path / "catalog"
    _write_root(base)

    with pytest.raises(CatalogError, match="sources directory"):
        load_catalog(base / "config.toml")


def test_loader_resolves_relative_app_paths_from_root(tmp_path: Path) -> None:
    base = tmp_path / "catalog"
    _write_root(base)
    _write_toml(base / "sources" / "a.toml", _source_toml("alpha"))

    config = load_catalog(base / "config.toml")

    assert config.app.database_path == (base / "data/parallax.db").resolve()


def test_loaded_sources_are_frozen() -> None:
    config = _catalog()
    source = config.source("ft-home")

    with pytest.raises(ValidationError):
        source.enabled = False

    with pytest.raises(ValidationError):
        config.http.max_attempts = 99

    with pytest.raises(TypeError):
        cast(Any, source.headers)["X-Test"] = "value"

    with pytest.raises(TypeError):
        cast(Any, source.endpoint.options)["test"] = True

    assert isinstance(source, Source)
