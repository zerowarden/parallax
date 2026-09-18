from __future__ import annotations

import pytest

from parallax.normalization import (
    canonicalize_url,
    normalize_title_for_version,
    normalize_url_for_identity,
)


def test_identity_normalization_lowercases_host_and_drops_default_port() -> None:
    assert (
        normalize_url_for_identity("HTTPS://EXAMPLE.TEST:443/story")
        == "https://example.test/story"
    )


def test_identity_normalization_drops_fragment_and_surrounding_whitespace() -> None:
    assert (
        normalize_url_for_identity("  https://example.test/story#section  ")
        == "https://example.test/story"
    )


def test_identity_normalization_keeps_non_default_port() -> None:
    assert (
        normalize_url_for_identity("https://example.test:8443/story")
        == "https://example.test:8443/story"
    )


def test_identity_normalization_preserves_full_query() -> None:
    url = "https://example.test/story?utm_source=a&utm_source=b&q=1&q=2&empty="
    assert normalize_url_for_identity(url) == url


@pytest.mark.parametrize(
    "key",
    [
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "fbclid",
        "gclid",
    ],
)
def test_canonical_url_removes_each_tracking_key(key: str) -> None:
    assert (
        canonicalize_url(f"https://example.test/story?{key}=value")
        == "https://example.test/story"
    )


def test_canonical_url_removes_tracking_keys_case_insensitively() -> None:
    assert (
        canonicalize_url("https://example.test/story?UTM_Source=x&FBCLID=y")
        == "https://example.test/story"
    )


def test_canonical_url_keeps_tracking_key_lookalikes() -> None:
    assert (
        canonicalize_url("https://example.test/story?utm_sourc=x&fbclid2=y")
        == "https://example.test/story?utm_sourc=x&fbclid2=y"
    )


def test_canonical_url_preserves_semantic_and_unrecognized_query_keys() -> None:
    url = "https://example.test/story?q=topic&wd=term&ref=home&rss=1"
    assert canonicalize_url(url) == url


def test_canonical_url_preserves_blank_and_repeated_parameters() -> None:
    url = "https://example.test/story?empty=&a=1&a=2&b="
    assert canonicalize_url(url) == url


def test_canonical_url_preserves_unrecognized_component_encoding() -> None:
    url = "https://example.test/story?q=hello%20world&flag&empty="
    assert canonicalize_url(url) == url


def test_canonical_url_keeps_order_after_removing_tracking_key() -> None:
    assert (
        canonicalize_url("https://example.test/story?b=2&utm_source=x&a=1&a=3&b=4")
        == "https://example.test/story?b=2&a=1&a=3&b=4"
    )


def test_canonical_url_removes_every_tracking_occurrence() -> None:
    assert (
        canonicalize_url("https://example.test/story?utm_source=a&utm_source=b")
        == "https://example.test/story"
    )


def test_title_normalization_collapses_whitespace_and_nbsp() -> None:
    assert (
        normalize_title_for_version("  Breaking\u00a0news \t here\n")
        == "Breaking news here"
    )


def test_title_normalization_applies_compatibility_normalization() -> None:
    assert normalize_title_for_version("Ｆｕｌｌ－ｗｉｄｔｈ") == "Full-width"


def test_title_normalization_preserves_case_punctuation_and_digits() -> None:
    base = normalize_title_for_version("China's GDP grows 5%")
    assert base != normalize_title_for_version("china's GDP grows 5%")
    assert base != normalize_title_for_version("Chinas GDP grows 5%")
    assert base != normalize_title_for_version("China's GDP grows 6%")
    assert base != normalize_title_for_version("China GDP grows 5%")


def test_title_normalization_does_not_convert_chinese_scripts() -> None:
    assert normalize_title_for_version("中国") != normalize_title_for_version("中國")
