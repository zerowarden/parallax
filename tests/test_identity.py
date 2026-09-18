from __future__ import annotations

from parallax.domain import HeadlineCandidate
from parallax.identity import identity_key
from parallax.normalization import canonicalize_url


def test_url_shaped_external_id_remains_an_opaque_upstream_id() -> None:
    candidate = HeadlineCandidate(
        title="Headline",
        url="https://example.test/story",
        external_id="HTTPS://EXAMPLE.TEST:443/story#section",
    )

    assert identity_key(candidate) == (
        "external:HTTPS://EXAMPLE.TEST:443/story#section"
    )


def test_distinct_url_shaped_external_ids_do_not_collapse() -> None:
    first = HeadlineCandidate(
        title="First",
        url="https://example.test/story",
        external_id="https://ids.example.test/item#first",
    )
    second = HeadlineCandidate(
        title="Second",
        url="https://example.test/story",
        external_id="https://ids.example.test/item#second",
    )

    assert identity_key(first) != identity_key(second)


def test_non_url_external_id_remains_preferred() -> None:
    candidate = HeadlineCandidate(
        title="Headline",
        url="https://example.test/story",
        external_id="article-42",
    )

    assert identity_key(candidate) == "external:article-42"


def test_missing_external_id_uses_canonical_url_identity() -> None:
    candidate = HeadlineCandidate(
        title="Headline",
        url="HTTPS://EXAMPLE.TEST:443/story#section",
    )

    assert identity_key(candidate) == "url:https://example.test/story"


def test_utm_variants_share_canonical_url_but_not_identity() -> None:
    first = HeadlineCandidate(
        title="Headline",
        url="https://example.test/story?utm_source=newsletter",
    )
    second = HeadlineCandidate(
        title="Headline",
        url="https://example.test/story?utm_source=social",
    )

    assert canonicalize_url(first.url) == canonicalize_url(second.url)
    assert identity_key(first) != identity_key(second)


def test_semantic_query_keys_remain_part_of_url_identity() -> None:
    first = HeadlineCandidate(
        title="First",
        url="https://example.test/search?q=alpha&wd=one",
    )
    second = HeadlineCandidate(
        title="Second",
        url="https://example.test/search?q=beta&wd=one",
    )

    assert canonicalize_url(first.url) != canonicalize_url(second.url)
    assert identity_key(first) != identity_key(second)
