from __future__ import annotations

from parallax.domain import HeadlineCandidate
from parallax.identity import identity_key


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
