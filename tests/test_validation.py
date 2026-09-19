from __future__ import annotations

import pytest

from parallax.config import ValidationConfig
from parallax.domain import HeadlineCandidate, ParsedBatch
from parallax.validation import BatchValidationError, BatchValidator


def test_validator_rejects_invalid_and_duplicate_items():
    validator = BatchValidator(ValidationConfig())
    batch = ParsedBatch(
        candidates=(
            HeadlineCandidate(
                title="Valid",
                url="https://example.com/a",
                external_id="1",
                position=1,
            ),
            HeadlineCandidate(
                title="Duplicate",
                url="https://example.com/b",
                external_id="1",
                position=2,
            ),
            HeadlineCandidate(
                title="",
                url="https://example.com/c",
                external_id="3",
                position=3,
            ),
        )
    )

    result = validator.validate("fixture", batch, provider_id="fixture")

    assert len(result.candidates) == 1
    assert result.rejected_count == 2


@pytest.mark.parametrize("external_first", [False, True])
def test_validator_rejects_url_fallback_and_external_identity_for_same_item(
    external_first: bool,
) -> None:
    validator = BatchValidator(ValidationConfig())
    external = HeadlineCandidate(
        title="With ID",
        url="https://example.com/story",
        external_id="story-1",
    )
    fallback = HeadlineCandidate(
        title="Without ID",
        url="https://example.com/story",
    )
    candidates = (external, fallback) if external_first else (fallback, external)

    result = validator.validate(
        "fixture",
        ParsedBatch(candidates=candidates),
        provider_id="fixture",
    )

    assert result.candidates == (candidates[0],)
    assert result.rejected_count == 1


def test_validator_preserves_distinct_external_ids_with_the_same_url() -> None:
    validator = BatchValidator(ValidationConfig())
    candidates = (
        HeadlineCandidate(
            title="First",
            url="https://example.com/story",
            external_id="story-1",
        ),
        HeadlineCandidate(
            title="Second",
            url="https://example.com/story",
            external_id="story-2",
        ),
    )

    result = validator.validate(
        "fixture",
        ParsedBatch(candidates=candidates),
        provider_id="fixture",
    )

    assert result.candidates == candidates
    assert result.rejected_count == 0


def test_validator_rejects_empty_batch_by_default():
    validator = BatchValidator(ValidationConfig())

    with pytest.raises(BatchValidationError, match="no valid headline items"):
        validator.validate("fixture", ParsedBatch(candidates=()), provider_id="fixture")


def test_validator_allows_configured_empty_batch():
    validator = BatchValidator(ValidationConfig(allow_empty_batches=True))

    result = validator.validate(
        "fixture", ParsedBatch(candidates=()), provider_id="fixture"
    )

    assert result.candidates == ()
    assert result.rejected_count == 0


@pytest.mark.parametrize(
    "url",
    [
        'https://example.com/story" target="blank',
        "https://example.com/story with space",
        "https://example.com/story\nnext",
        "https://example.com/story\x00",
        "https://example.com/<story>",
    ],
)
def test_validator_rejects_urls_with_illegal_characters(url: str):
    validator = BatchValidator(ValidationConfig(allow_empty_batches=True))
    batch = ParsedBatch(
        candidates=(
            HeadlineCandidate(
                title="Headline",
                url=url,
                external_id="1",
                position=1,
            ),
        )
    )

    result = validator.validate("fixture", batch, provider_id="fixture")

    assert result.candidates == ()
    assert result.rejected_count == 1
