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

    result = validator.validate("fixture", batch)

    assert len(result.candidates) == 1
    assert result.rejected_count == 2


def test_validator_rejects_empty_batch_by_default():
    validator = BatchValidator(ValidationConfig())

    with pytest.raises(BatchValidationError, match="no valid headline items"):
        validator.validate("fixture", ParsedBatch(candidates=()))


def test_validator_allows_configured_empty_batch():
    validator = BatchValidator(ValidationConfig(allow_empty_batches=True))

    result = validator.validate("fixture", ParsedBatch(candidates=()))

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

    result = validator.validate("fixture", batch)

    assert result.candidates == ()
    assert result.rejected_count == 1
