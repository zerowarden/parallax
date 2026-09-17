from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

from parallax.config import ValidationConfig
from parallax.domain import (
    HeadlineCandidate,
    ParsedBatch,
    ValidatedBatch,
)
from parallax.identity import identity_key

LOGGER = logging.getLogger(__name__)

_ILLEGAL_URL_CHARACTERS = frozenset(' "<>\\^`{|}')


def _has_illegal_url_characters(url: str) -> bool:
    return any(
        character.isspace()
        or character in _ILLEGAL_URL_CHARACTERS
        or ord(character) < 0x20
        or ord(character) == 0x7F
        for character in url
    )


class BatchValidationError(RuntimeError):
    pass


class BatchValidator:
    def __init__(self, config: ValidationConfig) -> None:
        self._config = config

    def validate(self, source_id: str, batch: ParsedBatch) -> ValidatedBatch:
        accepted: list[HeadlineCandidate] = []
        warnings = list(batch.warnings)
        rejected = 0
        seen: set[str] = set()
        now = datetime.now(UTC)
        future_limit = now + timedelta(seconds=self._config.max_future_seconds)

        for candidate in batch.candidates:
            reason = self._rejection_reason(candidate, future_limit)
            if reason:
                rejected += 1
                LOGGER.warning(
                    "operation=validate_reject source_id=%s reason=%s title=%r",
                    source_id,
                    reason,
                    candidate.title[:120],
                )
                continue

            key = identity_key(candidate)
            if key in seen:
                rejected += 1
                warnings.append(f"duplicate identity in batch: {key}")
                LOGGER.warning(
                    "operation=validate_duplicate source_id=%s identity=%s",
                    source_id,
                    key,
                )
                continue
            seen.add(key)
            accepted.append(candidate)

        if not accepted and not self._config.allow_empty_batches:
            raise BatchValidationError(
                f"Source {source_id} produced no valid headline items"
            )

        LOGGER.info(
            "operation=validate_batch source_id=%s accepted=%s rejected=%s warnings=%s",
            source_id,
            len(accepted),
            rejected,
            len(warnings),
        )
        return ValidatedBatch(
            candidates=tuple(accepted),
            rejected_count=rejected,
            warnings=tuple(warnings),
        )

    def _rejection_reason(
        self,
        candidate: HeadlineCandidate,
        future_limit: datetime,
    ) -> str | None:
        title = candidate.title.strip()
        if not title:
            return "empty_title"
        if len(title) > self._config.max_title_length:
            return "title_too_long"

        if _has_illegal_url_characters(candidate.url):
            return "invalid_url"

        try:
            split = urlsplit(candidate.url.strip())
            _ = split.port
        except ValueError:
            return "invalid_url"
        if split.scheme not in {"http", "https"} or not split.hostname:
            return "invalid_url"

        if candidate.position is not None and candidate.position < 1:
            return "invalid_position"

        if candidate.published_at is not None:
            published = candidate.published_at
            if published.tzinfo is None:
                return "naive_published_at"
            if published.astimezone(UTC) > future_limit:
                return "published_at_too_far_in_future"
        return None
