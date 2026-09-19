from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from parallax.config import Source
from parallax.domain import (
    EntityKind,
    HeadlineCandidate,
    ItemKind,
    ItemVariant,
    ValidatedBatch,
)
from parallax.processing.changelog import ChangeLogReader
from parallax.processing.input import AnalysisInputReader
from parallax.storage import Storage
from source_factory import make_source

OBSERVED_AT = datetime(2026, 9, 18, 12, 0, tzinfo=UTC)
CONSUMER = "classifier-v1"


def _source(
    source_id: str = "fixture",
    *,
    item_kind: ItemKind = "article",
    entity_kind: EntityKind | None = None,
    item_variant: ItemVariant | None = None,
    language: str = "en-US",
    enabled: bool = True,
) -> Source:
    return make_source(
        id=source_id,
        url=f"https://example.com/{source_id}.xml",
        item_kind=item_kind,
        entity_kind=entity_kind,
        item_variant=item_variant,
        language=language,
        enabled=enabled,
    )


def _record(
    storage: Storage,
    source: Source,
    candidates: tuple[HeadlineCandidate, ...],
) -> None:
    run_id = storage.start_fetch_run(source.id)
    storage.record_success(
        source,
        run_id,
        200,
        ValidatedBatch(candidates=candidates, rejected_count=0),
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
        OBSERVED_AT,
    )


def _candidate(
    external_id: str,
    title: str,
    *,
    position: int = 1,
    published_at: datetime | None = None,
) -> HeadlineCandidate:
    return HeadlineCandidate(
        title=title,
        url=f"https://example.com/{external_id}?utm_source=fixture",
        external_id=external_id,
        published_at=published_at,
        position=position,
    )


def test_independent_consumer_checkpoint(tmp_path: Path):
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    _record(storage, source, (_candidate("1", "Headline"),))

    reader = ChangeLogReader(storage, CONSUMER)
    events = list(reader.pending())
    assert len(events) == 1
    reader.checkpoint(events[-1].seq)
    assert list(reader.pending()) == []
    storage.close()


def test_analysis_input_projects_committed_item_version(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source(item_kind="article", item_variant="flash", language="zh-CN")
    storage.sync_sources([source])
    published = datetime(2026, 9, 17, 8, 30, tzinfo=UTC)
    _record(
        storage,
        source,
        (_candidate("1", "Headline", published_at=published),),
    )

    reader = AnalysisInputReader(storage, CONSUMER)
    items = list(reader.pending())
    storage.close()

    assert len(items) == 1
    item = items[0]
    assert item.change_seq == 1
    assert item.event_type == "item_created"
    assert item.title == "Headline"
    assert item.source_id == "fixture"
    assert item.source_language == "zh-CN"
    assert item.source_enabled is True
    assert item.stream_kind == "latest"
    assert item.item_kind == "article"
    assert item.entity_kind is None
    assert item.item_variant == "flash"
    assert item.original_url == "https://example.com/1?utm_source=fixture"
    assert item.canonical_url == "https://example.com/1"
    assert item.published_at == published
    assert item.first_seen_at == OBSERVED_AT
    assert item.first_seen_at.tzinfo is UTC


def test_analysis_input_selects_exact_event_version_title(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    _record(storage, source, (_candidate("1", "Original title"),))
    _record(storage, source, (_candidate("1", "Updated title"),))

    reader = AnalysisInputReader(storage, CONSUMER)
    items = list(reader.pending())
    storage.close()

    assert [item.event_type for item in items] == [
        "item_created",
        "headline_version_created",
    ]
    assert [item.title for item in items] == ["Original title", "Updated title"]
    assert items[0].item_id == items[1].item_id
    assert items[0].item_version_id != items[1].item_version_id


def test_analysis_input_keeps_unknown_publication_time_null(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    _record(storage, source, (_candidate("1", "Undated"),))

    reader = AnalysisInputReader(storage, CONSUMER)
    items = list(reader.pending())
    storage.close()

    assert items[0].published_at is None


def test_analysis_input_includes_disabled_sources(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    _record(storage, source, (_candidate("1", "Headline"),))
    storage.sync_sources([source.model_copy(update={"enabled": False})])

    reader = AnalysisInputReader(storage, CONSUMER)
    items = list(reader.pending())
    storage.close()

    assert len(items) == 1
    assert items[0].source_enabled is False


def test_analysis_input_orders_batches_and_resumes_from_checkpoint(
    tmp_path: Path,
) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    _record(
        storage,
        source,
        tuple(
            _candidate(str(index), f"Headline {index}", position=index)
            for index in range(1, 6)
        ),
    )

    reader = AnalysisInputReader(storage, CONSUMER)
    first = list(reader.pending(limit=2))
    assert [item.change_seq for item in first] == [1, 2]
    assert [item.title for item in first] == ["Headline 1", "Headline 2"]

    reader.checkpoint(first[-1].change_seq)
    second = list(reader.pending(limit=2))
    assert [item.change_seq for item in second] == [3, 4]

    reader.checkpoint(second[-1].change_seq)
    remaining = list(reader.pending())
    assert [item.change_seq for item in remaining] == [5]

    reader.checkpoint(remaining[-1].change_seq)
    assert list(reader.pending()) == []
    storage.close()


def test_analysis_input_fails_without_advancing_on_version_mismatch(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "parallax.db"
    storage = Storage(database_path)
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    _record(
        storage,
        source,
        (
            _candidate("1", "First headline", position=1),
            _candidate("2", "Second headline", position=2),
        ),
    )

    with sqlite3.connect(database_path) as connection:
        other_version_id = connection.execute(
            "SELECT item_version_id FROM change_log WHERE seq = 2"
        ).fetchone()[0]
        connection.execute(
            "UPDATE change_log SET item_version_id = ? WHERE seq = 1",
            (other_version_id,),
        )

    reader = AnalysisInputReader(storage, CONSUMER)
    with pytest.raises(ValueError, match="missing item version"):
        list(reader.pending())

    assert storage.get_consumer_checkpoint(CONSUMER) == 0
    storage.close()
