from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from parallax.config import SourceConfig
from parallax.domain import HeadlineCandidate, ValidatedBatch
from parallax.storage import Storage


def _source() -> SourceConfig:
    return SourceConfig(
        id="fixture",
        name="Fixture",
        region="US",
        language="en-US",
        adapter="rss",
        url="https://example.com/rss.xml",
    )


def test_change_log_has_source_and_created_at_index(tmp_path: Path) -> None:
    database_path = tmp_path / "parallax.db"
    storage = Storage(database_path)
    storage.initialize()
    storage.close()

    with sqlite3.connect(database_path) as connection:
        indexes = {
            row[1]: row for row in connection.execute("PRAGMA index_list(change_log)")
        }
        columns = [
            row[2]
            for row in connection.execute(
                "PRAGMA index_info(idx_change_log_source_created)"
            )
        ]

    assert "idx_change_log_source_created" in indexes
    assert columns == ["source_id", "created_at"]


def test_storage_preserves_versions_and_is_idempotent(tmp_path: Path):
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])

    first = ValidatedBatch(
        candidates=(
            HeadlineCandidate(
                title="Original title",
                url="https://example.com/1",
                external_id="item-1",
                position=1,
            ),
        ),
        rejected_count=0,
    )
    run_one = storage.start_fetch_run(source.id)
    summary_one = storage.record_success(
        source,
        run_one,
        200,
        first,
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
    )

    run_two = storage.start_fetch_run(source.id)
    summary_two = storage.record_success(
        source,
        run_two,
        200,
        first,
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
    )

    changed = ValidatedBatch(
        candidates=(
            HeadlineCandidate(
                title="Updated title",
                url="https://example.com/1",
                external_id="item-1",
                position=1,
            ),
        ),
        rejected_count=0,
    )
    run_three = storage.start_fetch_run(source.id)
    summary_three = storage.record_success(
        source,
        run_three,
        200,
        changed,
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
    )

    assert summary_one.new_item_count == 1
    assert summary_one.new_version_count == 1
    assert summary_two.new_item_count == 0
    assert summary_two.new_version_count == 0
    assert summary_three.new_item_count == 0
    assert summary_three.new_version_count == 1

    rows = storage.latest_headlines(limit_per_source=10)
    assert len(rows) == 1
    assert rows[0].title == "Updated title"

    changes = storage.changes_after(0)
    assert [event.event_type for event in changes] == [
        "item_created",
        "headline_version_created",
    ]
    storage.close()


def test_latest_headlines_are_unbounded_by_default(tmp_path: Path):
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    candidates = tuple(
        HeadlineCandidate(
            title=f"Headline {index}",
            url=f"https://example.com/{index}",
            external_id=f"item-{index}",
            position=index,
        )
        for index in range(1, 26)
    )
    run_id = storage.start_fetch_run(source.id)
    storage.record_success(
        source,
        run_id,
        200,
        ValidatedBatch(candidates=candidates, rejected_count=0),
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
    )

    assert len(storage.latest_headlines()) == 25
    assert len(storage.latest_headlines(limit_per_source=10)) == 10
    storage.close()


def test_latest_fetch_runs_returns_one_run_per_source(tmp_path: Path):
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    batch = ValidatedBatch(
        candidates=(
            HeadlineCandidate(
                title="Headline",
                url="https://example.com/1",
                external_id="item-1",
                position=1,
            ),
        ),
        rejected_count=0,
    )
    for _ in range(3):
        run_id = storage.start_fetch_run(source.id)
        storage.record_success(
            source,
            run_id,
            200,
            batch,
            None,
            None,
            datetime.now(UTC) + timedelta(minutes=10),
        )

    latest = storage.latest_fetch_runs()
    history = storage.recent_fetch_runs(limit=10)
    assert len(latest) == 1
    assert latest[0].id == history[0].id
    assert len(history) == 3
    storage.close()


def test_later_publication_time_fills_existing_unknown_value(tmp_path: Path):
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])

    missing_date = ValidatedBatch(
        candidates=(
            HeadlineCandidate(
                title="Headline",
                url="https://example.com/1",
                external_id="item-1",
                position=1,
            ),
        ),
        rejected_count=0,
    )
    run_one = storage.start_fetch_run(source.id)
    storage.record_success(
        source,
        run_one,
        200,
        missing_date,
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
    )

    published = datetime(2026, 9, 16, 6, 30, tzinfo=UTC)
    with_date = ValidatedBatch(
        candidates=(
            HeadlineCandidate(
                title="Headline",
                url="https://example.com/1",
                external_id="item-1",
                published_at=published,
                raw_published_at="1789540200",
                position=1,
            ),
        ),
        rejected_count=0,
    )
    run_two = storage.start_fetch_run(source.id)
    storage.record_success(
        source,
        run_two,
        200,
        with_date,
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
    )

    rows = storage.latest_headlines()
    assert len(rows) == 1
    assert rows[0].published_at == published.isoformat()
    storage.close()


def test_record_history_stores_items_without_snapshot_or_schedule(
    tmp_path: Path,
):
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    batch = ValidatedBatch(
        candidates=(
            HeadlineCandidate(
                title="Backfilled headline",
                url="https://example.com/old",
                external_id="old-1",
                published_at=datetime(2026, 9, 1, tzinfo=UTC),
            ),
        ),
        rejected_count=0,
    )

    run_id = storage.start_fetch_run(source.id)
    summary = storage.record_history(source, run_id, batch)
    runs = storage.recent_fetch_runs()
    snapshots = storage.latest_headlines()
    state = storage.get_stream_state(source.id)
    changes = storage.changes_after(0)
    storage.close()

    assert summary.status == "history"
    assert summary.new_item_count == 1
    assert runs[0].status == "history"
    assert snapshots == []
    assert state.last_success_at is None
    assert {event.event_type for event in changes} == {"item_created"}


def test_sync_sources_disables_sources_removed_from_config(tmp_path: Path):
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    retained = _source()
    removed = SourceConfig(
        id="removed",
        name="Removed",
        region="US",
        language="en-US",
        adapter="rss",
        url="https://example.com/removed.xml",
    )
    storage.sync_sources([retained, removed])
    batch = ValidatedBatch(
        candidates=(
            HeadlineCandidate(
                title="Removed headline",
                url="https://example.com/removed/1",
                external_id="removed-1",
                position=1,
            ),
        ),
        rejected_count=0,
    )
    run_id = storage.start_fetch_run(removed.id)
    storage.record_success(
        removed,
        run_id,
        200,
        batch,
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
    )

    storage.sync_sources([retained])

    assert storage.latest_headlines() == []
    assert storage.latest_headlines(source_id="removed") == []
    historical = storage.latest_headlines(source_id="removed", enabled_only=False)
    assert [row.title for row in historical] == ["Removed headline"]
    with sqlite3.connect(tmp_path / "parallax.db") as connection:
        enabled = connection.execute(
            "SELECT enabled FROM sources WHERE source_id = 'removed'"
        ).fetchone()
    storage.close()

    assert enabled == (0,)


def test_latest_headlines_only_returns_enabled_sources(tmp_path: Path):
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    enabled = _source()
    disabled = SourceConfig(
        id="disabled",
        name="Disabled",
        region="US",
        language="en-US",
        adapter="rss",
        url="https://example.com/disabled.xml",
        enabled=False,
    )
    storage.sync_sources([enabled, disabled])
    for source in (enabled, disabled):
        run_id = storage.start_fetch_run(source.id)
        storage.record_success(
            source,
            run_id,
            200,
            ValidatedBatch(
                candidates=(
                    HeadlineCandidate(
                        title=f"{source.id} headline",
                        url=f"https://example.com/{source.id}",
                        external_id=source.id,
                        position=1,
                    ),
                ),
                rejected_count=0,
            ),
            None,
            None,
            datetime.now(UTC) + timedelta(minutes=10),
        )

    rows = storage.latest_headlines()
    enabled_rows = storage.latest_headlines(source_id="disabled")
    disabled_rows = storage.latest_headlines(source_id="disabled", enabled_only=False)
    storage.close()

    assert [row.source_id for row in rows] == ["fixture"]
    assert enabled_rows == []
    assert [row.source_id for row in disabled_rows] == ["disabled"]
