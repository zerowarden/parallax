from __future__ import annotations

import hashlib
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from parallax.config import SourceConfig
from parallax.domain import HeadlineCandidate, ValidatedBatch
from parallax.storage import Storage

OBSERVED_AT = datetime(2026, 9, 18, 12, 0, tzinfo=UTC)


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
        OBSERVED_AT,
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
        OBSERVED_AT,
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
        OBSERVED_AT,
    )

    assert summary_one.new_item_count == 1
    assert summary_one.new_version_count == 1
    assert summary_two.new_item_count == 0
    assert summary_two.new_version_count == 0
    assert summary_three.new_item_count == 0
    assert summary_three.new_version_count == 1

    rows = storage.latest_snapshot_headlines(limit_per_source=10)
    assert len(rows) == 1
    assert rows[0].title == "Updated title"

    changes = storage.changes_after(0)
    assert [event.event_type for event in changes] == [
        "item_created",
        "headline_version_created",
    ]
    storage.close()


def test_latest_snapshot_headlines_are_unbounded_by_default(tmp_path: Path):
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
        OBSERVED_AT,
    )

    assert len(storage.latest_snapshot_headlines()) == 25
    assert len(storage.latest_snapshot_headlines(limit_per_source=10)) == 10
    storage.close()


def test_latest_snapshot_headlines_keep_position_order_and_classification(
    tmp_path: Path,
):
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    candidates = tuple(
        HeadlineCandidate(
            title=f"Headline {position}",
            url=f"https://example.com/{position}",
            external_id=f"item-{position}",
            position=position,
        )
        for position in (2, 1, 3)
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
        OBSERVED_AT,
    )

    rows = storage.latest_snapshot_headlines()
    storage.close()

    assert [row.position for row in rows] == [1, 2, 3]
    assert [row.title for row in rows] == ["Headline 1", "Headline 2", "Headline 3"]
    assert {row.stream_kind for row in rows} == {"latest"}
    assert {row.item_kind for row in rows} == {"article"}
    assert all(row.item_id > 0 for row in rows)


def test_out_of_order_observations_preserve_temporal_bounds_and_latest_snapshot(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "parallax.db"
    storage = Storage(database_path)
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    older_observation = OBSERVED_AT
    newer_observation = OBSERVED_AT + timedelta(minutes=5)

    def batch(url: str, position: int) -> ValidatedBatch:
        return ValidatedBatch(
            candidates=(
                HeadlineCandidate(
                    title="Headline",
                    url=url,
                    external_id="item-1",
                    position=position,
                ),
            ),
            rejected_count=0,
        )

    older_run = storage.start_fetch_run(source.id)
    newer_run = storage.start_fetch_run(source.id)
    storage.record_success(
        source,
        newer_run,
        200,
        batch("https://example.com/newer", 1),
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
        newer_observation,
    )
    storage.record_success(
        source,
        older_run,
        200,
        batch("https://example.com/older", 2),
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
        older_observation,
    )

    rows = storage.latest_snapshot_headlines()
    storage.close()
    with sqlite3.connect(database_path) as connection:
        item_times = connection.execute(
            "SELECT first_seen_at, last_seen_at FROM items"
        ).fetchone()
        version_times = connection.execute(
            "SELECT first_seen_at, last_seen_at FROM item_versions"
        ).fetchone()

    assert len(rows) == 1
    assert rows[0].position == 1
    assert rows[0].url == "https://example.com/newer"
    assert rows[0].canonical_url == "https://example.com/newer"
    assert item_times == (
        older_observation.isoformat(),
        newer_observation.isoformat(),
    )
    assert version_times == item_times


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
            OBSERVED_AT,
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
        OBSERVED_AT,
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
        OBSERVED_AT,
    )

    rows = storage.latest_snapshot_headlines()
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
    snapshots = storage.latest_snapshot_headlines()
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
        OBSERVED_AT,
    )

    storage.sync_sources([retained])

    assert storage.latest_snapshot_headlines() == []
    assert storage.latest_snapshot_headlines(source_id="removed") == []
    historical = storage.latest_snapshot_headlines(
        source_id="removed", enabled_only=False
    )
    assert [row.title for row in historical] == ["Removed headline"]
    with sqlite3.connect(tmp_path / "parallax.db") as connection:
        enabled = connection.execute(
            "SELECT enabled FROM sources WHERE source_id = 'removed'"
        ).fetchone()
    storage.close()

    assert enabled == (0,)


def test_latest_snapshot_headlines_only_returns_enabled_sources(tmp_path: Path):
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
            OBSERVED_AT,
        )

    rows = storage.latest_snapshot_headlines()
    enabled_rows = storage.latest_snapshot_headlines(source_id="disabled")
    disabled_rows = storage.latest_snapshot_headlines(
        source_id="disabled", enabled_only=False
    )
    storage.close()

    assert [row.source_id for row in rows] == ["fixture"]
    assert enabled_rows == []
    assert [row.source_id for row in disabled_rows] == ["disabled"]


def test_tracking_query_variants_share_canonical_url_not_item_identity(
    tmp_path: Path,
) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])

    run_id = storage.start_fetch_run(source.id)
    storage.record_success(
        source,
        run_id,
        200,
        ValidatedBatch(
            candidates=(
                HeadlineCandidate(
                    title="Newsletter headline",
                    url="https://example.com/1?utm_source=newsletter",
                    position=1,
                ),
                HeadlineCandidate(
                    title="Social headline",
                    url="https://example.com/1?utm_source=social",
                    position=2,
                ),
            ),
            rejected_count=0,
        ),
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
        OBSERVED_AT,
    )

    rows = storage.latest_snapshot_headlines()
    storage.close()

    assert len(rows) == 2
    assert {row.canonical_url for row in rows} == {"https://example.com/1"}


def test_formatting_only_title_change_reuses_version(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])

    run_one = storage.start_fetch_run(source.id)
    storage.record_success(
        source,
        run_one,
        200,
        ValidatedBatch(
            candidates=(
                HeadlineCandidate(
                    title="Breaking  news\u00a0today",
                    url="https://example.com/1",
                    external_id="item-1",
                    position=1,
                ),
            ),
            rejected_count=0,
        ),
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
        OBSERVED_AT,
    )

    run_two = storage.start_fetch_run(source.id)
    summary = storage.record_success(
        source,
        run_two,
        200,
        ValidatedBatch(
            candidates=(
                HeadlineCandidate(
                    title="  Breaking news today  ",
                    url="https://example.com/1",
                    external_id="item-1",
                    position=1,
                ),
            ),
            rejected_count=0,
        ),
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
        OBSERVED_AT,
    )

    rows = storage.latest_snapshot_headlines()
    changes = storage.changes_after(0)
    storage.close()

    assert summary.new_version_count == 0
    assert [row.title for row in rows] == ["Breaking  news\u00a0today"]
    assert [event.event_type for event in changes] == ["item_created"]


def test_legacy_title_hash_is_reused_without_new_version(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])

    legacy_title = "Breaking  news"
    run_one = storage.start_fetch_run(source.id)
    storage.record_success(
        source,
        run_one,
        200,
        ValidatedBatch(
            candidates=(
                HeadlineCandidate(
                    title=legacy_title,
                    url="https://example.com/1",
                    external_id="item-1",
                    position=1,
                ),
            ),
            rejected_count=0,
        ),
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
        OBSERVED_AT,
    )

    with sqlite3.connect(tmp_path / "parallax.db") as connection:
        connection.execute(
            "UPDATE item_versions SET title_hash = ?",
            (hashlib.sha256(legacy_title.encode("utf-8")).hexdigest(),),
        )

    run_two = storage.start_fetch_run(source.id)
    summary = storage.record_success(
        source,
        run_two,
        200,
        ValidatedBatch(
            candidates=(
                HeadlineCandidate(
                    title="Breaking\u00a0news",
                    url="https://example.com/1",
                    external_id="item-1",
                    position=1,
                ),
            ),
            rejected_count=0,
        ),
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
        OBSERVED_AT,
    )

    rows = storage.latest_snapshot_headlines()
    changes = storage.changes_after(0)
    storage.close()

    assert summary.new_version_count == 0
    assert [row.title for row in rows] == [legacy_title]
    assert [event.event_type for event in changes] == ["item_created"]


def test_success_commit_uses_observation_time_for_seen_fields(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    observed_at = datetime(2026, 9, 17, 3, 4, 5, tzinfo=UTC)

    run_id = storage.start_fetch_run(source.id)
    storage.record_success(
        source,
        run_id,
        200,
        ValidatedBatch(
            candidates=(
                HeadlineCandidate(
                    title="Headline",
                    url="https://example.com/1",
                    external_id="item-1",
                    position=1,
                ),
            ),
            rejected_count=0,
        ),
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
        observed_at,
    )

    rows = storage.latest_snapshot_headlines()
    runs = storage.recent_fetch_runs()
    storage.close()

    with sqlite3.connect(tmp_path / "parallax.db") as connection:
        snapshot_observed_at = connection.execute(
            "SELECT observed_at FROM snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()

    assert rows[0].first_seen_at == observed_at.isoformat()
    assert snapshot_observed_at == (observed_at.isoformat(),)
    assert runs[0].finished_at is not None
    assert datetime.fromisoformat(runs[0].finished_at) > observed_at


def test_failed_commit_rolls_back_and_preserves_prior_snapshot(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])

    run_one = storage.start_fetch_run(source.id)
    storage.record_success(
        source,
        run_one,
        200,
        ValidatedBatch(
            candidates=(
                HeadlineCandidate(
                    title="Original headline",
                    url="https://example.com/1",
                    external_id="item-1",
                    position=1,
                ),
            ),
            rejected_count=0,
        ),
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
        OBSERVED_AT,
    )

    failing_run = storage.start_fetch_run(source.id)
    with pytest.raises(TypeError):
        storage.record_success(
            source,
            failing_run,
            200,
            ValidatedBatch(
                candidates=(
                    HeadlineCandidate(
                        title="Broken metrics",
                        url="https://example.com/2",
                        external_id="item-2",
                        position=1,
                        metrics={"bad": object()},
                    ),
                ),
                rejected_count=0,
            ),
            None,
            None,
            datetime.now(UTC) + timedelta(minutes=10),
            OBSERVED_AT,
        )

    rows = storage.latest_snapshot_headlines()
    runs = storage.latest_fetch_runs(enabled_only=False)
    state = storage.get_stream_state(source.id)
    changes = storage.changes_after(0)
    storage.close()

    assert [row.title for row in rows] == ["Original headline"]
    assert [event.event_type for event in changes] == ["item_created"]
    assert runs[0].id == failing_run
    assert runs[0].status == "running"
    assert state.last_success_at is not None


def test_sync_sources_reconciles_stored_item_kind(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    first = _source()
    second = SourceConfig(
        id="other",
        name="Other",
        region="US",
        language="en-US",
        adapter="rss",
        url="https://example.com/other.xml",
        item_kind="trend",
    )
    storage.sync_sources([first, second])

    for source in (first, second):
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
            OBSERVED_AT,
        )

    with sqlite3.connect(tmp_path / "parallax.db") as connection:
        before = connection.execute(
            "SELECT id, identity_key, canonical_url FROM items ORDER BY id"
        ).fetchall()

    storage.sync_sources([first.model_copy(update={"item_kind": "flash"}), second])

    with sqlite3.connect(tmp_path / "parallax.db") as connection:
        after = connection.execute(
            "SELECT id, identity_key, canonical_url FROM items ORDER BY id"
        ).fetchall()
        kinds = dict(connection.execute("SELECT source_id, item_kind FROM items"))
        versions = connection.execute("SELECT COUNT(*) FROM item_versions").fetchone()
        snapshots = connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()
    storage.close()

    assert kinds == {"fixture": "flash", "other": "trend"}
    assert after == before
    assert versions == (2,)
    assert snapshots == (2,)
