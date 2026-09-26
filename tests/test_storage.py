from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from parallax.config import Source
from parallax.domain import BrowseView, HeadlineCandidate, ObservedBatch, ValidatedBatch
from parallax.storage import SCHEMA_VERSION, Storage, foundation
from source_factory import make_source

OBSERVED_AT = datetime(2026, 9, 18, 12, 0, tzinfo=UTC)


def _source(**overrides: Any):
    overrides.setdefault("url", "https://example.com/rss.xml")
    return make_source(**overrides)


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


def test_items_have_first_seen_index(tmp_path: Path) -> None:
    database_path = tmp_path / "parallax.db"
    storage = Storage(database_path)
    storage.initialize()
    storage.close()

    with sqlite3.connect(database_path) as connection:
        indexes = {
            row[1]: row for row in connection.execute("PRAGMA index_list(items)")
        }
        columns = [
            row[2]
            for row in connection.execute("PRAGMA index_info(idx_items_first_seen)")
        ]

    assert "idx_items_first_seen" in indexes
    assert columns == ["first_seen_at"]


def test_initialize_records_schema_version_and_rejects_mismatch(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "parallax.db"
    storage = Storage(database_path)
    storage.initialize()
    storage.close()

    with sqlite3.connect(database_path) as connection:
        recorded = connection.execute("SELECT version FROM schema_meta").fetchone()
        connection.execute("UPDATE schema_meta SET version = ?", (SCHEMA_VERSION + 1,))

    with pytest.raises(RuntimeError, match="Unsupported schema version"):
        Storage(database_path)

    assert recorded == (SCHEMA_VERSION,)


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
    run_one = storage.start_fetch_run(source.id, "live")
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

    run_two = storage.start_fetch_run(source.id, "live")
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
    run_three = storage.start_fetch_run(source.id, "live")
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
    run_id = storage.start_fetch_run(source.id, "live")
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
    run_id = storage.start_fetch_run(source.id, "live")
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
    newer_observation = OBSERVED_AT + timedelta(microseconds=5)

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

    older_run = storage.start_fetch_run(source.id, "live")
    newer_run = storage.start_fetch_run(source.id, "live")
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
        older_observation.isoformat(timespec="microseconds"),
        newer_observation.isoformat(timespec="microseconds"),
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
        run_id = storage.start_fetch_run(source.id, "live")
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
    run_one = storage.start_fetch_run(source.id, "live")
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
    run_two = storage.start_fetch_run(source.id, "live")
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
    assert rows[0].published_at == published
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

    run_id = storage.start_fetch_run(source.id, "history")
    summary = storage.record_history(
        source, run_id, ObservedBatch.from_validated(batch, OBSERVED_AT)
    )
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
    removed = _source(id="removed", url="https://example.com/removed.xml")
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
    run_id = storage.start_fetch_run(removed.id, "live")
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
    disabled = _source(
        id="disabled", url="https://example.com/disabled.xml", enabled=False
    )
    storage.sync_sources([enabled, disabled])
    for source in (enabled, disabled):
        run_id = storage.start_fetch_run(source.id, "live")
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

    run_id = storage.start_fetch_run(source.id, "live")
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

    run_one = storage.start_fetch_run(source.id, "live")
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

    run_two = storage.start_fetch_run(source.id, "live")
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


def test_success_commit_uses_observation_time_for_seen_fields(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    observed_at = datetime(2026, 9, 17, 3, 4, 5, tzinfo=UTC)

    run_id = storage.start_fetch_run(source.id, "live")
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

    assert rows[0].first_seen_at == observed_at
    assert snapshot_observed_at == (observed_at.isoformat(timespec="microseconds"),)
    assert runs[0].finished_at is not None
    assert runs[0].finished_at > observed_at


def test_failed_commit_rolls_back_and_preserves_prior_snapshot(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])

    run_one = storage.start_fetch_run(source.id, "live")
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

    failing_run = storage.start_fetch_run(source.id, "live")
    # Deliberately violate the typed metrics contract to inject a storage failure.
    unserializable: Any = object()
    with pytest.raises(ValueError, match="candidate metrics"):
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
                        metrics={"bad": unserializable},
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


def test_transaction_rolls_back_when_interrupted(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()

    with pytest.raises(KeyboardInterrupt), foundation.transaction(storage._connection):
        storage._connection.execute("""
            INSERT INTO consumer_checkpoints(consumer_name, last_seq, updated_at)
            VALUES ('interrupted', 1, '2026-01-01T00:00:00+00:00')
            """)
        raise KeyboardInterrupt

    assert not storage._connection.in_transaction
    assert storage.get_consumer_checkpoint("interrupted") == 0
    storage.advance_consumer_checkpoint("after-interrupt", 1)
    assert storage.get_consumer_checkpoint("after-interrupt") == 1
    storage.close()


@pytest.mark.parametrize(
    "operation,failure_table",
    [("refresh", "fetch_runs"), ("refresh", "stream_state"), ("history", "fetch_runs")],
)
def test_late_ingestion_failure_rolls_back_every_persisted_effect(
    tmp_path: Path, operation: str, failure_table: str
) -> None:
    storage = Storage(tmp_path / "archive.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    original = HeadlineCandidate(title="Original", url="https://example.com/1")
    first_run = storage.start_fetch_run(source.id, "live")
    storage.record_success(
        source,
        first_run,
        200,
        ValidatedBatch((original,), 0),
        "original-etag",
        "original-modified",
        OBSERVED_AT,
        OBSERVED_AT,
        "original-request",
    )
    run = storage.start_fetch_run(
        source.id, "history" if operation == "history" else "live"
    )
    # Fail after items, aliases, observations, versions, events and (for refresh)
    # snapshot entries have been written. Even the run update must roll back.
    failure_condition = (
        f"NEW.id = {run} AND NEW.finished_at IS NOT NULL"
        if failure_table == "fetch_runs"
        else "NEW.request_identity = 'new-request'"
    )
    storage._connection.execute(f"""
        CREATE TEMP TRIGGER reject_ingestion AFTER UPDATE ON {failure_table}
        WHEN {failure_condition}
        BEGIN SELECT RAISE(ABORT, 'injected late failure'); END
        """)
    tables = (
        "items",
        "item_url_identities",
        "observations",
        "item_versions",
        "snapshots",
        "snapshot_entries",
        "change_log",
        "fetch_runs",
        "stream_state",
    )
    before = {
        table: storage._connection.execute(
            f"SELECT * FROM {table} ORDER BY rowid"
        ).fetchall()
        for table in tables
    }
    batch = ValidatedBatch(
        (
            HeadlineCandidate(
                title="Revised", url=original.url, external_id="promoted-id"
            ),
            HeadlineCandidate(title="New", url="https://example.com/2"),
        ),
        0,
    )
    try:
        with pytest.raises(sqlite3.IntegrityError, match="injected late failure"):
            if operation == "refresh":
                storage.record_success(
                    source,
                    run,
                    200,
                    batch,
                    "new-etag",
                    "new-modified",
                    OBSERVED_AT + timedelta(hours=1),
                    OBSERVED_AT,
                    "new-request",
                )
            else:
                storage.record_history(
                    source, run, ObservedBatch.from_validated(batch, OBSERVED_AT)
                )
        assert not storage._connection.in_transaction
        for table in tables:
            assert (
                storage._connection.execute(
                    f"SELECT * FROM {table} ORDER BY rowid"
                ).fetchall()
                == before[table]
            ), table
    finally:
        storage.close()


class _CommitFailingConnection:
    def __init__(self) -> None:
        self.in_transaction = False
        self.statements: list[str] = []

    def execute(self, sql: str) -> None:
        self.statements.append(sql)
        if sql == "BEGIN IMMEDIATE":
            self.in_transaction = True
        elif sql == "COMMIT":
            raise sqlite3.OperationalError("disk I/O error")
        elif sql == "ROLLBACK":
            self.in_transaction = False

    def close(self) -> None:
        pass


def test_transaction_rolls_back_when_commit_fails(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage._connection.close()
    connection = _CommitFailingConnection()
    storage._connection = connection  # type: ignore[assignment]

    expected = pytest.raises(sqlite3.OperationalError, match="disk I/O error")
    with expected, foundation.transaction(storage._connection):
        pass

    assert connection.statements == ["BEGIN IMMEDIATE", "COMMIT", "ROLLBACK"]
    assert not connection.in_transaction
    storage.close()


def test_transaction_discards_writes_when_deferred_commit_fails(
    tmp_path: Path,
) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()

    expected = pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY")
    with expected, foundation.transaction(storage._connection):
        storage._connection.execute("PRAGMA defer_foreign_keys = ON")
        storage._connection.execute("""
            INSERT INTO change_log(event_type, source_id, item_id, created_at)
            VALUES ('item_created', 'missing-source', 1, '2026-01-01T00:00:00+00:00')
            """)

    assert not storage._connection.in_transaction
    assert storage.changes_after(0) == []
    storage.close()


def test_canonical_items_collapse_across_provider_channels(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    local = _source(id="channel-local", provider_id="rthk")
    international = _source(id="channel-world", provider_id="rthk")
    storage.sync_sources([local, international])

    def observe(source: Source, observed_at: datetime) -> None:
        run_id = storage.start_fetch_run(source.id, "live")
        storage.record_success(
            source,
            run_id,
            200,
            ValidatedBatch(
                candidates=(
                    HeadlineCandidate(
                        title="Shared story",
                        url="https://example.com/story/1",
                        external_id="story-1",
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

    observe(local, OBSERVED_AT)
    observe(international, OBSERVED_AT + timedelta(minutes=5))

    with sqlite3.connect(tmp_path / "parallax.db") as connection:
        items = connection.execute("SELECT COUNT(*) FROM items").fetchone()
        observations = connection.execute(
            "SELECT COUNT(*) FROM observations"
        ).fetchone()
    events = [event.event_type for event in storage.changes_after(0)]
    snapshots = storage.latest_snapshot_headlines()
    storage.close()

    assert items == (1,)
    assert observations == (2,)
    assert events == ["item_created", "item_observed"]
    assert {row.source_id for row in snapshots} == {
        "channel-local",
        "channel-world",
    }


@pytest.mark.parametrize(
    "external_ids",
    [(None, "story-1"), ("story-1", None)],
)
def test_item_identity_is_stable_when_external_id_appears_or_disappears(
    tmp_path: Path,
    external_ids: tuple[str | None, str | None],
) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])

    for index, external_id in enumerate(external_ids):
        run_id = storage.start_fetch_run(source.id, "live")
        storage.record_success(
            source,
            run_id,
            200,
            ValidatedBatch(
                candidates=(
                    HeadlineCandidate(
                        title="Shared story",
                        url="https://example.com/story/1",
                        external_id=external_id,
                        position=1,
                    ),
                ),
                rejected_count=0,
            ),
            None,
            None,
            datetime.now(UTC) + timedelta(minutes=10),
            OBSERVED_AT + timedelta(minutes=index),
        )

    with sqlite3.connect(tmp_path / "parallax.db") as connection:
        items = connection.execute(
            "SELECT identity_key, external_id FROM items"
        ).fetchall()
        observations = connection.execute(
            "SELECT COUNT(*) FROM observations"
        ).fetchone()
    storage.close()

    assert items == [("external:fixture:story-1", "story-1")]
    assert observations == (1,)


def test_distinct_external_ids_with_same_url_remain_distinct(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])

    for index, external_id in enumerate(("story-1", "story-2")):
        run_id = storage.start_fetch_run(source.id, "live")
        storage.record_success(
            source,
            run_id,
            200,
            ValidatedBatch(
                candidates=(
                    HeadlineCandidate(
                        title=f"Story {index}",
                        url="https://example.com/story",
                        external_id=external_id,
                    ),
                ),
                rejected_count=0,
            ),
            None,
            None,
            datetime.now(UTC) + timedelta(minutes=10),
            OBSERVED_AT + timedelta(minutes=index),
        )

    with sqlite3.connect(tmp_path / "parallax.db") as connection:
        identities = connection.execute(
            "SELECT identity_key FROM items ORDER BY identity_key"
        ).fetchall()
    storage.close()

    assert identities == [
        ("external:fixture:story-1",),
        ("external:fixture:story-2",),
    ]


def test_identity_and_alias_candidates_collapse_into_one_snapshot_entry(
    tmp_path: Path,
) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])

    first_run = storage.start_fetch_run(source.id, "live")
    storage.record_success(
        source,
        first_run,
        200,
        ValidatedBatch(
            candidates=(
                HeadlineCandidate(
                    title="Original headline",
                    url="https://example.com/old",
                    external_id="story-1",
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

    second_run = storage.start_fetch_run(source.id, "live")
    summary = storage.record_success(
        source,
        second_run,
        200,
        ValidatedBatch(
            candidates=(
                HeadlineCandidate(
                    title="Relocated headline",
                    url="https://example.com/new",
                    external_id="story-1",
                    position=2,
                ),
                HeadlineCandidate(
                    title="Stale alias headline",
                    url="https://example.com/old",
                    position=1,
                ),
            ),
            rejected_count=0,
        ),
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
        OBSERVED_AT + timedelta(minutes=5),
    )

    rows = storage.latest_snapshot_headlines()
    runs = storage.recent_fetch_runs()
    changes = storage.changes_after(0)
    with sqlite3.connect(tmp_path / "parallax.db") as connection:
        snapshot_count = connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()
        entry_count = connection.execute(
            "SELECT COUNT(*) FROM snapshot_entries"
        ).fetchone()
        versions = connection.execute(
            "SELECT title FROM item_versions ORDER BY id"
        ).fetchall()
    storage.close()

    assert summary.item_count == 1
    assert summary.new_item_count == 0
    assert summary.new_version_count == 1
    assert runs[0].status == "success"
    assert runs[0].item_count == 1
    assert snapshot_count == (2,)
    assert entry_count == (2,)
    assert [row.title for row in rows] == ["Relocated headline"]
    assert [row.position for row in rows] == [2]
    assert [row.url for row in rows] == ["https://example.com/new"]
    assert versions == [("Original headline",), ("Relocated headline",)]
    assert [event.event_type for event in changes] == [
        "item_created",
        "headline_version_created",
    ]


def test_two_recorded_aliases_collapse_into_one_snapshot_entry(
    tmp_path: Path,
) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])

    for index, url in enumerate(("https://example.com/old", "https://example.com/new")):
        run_id = storage.start_fetch_run(source.id, "live")
        storage.record_success(
            source,
            run_id,
            200,
            ValidatedBatch(
                candidates=(
                    HeadlineCandidate(
                        title="Headline",
                        url=url,
                        external_id="story-1",
                        position=index + 1,
                    ),
                ),
                rejected_count=0,
            ),
            None,
            None,
            datetime.now(UTC) + timedelta(minutes=10),
            OBSERVED_AT + timedelta(minutes=index),
        )

    run_id = storage.start_fetch_run(source.id, "live")
    summary = storage.record_success(
        source,
        run_id,
        200,
        ValidatedBatch(
            candidates=(
                HeadlineCandidate(
                    title="First alias",
                    url="https://example.com/old",
                    position=1,
                ),
                HeadlineCandidate(
                    title="Second alias",
                    url="https://example.com/new",
                    position=2,
                ),
            ),
            rejected_count=0,
        ),
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
        OBSERVED_AT + timedelta(minutes=5),
    )

    rows = storage.latest_snapshot_headlines()
    storage.close()

    assert summary.item_count == 1
    assert summary.new_item_count == 0
    assert summary.new_version_count == 1
    assert len(rows) == 1
    assert rows[0].title == "First alias"
    assert rows[0].position == 1


def test_browse_views_use_explicit_surfaces(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    news = _source(
        id="news-source",
        provider_id="shared",
        surfaces=("news",),
    )
    discover = _source(
        id="discover-source",
        provider_id="shared",
        surfaces=("discover",),
    )
    storage.sync_sources([news, discover])

    for source, observed_at in (
        (news, OBSERVED_AT),
        (discover, OBSERVED_AT + timedelta(minutes=5)),
    ):
        run_id = storage.start_fetch_run(source.id, "live")
        storage.record_success(
            source,
            run_id,
            200,
            ValidatedBatch(
                candidates=(
                    HeadlineCandidate(
                        title="Shared story",
                        url="https://example.com/shared/1",
                        external_id="shared-1",
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

    since = OBSERVED_AT - timedelta(days=1)
    until = OBSERVED_AT + timedelta(days=1)

    def browse(view: BrowseView) -> list[str]:
        rows = storage.browse_headlines(
            view=view, since=since, until=until, limit=10, offset=0
        )
        assert storage.count_browse_headlines(
            view=view, since=since, until=until
        ) == len(rows)
        return [row.source_id for row in rows]

    assert browse("all") == ["discover-source"]
    assert browse("news") == ["news-source"]
    assert browse("discover") == ["discover-source"]
    storage.close()


def test_browse_membership_is_per_observation(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    news = _source(id="news-source", surfaces=("news",))
    discover = _source(id="discover-source", surfaces=("discover",))
    storage.sync_sources([news, discover])

    def observe(source: Source, url: str, observed_at: datetime) -> None:
        run_id = storage.start_fetch_run(source.id, "live")
        storage.record_success(
            source,
            run_id,
            200,
            ValidatedBatch(
                candidates=(
                    HeadlineCandidate(
                        title="Headline",
                        url=url,
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

    observe(news, "https://example.com/news/1", OBSERVED_AT)
    observe(discover, "https://example.com/discover/1", OBSERVED_AT)
    observe(discover, "https://example.com/news/1", OBSERVED_AT + timedelta(minutes=1))

    since = OBSERVED_AT - timedelta(days=1)
    until = OBSERVED_AT + timedelta(days=1)
    news_rows = storage.browse_headlines(
        view="news", since=since, until=until, limit=10, offset=0
    )
    discover_rows = storage.browse_headlines(
        view="discover", since=since, until=until, limit=10, offset=0
    )
    all_rows = storage.browse_headlines(
        view="all", since=since, until=until, limit=10, offset=0
    )
    storage.close()

    assert [row.url for row in news_rows] == ["https://example.com/news/1"]
    assert {row.url for row in discover_rows} == {
        "https://example.com/discover/1",
        "https://example.com/news/1",
    }
    assert len(all_rows) == 2
    assert {
        row.source_id for row in all_rows if row.url == "https://example.com/news/1"
    } == {"discover-source"}


def test_current_title_tracks_a_b_a_within_one_second(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "archive.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    for micros, title in [(100000, "A"), (300000, "B"), (800000, "A")]:
        observed = OBSERVED_AT + timedelta(microseconds=micros)
        storage.record_success(
            source,
            storage.start_fetch_run(source.id, "live"),
            200,
            ValidatedBatch(
                (HeadlineCandidate(title, "https://example.com/1", "one"),), 0
            ),
            None,
            None,
            observed,
            observed,
        )
    rows = storage.browse_headlines(
        view="all",
        since=OBSERVED_AT,
        until=OBSERVED_AT + timedelta(seconds=1),
        limit=10,
        offset=0,
    )
    assert [row.title for row in rows] == ["A"]
    assert [row.title for row in storage.latest_snapshot_headlines()] == ["A"]
    versions = storage._connection.execute(
        "SELECT title, last_seen_at FROM item_versions ORDER BY id"
    ).fetchall()
    assert [(row[0], row[1]) for row in versions] == [
        ("A", "2026-09-18T12:00:00.800000+00:00"),
        ("B", "2026-09-18T12:00:00.300000+00:00"),
    ]
    storage.close()


def test_sync_empty_catalog_disables_all_without_deleting_history(
    tmp_path: Path,
) -> None:
    storage = Storage(tmp_path / "archive.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    run = storage.start_fetch_run(source.id, "live")
    storage.record_success(
        source,
        run,
        200,
        ValidatedBatch((HeadlineCandidate("A", "https://example.com/1"),), 0),
        None,
        None,
        OBSERVED_AT,
        OBSERVED_AT,
    )
    storage.sync_sources([])
    assert storage.sources() == ()
    assert storage.stream_states(enabled_only=True) == []
    assert storage.latest_snapshot_headlines() == []
    assert len(storage.latest_snapshot_headlines(enabled_only=False)) == 1
    storage.close()


def test_full_response_replaces_validators_while_304_preserves_omissions(
    tmp_path: Path,
) -> None:
    storage = Storage(tmp_path / "archive.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    batch = ValidatedBatch((HeadlineCandidate("A", "https://example.com/1"),), 0)
    storage.record_success(
        source,
        storage.start_fetch_run(source.id, "live"),
        200,
        batch,
        '"one"',
        "yesterday",
        OBSERVED_AT,
        OBSERVED_AT,
        "identity",
    )
    storage.record_not_modified(
        source.id,
        storage.start_fetch_run(source.id, "live"),
        '"two"',
        None,
        OBSERVED_AT,
        "identity",
    )
    state = storage.get_stream_state(source.id)
    assert (state.etag, state.last_modified, state.request_identity) == (
        '"two"',
        "yesterday",
        "identity",
    )
    storage.record_success(
        source,
        storage.start_fetch_run(source.id, "live"),
        200,
        batch,
        None,
        None,
        OBSERVED_AT,
        OBSERVED_AT,
        "identity",
    )
    state = storage.get_stream_state(source.id)
    assert (state.etag, state.last_modified) == (None, None)
    run = storage.start_fetch_run(source.id, "live")
    state = storage.get_stream_state(source.id)
    with pytest.raises(ValueError, match="request identity"):
        storage.record_not_modified(
            source.id, run, None, None, OBSERVED_AT, "different"
        )
    assert storage.get_stream_state(source.id) == state
    assert storage.recent_fetch_runs()[0].status == "running"
    storage.close()


def test_rollback_failure_does_not_replace_original_error(tmp_path: Path) -> None:
    class BrokenRollback(_CommitFailingConnection):
        def execute(self, sql: str) -> None:
            if sql == "ROLLBACK":
                raise KeyboardInterrupt("cleanup interrupted")
            super().execute(sql)

    storage = Storage(tmp_path / "archive.db")
    storage._connection.close()
    storage._connection = BrokenRollback()  # type: ignore[assignment]
    with (
        pytest.raises(sqlite3.OperationalError, match="disk I/O error"),
        foundation.transaction(storage._connection),
    ):
        pass
    storage.close()


@pytest.mark.parametrize("view", ["all", "news", "discover"])
@pytest.mark.parametrize(
    "selected_source", [None, "news", "discover", "disabled", "missing"]
)
@pytest.mark.parametrize("query", [None, "Shared", "Absent"])
def test_browse_count_and_rows_share_source_eligibility(
    tmp_path: Path, view: BrowseView, selected_source: str | None, query: str | None
) -> None:
    storage = Storage(tmp_path / "archive.db")
    storage.initialize()
    sources = [
        _source(id="news", surfaces=("news",)),
        _source(id="discover", surfaces=("discover",)),
        _source(id="disabled", surfaces=("news", "discover"), enabled=False),
    ]
    storage.sync_sources(sources)
    for source in sources:
        storage.record_success(
            source,
            storage.start_fetch_run(source.id, "live"),
            200,
            ValidatedBatch(
                (
                    HeadlineCandidate("Shared title", "https://example.com/shared"),
                    HeadlineCandidate(source.id, f"https://example.com/{source.id}"),
                ),
                0,
            ),
            None,
            None,
            OBSERVED_AT,
            OBSERVED_AT,
        )
    since, until = OBSERVED_AT - timedelta(days=1), OBSERVED_AT + timedelta(days=1)
    rows = storage.browse_headlines(
        view=view,
        since=since,
        until=until,
        source_id=selected_source,
        query=query,
        limit=100,
        offset=0,
    )
    count = storage.count_browse_headlines(
        view=view, since=since, until=until, source_id=selected_source, query=query
    )
    eligible = {
        source.id
        for source in sources
        if source.enabled
        and (view == "all" or view in source.surfaces)
        and (selected_source is None or source.id == selected_source)
    }
    expected = 0
    if eligible and query != "Absent":
        expected = 1 if query == "Shared" else len(eligible) + 1
    assert count == len(rows) == expected
    assert {row.source_id for row in rows} <= eligible
    storage.close()


@pytest.mark.parametrize("run_kind", ["live", "history"])
def test_abandoned_recovery_only_updates_live_state(
    tmp_path: Path, run_kind: str
) -> None:
    storage = Storage(tmp_path / "archive.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    try:
        kind = "history" if run_kind == "history" else "live"
        storage.start_fetch_run(source.id, kind)
        before = storage.get_stream_state(source.id)
        storage.recover_abandoned_runs()
        after = storage.get_stream_state(source.id)
        run = storage.recent_fetch_runs()[0]
        assert run.run_kind == kind
        assert run.status == "failed"
        assert run.error_type == "AbandonedRun"
        assert run.finished_at is not None and run.finished_at.tzinfo is UTC
        assert run.started_at.tzinfo is UTC
        if kind == "history":
            assert after == before
        else:
            assert after.consecutive_failures == before.consecutive_failures + 1
            assert after.next_run_at is not None
        storage.recover_abandoned_runs()
        assert storage.get_stream_state(source.id) == after
    finally:
        storage.close()


def test_history_url_updates_cannot_rewrite_live_snapshot(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "archive.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    live = HeadlineCandidate(
        "Live title",
        "https://example.com/live?utm_source=feed",
        external_id="story",
        position=1,
        metrics={"score": 42},
    )
    try:
        storage.record_success(
            source,
            storage.start_fetch_run(source.id, "live"),
            200,
            ValidatedBatch((live,), 0),
            "etag",
            "modified",
            OBSERVED_AT,
            OBSERVED_AT,
            "request",
        )
        before = storage.latest_snapshot_headlines()
        state = storage.get_stream_state(source.id)
        snapshot = storage._connection.execute(
            "SELECT * FROM snapshot_entries"
        ).fetchall()
        history = HeadlineCandidate(
            "History title",
            "https://example.com/history",
            external_id="story",
            position=9,
            metrics={"score": 1},
        )
        for _ in range(2):
            storage.record_history(
                source,
                storage.start_fetch_run(source.id, "history"),
                ObservedBatch.from_validated(
                    ValidatedBatch((history,), 0), OBSERVED_AT + timedelta(hours=1)
                ),
            )
        assert storage.latest_snapshot_headlines() == before
        assert before[0].url == live.url
        assert before[0].canonical_url == "https://example.com/live"
        assert storage.get_stream_state(source.id) == state
        assert (
            storage._connection.execute("SELECT * FROM snapshot_entries").fetchall()
            == snapshot
        )
        current = storage.browse_headlines(
            view="all",
            since=OBSERVED_AT,
            until=OBSERVED_AT + timedelta(days=1),
            limit=10,
            offset=0,
        )
        assert len(current) == 1
        assert current[0].item_id == before[0].item_id
        assert current[0].url == history.url
        assert current[0].title == history.title
        assert [run.new_version_count for run in storage.recent_fetch_runs()] == [
            0,
            1,
            1,
        ]
    finally:
        storage.close()


@pytest.mark.parametrize("run_kind", ["live", "history"])
@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), object()])
def test_non_json_metrics_roll_back_whole_batch(
    tmp_path: Path,
    run_kind: str,
    bad: Any,
) -> None:
    storage = Storage(tmp_path / "archive.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    kind = "history" if run_kind == "history" else "live"
    run = storage.start_fetch_run(source.id, kind)
    before = storage.get_stream_state(source.id)
    batch = ValidatedBatch(
        (
            HeadlineCandidate(
                "Valid", "https://example.com/valid", metrics={"value": 1}
            ),
            HeadlineCandidate(
                "Invalid", "https://example.com/invalid", metrics={"nested": [bad]}
            ),
        ),
        0,
    )
    try:
        with pytest.raises(ValueError, match="candidate metrics.*finite JSON"):
            if kind == "history":
                storage.record_history(
                    source, run, ObservedBatch.from_validated(batch, OBSERVED_AT)
                )
            else:
                storage.record_success(
                    source, run, 200, batch, None, None, OBSERVED_AT, OBSERVED_AT
                )
        assert storage.get_stream_state(source.id) == before
        assert storage.changes_after(0) == []
        assert storage.latest_snapshot_headlines() == []
        assert storage.recent_fetch_runs()[0].status == "running"
        for table in (
            "items",
            "item_versions",
            "observations",
            "snapshots",
            "item_url_identities",
        ):
            assert (
                storage._connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[
                    0
                ]
                == 0
            )
    finally:
        storage.close()


def test_old_layout_fails_with_recreation_guidance(tmp_path: Path) -> None:
    database = tmp_path / "old.db"
    with sqlite3.connect(database) as connection:
        connection.executescript(
            "CREATE TABLE schema_meta(version INTEGER); "
            "INSERT INTO schema_meta VALUES (1);"
        )
    for read_only in (False, True):
        with pytest.raises(RuntimeError, match="older schema layout.*recreate"):
            Storage(database, read_only=read_only)


def test_history_run_cannot_commit_a_live_snapshot(tmp_path: Path) -> None:
    storage = Storage(tmp_path / "archive.db")
    storage.initialize()
    source = _source()
    storage.sync_sources([source])
    before = storage.get_stream_state(source.id)
    try:
        run = storage.start_fetch_run(source.id, "history")
        with pytest.raises(ValueError, match="running live run"):
            storage.record_success(
                source,
                run,
                200,
                ValidatedBatch(
                    (HeadlineCandidate("Title", "https://example.com/1"),), 0
                ),
                None,
                None,
                OBSERVED_AT,
                OBSERVED_AT,
            )
        assert storage.get_stream_state(source.id) == before
        assert storage.latest_snapshot_headlines() == []
        assert storage.recent_fetch_runs()[0].status == "running"
    finally:
        storage.close()
