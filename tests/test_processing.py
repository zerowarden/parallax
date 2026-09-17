from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from parallax.config import SourceConfig
from parallax.domain import HeadlineCandidate, ValidatedBatch
from parallax.processing.changelog import ChangeLogReader
from parallax.storage import Storage


def test_independent_consumer_checkpoint(tmp_path: Path):
    storage = Storage(tmp_path / "parallax.db")
    storage.initialize()
    source = SourceConfig(
        id="fixture",
        name="Fixture",
        region="US",
        language="en-US",
        adapter="rss",
        url="https://example.com/rss.xml",
    )
    storage.sync_sources([source])
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
                    external_id="1",
                    position=1,
                ),
            ),
            rejected_count=0,
        ),
        None,
        None,
        datetime.now(UTC) + timedelta(minutes=10),
    )

    reader = ChangeLogReader(storage, "classifier-v1")
    events = list(reader.pending())
    assert len(events) == 1
    reader.checkpoint(events[-1].seq)
    assert list(reader.pending()) == []
    storage.close()
