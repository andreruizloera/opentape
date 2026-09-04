"""Schema round-trip and validation tests."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from opentape import SCHEMA_VERSION, SchemaError, Tape
from opentape.events import BookLevel, OrderBookSnapshot, Resolution, Trade, from_row, to_row
from opentape.schema import TAPE_SCHEMA, validate_frame
from tests.conftest import ts


def test_parquet_round_trip_preserves_events(sample_tape: Tape, tmp_path: Path) -> None:
    path = tmp_path / "tape.parquet"
    sample_tape.write(path)
    back = Tape.read(path)
    assert len(back) == len(sample_tape)
    assert back.frame.equals(sample_tape.frame)
    original = list(sample_tape.replay(speed="max"))
    reread = list(back.replay(speed="max"))
    assert reread == original


def test_round_trip_preserves_dtypes(sample_tape: Tape, tmp_path: Path) -> None:
    path = tmp_path / "tape.parquet"
    sample_tape.write(path)
    back = Tape.read(path)
    assert dict(back.frame.schema) == TAPE_SCHEMA


def test_schema_version_column_is_stamped(sample_tape: Tape) -> None:
    versions = sample_tape.frame.get_column("schema_version").unique().to_list()
    assert versions == [SCHEMA_VERSION]
    assert sample_tape.schema_version == SCHEMA_VERSION


def test_event_row_round_trip_every_type(sample_events) -> None:
    for event in sample_events:
        assert from_row(to_row(event)) == event


def test_book_levels_survive_round_trip(tmp_path: Path) -> None:
    snap = OrderBookSnapshot(
        seq=0,
        ts=ts(0),
        market_id="M",
        source="s",
        outcome="YES",
        bids=(BookLevel(0.41, 10.0),),
        asks=(BookLevel(0.44, 5.0), BookLevel(0.45, 9.0)),
    )
    tape = Tape.from_events([snap])
    path = tmp_path / "b.parquet"
    tape.write(path)
    (back,) = list(Tape.read(path).replay(speed="max"))
    assert isinstance(back, OrderBookSnapshot)
    assert back.bids == snap.bids
    assert back.asks == snap.asks


def test_missing_column_rejected() -> None:
    df = pl.DataFrame({"seq": [1]})
    with pytest.raises(SchemaError, match="missing required columns"):
        validate_frame(df)


def test_unknown_event_type_rejected(sample_tape: Tape) -> None:
    df = sample_tape.frame.with_columns(pl.lit("mystery").alias("event_type"))
    with pytest.raises(SchemaError, match="unknown event types"):
        validate_frame(df)


def test_future_schema_version_rejected(sample_tape: Tape) -> None:
    df = sample_tape.frame.with_columns(
        pl.lit(SCHEMA_VERSION + 1, dtype=pl.UInt16).alias("schema_version")
    )
    with pytest.raises(SchemaError, match="upgrade opentape"):
        validate_frame(df)


def test_null_seq_rejected(sample_tape: Tape) -> None:
    df = sample_tape.frame.with_columns(pl.lit(None, dtype=pl.UInt64).alias("seq"))
    with pytest.raises(SchemaError, match="seq and ts"):
        validate_frame(df)


def test_naive_timestamp_rejected() -> None:
    naive = ts(0).replace(tzinfo=None)
    with pytest.raises(SchemaError, match="naive"):
        to_row(Trade(seq=0, ts=naive, market_id="M", source="s", price=0.5, size=1.0))


def test_non_utc_timestamps_normalized_to_utc() -> None:
    from datetime import timedelta, timezone

    offset = timezone(timedelta(hours=-6))
    local = ts(0).astimezone(offset)
    tape = Tape.from_events(
        [Trade(seq=0, ts=local, market_id="M", source="s", price=0.5, size=1.0)]
    )
    assert tape.frame.get_column("ts").dtype == pl.Datetime("us", "UTC")
    (event,) = list(tape.replay(speed="max"))
    assert event.ts == ts(0)


def test_empty_tape_round_trip(tmp_path: Path) -> None:
    tape = Tape.from_events([])
    path = tmp_path / "empty.parquet"
    tape.write(path)
    back = Tape.read(path)
    assert len(back) == 0
    assert back.time_range() is None
    assert back.market_ids() == []


def test_resolution_settlement_round_trip(tmp_path: Path) -> None:
    res = Resolution(seq=0, ts=ts(0), market_id="M", source="s", outcome="NO", settlement=0.0)
    tape = Tape.from_events([res])
    path = tmp_path / "r.parquet"
    tape.write(path)
    (back,) = list(Tape.read(path).replay(speed="max"))
    assert back == res


def test_read_missing_file_gives_clean_error(tmp_path: Path) -> None:
    from opentape import OpenTapeError

    with pytest.raises(OpenTapeError, match="no such file"):
        Tape.read(tmp_path / "nope.parquet")
