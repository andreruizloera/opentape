"""DuckDB sql() correctness tests."""

from __future__ import annotations

import polars as pl
import pytest

from opentape import OpenTapeError, Tape


def test_trades_view_matches_frame_filter(sample_tape: Tape) -> None:
    out = sample_tape.sql("SELECT * FROM trades ORDER BY seq")
    expected = sample_tape.frame.filter(pl.col("event_type") == "trade")
    assert out.height == expected.height == 3
    assert out.get_column("price").to_list() == expected.get_column("price").to_list()
    assert out.get_column("trade_id").to_list() == ["t-1", "t-2", "t-3"]


def test_price_predicate(sample_tape: Tape) -> None:
    out = sample_tape.sql("SELECT seq, price FROM trades WHERE price > 0.59 ORDER BY seq")
    assert out.get_column("seq").to_list() == [3, 5]


def test_all_views_exist_with_expected_counts(sample_tape: Tape) -> None:
    counts = {
        "markets": 1,
        "snapshots": 1,
        "deltas": 1,
        "trades": 3,
        "status": 2,
        "resolutions": 1,
        "events": 9,
    }
    for view, expected in counts.items():
        got = sample_tape.sql(f"SELECT count(*) AS n FROM {view}").item()
        assert got == expected, view


def test_resolution_settlement_alias(sample_tape: Tape) -> None:
    out = sample_tape.sql("SELECT outcome, settlement FROM resolutions")
    assert out.row(0) == ("YES", 1.0)


def test_join_across_views(sample_tape: Tape) -> None:
    out = sample_tape.sql(
        """
        SELECT t.market_id, count(*) AS n_trades, r.outcome AS resolved
        FROM trades t JOIN resolutions r USING (market_id)
        GROUP BY t.market_id, r.outcome
        """
    )
    assert out.row(0) == ("M1", 3, "YES")


def test_timestamps_come_back_utc(sample_tape: Tape) -> None:
    out = sample_tape.sql("SELECT ts FROM trades LIMIT 1")
    assert out.get_column("ts").dtype == pl.Datetime("us", "UTC")


def test_aggregate_vwap(sample_tape: Tape) -> None:
    out = sample_tape.sql("SELECT sum(price * size) / sum(size) AS vwap FROM trades")
    vwap = (0.60 * 100 + 0.61 * 50 + 0.58 * 80) / 230
    assert out.item() == pytest.approx(vwap)


def test_nested_book_columns_queryable(sample_tape: Tape) -> None:
    out = sample_tape.sql("SELECT bids[1].price AS best_bid, len(asks) AS ask_depth FROM snapshots")
    assert out.row(0) == (0.58, 2)


def test_sql_error_is_clean(sample_tape: Tape) -> None:
    with pytest.raises(OpenTapeError, match="SQL error"):
        sample_tape.sql("SELECT * FROM not_a_view")
