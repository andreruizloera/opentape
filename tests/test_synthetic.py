"""Tests for the synthetic sample generator and the bundled parquet."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from opentape import Tape
from opentape.events import OrderBookSnapshot, Resolution
from opentape.synthetic import SPECS, generate

SAMPLE = Path(__file__).parent.parent / "examples" / "sample.parquet"


def test_generator_is_deterministic() -> None:
    a = generate(seed=42)
    b = generate(seed=42)
    assert a.frame.equals(b.frame)


def test_different_seeds_differ() -> None:
    assert not generate(seed=42).frame.equals(generate(seed=7).frame)


def test_bundled_sample_matches_generator_output() -> None:
    # The committed parquet must be exactly what the committed script produces.
    assert SAMPLE.exists(), "run: python examples/generate_sample.py"
    assert Tape.read(SAMPLE).frame.equals(generate(seed=42).frame)


def test_sample_covers_every_event_type() -> None:
    counts = generate(seed=42).summary().event_counts
    assert all(counts[t] > 0 for t in counts), counts


def test_sample_prices_are_fractions_of_one() -> None:
    df = generate(seed=42).frame
    prices = df.filter(pl.col("price").is_not_null()).get_column("price")
    assert prices.min() >= 0.0
    assert prices.max() <= 1.0


def test_sample_sequence_is_dense_and_ordered() -> None:
    df = generate(seed=42).frame
    assert df.get_column("seq").to_list() == list(range(df.height))
    assert df.get_column("ts").is_sorted()


def test_sample_resolutions_match_specs() -> None:
    tape = generate(seed=42)
    resolutions = {e.market_id: e for e in tape.replay(speed="max") if isinstance(e, Resolution)}
    expected = {s.market_id: s.resolves_to for s in SPECS if s.resolves_to is not None}
    assert set(resolutions) == set(expected)
    for market_id, outcome in expected.items():
        res = resolutions[market_id]
        assert res.outcome == outcome
        assert res.settlement == (1.0 if outcome == "YES" else 0.0)


def test_sample_books_are_crossed_free_and_sorted() -> None:
    tape = generate(seed=42)
    snaps = [e for e in tape.replay(speed="max") if isinstance(e, OrderBookSnapshot)]
    assert snaps
    for snap in snaps:
        bid_prices = [lv.price for lv in snap.bids]
        ask_prices = [lv.price for lv in snap.asks]
        assert bid_prices == sorted(bid_prices, reverse=True)
        assert ask_prices == sorted(ask_prices)
        if bid_prices and ask_prices:
            assert bid_prices[0] < ask_prices[0]  # never crossed
