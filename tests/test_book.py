"""Tests for order book reconstruction."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from opentape.book import reconstruct
from opentape.errors import OpenTapeError
from opentape.events import BookDelta, BookLevel, Event, OrderBookSnapshot, Trade
from opentape.live.base import BookQuote, BookTracker
from opentape.tape import Tape

START = datetime(2026, 3, 2, 14, 30, tzinfo=UTC)


def ts(second: float) -> datetime:
    return START + timedelta(seconds=second)


def snapshot(second: float, bids: dict[float, float], asks: dict[float, float]) -> Event:
    return OrderBookSnapshot(
        seq=0,
        ts=ts(second),
        market_id="M",
        source="test",
        outcome="YES",
        bids=tuple(BookLevel(p, s) for p, s in sorted(bids.items(), reverse=True)),
        asks=tuple(BookLevel(p, s) for p, s in sorted(asks.items())),
    )


def delta(second: float, side: str, price: float, size: float, market: str = "M") -> Event:
    return BookDelta(
        seq=0,
        ts=ts(second),
        market_id=market,
        source="test",
        outcome="YES",
        side=side,
        price=price,
        size=size,
    )


def test_a_snapshot_alone_is_the_book() -> None:
    book = reconstruct([snapshot(0, {0.6: 10, 0.59: 5}, {0.62: 8})], market_id="M")
    assert [(lv.price, lv.size) for lv in book.bids] == [(0.6, 10.0), (0.59, 5.0)]
    assert [(lv.price, lv.size) for lv in book.asks] == [(0.62, 8.0)]
    assert book.deltas_applied == 0
    assert book.as_of == ts(0)


def test_a_delta_sets_the_new_absolute_size() -> None:
    book = reconstruct(
        [snapshot(0, {0.6: 10}, {}), delta(1, "bid", 0.6, 25)],
        market_id="M",
    )
    assert [(lv.price, lv.size) for lv in book.bids] == [(0.6, 25.0)]
    assert book.deltas_applied == 1


def test_a_delta_can_add_a_level_the_snapshot_did_not_have() -> None:
    book = reconstruct([snapshot(0, {0.6: 10}, {}), delta(1, "bid", 0.55, 40)], market_id="M")
    assert [lv.price for lv in book.bids] == [0.6, 0.55]


def test_a_delta_of_size_zero_removes_the_level() -> None:
    book = reconstruct(
        [snapshot(0, {0.6: 10, 0.59: 5}, {}), delta(1, "bid", 0.6, 0)], market_id="M"
    )
    assert [lv.price for lv in book.bids] == [0.59]


def test_a_later_snapshot_replaces_the_book_rather_than_merging_into_it() -> None:
    # A snapshot is the whole book, so a level the first one had and
    # the second does not is gone, not still resting.
    book = reconstruct(
        [snapshot(0, {0.6: 10, 0.59: 5}, {}), delta(1, "bid", 0.58, 3), snapshot(2, {0.5: 1}, {})],
        market_id="M",
    )
    assert [(lv.price, lv.size) for lv in book.bids] == [(0.5, 1.0)]
    assert book.snapshot_ts == ts(2)
    assert book.deltas_applied == 0


def test_deltas_before_any_snapshot_are_skipped_not_applied() -> None:
    # Applying them would invent a book made only of levels that moved.
    book = reconstruct(
        [delta(0, "bid", 0.9, 100), snapshot(1, {0.6: 10}, {}), delta(2, "bid", 0.59, 4)],
        market_id="M",
    )
    assert [lv.price for lv in book.bids] == [0.6, 0.59]
    assert book.deltas_applied == 1


def test_deltas_with_no_snapshot_at_all_are_an_error_not_a_partial_book() -> None:
    with pytest.raises(OpenTapeError, match="cannot be rebuilt from deltas alone"):
        reconstruct([delta(0, "bid", 0.6, 10)], market_id="M")


def test_events_for_other_markets_are_ignored() -> None:
    book = reconstruct(
        [snapshot(0, {0.6: 10}, {}), delta(1, "bid", 0.6, 99, market="OTHER")],
        market_id="M",
    )
    assert [(lv.price, lv.size) for lv in book.bids] == [(0.6, 10.0)]


def test_trades_do_not_move_the_book() -> None:
    # Only book events are book events; a trade is a consequence, and
    # the venue reports the resulting level change separately.
    trade = Trade(seq=0, ts=ts(1), market_id="M", source="test", price=0.6, size=5.0, side="buy")
    book = reconstruct([snapshot(0, {0.6: 10}, {}), trade], market_id="M")
    assert [(lv.price, lv.size) for lv in book.bids] == [(0.6, 10.0)]


def test_as_of_is_the_last_event_applied() -> None:
    book = reconstruct([snapshot(0, {0.6: 10}, {}), delta(7, "bid", 0.6, 11)], market_id="M")
    assert book.as_of == ts(7)


def test_top_of_book_spread_and_mid() -> None:
    book = reconstruct([snapshot(0, {0.6: 10, 0.5: 1}, {0.62: 8, 0.7: 2})], market_id="M")
    assert book.best_bid == BookLevel(0.6, 10.0)
    assert book.best_ask == BookLevel(0.62, 8.0)
    assert book.spread == 0.02
    assert book.mid == 0.61


def test_a_one_sided_book_has_no_spread_or_mid() -> None:
    # Returning the one price that exists would read as a mid and be
    # wrong by up to the whole spread.
    book = reconstruct([snapshot(0, {0.6: 10}, {})], market_id="M")
    assert book.spread is None
    assert book.mid is None
    assert book.best_ask is None


def test_depth_truncates_both_sides() -> None:
    book = reconstruct([snapshot(0, {0.6: 1, 0.59: 1, 0.58: 1}, {0.62: 1, 0.63: 1})], market_id="M")
    shallow = book.depth(2)
    assert len(shallow.bids) == 2 and len(shallow.asks) == 2
    assert shallow.deltas_applied == book.deltas_applied


# -- the round trip -------------------------------------------------------


def test_capturing_a_book_and_rebuilding_it_gives_the_same_book() -> None:
    """The delta encoder and the folder must be inverse.

    This is the property the whole snapshot-plus-delta representation
    rests on: whatever a capture writes for a sequence of books, folding
    it back has to return the last of those books exactly. A rounding
    slip or a missed removal on either side shows up here and nowhere
    else.
    """
    quotes = [
        BookQuote(bids=(BookLevel(0.60, 10.0), BookLevel(0.59, 5.0)), asks=(BookLevel(0.62, 8.0),)),
        # a size change, a new level, and a level that disappears
        BookQuote(bids=(BookLevel(0.60, 25.0), BookLevel(0.58, 3.0)), asks=(BookLevel(0.62, 8.0),)),
        # both sides move, and the complementary price exercises rounding
        BookQuote(
            bids=(BookLevel(0.60, 25.0),),
            asks=(BookLevel(round(1.0 - 0.07, 6), 12.0), BookLevel(0.62, 1.0)),
        ),
        # everything on one side goes away
        BookQuote(bids=(), asks=(BookLevel(0.62, 1.0),)),
    ]
    tracker = BookTracker()
    events: list[Event] = []
    for i, q in enumerate(quotes):
        events.extend(tracker.update("M", ts(i), q, source="test"))

    book = reconstruct(events, market_id="M")
    assert [(lv.price, lv.size) for lv in book.bids] == [
        (lv.price, lv.size) for lv in quotes[-1].bids
    ]
    assert [(lv.price, lv.size) for lv in book.asks] == sorted(
        (lv.price, lv.size) for lv in quotes[-1].asks
    )


def test_rebuilding_at_each_step_matches_the_book_at_that_step() -> None:
    quotes = [
        BookQuote(bids=(BookLevel(0.60, 10.0),), asks=(BookLevel(0.62, 8.0),)),
        BookQuote(bids=(BookLevel(0.60, 4.0), BookLevel(0.55, 9.0)), asks=()),
        BookQuote(bids=(BookLevel(0.55, 9.0),), asks=(BookLevel(0.70, 2.0),)),
    ]
    tracker = BookTracker()
    events: list[Event] = []
    for i, q in enumerate(quotes):
        events.extend(tracker.update("M", ts(i), q, source="test"))

    for i, expected in enumerate(quotes):
        upto = [e for e in events if e.ts <= ts(i)]
        book = reconstruct(upto, market_id="M")
        assert [(lv.price, lv.size) for lv in book.bids] == [
            (lv.price, lv.size) for lv in expected.bids
        ]
        assert [(lv.price, lv.size) for lv in book.asks] == [
            (lv.price, lv.size) for lv in expected.asks
        ]


# -- Tape.book_at ---------------------------------------------------------


def tape_of(events: list[Event]) -> Tape:
    from dataclasses import replace

    return Tape.from_events([replace(e, seq=i) for i, e in enumerate(events)])


def test_book_at_defaults_to_the_end_of_the_tape() -> None:
    tape = tape_of([snapshot(0, {0.6: 10}, {}), delta(5, "bid", 0.6, 42)])
    assert tape.book_at("M").as_of == ts(5)


def test_book_at_stops_at_the_timestamp_asked_for() -> None:
    tape = tape_of([snapshot(0, {0.6: 10}, {}), delta(5, "bid", 0.6, 42)])
    book = tape.book_at("M", ts(2))
    assert [(lv.price, lv.size) for lv in book.bids] == [(0.6, 10.0)]
    assert book.as_of == ts(0)


def test_book_at_names_the_markets_it_does_have() -> None:
    tape = tape_of([snapshot(0, {0.6: 10}, {})])
    with pytest.raises(OpenTapeError, match="markets: M"):
        tape.book_at("NOPE")


def test_book_at_rejects_a_naive_timestamp() -> None:
    tape = tape_of([snapshot(0, {0.6: 10}, {})])
    with pytest.raises(OpenTapeError, match="naive"):
        tape.book_at("M", datetime(2026, 3, 2, 14, 30))


def test_book_at_before_the_first_snapshot_is_an_error() -> None:
    tape = tape_of([snapshot(10, {0.6: 10}, {})])
    with pytest.raises(OpenTapeError, match="no order book snapshot"):
        tape.book_at("M", ts(1))
