"""Tests for the streaming seam and the Polymarket websocket source.

Every message here is either a real recorded frame (see
tests/fixtures/live/README.md) or a small edit of one, and the parser is
pure, so nothing in this module touches the network.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from opentape.errors import LiveError
from opentape.events import BookLevel
from opentape.live.base import BookQuote
from opentape.live.polymarket import PolymarketLive
from opentape.live.polymarket_stream import PolymarketStream
from opentape.live.stream import BookMirror, StreamBook, StreamLevel, StreamTrade
from tests.wsserver import load_messages

LIVE_FIXTURES = Path(__file__).parent / "fixtures" / "live"
MARKET = "lol-ig1-lgd-2026-09-08"
YES = "114602688616062054693464639818826590702731982636255124778328943187816056373369"
NO = "112222077780534073126353733665449794547376835979729917699348268168151417239731"


def load(name: str) -> Any:
    return json.loads((LIVE_FIXTURES / name).read_text())


def recorded_messages() -> list[str]:
    return load_messages(LIVE_FIXTURES / "polymarket_ws.jsonl")


class FakeFetcher:
    def __init__(self, routes: dict[str, Any]) -> None:
        self.routes = routes

    def __call__(self, url: str, params: dict[str, Any] | None = None) -> Any:
        for needle, payload in self.routes.items():
            if needle in url:
                return payload
        raise AssertionError(f"no fake route matches {url}")


def stream(described: bool = True) -> PolymarketStream:
    market = load("polymarket_ws_market.json")
    fetch = FakeFetcher(
        {
            # A slug is resolved to a condition id through gamma first.
            "gamma-api": [{"conditionId": market["condition_id"]}],
            "/markets/": market,
        }
    )
    source = PolymarketStream(PolymarketLive(fetch))
    if described:
        source.describe(MARKET)
    return source


def ts(millis: int) -> datetime:
    return datetime.fromtimestamp(millis / 1000, tz=UTC)


# -- setup ----------------------------------------------------------------


def test_describe_names_the_market_and_both_outcomes() -> None:
    described = stream().describe(MARKET)
    assert described.market_id == MARKET
    assert described.status == "open"
    assert described.outcomes == ("Invictus Gaming", "LGD Gaming")


def test_subscribing_asks_for_both_outcome_tokens() -> None:
    """A fill is published against the token it executed on."""
    payload = json.loads(stream().subscribe_message([MARKET]))
    assert payload["type"] == "market"
    assert sorted(payload["assets_ids"]) == sorted([YES, NO])


def test_subscribing_before_describing_says_so_instead_of_sending_nothing() -> None:
    with pytest.raises(LiveError, match="describe\\(\\) must run first"):
        stream(described=False).subscribe_message([MARKET])


# -- parsing --------------------------------------------------------------


def test_the_recorded_session_parses_into_every_update_type() -> None:
    source = stream()
    books = levels = trades = 0
    for message in recorded_messages():
        for update in source.parse(message):
            books += isinstance(update, StreamBook)
            levels += isinstance(update, StreamLevel)
            trades += isinstance(update, StreamTrade)
    # Six book messages were recorded, three of them for the NO token,
    # which this source deliberately ignores.
    assert (books, trades) == (3, 2)
    assert levels > 50


def test_an_empty_frame_is_not_an_error() -> None:
    """The venue sends empty text frames as a keepalive."""
    assert stream().parse("") == []
    assert stream().parse("   ") == []


def test_an_unknown_event_type_is_ignored_rather_than_refused() -> None:
    """A feed may publish event types a consumer did not ask about."""
    message = json.dumps({"event_type": "tick_size_change", "asset_id": YES, "market": "0x1"})
    assert stream().parse(message) == []


def test_an_asset_this_capture_does_not_track_is_ignored() -> None:
    message = json.dumps(
        {"event_type": "book", "asset_id": "999", "timestamp": "1788786313298", "bids": []}
    )
    assert stream().parse(message) == []


def test_a_message_that_is_not_json_is_refused() -> None:
    with pytest.raises(LiveError, match="not JSON"):
        stream().parse("<html>rate limited</html>")


def test_the_yes_book_is_taken_and_the_no_book_is_not() -> None:
    """The NO book is the YES book mirrored; both would double it."""
    source = stream()
    first = json.loads(recorded_messages()[0])
    assert len(first) == 2, "the recorded first frame carries both tokens' books"
    updates = source.parse(recorded_messages()[0])
    assert len(updates) == 1
    book = updates[0]
    assert isinstance(book, StreamBook)
    assert book.market_id == MARKET
    assert book.quote.bids[0].price > book.quote.bids[-1].price, "bids sort high to low"
    assert book.quote.asks[0].price < book.quote.asks[-1].price, "asks sort low to high"


def test_a_price_change_becomes_one_level_per_yes_entry() -> None:
    message = json.dumps(
        {
            "event_type": "price_change",
            "market": "0xd23b",
            "timestamp": "1788786330404",
            "price_changes": [
                {"asset_id": YES, "price": "0.62", "size": "32060.41", "side": "BUY"},
                {"asset_id": NO, "price": "0.38", "size": "32060.41", "side": "SELL"},
            ],
        }
    )
    updates = stream().parse(message)
    assert len(updates) == 1, "the NO entry restates the YES one and is not a second change"
    level = updates[0]
    assert isinstance(level, StreamLevel)
    assert (level.side, level.price, level.size) == ("bid", 0.62, 32060.41)
    assert level.ts == ts(1788786330404)


def test_a_resting_sell_is_an_ask_and_a_zero_size_removes_the_level() -> None:
    message = json.dumps(
        {
            "event_type": "price_change",
            "timestamp": "1788786330404",
            "price_changes": [{"asset_id": YES, "price": "0.91", "size": "0", "side": "SELL"}],
        }
    )
    level = stream().parse(message)[0]
    assert isinstance(level, StreamLevel)
    assert (level.side, level.price, level.size) == ("ask", 0.91, 0.0)


def test_a_yes_trade_is_taken_as_published() -> None:
    message = json.dumps(
        {
            "event_type": "last_trade_price",
            "asset_id": YES,
            "price": "0.62",
            "size": "31.74",
            "side": "SELL",
            "timestamp": "1788786353325",
            "transaction_hash": "0x99fc",
        }
    )
    trade = stream().parse(message)[0]
    assert isinstance(trade, StreamTrade)
    assert (trade.tick.price, trade.tick.size, trade.tick.side) == (0.62, 31.74, "sell")


def test_a_no_trade_is_converted_into_yes_terms() -> None:
    """A BUY of NO at p is a SELL of YES at 1 - p."""
    message = json.dumps(
        {
            "event_type": "last_trade_price",
            "asset_id": NO,
            "price": "0.38",
            "size": "31.74",
            "side": "BUY",
            "timestamp": "1788786353325",
            "transaction_hash": "0x99fc",
        }
    )
    trade = stream().parse(message)[0]
    assert isinstance(trade, StreamTrade)
    assert (trade.tick.price, trade.tick.side) == (0.62, "sell")


def test_one_fill_published_against_both_tokens_reduces_to_one_key() -> None:
    """The dedupe key is built from the YES-terms view, not the raw one."""
    source = stream()
    common = {
        "event_type": "last_trade_price",
        "size": "31.74",
        "timestamp": "1788786353325",
        "transaction_hash": "0x99fc",
    }
    as_yes = source.parse(json.dumps({**common, "asset_id": YES, "price": "0.62", "side": "SELL"}))
    as_no = source.parse(json.dumps({**common, "asset_id": NO, "price": "0.38", "side": "BUY"}))
    assert as_yes[0].tick.trade_id == as_no[0].tick.trade_id


def test_two_different_fills_keep_different_keys() -> None:
    source = stream()
    common = {
        "event_type": "last_trade_price",
        "asset_id": YES,
        "side": "BUY",
        "timestamp": "1788786353325",
        "transaction_hash": "0x99fc",
    }
    one = source.parse(json.dumps({**common, "price": "0.62", "size": "31.74"}))
    two = source.parse(json.dumps({**common, "price": "0.63", "size": "31.74"}))
    assert one[0].tick.trade_id != two[0].tick.trade_id


@pytest.mark.parametrize(
    "event, message",
    [
        (
            {
                "event_type": "last_trade_price",
                "asset_id": YES,
                "price": "0.6",
                "size": "1",
                "side": "HOLD",
                "timestamp": "1788786353325",
            },
            "side must be BUY or SELL",
        ),
        (
            {
                "event_type": "last_trade_price",
                "asset_id": YES,
                "price": "1.4",
                "size": "1",
                "side": "BUY",
                "timestamp": "1788786353325",
            },
            "is above 1.0",
        ),
        (
            {
                "event_type": "last_trade_price",
                "asset_id": YES,
                "price": "0.6",
                "size": "-1",
                "side": "BUY",
                "timestamp": "1788786353325",
            },
            "is below 0.0",
        ),
        (
            {"event_type": "book", "asset_id": YES, "timestamp": "not-a-number", "bids": []},
            "bad timestamp",
        ),
        (
            {"event_type": "book", "asset_id": YES, "timestamp": "1788786313298", "bids": {}},
            "expected an array of levels",
        ),
        (
            {"event_type": "price_change", "timestamp": "1788786313298", "price_changes": {}},
            "no price_changes array",
        ),
    ],
)
def test_a_malformed_payload_is_refused_by_name(event: dict[str, Any], message: str) -> None:
    with pytest.raises(LiveError, match=message):
        stream().parse(json.dumps(event))


# -- the mirror -----------------------------------------------------------


def book(bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> StreamBook:
    return StreamBook(
        market_id="M1",
        ts=ts(1788786313298),
        quote=BookQuote(
            bids=tuple(BookLevel(p, s) for p, s in bids),
            asks=tuple(BookLevel(p, s) for p, s in asks),
        ),
    )


def change(side: str, price: float, size: float) -> StreamLevel:
    return StreamLevel(market_id="M1", ts=ts(1788786330404), side=side, price=price, size=size)


def test_a_level_update_before_any_snapshot_is_dropped() -> None:
    """Half a book is worse than an honest gap."""
    mirror = BookMirror()
    assert mirror.level(change("bid", 0.62, 100.0), source="s") == []
    assert not mirror.has("M1")


def test_a_snapshot_seeds_the_book_and_emits_it() -> None:
    mirror = BookMirror()
    events, check = mirror.snapshot(book([(0.62, 100.0)], [(0.63, 50.0)]), source="s")
    assert check is None, "there was nothing to compare the first snapshot against"
    assert len(events) == 1
    assert events[0].bids == (BookLevel(0.62, 100.0),)


def test_a_level_size_replaces_rather_than_adds() -> None:
    """Established by replaying a recorded session both ways."""
    mirror = BookMirror()
    mirror.snapshot(book([(0.62, 100.0)], []), source="s")
    events = mirror.level(change("bid", 0.62, 30.0), source="s")
    assert events[0].size == 30.0
    _, check = mirror.snapshot(book([(0.62, 30.0)], []), source="s")
    assert check is not None and check.agreed


def test_a_zero_size_removes_the_level() -> None:
    mirror = BookMirror()
    mirror.snapshot(book([(0.62, 100.0), (0.61, 50.0)], []), source="s")
    events = mirror.level(change("bid", 0.62, 0.0), source="s")
    assert events[0].size == 0.0
    _, check = mirror.snapshot(book([(0.61, 50.0)], []), source="s")
    assert check is not None and check.agreed


def test_a_republished_level_at_the_size_it_already_had_writes_nothing() -> None:
    """A delta must mean a change happened."""
    mirror = BookMirror()
    mirror.snapshot(book([(0.62, 100.0)], []), source="s")
    assert mirror.level(change("bid", 0.62, 100.0), source="s") == []


def test_a_divergence_is_reported_with_the_levels_that_disagree() -> None:
    mirror = BookMirror()
    mirror.snapshot(book([(0.62, 100.0)], []), source="s")
    _, check = mirror.snapshot(book([(0.62, 555.0)], []), source="s")
    assert check is not None and not check.agreed
    assert check.differences["bid"] == {0.62: (100.0, 555.0)}
    assert "DIVERGED" in check.summary()


def test_the_published_snapshot_wins_after_a_divergence() -> None:
    mirror = BookMirror()
    mirror.snapshot(book([(0.62, 100.0)], []), source="s")
    mirror.snapshot(book([(0.62, 555.0)], []), source="s")
    _, check = mirror.snapshot(book([(0.62, 555.0)], []), source="s")
    assert check is not None and check.agreed


def test_dropping_a_book_stops_deltas_until_the_next_snapshot() -> None:
    """What a reconnect does: the held book is of unknown age."""
    mirror = BookMirror()
    mirror.snapshot(book([(0.62, 100.0)], []), source="s")
    mirror.drop("M1")
    assert mirror.level(change("bid", 0.62, 30.0), source="s") == []


def test_the_recorded_session_rebuilds_the_venues_own_next_snapshot() -> None:
    """The whole claim of a streamed tape, checked against real data.

    The recording holds three YES books with real level changes between
    them. Applying only those changes to the first book has to reproduce
    the second exactly, and then the third, or the deltas written to a
    tape would not be enough to rebuild the market.
    """
    source = stream()
    mirror = BookMirror()
    checks = []
    for message in recorded_messages():
        for update in source.parse(message):
            if isinstance(update, StreamBook):
                _, check = mirror.snapshot(update, source="s")
                if check is not None:
                    checks.append(check)
            elif isinstance(update, StreamLevel):
                mirror.level(update, source="s")
    assert len(checks) == 2, "two comparisons were possible in the recording"
    assert all(check.agreed for check in checks), [c.summary() for c in checks]
