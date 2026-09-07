"""Tests for the Kalshi and Polymarket live sources.

Every payload here is a real recorded response (see
tests/fixtures/live/README.md). The fetcher is injected, so nothing in
this module touches the network.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from opentape.errors import LiveError
from opentape.live.kalshi import KalshiLive
from opentape.live.polymarket import PolymarketLive

LIVE_FIXTURES = Path(__file__).parent / "fixtures" / "live"


def load(name: str) -> Any:
    return json.loads((LIVE_FIXTURES / name).read_text())


class FakeFetcher:
    """Answer by matching a substring of the requested URL."""

    def __init__(self, routes: dict[str, Any]) -> None:
        self.routes = routes
        self.calls: list[tuple[str, dict[str, Any] | None]] = []

    def __call__(self, url: str, params: dict[str, Any] | None = None) -> Any:
        self.calls.append((url, params))
        for needle, payload in self.routes.items():
            if needle in url:
                if isinstance(payload, Exception):
                    raise payload
                return payload
        raise AssertionError(f"no fake route matches {url}")


# -- Kalshi ---------------------------------------------------------------


def kalshi(**extra: Any) -> tuple[KalshiLive, FakeFetcher]:
    routes: dict[str, Any] = {
        "/orderbook": load("kalshi_orderbook.json"),
        "/markets/trades": load("kalshi_trades.json"),
        "/events": load("kalshi_events.json"),
        "/markets/": load("kalshi_market.json"),
    }
    routes.update(extra)
    fetch = FakeFetcher(routes)
    return KalshiLive(fetch), fetch


def test_kalshi_yes_levels_become_bids_in_descending_price_order() -> None:
    source, _ = kalshi()
    book = source.book("KXELONMARS-99")
    assert [lv.price for lv in book.bids] == [0.10, 0.09, 0.08, 0.07]
    assert book.bids[0].size == 111.0


def test_kalshi_no_levels_become_asks_at_the_complementary_price() -> None:
    # A resting NO bid at 0.88 is the same order as a YES ask at 0.12.
    source, _ = kalshi()
    book = source.book("KXELONMARS-99")
    assert [lv.price for lv in book.asks] == [0.12, 0.13, 0.14, 0.15]
    assert book.asks[0].size == 162.09


def test_kalshi_book_has_no_venue_timestamp() -> None:
    # The endpoint publishes none, so the daemon must stamp capture time.
    source, _ = kalshi()
    assert source.book("KXELONMARS-99").ts is None


def test_kalshi_accepts_the_legacy_integer_cent_shape() -> None:
    legacy = {"orderbook": {"yes": [[61, 500]], "no": [[37, 450]]}}
    source, _ = kalshi(**{"/orderbook": legacy})
    book = source.book("X")
    assert [(lv.price, lv.size) for lv in book.bids] == [(0.61, 500.0)]
    assert [(lv.price, lv.size) for lv in book.asks] == [(0.63, 450.0)]


def test_kalshi_an_empty_book_is_empty_not_an_error() -> None:
    source, _ = kalshi(**{"/orderbook": {"orderbook_fp": {"yes_dollars": [], "no_dollars": []}}})
    book = source.book("X")
    assert book.bids == () and book.asks == ()


def test_kalshi_zero_size_levels_are_dropped() -> None:
    source, _ = kalshi(
        **{"/orderbook": {"orderbook_fp": {"yes_dollars": [["0.60", "0"]], "no_dollars": []}}}
    )
    assert source.book("X").bids == ()


def test_kalshi_a_missing_orderbook_is_a_named_error() -> None:
    source, _ = kalshi(**{"/orderbook": {"nothing": True}})
    with pytest.raises(LiveError, match="no orderbook"):
        source.book("KXELONMARS-99")


def test_kalshi_an_out_of_range_price_is_rejected() -> None:
    source, _ = kalshi(
        **{"/orderbook": {"orderbook_fp": {"yes_dollars": [["1.40", "5"]], "no_dollars": []}}}
    )
    with pytest.raises(LiveError, match="outside"):
        source.book("X")


def test_kalshi_trades_map_to_yes_terms_and_sort_oldest_first() -> None:
    source, _ = kalshi()
    ticks = source.trades("KXBTC15M-26SEP070015-15")
    assert ticks
    assert all(ticks[i].ts <= ticks[i + 1].ts for i in range(len(ticks) - 1))
    # taker_side "no" is a canonical sell, and the price is the YES price.
    first = next(t for t in ticks if t.trade_id == "07219259-5ed9-b201-cae0-f1823a26c295")
    assert (first.side, first.price, first.size) == ("sell", 0.56, 12.94)


def test_kalshi_a_yes_taker_is_a_buy() -> None:
    source, _ = kalshi()
    tick = next(
        t for t in source.trades("X") if t.trade_id == "0721925a-0241-98f2-80a6-46ddca506c83"
    )
    assert tick.side == "buy"


def test_kalshi_a_trade_without_an_id_is_refused() -> None:
    # Without an id the trade cannot be deduplicated, so it would be
    # written once per poll. That is worse than failing loudly.
    payload = {"trades": [{"created_time": "2026-01-01T00:00:00Z", "taker_side": "yes"}]}
    source, _ = kalshi(**{"/markets/trades": payload})
    with pytest.raises(LiveError, match="deduplicated"):
        source.trades("X")


def test_kalshi_an_unknown_taker_side_is_refused() -> None:
    payload = {
        "trades": [
            {
                "trade_id": "t1",
                "created_time": "2026-01-01T00:00:00Z",
                "taker_side": "sideways",
                "yes_price_dollars": "0.5",
                "count_fp": "1",
            }
        ]
    }
    source, _ = kalshi(**{"/markets/trades": payload})
    with pytest.raises(LiveError, match="taker_side"):
        source.trades("X")


def test_kalshi_describe_maps_the_venue_status_word() -> None:
    source, _ = kalshi()
    described = source.describe("KXELONMARS-99")
    assert described.market_id == "KXELONMARS-99"
    assert described.status == "open"  # the venue says "active"
    assert "Elon Musk" in described.title


def test_kalshi_describe_keeps_an_unmapped_status_verbatim() -> None:
    source, _ = kalshi(**{"/markets/": {"market": {"ticker": "X", "status": "brand_new"}}})
    assert source.describe("X").status == "brand_new"


def test_kalshi_discovery_only_lists_markets_with_a_two_sided_quote() -> None:
    source, _ = kalshi()
    refs = source.list_markets(limit=50)
    assert refs
    listed = {r.market_id for r in refs}
    events = load("kalshi_events.json")["events"]
    for event in events:
        for market in event["markets"]:
            bid = float(market.get("yes_bid_dollars") or 0)
            ask = float(market.get("yes_ask_dollars") or 0)
            two_sided = 0.0 < bid < ask < 1.0
            assert (market["ticker"] in listed) == two_sided


def test_kalshi_discovery_respects_the_limit_and_the_search_text() -> None:
    source, _ = kalshi()
    assert len(source.list_markets(limit=1)) == 1
    assert source.list_markets(limit=50, search="zzz-no-such-market") == []


def test_kalshi_discovery_goes_through_events_not_the_flat_market_list() -> None:
    # The flat listing is dominated by auto-generated combination
    # markets, thousands of which have no book at all.
    source, fetch = kalshi()
    source.list_markets(limit=1)
    assert any("/events" in url for url, _ in fetch.calls)


# -- Polymarket -----------------------------------------------------------


def polymarket(**extra: Any) -> tuple[PolymarketLive, FakeFetcher]:
    routes: dict[str, Any] = {
        "/sampling-markets": load("polymarket_sampling_markets.json"),
        "/book": load("polymarket_book.json"),
        "data-api": load("polymarket_trades.json"),
        "gamma-api": [{"conditionId": load("polymarket_market.json")["condition_id"]}],
        "/markets/": load("polymarket_market.json"),
    }
    routes.update(extra)
    fetch = FakeFetcher(routes)
    return PolymarketLive(fetch), fetch


SLUG = "xi-jinping-out-before-2027"


def test_polymarket_book_sorts_bids_down_and_asks_up() -> None:
    source, _ = polymarket()
    book = source.book(SLUG)
    assert [lv.price for lv in book.bids] == [0.042, 0.041, 0.04, 0.039]
    assert [lv.price for lv in book.asks] == [0.043, 0.044, 0.045, 0.046]


def test_polymarket_book_keeps_the_venue_timestamp() -> None:
    # Unlike Kalshi, the CLOB publishes a book timestamp, so the tape
    # can record when the venue said the book was, not when we asked.
    source, _ = polymarket()
    ts = source.book(SLUG).ts
    assert ts is not None
    assert ts.year == 2026


def test_polymarket_a_no_token_trade_becomes_a_complementary_yes_trade() -> None:
    # A SELL of NO at 0.957 is a BUY of YES at 0.043.
    source, _ = polymarket()
    ticks = source.trades(SLUG)
    raw = load("polymarket_trades.json")
    sell_no = next(t for t in raw if t["side"] == "SELL" and t["price"] == 0.957)
    tick = next(t for t in ticks if t.size == sell_no["size"] and t.price == 0.043)
    assert tick.side == "buy"


def test_polymarket_the_no_side_flip_covers_both_directions() -> None:
    source, _ = polymarket()
    raw = load("polymarket_trades.json")
    market = load("polymarket_market.json")
    no_token = next(t["token_id"] for t in market["tokens"] if t["outcome"] == "No")
    ticks = source.trades(SLUG)
    for original, tick in zip(raw, sorted(ticks, key=lambda t: -t.ts.timestamp()), strict=False):
        if original["asset"] != no_token:
            continue
        assert tick.side == ("sell" if original["side"] == "BUY" else "buy")
        assert tick.price == pytest.approx(1.0 - original["price"], abs=1e-6)


def test_polymarket_trades_on_another_market_token_are_ignored() -> None:
    raw = load("polymarket_trades.json")
    stranger = dict(raw[0], asset="99999", transactionHash="0xdead")
    source, _ = polymarket(**{"data-api": [*raw, stranger]})
    assert len(source.trades(SLUG)) == len(raw)


def test_polymarket_trade_keys_are_distinct_for_distinct_fills() -> None:
    source, _ = polymarket()
    ticks = source.trades(SLUG)
    assert len({t.trade_id for t in ticks}) == len(ticks)


def test_polymarket_pins_yes_by_name_not_by_token_order() -> None:
    market = load("polymarket_market.json")
    flipped = dict(market, tokens=list(reversed(market["tokens"])))
    source, _ = polymarket(**{"/markets/": flipped})
    straight, _ = polymarket()
    assert source.describe(SLUG).outcomes == straight.describe(SLUG).outcomes == ("Yes", "No")
    assert source.book(SLUG).bids == straight.book(SLUG).bids


def test_polymarket_a_non_yes_no_binary_market_uses_the_first_outcome_as_yes() -> None:
    # Up/Down is just as binary as Yes/No, and it is the busiest series
    # on the venue, so refusing it would refuse the best data there is.
    market = dict(
        load("polymarket_market.json"),
        market_slug="btc-updown-5m",
        question="Bitcoin Up or Down",
        tokens=[
            {"token_id": "111", "outcome": "Up"},
            {"token_id": "222", "outcome": "Down"},
        ],
    )
    source, _ = polymarket(**{"/markets/": market, "gamma-api": [{"conditionId": "0xabc"}]})
    described = source.describe("btc-updown-5m")
    assert described.outcomes == ("Up", "Down")


def test_polymarket_a_market_that_is_not_binary_is_refused_by_name() -> None:
    market = dict(
        load("polymarket_market.json"),
        tokens=[{"token_id": str(i), "outcome": o} for i, o in enumerate("ABC")],
    )
    source, _ = polymarket(**{"/markets/": market})
    with pytest.raises(LiveError, match="3 outcome tokens"):
        source.describe(SLUG)


def test_polymarket_status_reflects_closed_and_halted_markets() -> None:
    base = load("polymarket_market.json")
    for patch, expected in (
        ({}, "open"),
        ({"closed": True}, "closed"),
        ({"active": False}, "halted"),
        ({"accepting_orders": False}, "halted"),
    ):
        source, _ = polymarket(**{"/markets/": dict(base, **patch)})
        assert source.describe(SLUG).status == expected


def test_polymarket_resolves_a_slug_through_gamma_but_not_a_condition_id() -> None:
    source, fetch = polymarket()
    source.describe(SLUG)
    assert any("gamma-api" in url for url, _ in fetch.calls)

    source, fetch = polymarket()
    source.describe(load("polymarket_market.json")["condition_id"])
    assert not any("gamma-api" in url for url, _ in fetch.calls)


def test_polymarket_an_unknown_slug_says_how_to_find_a_real_one() -> None:
    source, _ = polymarket(**{"gamma-api": []})
    with pytest.raises(LiveError, match="opentape markets"):
        source.describe("no-such-market")


def test_polymarket_a_resolved_market_is_only_fetched_once() -> None:
    source, fetch = polymarket()
    source.describe(SLUG)
    source.book(SLUG)
    source.trades(SLUG)
    assert sum(1 for url, _ in fetch.calls if "/markets/" in url) == 1


def test_polymarket_discovery_skips_closed_markets() -> None:
    doc = load("polymarket_sampling_markets.json")
    doc["data"][0]["closed"] = True
    closed_slug = doc["data"][0]["market_slug"]
    source, _ = polymarket(**{"/sampling-markets": doc})
    assert closed_slug not in {r.market_id for r in source.list_markets(limit=50)}


def test_the_two_sources_tag_the_transport_not_just_the_venue() -> None:
    # A polled tape and a streamed tape are different things, and the
    # source column has to say which one a reader is holding.
    assert KalshiLive.source_tag == "kalshi-rest-poll"
    assert PolymarketLive.source_tag == "polymarket-rest-poll"


# -- Polymarket: the lifecycle is not cached ------------------------------


class MutableFetcher(FakeFetcher):
    """A fetcher whose market document can be changed between calls.

    The market route answers ONLY the condition id, exactly as the real
    CLOB does. A fake that also answered the slug would have hidden a
    real bug: a refresh that re-requested the slug returned HTTP 404
    against the live venue, and the permissive fake passed.
    """

    def __init__(self, routes: dict[str, Any], market: dict[str, Any]) -> None:
        super().__init__(routes)
        self.market = market

    def __call__(self, url: str, params: dict[str, Any] | None = None) -> Any:
        if "/markets/" in url and "sampling" not in url:
            self.calls.append((url, params))
            if not url.endswith(self.market["condition_id"]):
                raise LiveError(f"polymarket: market not found for {url!r}")
            return self.market
        return super().__call__(url, params)


def mutable_polymarket() -> tuple[PolymarketLive, MutableFetcher]:
    market = dict(load("polymarket_market.json"))
    routes: dict[str, Any] = {
        "/sampling-markets": load("polymarket_sampling_markets.json"),
        "/book": load("polymarket_book.json"),
        "data-api": load("polymarket_trades.json"),
        "gamma-api": [{"conditionId": market["condition_id"]}],
    }
    fetch = MutableFetcher(routes, market)
    return PolymarketLive(fetch), fetch


def test_polymarket_describe_sees_a_market_that_closed_since_the_last_call() -> None:
    # The resolution cache holds identity, which is stable. Serving it
    # to a status check would report the market's opening status for as
    # long as a capture ran, which is exactly what the capture daemon's
    # periodic re-read is there to avoid.
    source, fetch = mutable_polymarket()
    assert source.describe(SLUG).status == "open"

    fetch.market = dict(fetch.market, closed=True)
    assert source.describe(SLUG).status == "closed"


def test_polymarket_describe_sees_a_halt_and_a_resume() -> None:
    source, fetch = mutable_polymarket()
    assert source.describe(SLUG).status == "open"

    fetch.market = dict(fetch.market, accepting_orders=False)
    assert source.describe(SLUG).status == "halted"

    fetch.market = dict(fetch.market, accepting_orders=True)
    assert source.describe(SLUG).status == "open"


def test_polymarket_a_refreshed_slug_is_not_resolved_through_gamma_twice() -> None:
    # A refresh re-reads the market document; the condition id is
    # already known, so it must not cost the slug lookup again.
    source, fetch = mutable_polymarket()
    source.describe(SLUG)
    gamma_first = sum(1 for url, _ in fetch.calls if "gamma-api" in url)
    source.describe(SLUG)
    gamma_after = sum(1 for url, _ in fetch.calls if "gamma-api" in url)
    assert gamma_first == 1
    assert gamma_after == 1


def test_polymarket_book_and_trades_still_use_the_cache() -> None:
    # The refresh is scoped to describe(). A poll loop asking for the
    # book every second must not re-resolve the market every second.
    source, fetch = mutable_polymarket()
    source.describe(SLUG)
    before = len([1 for url, _ in fetch.calls if "/markets/" in url])
    for _ in range(5):
        source.book(SLUG)
        source.trades(SLUG)
    after = len([1 for url, _ in fetch.calls if "/markets/" in url])
    assert after == before


def test_polymarket_a_refresh_asks_the_clob_by_condition_id_not_by_slug() -> None:
    # The CLOB's market endpoint answers to a condition id only. The
    # tape's canonical spelling is the SLUG, so a refresh that reuses
    # the canonical id gets HTTP 404 from the live venue and every
    # status check fails silently into a counter.
    source, fetch = mutable_polymarket()
    source.describe(SLUG)
    source.describe(SLUG)
    market_calls = [url for url, _ in fetch.calls if "/markets/" in url]
    assert len(market_calls) == 2
    for url in market_calls:
        assert url.endswith(fetch.market["condition_id"])
        assert SLUG not in url


def polymarket_for(doc: Any) -> tuple[PolymarketLive, FakeFetcher]:
    """A source whose CLOB market document is ``doc``, slug included."""
    return polymarket(**{"/markets/": doc, "gamma-api": [{"conditionId": doc["condition_id"]}]})


# -- settlement, on both venues -------------------------------------------
#
# A market stopping trading and a market settling are two different
# facts arriving at two different times, and the fixtures prove it
# rather than the tests asserting it: kalshi_market_closed.json is a
# real market that had been closed for 28 minutes and still published
# an empty result.


def test_kalshi_an_open_market_has_no_resolution() -> None:
    source, _ = kalshi()
    assert source.describe("KXELONMARS-99").resolution is None


def test_kalshi_a_closed_market_is_not_a_settled_one() -> None:
    # The whole reason a resolution row exists. This fixture's market
    # closed at 17:30 UTC and was still reporting result "" when it was
    # recorded, despite advertising settlement_timer_seconds: 5.
    doc = load("kalshi_market_closed.json")
    assert doc["market"]["status"] == "closed"
    assert doc["market"]["result"] == ""
    source, _ = kalshi(**{"/markets/": doc})
    described = source.describe(doc["market"]["ticker"])
    assert described.status == "closed"
    assert described.resolution is None


def test_kalshi_a_settled_market_reports_its_winner_and_venue_timestamp() -> None:
    doc = load("kalshi_market_settled.json")
    source, _ = kalshi(**{"/markets/": doc})
    resolution = source.describe(doc["market"]["ticker"]).resolution
    assert resolution is not None
    assert resolution.outcome == "YES"
    assert resolution.settlement == 1.0
    # Kalshi publishes settlement_ts, so the row does not have to be
    # stamped with local observation time.
    assert resolution.ts is not None
    assert resolution.ts.isoformat().startswith("2026-09-07T17:45:15")


def test_kalshi_a_no_result_pays_the_no_side_not_zero() -> None:
    # settlement_value_dollars is the YES contract's value, so it is
    # "0.0000" on a market that resolved NO. Taking it at face value
    # would write "NO won and pays 0.00", which is the opposite of what
    # happened.
    doc = load("kalshi_market_settled.json")
    doc["market"] = dict(doc["market"], result="no", settlement_value_dollars="0.0000")
    source, _ = kalshi(**{"/markets/": doc})
    resolution = source.describe(doc["market"]["ticker"]).resolution
    assert resolution is not None
    assert resolution.outcome == "NO"
    assert resolution.settlement == 1.0


def test_kalshi_reads_the_legacy_integer_cent_settlement_value() -> None:
    doc = load("kalshi_market_settled.json")
    doc["market"] = dict(doc["market"], settlement_value_dollars=None, settlement_value=100)
    source, _ = kalshi(**{"/markets/": doc})
    resolution = source.describe(doc["market"]["ticker"]).resolution
    assert resolution is not None
    assert resolution.settlement == 1.0


def test_kalshi_a_result_with_no_settlement_value_is_refused_not_guessed() -> None:
    doc = load("kalshi_market_settled.json")
    doc["market"] = dict(doc["market"], settlement_value_dollars=None, settlement_value=None)
    source, _ = kalshi(**{"/markets/": doc})
    with pytest.raises(LiveError, match="publishes no settlement value"):
        source.describe(doc["market"]["ticker"])


def test_kalshi_a_non_binary_result_is_refused_by_name() -> None:
    doc = load("kalshi_market_settled.json")
    doc["market"] = dict(doc["market"], result="void")
    source, _ = kalshi(**{"/markets/": doc})
    with pytest.raises(LiveError, match="'void'"):
        source.describe(doc["market"]["ticker"])


def test_polymarket_an_open_market_has_no_resolution() -> None:
    # Both tokens carry winner: false while the market trades, so the
    # live price on each is not mistaken for a settlement value.
    source, _ = polymarket()
    assert source.describe(SLUG).resolution is None


def test_polymarket_a_resolved_market_names_the_winning_outcome() -> None:
    doc = load("polymarket_market_resolved.json")
    source, _ = polymarket_for(doc)
    described = source.describe(doc["market_slug"])
    assert described.status == "closed"
    assert described.resolution is not None
    assert described.resolution.outcome == "No"
    assert described.resolution.settlement == 1.0
    # The venue publishes no settlement time, so the daemon has to
    # stamp the row when it observed the change.
    assert described.resolution.ts is None


def test_polymarket_the_winner_is_recoverable_as_a_yes_settlement() -> None:
    # A tape is in YES terms, so what a reader wants is what YES paid.
    # It is recoverable from the tape alone: the market row lists
    # outcomes with the YES side first.
    doc = load("polymarket_market_resolved.json")
    source, _ = polymarket_for(doc)
    described = source.describe(doc["market_slug"])
    assert described.outcomes == ("Yes", "No")
    assert described.resolution is not None
    yes_settled = (
        described.resolution.settlement
        if described.resolution.outcome == described.outcomes[0]
        else 1.0 - described.resolution.settlement
    )
    assert yes_settled == 0.0


def test_polymarket_two_winning_tokens_are_refused_and_both_named() -> None:
    # A contradictory document. Choosing one would put an invented
    # settlement on a tape that exists to be trusted.
    doc = load("polymarket_market_resolved.json")
    doc = dict(doc, tokens=[dict(t, winner=True) for t in doc["tokens"]])
    source, _ = polymarket_for(doc)
    with pytest.raises(LiveError, match="marks 2 outcomes as the winner") as excinfo:
        source.describe(doc["market_slug"])
    assert "'Yes'" in str(excinfo.value)
    assert "'No'" in str(excinfo.value)


def test_polymarket_a_winning_token_with_no_name_is_refused() -> None:
    doc = load("polymarket_market_resolved.json")
    tokens = [dict(t) for t in doc["tokens"]]
    tokens[1]["outcome"] = ""
    doc = dict(doc, tokens=tokens)
    source, _ = polymarket_for(doc)
    with pytest.raises(LiveError, match="no outcome name"):
        source.describe(doc["market_slug"])
