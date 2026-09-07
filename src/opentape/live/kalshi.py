"""Kalshi live source, over the public unauthenticated REST endpoints.

Endpoints used, all readable without an API key as of 2026-09-06:

    GET /trade-api/v2/markets                       market discovery
    GET /trade-api/v2/markets/{ticker}              title and status
    GET /trade-api/v2/markets/{ticker}/orderbook    full book
    GET /trade-api/v2/markets/trades?ticker=...     public executions

Canonical mapping, matching the ``kalshi-style`` file adapter:

- Prices become fractions of 1. The current API returns decimal strings
  (``"0.9500"``) under ``*_dollars`` keys; older responses return
  integer cents. Both are accepted, because a capture that breaks on a
  field rename is worse than one that reads two spellings.
- ``yes`` levels are resting YES bids and map to canonical bids. A
  resting NO bid at ``q`` is the same order as a YES ask at ``1 - q``,
  so NO levels map to canonical asks at the complementary price.
- ``taker_side: "yes"`` is a canonical ``buy``, ``"no"`` a ``sell``.

The orderbook response carries no timestamp, so book events captured
from Kalshi are stamped with local capture time. Trades carry
``created_time`` and keep it, and a settlement carries ``settlement_ts``
and keeps it, which makes a resolution row the one lifecycle row on a
Kalshi tape that is stamped with the venue's own clock rather than the
capture's.
"""

from __future__ import annotations

from typing import Any, ClassVar

from opentape.adapters._common import parse_ts
from opentape.errors import LiveError
from opentape.events import BookLevel
from opentape.live.base import (
    PRICE_DECIMALS,
    BookQuote,
    LiveSource,
    MarketDescription,
    MarketRef,
    MarketResolution,
    TradeTick,
)
from opentape.live.http import Fetcher

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

_STATUS_MAP = {
    "active": "open",
    "initialized": "open",
    "closed": "closed",
    "settled": "closed",
    "finalized": "closed",
    "determined": "closed",
    "paused": "halted",
}


def _price(value: Any, *, context: str) -> float:
    """Read a Kalshi price as a fraction of 1, from dollars or cents."""
    if isinstance(value, bool) or value is None:
        raise LiveError(f"{context}: bad price {value!r}")
    if isinstance(value, str):
        try:
            price = float(value)
        except ValueError:
            raise LiveError(f"{context}: bad price {value!r}") from None
    elif isinstance(value, (int, float)):
        # Integer cents in the legacy shape; anything in [0, 1] is
        # already a fraction and is taken at face value.
        price = float(value) / 100.0 if float(value) > 1.0 else float(value)
    else:
        raise LiveError(f"{context}: bad price {value!r}")
    if not 0.0 <= price <= 1.0:
        raise LiveError(f"{context}: price {price} is outside [0, 1]")
    return round(price, PRICE_DECIMALS)


def _size(value: Any, *, context: str) -> float:
    try:
        size = float(value)
    except (TypeError, ValueError):
        raise LiveError(f"{context}: bad size {value!r}") from None
    if size < 0:
        raise LiveError(f"{context}: size {size} is negative")
    return size


def _pairs(raw: Any, *, context: str) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    for pair in raw or []:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            raise LiveError(f"{context}: bad level {pair!r}, expected [price, size]")
        out.append((_price(pair[0], context=context), _size(pair[1], context=context)))
    return out


class KalshiLive(LiveSource):
    """Read Kalshi's public market data through an injected fetcher."""

    key: ClassVar[str] = "kalshi"
    source_tag: ClassVar[str] = "kalshi-rest-poll"

    def __init__(
        self, fetch: Fetcher, *, base_url: str = BASE_URL, discovery_pages: int = 3
    ) -> None:
        self._fetch = fetch
        self._base = base_url.rstrip("/")
        self.discovery_pages = discovery_pages

    def list_markets(self, *, limit: int, search: str | None = None) -> list[MarketRef]:
        """List open binary markets that currently show a two-sided quote.

        Discovery goes through ``/events`` rather than ``/markets``
        because the flat market listing is dominated by auto-generated
        combination markets, thousands of which have no book at all.
        Requiring a bid and an ask keeps the answer to markets there is
        actually something to capture from, which is the only reason to
        run this command.
        """
        found: list[MarketRef] = []
        cursor: str | None = None
        needle = (search or "").lower()
        for _ in range(self.discovery_pages):
            doc = self._fetch(
                f"{self._base}/events",
                {"limit": 200, "status": "open", "with_nested_markets": "true", "cursor": cursor},
            )
            events = _expect_list(doc, "events", context="events listing")
            for event in events:
                for market in event.get("markets") or []:
                    if not isinstance(market, dict) or market.get("market_type") != "binary":
                        continue
                    ticker = str(market.get("ticker") or "")
                    title = str(market.get("title") or "")
                    if not ticker or not _two_sided(market):
                        continue
                    if needle and needle not in title.lower() and needle not in ticker.lower():
                        continue
                    detail = str(market.get("yes_sub_title") or market.get("subtitle") or "")
                    found.append(MarketRef(market_id=ticker, title=title, detail=detail))
                    if len(found) >= limit:
                        return found
            cursor = doc.get("cursor")
            if not cursor or not events:
                break
        return found

    def describe(self, market_id: str) -> MarketDescription:
        doc = self._fetch(f"{self._base}/markets/{market_id}")
        market = doc.get("market") if isinstance(doc, dict) else None
        if not isinstance(market, dict):
            raise LiveError(f"kalshi: no market object in the response for {market_id!r}")
        raw_status = str(market.get("status") or "")
        title = str(market.get("title") or market_id)
        detail = str(market.get("yes_sub_title") or market.get("subtitle") or "")
        return MarketDescription(
            market_id=str(market.get("ticker") or market_id),
            title=f"{title} {detail}".strip() if detail else title,
            status=_STATUS_MAP.get(raw_status, raw_status or "unknown"),
            resolution=_resolution(market, market_id),
        )

    def book(self, market_id: str) -> BookQuote:
        doc = self._fetch(f"{self._base}/markets/{market_id}/orderbook", {"depth": 100})
        if not isinstance(doc, dict):
            raise LiveError(f"kalshi: unexpected orderbook response for {market_id!r}")
        # "orderbook_fp" is the current decimal-string shape; "orderbook"
        # is the older integer-cent one.
        book = doc.get("orderbook_fp")
        if not isinstance(book, dict):
            book = doc.get("orderbook")
        if not isinstance(book, dict):
            raise LiveError(f"kalshi: no orderbook in the response for {market_id!r}")
        context = f"kalshi orderbook {market_id}"
        yes = _pairs(book.get("yes_dollars") or book.get("yes"), context=f"{context} yes")
        no = _pairs(book.get("no_dollars") or book.get("no"), context=f"{context} no")
        bids = tuple(
            BookLevel(price=p, size=s) for p, s in sorted(yes, key=lambda x: -x[0]) if s > 0
        )
        asks = tuple(
            BookLevel(price=round(1.0 - p, PRICE_DECIMALS), size=s)
            for p, s in sorted(no, key=lambda x: -x[0])
            if s > 0
        )
        # Kalshi publishes no book timestamp, so ts stays None and the
        # daemon stamps local capture time.
        return BookQuote(bids=bids, asks=asks, ts=None)

    def trades(self, market_id: str, *, limit: int = 100) -> list[TradeTick]:
        doc = self._fetch(f"{self._base}/markets/trades", {"ticker": market_id, "limit": limit})
        raw = _expect_list(doc, "trades", context=f"kalshi trades {market_id}")
        ticks: list[TradeTick] = []
        for i, trade in enumerate(raw):
            context = f"kalshi trades {market_id}[{i}]"
            trade_id = str(trade.get("trade_id") or "")
            if not trade_id:
                raise LiveError(f"{context}: trade has no trade_id, so it cannot be deduplicated")
            taker = trade.get("taker_side")
            if taker not in ("yes", "no"):
                raise LiveError(f"{context}: taker_side must be 'yes' or 'no', got {taker!r}")
            price_raw = trade.get("yes_price_dollars")
            if price_raw is None:
                price_raw = trade.get("yes_price")
            size_raw = trade.get("count_fp")
            if size_raw is None:
                size_raw = trade.get("count")
            ticks.append(
                TradeTick(
                    ts=parse_ts(trade.get("created_time"), context=context),
                    price=_price(price_raw, context=context),
                    size=_size(size_raw, context=context),
                    side="buy" if taker == "yes" else "sell",
                    trade_id=trade_id,
                )
            )
        ticks.sort(key=lambda t: t.ts)
        return ticks


def _resolution(market: dict[str, Any], market_id: str) -> MarketResolution | None:
    """Read Kalshi's settlement, or None while the market has not settled.

    ``result`` is the discriminator and it is empty until the market
    settles, which was checked against the live API rather than assumed:
    a market in status ``closed`` answers ``result: ""`` with
    ``settlement_value_dollars`` and ``settlement_ts`` both absent,
    while a ``settled`` or ``finalized`` one answers ``result: "yes"``
    or ``"no"`` with both fields present.

    ``settlement_value_dollars`` is the YES contract's value, so it is
    ``"0.0000"`` on a market that resolved NO. The event records what
    the WINNER pays, which is the complement in that case. Taking the
    field at face value would write "NO won and pays 0.00" onto the
    tape, which is the opposite of what happened.
    """
    result = str(market.get("result") or "").strip().lower()
    if not result:
        return None
    if result not in ("yes", "no"):
        raise LiveError(
            f"kalshi: market {market_id!r} settled to result {result!r}, which is neither "
            f"'yes' nor 'no'; schema v1 describes binary markets, so this cannot be "
            f"recorded as a resolution without guessing what it means"
        )
    raw_value = market.get("settlement_value_dollars")
    if raw_value is None:
        raw_value = market.get("settlement_value")
    if raw_value is None:
        # A result with no value is a half-published settlement. Say
        # nothing rather than invent 1.0; the next check will see the
        # complete document.
        raise LiveError(
            f"kalshi: market {market_id!r} reports result {result!r} but publishes no "
            f"settlement value yet, so its settlement is not recorded"
        )
    yes_value = _price(raw_value, context=f"kalshi settlement {market_id}")
    settlement = yes_value if result == "yes" else round(1.0 - yes_value, PRICE_DECIMALS)
    raw_ts = market.get("settlement_ts")
    return MarketResolution(
        outcome="YES" if result == "yes" else "NO",
        settlement=settlement,
        ts=parse_ts(raw_ts, context=f"kalshi settlement_ts {market_id}") if raw_ts else None,
    )


def _two_sided(market: dict[str, Any]) -> bool:
    """True when a listed market shows both a resting bid and a resting ask."""
    try:
        bid = float(market.get("yes_bid_dollars") or market.get("yes_bid") or 0)
        ask = float(market.get("yes_ask_dollars") or market.get("yes_ask") or 0)
    except (TypeError, ValueError):
        return False
    if bid > 1.0 or ask > 1.0:  # legacy integer cents
        bid, ask = bid / 100.0, ask / 100.0
    return 0.0 < bid < ask < 1.0


def _expect_list(doc: Any, key: str, *, context: str) -> list[dict[str, Any]]:
    if not isinstance(doc, dict) or not isinstance(doc.get(key), list):
        raise LiveError(f"{context}: expected a JSON object with a {key!r} array")
    items = doc[key]
    if any(not isinstance(item, dict) for item in items):
        raise LiveError(f"{context}: every entry in {key!r} must be an object")
    return items
