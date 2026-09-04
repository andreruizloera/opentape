"""Adapter for Kalshi-style data shapes.

This adapter converts files whose shape follows the public Kalshi API
conventions: prices in integer cents (1 to 99), binary YES/NO markets,
order books quoted as resting YES bids and NO bids, taker side named
"yes" or "no". It works on local JSON files shaped like the documented
API payloads; it does not talk to any API (live capture is roadmap,
see ROADMAP.md).

Expected input: one JSON object with any of these keys.

    {
      "market": {
        "ticker": "FEDCUT-26SEP", "title": "...",
        "open_time": "2026-03-01T14:00:00Z", "status": "active"
      },
      "trades": [
        {"trade_id": "t1", "ticker": "FEDCUT-26SEP",
         "created_time": "2026-03-02T14:30:05Z",
         "yes_price": 62, "count": 40, "taker_side": "yes"}
      ],
      "orderbook_snapshots": [
        {"ticker": "...", "ts": "...",
         "orderbook": {"yes": [[61, 500], [60, 900]],
                        "no": [[37, 450], [36, 800]]}}
      ],
      "orderbook_deltas": [
        {"ticker": "...", "ts": "...", "price": 61, "delta": -100,
         "side": "yes"}
      ]
    }

Canonical mapping (everything is expressed in YES terms):

- cents become fractions of 1 (``62`` becomes ``0.62``).
- YES bids map to canonical bids. A resting NO bid at ``c`` cents is
  the same order as a YES ask at ``1 - c/100``, so NO levels map to
  canonical asks at the complementary price.
- ``taker_side: "yes"`` becomes a canonical ``buy`` (aggressor lifted
  YES), ``"no"`` becomes ``sell``.
- ``orderbook_deltas`` carry a signed size change; because the
  canonical BookDelta stores the new absolute size, this adapter
  replays deltas against the running book it maintains from the most
  recent snapshot. A delta for an unseen level with a positive change
  creates the level; a negative change without a known level is an
  input error.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from opentape.adapters._common import load_json, parse_ts, sequence_events
from opentape.errors import AdapterError
from opentape.events import (
    BookDelta,
    BookLevel,
    Event,
    Market,
    MarketStatus,
    OrderBookSnapshot,
    Trade,
)
from opentape.tape import Tape

SOURCE = "kalshi-style"
OUTCOME = "YES"

_STATUS_MAP = {"active": "open", "closed": "closed", "settled": "closed", "paused": "halted"}


def _cents(value: Any, *, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 100:
        raise AdapterError(
            f"{context}: expected a price in integer cents (0 to 100), got {value!r}"
        )
    return value / 100.0


def _yes_levels(raw: Any, *, context: str) -> dict[float, float]:
    """Kalshi YES bids as {canonical price: size}."""
    levels: dict[float, float] = {}
    for pair in raw or []:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            raise AdapterError(f"{context}: bad level {pair!r}, expected [cents, size]")
        cents, size = pair
        levels[_cents(cents, context=context)] = float(size)
    return levels


def _no_levels_as_asks(raw: Any, *, context: str) -> dict[float, float]:
    """Kalshi NO bids as canonical YES asks at the complementary price."""
    return {
        round(1.0 - price, 6): size for price, size in _yes_levels(raw, context=context).items()
    }


def _book_tuple(levels: dict[float, float], *, reverse: bool) -> tuple[BookLevel, ...]:
    return tuple(
        BookLevel(price=p, size=s) for p, s in sorted(levels.items(), reverse=reverse) if s > 0
    )


def convert(path: str | Path) -> Tape:
    """Convert a Kalshi-style JSON file into a Tape."""
    path = Path(path)
    doc = load_json(path)
    if not isinstance(doc, dict):
        raise AdapterError(f"{path}: expected a JSON object, got {type(doc).__name__}")

    events: list[Event] = []

    market = doc.get("market")
    if market:
        ticker = market.get("ticker")
        if not ticker:
            raise AdapterError(f"{path}: market object needs a 'ticker'")
        ts = parse_ts(market.get("open_time"), context=f"{path} market.open_time")
        events.append(
            Market(
                seq=0,
                ts=ts,
                market_id=str(ticker),
                source=SOURCE,
                title=str(market.get("title") or ""),
                outcomes=("YES", "NO"),
            )
        )
        raw_status = market.get("status")
        if raw_status:
            events.append(
                MarketStatus(
                    seq=0,
                    ts=ts,
                    market_id=str(ticker),
                    source=SOURCE,
                    status=_STATUS_MAP.get(str(raw_status), str(raw_status)),
                )
            )

    for i, trade in enumerate(doc.get("trades") or []):
        context = f"{path} trades[{i}]"
        taker = trade.get("taker_side")
        if taker not in ("yes", "no"):
            raise AdapterError(f"{context}: taker_side must be 'yes' or 'no', got {taker!r}")
        events.append(
            Trade(
                seq=0,
                ts=parse_ts(trade.get("created_time"), context=context),
                market_id=str(trade.get("ticker") or ""),
                source=SOURCE,
                outcome=OUTCOME,
                side="buy" if taker == "yes" else "sell",
                price=_cents(trade.get("yes_price"), context=context),
                size=float(trade.get("count") or 0),
                trade_id=trade.get("trade_id"),
            )
        )

    # Running YES-terms books per ticker, so signed deltas can be
    # converted into absolute canonical sizes.
    books: dict[str, tuple[dict[float, float], dict[float, float]]] = {}

    for i, snap in enumerate(doc.get("orderbook_snapshots") or []):
        context = f"{path} orderbook_snapshots[{i}]"
        ticker = str(snap.get("ticker") or "")
        ob = snap.get("orderbook") or {}
        bids = _yes_levels(ob.get("yes"), context=context)
        asks = _no_levels_as_asks(ob.get("no"), context=context)
        books[ticker] = (bids, asks)
        events.append(
            OrderBookSnapshot(
                seq=0,
                ts=parse_ts(snap.get("ts"), context=context),
                market_id=ticker,
                source=SOURCE,
                outcome=OUTCOME,
                bids=_book_tuple(bids, reverse=True),
                asks=_book_tuple(asks, reverse=False),
            )
        )

    for i, delta in enumerate(doc.get("orderbook_deltas") or []):
        context = f"{path} orderbook_deltas[{i}]"
        ticker = str(delta.get("ticker") or "")
        side = delta.get("side")
        if side not in ("yes", "no"):
            raise AdapterError(f"{context}: side must be 'yes' or 'no', got {side!r}")
        change = delta.get("delta")
        if not isinstance(change, (int, float)) or isinstance(change, bool):
            raise AdapterError(f"{context}: 'delta' must be a signed number, got {change!r}")
        cents = _cents(delta.get("price"), context=context)
        if side == "yes":
            canonical_side, price = "bid", cents
        else:
            canonical_side, price = "ask", round(1.0 - cents, 6)
        bids, asks = books.setdefault(ticker, ({}, {}))
        book = bids if canonical_side == "bid" else asks
        new_size = book.get(price, 0.0) + float(change)
        if new_size < 0:
            raise AdapterError(
                f"{context}: delta of {change} takes level {price} below zero "
                f"(known size {book.get(price, 0.0)}); snapshot missing or out of order"
            )
        book[price] = new_size
        events.append(
            BookDelta(
                seq=0,
                ts=parse_ts(delta.get("ts"), context=context),
                market_id=ticker,
                source=SOURCE,
                outcome=OUTCOME,
                side=canonical_side,
                price=price,
                size=new_size,
            )
        )

    if not events:
        raise AdapterError(f"{path}: no recognizable Kalshi-style content found")
    return Tape.from_events(sequence_events(events))
