"""Adapter for Polymarket-style data shapes.

This adapter converts files whose shape follows the public Polymarket
CLOB conventions: prices as decimal strings ("0.62"), sizes as decimal
strings, sides "BUY"/"SELL", unix-second or unix-millisecond
timestamps, hex condition ids. It works on local JSON files shaped like
the documented API payloads; it does not talk to any API (live capture
is roadmap, see ROADMAP.md).

Expected input: one JSON object with any of these keys.

    {
      "market": {
        "condition_id": "0xabc...", "question": "...",
        "outcomes": ["Yes", "No"], "created_at": "2026-03-01T00:00:00Z",
        "active": true
      },
      "trades": [
        {"id": "tr-1", "market": "0xabc...", "outcome": "Yes",
         "price": "0.62", "size": "150.5", "side": "BUY",
         "timestamp": 1772461805}
      ],
      "books": [
        {"market": "0xabc...", "outcome": "Yes", "timestamp": 1772461800000,
         "bids": [{"price": "0.61", "size": "400"}],
         "asks": [{"price": "0.63", "size": "350"}]}
      ],
      "price_changes": [
        {"market": "0xabc...", "outcome": "Yes", "price": "0.61",
         "size": "250", "side": "BUY", "timestamp": 1772461810}
      ]
    }

Canonical mapping:

- decimal-string prices parse to floats in [0, 1] unchanged (they are
  already fractions of 1).
- ``side: "BUY"`` becomes ``buy``, ``"SELL"`` becomes ``sell``; in
  ``price_changes``, BUY levels are canonical bids and SELL levels are
  canonical asks, and ``size`` is the new resting size at that level
  (0 removes it), matching the canonical BookDelta.
- ``books`` map directly to snapshots (best levels first).
- the ``outcome`` string is carried through as-is.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from opentape.adapters._common import (
    load_json,
    parse_fraction,
    parse_size,
    parse_ts,
    sequence_events,
)
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

SOURCE = "polymarket-style"


def _levels(raw: Any, *, context: str, reverse: bool) -> tuple[BookLevel, ...]:
    levels = []
    for item in raw or []:
        if not isinstance(item, dict):
            raise AdapterError(f"{context}: bad level {item!r}, expected an object")
        levels.append(
            BookLevel(
                price=parse_fraction(item.get("price"), context=context),
                size=parse_size(item.get("size"), context=context),
            )
        )
    return tuple(sorted(levels, key=lambda lv: lv.price, reverse=reverse))


def convert(path: str | Path) -> Tape:
    """Convert a Polymarket-style JSON file into a Tape."""
    path = Path(path)
    doc = load_json(path)
    if not isinstance(doc, dict):
        raise AdapterError(f"{path}: expected a JSON object, got {type(doc).__name__}")

    events: list[Event] = []

    market = doc.get("market")
    if market:
        condition_id = market.get("condition_id")
        if not condition_id:
            raise AdapterError(f"{path}: market object needs a 'condition_id'")
        ts = parse_ts(market.get("created_at"), context=f"{path} market.created_at")
        outcomes = tuple(map(str, market.get("outcomes") or ("Yes", "No")))
        events.append(
            Market(
                seq=0,
                ts=ts,
                market_id=str(condition_id),
                source=SOURCE,
                title=str(market.get("question") or ""),
                outcomes=outcomes,
            )
        )
        if "active" in market:
            events.append(
                MarketStatus(
                    seq=0,
                    ts=ts,
                    market_id=str(condition_id),
                    source=SOURCE,
                    status="open" if market["active"] else "closed",
                )
            )

    for i, trade in enumerate(doc.get("trades") or []):
        context = f"{path} trades[{i}]"
        side = trade.get("side")
        if side not in ("BUY", "SELL"):
            raise AdapterError(f"{context}: side must be 'BUY' or 'SELL', got {side!r}")
        events.append(
            Trade(
                seq=0,
                ts=parse_ts(trade.get("timestamp"), context=context),
                market_id=str(trade.get("market") or ""),
                source=SOURCE,
                outcome=trade.get("outcome"),
                side=side.lower(),
                price=parse_fraction(trade.get("price"), context=context),
                size=parse_size(trade.get("size"), context=context),
                trade_id=trade.get("id"),
            )
        )

    for i, book in enumerate(doc.get("books") or []):
        context = f"{path} books[{i}]"
        events.append(
            OrderBookSnapshot(
                seq=0,
                ts=parse_ts(book.get("timestamp"), context=context),
                market_id=str(book.get("market") or ""),
                source=SOURCE,
                outcome=book.get("outcome"),
                bids=_levels(book.get("bids"), context=context, reverse=True),
                asks=_levels(book.get("asks"), context=context, reverse=False),
            )
        )

    for i, change in enumerate(doc.get("price_changes") or []):
        context = f"{path} price_changes[{i}]"
        side = change.get("side")
        if side not in ("BUY", "SELL"):
            raise AdapterError(f"{context}: side must be 'BUY' or 'SELL', got {side!r}")
        events.append(
            BookDelta(
                seq=0,
                ts=parse_ts(change.get("timestamp"), context=context),
                market_id=str(change.get("market") or ""),
                source=SOURCE,
                outcome=change.get("outcome"),
                side="bid" if side == "BUY" else "ask",
                price=parse_fraction(change.get("price"), context=context),
                size=parse_size(change.get("size"), context=context),
            )
        )

    if not events:
        raise AdapterError(f"{path}: no recognizable Polymarket-style content found")
    return Tape.from_events(sequence_events(events))
