"""Generic CSV/JSON adapter.

This adapter accepts data already expressed in canonical terms and
turns it into a tape. It exists so any pipeline can produce OpenTape
files without depending on an exchange-specific shape.

JSON input (``.json``): either a bare list of event objects, or an
object ``{"source": "...", "events": [...]}``. Each event object:

    {"type": "trade", "ts": "2026-03-02T14:30:00Z", "market_id": "M1",
     "outcome": "YES", "side": "buy", "price": 0.62, "size": 40}

- ``type``: one of market, book_snapshot, book_delta, trade,
  market_status, resolution.
- ``ts``: ISO 8601 with timezone, or epoch seconds / millis / micros.
- ``price``: decimal fraction of 1.
- book_snapshot events carry ``bids`` and ``asks`` as lists of
  ``{"price": p, "size": s}`` objects or ``[p, s]`` pairs.
- market events carry ``title`` and optional ``outcomes`` (list).
- resolution events carry ``outcome`` and ``settlement``.
- ``source`` may be set per event; it falls back to the top-level
  ``source``, then to "generic".

CSV input (``.csv``): flat columns
``type,ts,market_id,outcome,side,price,size,status,title,outcomes,source``
(extra columns ignored, ``outcomes`` pipe-separated). CSV cannot carry
nested order books, so ``book_snapshot`` rows are rejected with a clear
error; use JSON for snapshots.

Sequence numbers are assigned by timestamp (stable for ties in file
order), so the output is a valid, replayable tape.
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from opentape import schema
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
    Resolution,
    Trade,
)
from opentape.tape import Tape

DEFAULT_SOURCE = "generic"


def _parse_levels(raw: Any, *, context: str) -> tuple[BookLevel, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise AdapterError(f"{context}: book levels must be a list, got {type(raw).__name__}")
    levels: list[BookLevel] = []
    for item in raw:
        if isinstance(item, dict):
            price, size = item.get("price"), item.get("size")
        elif isinstance(item, (list, tuple)) and len(item) == 2:
            price, size = item
        else:
            raise AdapterError(f"{context}: bad book level {item!r}")
        levels.append(
            BookLevel(
                price=parse_fraction(price, context=context),
                size=parse_size(size, context=context),
            )
        )
    return tuple(levels)


def _event_from_obj(obj: dict[str, Any], default_source: str, context: str) -> Event:
    etype = obj.get("type")
    market_id = obj.get("market_id")
    if not etype or not market_id:
        raise AdapterError(f"{context}: every event needs 'type' and 'market_id'")
    base = {
        "seq": 0,  # reassigned by sequence_events
        "ts": parse_ts(obj.get("ts"), context=context),
        "market_id": str(market_id),
        "source": str(obj.get("source") or default_source),
    }
    outcome = obj.get("outcome")
    if etype == schema.EVENT_MARKET:
        outcomes = obj.get("outcomes") or ["YES", "NO"]
        return Market(**base, title=str(obj.get("title") or ""), outcomes=tuple(map(str, outcomes)))
    if etype == schema.EVENT_SNAPSHOT:
        return OrderBookSnapshot(
            **base,
            outcome=outcome,
            bids=_parse_levels(obj.get("bids"), context=context),
            asks=_parse_levels(obj.get("asks"), context=context),
        )
    if etype == schema.EVENT_DELTA:
        side = obj.get("side")
        if side not in ("bid", "ask"):
            raise AdapterError(f"{context}: book_delta side must be 'bid' or 'ask', got {side!r}")
        return BookDelta(
            **base,
            outcome=outcome,
            side=side,
            price=parse_fraction(obj.get("price"), context=context),
            size=parse_size(obj.get("size"), context=context),
        )
    if etype == schema.EVENT_TRADE:
        return Trade(
            **base,
            outcome=outcome,
            side=obj.get("side"),
            price=parse_fraction(obj.get("price"), context=context),
            size=parse_size(obj.get("size"), context=context),
            trade_id=obj.get("trade_id"),
        )
    if etype == schema.EVENT_STATUS:
        status = obj.get("status")
        if not status:
            raise AdapterError(f"{context}: market_status needs 'status'")
        return MarketStatus(**base, status=str(status))
    if etype == schema.EVENT_RESOLUTION:
        if outcome is None:
            raise AdapterError(f"{context}: resolution needs 'outcome'")
        settlement = obj.get("settlement", obj.get("price"))
        return Resolution(
            **base,
            outcome=str(outcome),
            settlement=parse_fraction(settlement, context=context),
        )
    raise AdapterError(f"{context}: unknown event type {etype!r}")


def _convert_json(path: Path) -> list[Event]:
    doc = load_json(path)
    if isinstance(doc, dict):
        default_source = str(doc.get("source") or DEFAULT_SOURCE)
        raw_events = doc.get("events")
    else:
        default_source, raw_events = DEFAULT_SOURCE, doc
    if not isinstance(raw_events, list):
        raise AdapterError(f"{path}: expected a list of events or an object with 'events'")
    return [
        _event_from_obj(obj, default_source, f"{path} event {i}")
        for i, obj in enumerate(raw_events)
    ]


def _convert_csv(path: Path) -> list[Event]:
    events: list[Event] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for i, row in enumerate(reader):
                context = f"{path} row {i + 2}"
                if row.get("type") == schema.EVENT_SNAPSHOT:
                    raise AdapterError(
                        f"{context}: book_snapshot events need nested books; use JSON input"
                    )
                obj: dict[str, Any] = {k: v for k, v in row.items() if v not in (None, "")}
                if "outcomes" in obj:
                    obj["outcomes"] = str(obj["outcomes"]).split("|")
                events.append(_event_from_obj(obj, DEFAULT_SOURCE, context))
    except OSError as exc:
        raise AdapterError(f"could not read {path}: {exc}") from exc
    return events


def convert(path: str | Path) -> Tape:
    """Convert a generic CSV or JSON file into a Tape."""
    path = Path(path)
    if path.suffix.lower() == ".csv":
        events = _convert_csv(path)
    elif path.suffix.lower() == ".json":
        events = _convert_json(path)
    else:
        raise AdapterError(f"generic adapter handles .csv and .json, not {path.suffix!r}")
    return Tape.from_events(sequence_events(events))
