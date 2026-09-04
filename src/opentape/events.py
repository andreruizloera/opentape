"""Typed event classes and row conversion for the canonical schema.

Each event maps to exactly one row of the flat tape table. ``to_row``
and ``from_row`` are the only places that know the mapping, so the
schema has a single source of truth on the Python side.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from opentape import schema
from opentape.errors import SchemaError


@dataclass(frozen=True, slots=True)
class BookLevel:
    """One order book level: price as a fraction of 1, size in contracts."""

    price: float
    size: float


@dataclass(frozen=True, slots=True, kw_only=True)
class _EventBase:
    seq: int
    ts: datetime
    market_id: str
    source: str


@dataclass(frozen=True, slots=True, kw_only=True)
class Market(_EventBase):
    """Market definition or metadata update."""

    title: str
    outcomes: tuple[str, ...] = ("YES", "NO")


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderBookSnapshot(_EventBase):
    """Full order book state at one instant, best levels first."""

    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    outcome: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class BookDelta(_EventBase):
    """One level changed: ``size`` is the new resting size (0 removes the level)."""

    side: str  # "bid" or "ask"
    price: float
    size: float
    outcome: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class Trade(_EventBase):
    """An execution. ``side`` is the aggressor: "buy" or "sell"."""

    price: float
    size: float
    side: str | None = None
    outcome: str | None = None
    trade_id: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class MarketStatus(_EventBase):
    """Lifecycle change: "open", "halted", "closed", ..."""

    status: str


@dataclass(frozen=True, slots=True, kw_only=True)
class Resolution(_EventBase):
    """Final settlement: the winning outcome and its settlement value."""

    outcome: str
    settlement: float


Event = Market | OrderBookSnapshot | BookDelta | Trade | MarketStatus | Resolution

_TYPE_TAGS: dict[type, str] = {
    Market: schema.EVENT_MARKET,
    OrderBookSnapshot: schema.EVENT_SNAPSHOT,
    BookDelta: schema.EVENT_DELTA,
    Trade: schema.EVENT_TRADE,
    MarketStatus: schema.EVENT_STATUS,
    Resolution: schema.EVENT_RESOLUTION,
}


def _levels_to_rows(levels: tuple[BookLevel, ...]) -> list[dict[str, float]]:
    return [{"price": lv.price, "size": lv.size} for lv in levels]


def _rows_to_levels(rows: list[dict[str, float]] | None) -> tuple[BookLevel, ...]:
    if not rows:
        return ()
    return tuple(BookLevel(price=r["price"], size=r["size"]) for r in rows)


def _as_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        raise SchemaError(f"event timestamp {ts!r} is naive; timestamps must be timezone-aware")
    return ts.astimezone(UTC)


def to_row(event: Event) -> dict[str, Any]:
    """Convert a typed event to one canonical row (dict keyed by column name)."""
    row: dict[str, Any] = dict.fromkeys(schema.COLUMNS)
    row["seq"] = event.seq
    row["ts"] = _as_utc(event.ts)
    row["event_type"] = _TYPE_TAGS[type(event)]
    row["market_id"] = event.market_id
    row["source"] = event.source
    row["schema_version"] = schema.SCHEMA_VERSION

    match event:
        case Market():
            row["title"] = event.title
            row["outcomes"] = list(event.outcomes)
        case OrderBookSnapshot():
            row["outcome"] = event.outcome
            row["bids"] = _levels_to_rows(event.bids)
            row["asks"] = _levels_to_rows(event.asks)
        case BookDelta():
            row["outcome"] = event.outcome
            row["side"] = event.side
            row["price"] = event.price
            row["size"] = event.size
        case Trade():
            row["outcome"] = event.outcome
            row["side"] = event.side
            row["price"] = event.price
            row["size"] = event.size
            row["event_id"] = event.trade_id
        case MarketStatus():
            row["status"] = event.status
        case Resolution():
            row["outcome"] = event.outcome
            row["price"] = event.settlement
    return row


def from_row(row: dict[str, Any]) -> Event:
    """Convert one canonical row back into its typed event."""
    base = {
        "seq": row["seq"],
        "ts": row["ts"],
        "market_id": row["market_id"],
        "source": row["source"],
    }
    event_type = row["event_type"]
    if event_type == schema.EVENT_MARKET:
        return Market(**base, title=row["title"] or "", outcomes=tuple(row["outcomes"] or ()))
    if event_type == schema.EVENT_SNAPSHOT:
        return OrderBookSnapshot(
            **base,
            outcome=row["outcome"],
            bids=_rows_to_levels(row["bids"]),
            asks=_rows_to_levels(row["asks"]),
        )
    if event_type == schema.EVENT_DELTA:
        return BookDelta(
            **base,
            outcome=row["outcome"],
            side=row["side"],
            price=row["price"],
            size=row["size"],
        )
    if event_type == schema.EVENT_TRADE:
        return Trade(
            **base,
            outcome=row["outcome"],
            side=row["side"],
            price=row["price"],
            size=row["size"],
            trade_id=row["event_id"],
        )
    if event_type == schema.EVENT_STATUS:
        return MarketStatus(**base, status=row["status"] or "")
    if event_type == schema.EVENT_RESOLUTION:
        return Resolution(**base, outcome=row["outcome"] or "", settlement=row["price"])
    raise SchemaError(f"unknown event type: {event_type!r}")
