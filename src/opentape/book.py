"""Reconstruct an order book from a tape's snapshots and deltas.

This is the inverse of what a capture does. A tape stores a book as one
snapshot followed by per-level changes, which is compact and
gap-detectable but is not a book you can read. Folding those back into
a ladder is the operation every backtest needs first, and it is pure:
events in, book out, no I/O.

The one rule that matters is that a book cannot be built from deltas
alone. A delta says what a level's size became, never what the rest of
the book was, so without a snapshot to seed it the result would be a
book containing only the levels that happened to change. That case is
an error naming the first snapshot on the tape, not a partial answer.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from opentape.errors import OpenTapeError
from opentape.events import BookDelta, BookLevel, Event, OrderBookSnapshot


@dataclass(frozen=True, slots=True)
class OrderBook:
    """A two-sided ladder, best levels first, with its own provenance.

    ``as_of`` is the timestamp of the last event folded in, which is
    the moment this book was really true. It is at or before the
    timestamp that was asked for, and the gap between them is how stale
    the answer is.
    """

    market_id: str
    as_of: datetime
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    snapshot_ts: datetime
    deltas_applied: int

    @property
    def best_bid(self) -> BookLevel | None:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> BookLevel | None:
        return self.asks[0] if self.asks else None

    @property
    def spread(self) -> float | None:
        """Best ask minus best bid, or None when either side is empty."""
        if not self.bids or not self.asks:
            return None
        return round(self.asks[0].price - self.bids[0].price, 6)

    @property
    def mid(self) -> float | None:
        """The midpoint, or None when either side is empty.

        A one-sided book has no midpoint. Returning the one price that
        does exist would read as a mid and be wrong by up to the whole
        spread, so it returns nothing instead.
        """
        if not self.bids or not self.asks:
            return None
        return round((self.bids[0].price + self.asks[0].price) / 2.0, 6)

    def depth(self, levels: int) -> OrderBook:
        """The same book truncated to ``levels`` a side."""
        from dataclasses import replace

        return replace(self, bids=self.bids[:levels], asks=self.asks[:levels])


def reconstruct(events: Iterable[Event], *, market_id: str) -> OrderBook:
    """Fold snapshots and deltas for one market into a book.

    ``events`` must already be limited to the window wanted and sorted
    in tape order. Events for other markets and of other types are
    ignored, so a caller can hand over a slice of a mixed tape.
    """
    bids: dict[float, float] = {}
    asks: dict[float, float] = {}
    snapshot_ts: datetime | None = None
    as_of: datetime | None = None
    deltas = 0

    for event in events:
        if event.market_id != market_id:
            continue
        if isinstance(event, OrderBookSnapshot):
            bids = {lv.price: lv.size for lv in event.bids}
            asks = {lv.price: lv.size for lv in event.asks}
            snapshot_ts = event.ts
            as_of = event.ts
            deltas = 0
        elif isinstance(event, BookDelta):
            if snapshot_ts is None:
                # Nothing to apply the change to yet. Skipping it is
                # right: a later snapshot will supply the whole book,
                # and applying it now would invent a book made only of
                # the levels that moved.
                continue
            side = bids if event.side == "bid" else asks
            if event.size <= 0:
                side.pop(event.price, None)
            else:
                side[event.price] = event.size
            as_of = event.ts
            deltas += 1

    if snapshot_ts is None or as_of is None:
        raise OpenTapeError(
            f"no order book snapshot for {market_id!r} at or before that time, so there is "
            f"nothing to apply deltas to; a book cannot be rebuilt from deltas alone"
        )

    return OrderBook(
        market_id=market_id,
        as_of=as_of,
        bids=tuple(
            BookLevel(price=p, size=s) for p, s in sorted(bids.items(), reverse=True) if s > 0
        ),
        asks=tuple(BookLevel(price=p, size=s) for p, s in sorted(asks.items()) if s > 0),
        snapshot_ts=snapshot_ts,
        deltas_applied=deltas,
    )
