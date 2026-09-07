"""The live-source interface and the state a poller needs to keep.

A venue source answers three questions about a market: what it is, what
its book looks like right now, and what has traded recently. Everything
else, including turning consecutive book snapshots into canonical
deltas and dropping trades that a poll already reported, is shared and
lives here.
"""

from __future__ import annotations

import abc
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from typing import ClassVar

from opentape.events import BookDelta, BookLevel, Event, OrderBookSnapshot

#: Prices are rounded to this many decimals before being used as book
#: keys. The YES/NO complement (1 - q) introduces binary float noise, so
#: without rounding a level can silently split into two keys.
PRICE_DECIMALS = 6


@dataclass(frozen=True, slots=True)
class MarketRef:
    """One market as returned by discovery."""

    market_id: str
    title: str
    detail: str = ""


@dataclass(frozen=True, slots=True)
class MarketResolution:
    """The venue's published settlement: which outcome won, and what it pays.

    This is deliberately not the same thing as a status of ``closed``.
    A market stops trading and settles at two different moments, and on
    Kalshi they are two different documents: a market in status
    ``closed`` carries ``result: ""`` and no settlement value at all,
    and only once it reaches ``settled`` or ``finalized`` does a result
    appear. A source returns ``None`` until the venue actually publishes
    a winner, so an unresolved market never produces a resolution row.

    ``ts`` is the venue's own settlement time where it publishes one and
    ``None`` where it does not, exactly as :class:`BookQuote` treats a
    book timestamp. Kalshi publishes ``settlement_ts``; Polymarket
    publishes nothing comparable, so a Polymarket resolution is stamped
    with the moment the capture observed it, which is an upper bound
    rather than the moment the venue decided.
    """

    #: The winning outcome, spelled the way the venue spells it, so it
    #: can be compared against the market row's ``outcomes``.
    outcome: str
    #: What the WINNING outcome pays. For an ordinary binary market this
    #: is 1.0. It is the winner's value and not the YES side's value;
    #: see the note in ``events.Resolution``.
    settlement: float
    ts: datetime | None = None


@dataclass(frozen=True, slots=True)
class MarketDescription:
    """What a source knows about a market before any polling starts."""

    market_id: str
    title: str
    status: str
    outcomes: tuple[str, ...] = ("YES", "NO")
    #: The venue's published settlement, or None while the market has
    #: not resolved. Read from the same document as ``status``, so
    #: watching for a resolution costs no extra request.
    resolution: MarketResolution | None = None


@dataclass(frozen=True, slots=True)
class BookQuote:
    """A full book in canonical YES terms.

    ``ts`` is the venue's own book timestamp where the venue publishes
    one, and None where it does not. A None means the caller has to
    stamp the quote with local capture time, which is a real loss of
    fidelity and is recorded as such rather than papered over.
    """

    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    ts: datetime | None = None


@dataclass(frozen=True, slots=True)
class TradeTick:
    """One execution in canonical YES terms."""

    ts: datetime
    price: float
    size: float
    side: str | None
    trade_id: str


class LiveSource(abc.ABC):
    """A read-only view of one venue's public endpoints.

    Implementations translate into canonical YES terms and do no I/O of
    their own beyond calling the injected fetcher.
    """

    #: The name used on the command line, for example "kalshi".
    key: ClassVar[str]
    #: Written into every row's ``source`` column. It names the venue AND
    #: the transport, so a consumer can tell a polled tape from a
    #: streamed one without reading the capture's documentation.
    source_tag: ClassVar[str]

    @abc.abstractmethod
    def list_markets(self, *, limit: int, search: str | None = None) -> list[MarketRef]:
        """Return up to ``limit`` markets currently open on the venue."""

    @abc.abstractmethod
    def describe(self, market_id: str) -> MarketDescription:
        """Return the market's identity and lifecycle status."""

    @abc.abstractmethod
    def book(self, market_id: str) -> BookQuote:
        """Return the current full order book, in YES terms."""

    @abc.abstractmethod
    def trades(self, market_id: str, *, limit: int = 100) -> list[TradeTick]:
        """Return recent executions, in YES terms, oldest first."""


def _levels(mapping: dict[float, float], *, reverse: bool) -> tuple[BookLevel, ...]:
    return tuple(
        BookLevel(price=p, size=s) for p, s in sorted(mapping.items(), reverse=reverse) if s > 0
    )


def as_book(pairs: list[tuple[float, float]]) -> dict[float, float]:
    """Collapse (price, size) pairs into a rounded price-keyed book."""
    book: dict[float, float] = {}
    for price, size in pairs:
        key = round(price, PRICE_DECIMALS)
        book[key] = book.get(key, 0.0) + size
    return book


@dataclass
class BookTracker:
    """Turn consecutive full book snapshots into snapshot and delta events.

    A polling REST feed hands back the whole book every time, but the
    canonical schema wants a snapshot followed by per-level changes.
    The first sight of a market emits a snapshot; after that only levels
    whose size actually changed produce a delta, and a level that
    vanished produces a delta of size 0, which is exactly what the
    schema means by a removal.

    ``force_snapshot`` exists for the case that matters most: a poll
    failed, so the tracker's idea of the book may be stale by an unknown
    amount. Emitting deltas across that gap would assert changes that
    were never observed, so the next successful poll re-snapshots
    instead.
    """

    resnapshot_every: int = 0
    _bids: dict[str, dict[float, float]] = field(default_factory=dict)
    _asks: dict[str, dict[float, float]] = field(default_factory=dict)
    _polls: dict[str, int] = field(default_factory=dict)

    def drop(self, market_id: str) -> None:
        """Forget a market's book, so the next update re-snapshots."""
        self._bids.pop(market_id, None)
        self._asks.pop(market_id, None)
        self._polls.pop(market_id, None)

    def update(
        self,
        market_id: str,
        ts: datetime,
        quote: BookQuote,
        *,
        source: str,
        force_snapshot: bool = False,
    ) -> list[Event]:
        """Return the events implied by moving from the held book to ``quote``."""
        new_bids = as_book([(lv.price, lv.size) for lv in quote.bids])
        new_asks = as_book([(lv.price, lv.size) for lv in quote.asks])

        count = self._polls.get(market_id, 0)
        periodic = self.resnapshot_every > 0 and count > 0 and count % self.resnapshot_every == 0
        first = market_id not in self._bids
        self._polls[market_id] = count + 1

        if first or force_snapshot or periodic:
            self._bids[market_id] = new_bids
            self._asks[market_id] = new_asks
            return [
                OrderBookSnapshot(
                    seq=0,
                    ts=ts,
                    market_id=market_id,
                    source=source,
                    outcome="YES",
                    bids=_levels(new_bids, reverse=True),
                    asks=_levels(new_asks, reverse=False),
                )
            ]

        events: list[Event] = []
        for side, held, fresh in (
            ("bid", self._bids[market_id], new_bids),
            ("ask", self._asks[market_id], new_asks),
        ):
            for price in sorted(set(held) | set(fresh)):
                before = held.get(price, 0.0)
                after = fresh.get(price, 0.0)
                if before == after:
                    continue
                events.append(
                    BookDelta(
                        seq=0,
                        ts=ts,
                        market_id=market_id,
                        source=source,
                        outcome="YES",
                        side=side,
                        price=price,
                        size=after,
                    )
                )
        self._bids[market_id] = new_bids
        self._asks[market_id] = new_asks
        return events


class TradeDeduper:
    """Remember which venue trade ids have already been written.

    Polls overlap, so the same trade comes back several times. The set
    is bounded because a long capture would otherwise grow it without
    limit; the bound only has to outlive the overlap between two polls,
    and the default is far larger than any single page of trades.
    """

    def __init__(self, capacity: int = 20_000) -> None:
        self.capacity = capacity
        self._seen: OrderedDict[str, None] = OrderedDict()

    def __len__(self) -> int:
        return len(self._seen)

    def is_new(self, trade_id: str) -> bool:
        """Record ``trade_id`` and report whether it had not been seen."""
        if trade_id in self._seen:
            return False
        self._seen[trade_id] = None
        while len(self._seen) > self.capacity:
            self._seen.popitem(last=False)
        return True
