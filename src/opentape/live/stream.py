"""The streaming seam: a venue's own change stream, in canonical terms.

:mod:`opentape.live.base` describes a source that is *asked* what the
book is. This module describes one that is *told*. The difference is the
whole point of a websocket transport, and it is worth stating precisely
because it changes what a tape means:

- A polled tape's ``BookDelta`` says "this level differs from the last
  time I looked". Anything that appeared and vanished between two polls
  is absent, and nothing in the file says how much was missed.
- A streamed tape's ``BookDelta`` says "the venue published this
  change". The sampling step is gone, so the tape is the venue's own
  sequence of changes rather than a reconstruction of it.

A :class:`StreamSource` is therefore split so that everything except the
socket is pure: it names a URL, builds a subscription string, and turns
one message of text into canonical updates. That last function is the
whole venue-specific surface, it takes a string and returns dataclasses,
and it is what the offline tests exercise against recorded frames.

:class:`BookMirror` is the other half. A change stream is only as good
as the book you build from it, so the mirror keeps that book and checks
it against the full snapshots the venue publishes anyway. A mismatch is
counted and reported rather than smoothed over: a stream that has
silently drifted is exactly the failure a streamed tape is vulnerable to
and a polled one is not.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime
from typing import ClassVar

from opentape.events import BookDelta, BookLevel, Event, OrderBookSnapshot
from opentape.live.base import PRICE_DECIMALS, BookQuote, MarketDescription, TradeTick


@dataclass(frozen=True, slots=True)
class StreamBook:
    """A full book the venue published, in canonical YES terms."""

    market_id: str
    ts: datetime
    quote: BookQuote


@dataclass(frozen=True, slots=True)
class StreamLevel:
    """One level the venue changed, in canonical YES terms.

    ``size`` is the level's new total size, not an increment, and zero
    means the level is gone. Venues differ on this and getting it
    backwards silently corrupts every book built from the stream, so a
    source is required to normalize to "absolute" here and to say in its
    own docstring how it knows.
    """

    market_id: str
    ts: datetime
    side: str
    price: float
    size: float


@dataclass(frozen=True, slots=True)
class StreamTrade:
    """One execution the venue published, in canonical YES terms."""

    market_id: str
    tick: TradeTick


StreamUpdate = StreamBook | StreamLevel | StreamTrade


class StreamSource(abc.ABC):
    """A read-only subscription to one venue's public change stream.

    Implementations do no I/O. The daemon owns the connection and hands
    each message here as text, which is what makes a recorded session
    replayable through exactly the code that runs live.
    """

    #: The name used on the command line, for example "polymarket".
    key: ClassVar[str]
    #: Written into every row's ``source`` column, naming the venue AND
    #: the transport so a consumer can tell a streamed tape from a
    #: polled one without reading the capture's documentation.
    source_tag: ClassVar[str]

    @abc.abstractmethod
    def stream_url(self) -> str:
        """The websocket endpoint to connect to."""

    @abc.abstractmethod
    def describe(self, market_id: str) -> MarketDescription:
        """Return the market's identity and status, before subscribing."""

    @abc.abstractmethod
    def subscribe_message(self, market_ids: list[str]) -> str:
        """The text that subscribes to ``market_ids`` once connected."""

    @abc.abstractmethod
    def parse(self, message: str) -> list[StreamUpdate]:
        """Turn one message of venue text into canonical updates.

        Pure: no sockets, no clock, no state beyond what the source
        resolved before subscribing. A message carrying nothing this
        capture cares about returns an empty list rather than raising,
        because a feed is free to publish event types a consumer did
        not ask about.
        """


def _levels(mapping: dict[float, float], *, reverse: bool) -> tuple[BookLevel, ...]:
    return tuple(
        BookLevel(price=p, size=s) for p, s in sorted(mapping.items(), reverse=reverse) if s > 0
    )


@dataclass
class MirrorCheck:
    """The result of comparing the mirrored book to a venue snapshot."""

    market_id: str
    agreed: bool
    #: Levels where the two disagreed, as price -> (mirrored, published).
    differences: dict[str, dict[float, tuple[float, float]]] = field(default_factory=dict)

    def summary(self) -> str:
        if self.agreed:
            return f"{self.market_id}: mirrored book matches the venue snapshot"
        counts = ", ".join(f"{len(v)} {k}" for k, v in self.differences.items() if v)
        return f"{self.market_id}: mirrored book DIVERGED from the venue snapshot ({counts})"


@dataclass
class BookMirror:
    """Hold the book a stream describes, and check it against snapshots.

    The mirror exists to answer one question a streamed tape cannot
    answer on its own: is the sequence of changes actually enough to
    rebuild the book? Polymarket publishes a full book periodically as
    well as every level change, so the answer is checkable for free,
    every time a snapshot arrives.

    A level update for a market with no mirrored book yet is DROPPED, not
    applied to an empty book. Building a book from a change stream joined
    mid-flight would produce a file that looks like a book and is
    missing every level nobody happened to touch, which is worse than an
    honest gap. Nothing is emitted for that market until its first
    snapshot arrives.
    """

    _bids: dict[str, dict[float, float]] = field(default_factory=dict)
    _asks: dict[str, dict[float, float]] = field(default_factory=dict)

    def has(self, market_id: str) -> bool:
        return market_id in self._bids

    def drop(self, market_id: str) -> None:
        """Forget a market's book, so the next snapshot starts it over."""
        self._bids.pop(market_id, None)
        self._asks.pop(market_id, None)

    def snapshot(self, book: StreamBook, *, source: str) -> tuple[list[Event], MirrorCheck | None]:
        """Adopt a published book, checking it against what was mirrored.

        The published snapshot always wins. It is what the venue says
        the book is, and the mirror is only a reconstruction of it.
        """
        fresh_bids = {round(lv.price, PRICE_DECIMALS): lv.size for lv in book.quote.bids}
        fresh_asks = {round(lv.price, PRICE_DECIMALS): lv.size for lv in book.quote.asks}
        check: MirrorCheck | None = None
        if self.has(book.market_id):
            check = self._compare(book.market_id, fresh_bids, fresh_asks)
        self._bids[book.market_id] = fresh_bids
        self._asks[book.market_id] = fresh_asks
        event = OrderBookSnapshot(
            seq=0,
            ts=book.ts,
            market_id=book.market_id,
            source=source,
            outcome="YES",
            bids=_levels(fresh_bids, reverse=True),
            asks=_levels(fresh_asks, reverse=False),
        )
        return [event], check

    def _compare(
        self, market_id: str, bids: dict[float, float], asks: dict[float, float]
    ) -> MirrorCheck:
        differences: dict[str, dict[float, tuple[float, float]]] = {"bid": {}, "ask": {}}
        for side, held, published in (
            ("bid", self._bids[market_id], bids),
            ("ask", self._asks[market_id], asks),
        ):
            live = {p: s for p, s in held.items() if s > 0}
            fresh = {p: s for p, s in published.items() if s > 0}
            for price in set(live) | set(fresh):
                mine, theirs = live.get(price, 0.0), fresh.get(price, 0.0)
                if mine != theirs:
                    differences[side][price] = (mine, theirs)
        agreed = not differences["bid"] and not differences["ask"]
        return MirrorCheck(market_id=market_id, agreed=agreed, differences=differences)

    def level(self, change: StreamLevel, *, source: str) -> list[Event]:
        """Apply one published level change and return the event it is."""
        if not self.has(change.market_id):
            return []
        side = self._bids if change.side == "bid" else self._asks
        book = side[change.market_id]
        price = round(change.price, PRICE_DECIMALS)
        if book.get(price, 0.0) == change.size:
            # The venue re-published a level at the size it already had.
            # Writing a delta for it would claim a change that did not
            # happen, on a tape whose whole promise is the opposite.
            return []
        if change.size <= 0:
            book.pop(price, None)
        else:
            book[price] = change.size
        return [
            BookDelta(
                seq=0,
                ts=change.ts,
                market_id=change.market_id,
                source=source,
                outcome="YES",
                side=change.side,
                price=price,
                size=max(change.size, 0.0),
            )
        ]
