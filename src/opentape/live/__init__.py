"""Live capture: fetch from a real venue and write canonical tapes.

Where the adapters in :mod:`opentape.adapters` convert a file someone
already has, the sources here read a venue's public endpoints directly.
Both ends produce the same schema, so a tape captured live and a tape
converted from a dump are the same kind of object.

Only endpoints that need no credentials are used. Adding an
authenticated venue means writing a :class:`LiveSource`; the fetcher is
injected, so nothing else has to change.
"""

from __future__ import annotations

from opentape.errors import LiveError
from opentape.live.base import (
    BookQuote,
    BookTracker,
    LiveSource,
    MarketDescription,
    MarketRef,
    TradeDeduper,
    TradeTick,
)
from opentape.live.daemon import (
    CaptureConfig,
    CaptureDaemon,
    CaptureStats,
    StreamConfig,
    StreamDaemon,
    StreamStats,
    parse_duration,
)
from opentape.live.http import Fetcher, HttpFetcher
from opentape.live.kalshi import KalshiLive
from opentape.live.polymarket import PolymarketLive
from opentape.live.polymarket_stream import PolymarketStream
from opentape.live.stream import BookMirror, StreamSource

#: Venues that can be captured, keyed by their command-line name.
SOURCES: dict[str, type[LiveSource]] = {
    KalshiLive.key: KalshiLive,
    PolymarketLive.key: PolymarketLive,
}

#: Venues that publish a public change stream. Kalshi is absent on
#: purpose and not by oversight: its websocket answers HTTP 401 to an
#: unauthenticated upgrade, so it cannot be captured without an API key
#: and this project only uses endpoints that need no credentials.
STREAM_SOURCES: dict[str, str] = {
    PolymarketStream.key: "polymarket",
}


def build_source(venue: str, fetch: Fetcher) -> LiveSource:
    """Construct the source registered under ``venue``."""
    try:
        cls = SOURCES[venue]
    except KeyError:
        known = ", ".join(sorted(SOURCES))
        raise LiveError(f"unknown venue {venue!r}; known venues: {known}") from None
    return cls(fetch)  # type: ignore[call-arg]


def build_stream_source(venue: str, fetch: Fetcher) -> StreamSource:
    """Construct the websocket source registered under ``venue``.

    A venue that has a REST source but no stream source is refused with
    the reason, because "not implemented" and "needs credentials this
    project does not use" are different answers and only one of them is
    worth waiting for.
    """
    if venue == PolymarketStream.key:
        return PolymarketStream(PolymarketLive(fetch))
    if venue in SOURCES:
        raise LiveError(
            f"{venue} has no public websocket this project can use: its stream endpoint "
            f"requires an API key, and opentape captures only unauthenticated endpoints. "
            f"Use --transport rest-poll for {venue}."
        )
    known = ", ".join(sorted(STREAM_SOURCES))
    raise LiveError(f"unknown venue {venue!r}; venues with a public stream: {known}")


__all__ = [
    "SOURCES",
    "STREAM_SOURCES",
    "BookMirror",
    "BookQuote",
    "BookTracker",
    "CaptureConfig",
    "CaptureDaemon",
    "CaptureStats",
    "Fetcher",
    "HttpFetcher",
    "KalshiLive",
    "LiveError",
    "LiveSource",
    "MarketDescription",
    "MarketRef",
    "PolymarketLive",
    "PolymarketStream",
    "StreamConfig",
    "StreamDaemon",
    "StreamSource",
    "StreamStats",
    "TradeDeduper",
    "TradeTick",
    "build_source",
    "build_stream_source",
    "parse_duration",
]
