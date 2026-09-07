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
from opentape.live.daemon import CaptureConfig, CaptureDaemon, CaptureStats, parse_duration
from opentape.live.http import Fetcher, HttpFetcher
from opentape.live.kalshi import KalshiLive
from opentape.live.polymarket import PolymarketLive

#: Venues that can be captured, keyed by their command-line name.
SOURCES: dict[str, type[LiveSource]] = {
    KalshiLive.key: KalshiLive,
    PolymarketLive.key: PolymarketLive,
}


def build_source(venue: str, fetch: Fetcher) -> LiveSource:
    """Construct the source registered under ``venue``."""
    try:
        cls = SOURCES[venue]
    except KeyError:
        known = ", ".join(sorted(SOURCES))
        raise LiveError(f"unknown venue {venue!r}; known venues: {known}") from None
    return cls(fetch)  # type: ignore[call-arg]


__all__ = [
    "SOURCES",
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
    "TradeDeduper",
    "TradeTick",
    "build_source",
    "parse_duration",
]
