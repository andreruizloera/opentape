"""Polymarket live source over the public market websocket.

    wss://ws-subscriptions-clob.polymarket.com/ws/market

Unauthenticated, like the REST endpoints in
:mod:`opentape.live.polymarket`, which this source reuses to turn a slug
into the market's two outcome tokens before it subscribes. Discovery and
identity stay on REST because they are one-shot questions; the websocket
is only for what changes.

The channel publishes three event types this source understands, and the
shapes below were recorded from the live feed on 2026-09-07 rather than
read out of documentation:

``book``
    A full book for one token: ``bids``, ``asks``, an epoch-millisecond
    ``timestamp`` as a string, and a ``hash``. Sent on subscribe and
    again periodically.
``price_change``
    A ``price_changes`` array, each entry naming an ``asset_id``, a
    ``price``, a ``size``, and a ``side`` of BUY or SELL. One message
    carries the change for BOTH outcome tokens, expressed twice: a YES
    bid at 0.62 and a NO ask at 0.38 are the same resting interest.
``last_trade_price``
    One execution: ``price``, ``size``, ``side``, and a
    ``transaction_hash``.

SIZE IS ABSOLUTE, NOT AN INCREMENT, and that was established by
experiment rather than assumed. A recorded session was replayed twice,
once reading ``size`` as the level's new total and once as a delta to
add, and each reconstruction was compared against the next full ``book``
the venue published. The absolute reading reproduced the venue's own
snapshot exactly on both outcome tokens across thirteen changes each;
the delta reading matched neither. :class:`~opentape.live.stream.BookMirror`
keeps making that comparison during a live capture, so a future change in
the venue's semantics shows up as a reported divergence rather than as a
quietly wrong tape.

Canonical mapping, matching the REST source exactly so the two transports
produce comparable tapes:

- The first outcome is the YES side, with Yes/No pinned by name when the
  market uses those words, and both names written to the tape's market
  row.
- The book and its level changes are taken from the YES token only. The
  NO token's copy of a change is the same resting interest seen from the
  other side, so reading both would write every change twice.
- Trades are taken from BOTH tokens, because a fill is published against
  the token it executed on. A NO buy at ``p`` is a YES sell at ``1 - p``,
  converted here, and the dedupe key folds the two publications of one
  fill together if the venue sends both.
"""

from __future__ import annotations

import json
from typing import Any, ClassVar

from opentape.adapters._common import parse_ts
from opentape.errors import LiveError
from opentape.events import BookLevel
from opentape.live.base import PRICE_DECIMALS, BookQuote, MarketDescription, TradeTick
from opentape.live.polymarket import PolymarketLive, _Resolved
from opentape.live.stream import StreamBook, StreamLevel, StreamSource, StreamTrade, StreamUpdate

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def _number(value: Any, *, context: str, low: float | None, high: float | None) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise LiveError(f"{context}: bad number {value!r}") from None
    if low is not None and number < low:
        raise LiveError(f"{context}: {number} is below {low}")
    if high is not None and number > high:
        raise LiveError(f"{context}: {number} is above {high}")
    return number


def _price(value: Any, *, context: str) -> float:
    return round(_number(value, context=context, low=0.0, high=1.0), PRICE_DECIMALS)


def _size(value: Any, *, context: str) -> float:
    return _number(value, context=context, low=0.0, high=None)


def _ts(value: Any, *, context: str):
    """Parse the feed's epoch-millisecond timestamp, which is a string."""
    if isinstance(value, str):
        try:
            value = int(value)
        except ValueError:
            raise LiveError(f"{context}: bad timestamp {value!r}") from None
    return parse_ts(value, context=context)


class PolymarketStream(StreamSource):
    """Subscribe to Polymarket's public market channel."""

    key: ClassVar[str] = "polymarket"
    source_tag: ClassVar[str] = "polymarket-ws"

    def __init__(self, rest: PolymarketLive, *, url: str = WS_URL) -> None:
        self._rest = rest
        self._url = url
        # asset id -> (market id, is the YES token), filled by describe().
        self._assets: dict[str, tuple[str, bool]] = {}
        self._resolved: dict[str, _Resolved] = {}

    # -- setup -------------------------------------------------------------

    def stream_url(self) -> str:
        return self._url

    def describe(self, market_id: str) -> MarketDescription:
        """Resolve the market over REST and remember its two token ids."""
        described = self._rest.describe(market_id)
        resolved = self._rest._resolve(market_id)
        self._resolved[described.market_id] = resolved
        self._assets[resolved.yes_token] = (described.market_id, True)
        self._assets[resolved.no_token] = (described.market_id, False)
        return described

    def subscribe_message(self, market_ids: list[str]) -> str:
        """Subscribe to both outcome tokens of every resolved market.

        Both, not just YES: a fill is published against the token it
        executed on, so subscribing to YES alone would miss every trade
        that happened to be written as a NO order.
        """
        assets = [
            token
            for market_id in market_ids
            for resolved in [self._resolved.get(market_id)]
            if resolved is not None
            for token in (resolved.yes_token, resolved.no_token)
        ]
        if not assets:
            raise LiveError(
                "polymarket: no market was resolved before subscribing; "
                "describe() must run first so the outcome tokens are known"
            )
        return json.dumps({"assets_ids": assets, "type": "market"})

    # -- parsing -----------------------------------------------------------

    def parse(self, message: str) -> list[StreamUpdate]:
        """Turn one websocket message into canonical updates.

        The channel sends a JSON array of events on subscribe and bare
        objects afterwards, and it sends empty text frames as a
        keepalive. All three are ordinary and none of them is an error.
        """
        text = message.strip()
        if not text:
            return []
        try:
            document = json.loads(text)
        except ValueError as exc:
            raise LiveError(f"polymarket stream: message is not JSON: {exc}") from exc
        events = document if isinstance(document, list) else [document]
        updates: list[StreamUpdate] = []
        for event in events:
            if isinstance(event, dict):
                updates.extend(self._one(event))
        return updates

    def _one(self, event: dict[str, Any]) -> list[StreamUpdate]:
        kind = event.get("event_type")
        if kind == "book":
            return self._book(event)
        if kind == "price_change":
            return self._price_change(event)
        if kind == "last_trade_price":
            return self._trade(event)
        # tick_size_change and anything the venue adds later.
        return []

    def _market_of(self, asset_id: Any) -> tuple[str, bool] | None:
        """The market and side an asset id belongs to, or None if untracked."""
        return self._assets.get(str(asset_id or ""))

    def _book(self, event: dict[str, Any]) -> list[StreamUpdate]:
        known = self._market_of(event.get("asset_id"))
        if known is None:
            return []
        market_id, is_yes = known
        if not is_yes:
            # The NO book is the YES book mirrored; taking both would
            # write one market's book twice.
            return []
        context = f"polymarket stream book {market_id}"
        return [
            StreamBook(
                market_id=market_id,
                ts=_ts(event.get("timestamp"), context=f"{context} timestamp"),
                quote=BookQuote(
                    bids=self._levels(event.get("bids"), context=f"{context} bids", reverse=True),
                    asks=self._levels(event.get("asks"), context=f"{context} asks", reverse=False),
                ),
            )
        ]

    @staticmethod
    def _levels(raw: Any, *, context: str, reverse: bool) -> tuple[BookLevel, ...]:
        if raw is None:
            return ()
        if not isinstance(raw, list):
            raise LiveError(f"{context}: expected an array of levels")
        levels: list[BookLevel] = []
        for entry in raw:
            if not isinstance(entry, dict):
                raise LiveError(f"{context}: bad level {entry!r}, expected an object")
            size = _size(entry.get("size"), context=context)
            if size <= 0:
                continue
            levels.append(BookLevel(price=_price(entry.get("price"), context=context), size=size))
        levels.sort(key=lambda level: level.price, reverse=reverse)
        return tuple(levels)

    def _price_change(self, event: dict[str, Any]) -> list[StreamUpdate]:
        changes = event.get("price_changes")
        if not isinstance(changes, list):
            raise LiveError("polymarket stream: price_change has no price_changes array")
        updates: list[StreamUpdate] = []
        for entry in changes:
            if not isinstance(entry, dict):
                raise LiveError("polymarket stream: bad price_changes entry")
            known = self._market_of(entry.get("asset_id"))
            if known is None:
                continue
            market_id, is_yes = known
            if not is_yes:
                # Already carried by this same message's YES entry.
                continue
            context = f"polymarket stream price_change {market_id}"
            side = str(entry.get("side") or "").upper()
            if side not in ("BUY", "SELL"):
                raise LiveError(f"{context}: side must be BUY or SELL, got {entry.get('side')!r}")
            # A resting BUY is a bid and a resting SELL is an ask, and
            # this is the YES token, so no complement is needed.
            updates.append(
                StreamLevel(
                    market_id=market_id,
                    ts=_ts(event.get("timestamp"), context=f"{context} timestamp"),
                    side="bid" if side == "BUY" else "ask",
                    price=_price(entry.get("price"), context=context),
                    size=_size(entry.get("size"), context=context),
                )
            )
        return updates

    def _trade(self, event: dict[str, Any]) -> list[StreamUpdate]:
        known = self._market_of(event.get("asset_id"))
        if known is None:
            return []
        market_id, is_yes = known
        context = f"polymarket stream trade {market_id}"
        side = str(event.get("side") or "").upper()
        if side not in ("BUY", "SELL"):
            raise LiveError(f"{context}: side must be BUY or SELL, got {event.get('side')!r}")
        price = _price(event.get("price"), context=context)
        if not is_yes:
            price = round(1.0 - price, PRICE_DECIMALS)
            side = "SELL" if side == "BUY" else "BUY"
        size = _size(event.get("size"), context=context)
        return [
            StreamTrade(
                market_id=market_id,
                tick=TradeTick(
                    ts=_ts(event.get("timestamp"), context=f"{context} timestamp"),
                    price=price,
                    size=size,
                    side=side.lower(),
                    trade_id=_trade_key(event, price, size, side),
                ),
            )
        ]


def _trade_key(event: dict[str, Any], price: float, size: float, side: str) -> str:
    """A dedupe key for a feed that publishes no per-fill id.

    Built from the YES-terms view of the fill rather than the raw one, so
    that if the venue publishes a fill twice, once against each outcome
    token, both publications reduce to the same key and the tape holds
    the trade once. Two genuinely separate fills in one transaction that
    agree on price, size, and side would collapse into one; that is the
    same known cost the REST source documents for the same missing id.
    """
    return "|".join(
        (
            str(event.get("transaction_hash") or ""),
            f"{price:.6f}",
            f"{size:.6f}",
            side.lower(),
        )
    )
