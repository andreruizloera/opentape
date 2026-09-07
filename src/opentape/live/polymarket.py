"""Polymarket live source, over the public unauthenticated REST endpoints.

Endpoints used, all readable without an API key as of 2026-09-06:

    GET clob.polymarket.com/sampling-markets      market discovery
    GET clob.polymarket.com/markets/{condition}   question and tokens
    GET clob.polymarket.com/book?token_id=...     full book for one token
    GET gamma-api.polymarket.com/markets?slug=... slug to condition id
    GET data-api.polymarket.com/trades?market=... public executions

All of these answer HTTP 403 to a request that sends no User-Agent,
which the shared fetcher always sends.

Canonical mapping, matching the ``polymarket-style`` file adapter:

- A market is identified by its slug where it has one, because a slug
  is readable in a replay; the condition id is accepted as input too
  and is used as the id when no slug exists.
- Only two-outcome markets are captured, since schema v1 describes
  binary markets. The outcomes need not be spelled Yes and No: Up/Down
  and two team names are equally binary, so the first outcome becomes
  the YES side and both names are written to the tape's market row.
  A market with any other number of outcomes is refused by name rather
  than silently mangled.
- The YES token's book is already in YES terms and is taken as is.
- A trade on the NO token is the same trade seen from the other side: a
  BUY of NO at ``p`` is a SELL of YES at ``1 - p``. Both the price and
  the side are converted, so a tape holds one consistent YES-terms
  trade stream rather than two half-streams that cannot be compared.
"""

from __future__ import annotations

from typing import Any, ClassVar

from opentape.adapters._common import parse_ts
from opentape.errors import LiveError
from opentape.events import BookLevel
from opentape.live.base import (
    PRICE_DECIMALS,
    BookQuote,
    LiveSource,
    MarketDescription,
    MarketRef,
    TradeTick,
)
from opentape.live.http import Fetcher

CLOB_URL = "https://clob.polymarket.com"
GAMMA_URL = "https://gamma-api.polymarket.com"
DATA_URL = "https://data-api.polymarket.com"


def _price(value: Any, *, context: str) -> float:
    try:
        price = float(value)
    except (TypeError, ValueError):
        raise LiveError(f"{context}: bad price {value!r}") from None
    if not 0.0 <= price <= 1.0:
        raise LiveError(f"{context}: price {price} is outside [0, 1]")
    return round(price, PRICE_DECIMALS)


def _size(value: Any, *, context: str) -> float:
    try:
        size = float(value)
    except (TypeError, ValueError):
        raise LiveError(f"{context}: bad size {value!r}") from None
    if size < 0:
        raise LiveError(f"{context}: size {size} is negative")
    return size


class _Resolved:
    """A market id resolved to the ids and the outcome mapping it needs.

    Two-outcome markets on Polymarket are not all spelled Yes/No: the
    high-volume crypto series is Up/Down, and a sports market is often
    two team names. All of them are binary and complementary, so all of
    them fit schema v1. The first outcome becomes the YES side and the
    pair of names is recorded on the tape's market row, which is what
    ``Market.outcomes`` is for, so a reader can always recover what YES
    meant. A Yes/No market is pinned explicitly rather than by
    position, so "YES" never depends on the order the venue happened to
    list the tokens in.
    """

    __slots__ = (
        "condition_id",
        "market_id",
        "no_token",
        "outcomes",
        "question",
        "raw",
        "yes_token",
    )

    def __init__(self, raw: dict[str, Any]) -> None:
        self.raw = raw
        self.condition_id = str(raw.get("condition_id") or "")
        slug = str(raw.get("market_slug") or "")
        self.market_id = slug or self.condition_id
        self.question = str(raw.get("question") or self.market_id)
        tokens = raw.get("tokens")
        if not isinstance(tokens, list) or len(tokens) != 2:
            count = len(tokens) if isinstance(tokens, list) else 0
            raise LiveError(
                f"polymarket: market {self.market_id!r} has {count} outcome tokens, not 2; "
                f"schema v1 describes binary markets only, so this one cannot be captured"
            )
        names = [str(t.get("outcome") or "").strip() for t in tokens]
        lowered = [n.lower() for n in names]
        if "yes" in lowered and "no" in lowered:
            order = [lowered.index("yes"), lowered.index("no")]
        else:
            order = [0, 1]
        yes_token, no_token = (tokens[i] for i in order)
        self.outcomes = tuple(names[i] or ("YES", "NO")[j] for j, i in enumerate(order))
        self.yes_token = str(yes_token.get("token_id") or "")
        self.no_token = str(no_token.get("token_id") or "")
        if not self.yes_token or not self.no_token:
            raise LiveError(f"polymarket: market {self.market_id!r} is missing an outcome token id")


class PolymarketLive(LiveSource):
    """Read Polymarket's public market data through an injected fetcher."""

    key: ClassVar[str] = "polymarket"
    source_tag: ClassVar[str] = "polymarket-rest-poll"

    def __init__(
        self,
        fetch: Fetcher,
        *,
        clob_url: str = CLOB_URL,
        gamma_url: str = GAMMA_URL,
        data_url: str = DATA_URL,
    ) -> None:
        self._fetch = fetch
        self._clob = clob_url.rstrip("/")
        self._gamma = gamma_url.rstrip("/")
        self._data = data_url.rstrip("/")
        self._cache: dict[str, _Resolved] = {}

    # -- discovery ---------------------------------------------------------

    def list_markets(self, *, limit: int, search: str | None = None) -> list[MarketRef]:
        doc = self._fetch(f"{self._clob}/sampling-markets")
        data = doc.get("data") if isinstance(doc, dict) else None
        if not isinstance(data, list):
            raise LiveError("polymarket: sampling-markets did not return a 'data' array")
        needle = (search or "").lower()
        found: list[MarketRef] = []
        for market in data:
            if not isinstance(market, dict):
                continue
            if not market.get("active") or market.get("closed"):
                continue
            question = str(market.get("question") or "")
            slug = str(market.get("market_slug") or market.get("condition_id") or "")
            if needle and needle not in question.lower() and needle not in slug.lower():
                continue
            found.append(
                MarketRef(
                    market_id=slug,
                    title=question,
                    detail=str(market.get("end_date_iso") or ""),
                )
            )
            if len(found) >= limit:
                break
        return found

    # -- resolution --------------------------------------------------------

    def _resolve(self, market_id: str) -> _Resolved:
        if market_id in self._cache:
            return self._cache[market_id]
        condition = market_id
        if not market_id.startswith("0x"):
            condition = self._condition_from_slug(market_id)
        doc = self._fetch(f"{self._clob}/markets/{condition}")
        if not isinstance(doc, dict) or not doc.get("condition_id"):
            raise LiveError(f"polymarket: no market found for {market_id!r}")
        resolved = _Resolved(doc)
        self._cache[market_id] = resolved
        self._cache[resolved.market_id] = resolved
        return resolved

    def _condition_from_slug(self, slug: str) -> str:
        doc = self._fetch(f"{self._gamma}/markets", {"slug": slug})
        entries = doc if isinstance(doc, list) else None
        if not entries:
            raise LiveError(
                f"polymarket: no market has slug {slug!r}; pass a slug from "
                f"'opentape markets --venue polymarket' or a 0x condition id"
            )
        condition = str(entries[0].get("conditionId") or "")
        if not condition:
            raise LiveError(f"polymarket: market {slug!r} has no condition id")
        return condition

    # -- data --------------------------------------------------------------

    def describe(self, market_id: str) -> MarketDescription:
        resolved = self._resolve(market_id)
        raw = resolved.raw
        if raw.get("closed"):
            status = "closed"
        elif not raw.get("active") or not raw.get("accepting_orders", True):
            status = "halted"
        else:
            status = "open"
        return MarketDescription(
            market_id=resolved.market_id,
            title=resolved.question,
            status=status,
            outcomes=resolved.outcomes,
        )

    def book(self, market_id: str) -> BookQuote:
        resolved = self._resolve(market_id)
        doc = self._fetch(f"{self._clob}/book", {"token_id": resolved.yes_token})
        if not isinstance(doc, dict):
            raise LiveError(f"polymarket: unexpected book response for {market_id!r}")
        context = f"polymarket book {resolved.market_id}"
        bids = self._levels(doc.get("bids"), context=f"{context} bids", reverse=True)
        asks = self._levels(doc.get("asks"), context=f"{context} asks", reverse=False)
        ts = None
        raw_ts = doc.get("timestamp")
        if raw_ts not in (None, ""):
            ts = parse_ts(int(raw_ts), context=f"{context} timestamp")
        return BookQuote(bids=bids, asks=asks, ts=ts)

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
        levels.sort(key=lambda lv: lv.price, reverse=reverse)
        return tuple(levels)

    def trades(self, market_id: str, *, limit: int = 100) -> list[TradeTick]:
        resolved = self._resolve(market_id)
        doc = self._fetch(f"{self._data}/trades", {"market": resolved.condition_id, "limit": limit})
        if not isinstance(doc, list):
            raise LiveError(f"polymarket: trades for {market_id!r} did not return an array")
        ticks: list[TradeTick] = []
        for i, trade in enumerate(doc):
            context = f"polymarket trades {resolved.market_id}[{i}]"
            if not isinstance(trade, dict):
                raise LiveError(f"{context}: expected an object")
            asset = str(trade.get("asset") or "")
            if asset not in (resolved.yes_token, resolved.no_token):
                # A trade on some other token is not this market's.
                continue
            side = str(trade.get("side") or "").upper()
            if side not in ("BUY", "SELL"):
                raise LiveError(f"{context}: side must be BUY or SELL, got {trade.get('side')!r}")
            price = _price(trade.get("price"), context=context)
            if asset == resolved.no_token:
                price = round(1.0 - price, PRICE_DECIMALS)
                side = "SELL" if side == "BUY" else "BUY"
            ticks.append(
                TradeTick(
                    ts=parse_ts(trade.get("timestamp"), context=context),
                    price=price,
                    size=_size(trade.get("size"), context=context),
                    side=side.lower(),
                    trade_id=_trade_key(trade, asset),
                )
            )
        ticks.sort(key=lambda t: t.ts)
        return ticks


def _trade_key(trade: dict[str, Any], asset: str) -> str:
    """A dedupe key for a feed that publishes no trade id.

    The public trades endpoint returns no per-fill identifier, so the
    key is composed from the fields that identify a fill: the
    transaction, the taker, the token, the size, and the price. Two
    genuinely separate fills that agree on every one of those, in one
    transaction, would collapse into one. That is the known cost of the
    endpoint not carrying an id, and it is preferred to the alternative
    of writing the same trade once per poll.
    """
    parts = (
        str(trade.get("transactionHash") or ""),
        str(trade.get("proxyWallet") or ""),
        asset,
        str(trade.get("size") or ""),
        str(trade.get("price") or ""),
    )
    return "|".join(parts)
