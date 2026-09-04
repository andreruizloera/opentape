"""Deterministic synthetic prediction-market tape generator.

Everything produced here is SYNTHETIC: no real exchange data is used or
imitated beyond generic microstructure shape. The generator is fully
seeded, so the same seed always yields the same tape, byte for byte at
the frame level. It exists so the quickstart works instantly and so
tests have realistic data.

Microstructure model, per market:

- the mid price follows a random walk with per-market drift, clamped
  to [0.02, 0.98], on an event clock with exponential inter-arrival
  times;
- a five-level ladder sits on each side of the mid at one-cent ticks;
  when the mid crosses a tick, the ladder shifts and emits book deltas
  (adds, removes, resizes);
- trades arrive at the touch with lognormal-ish sizes, aggressor side
  biased toward the drift;
- each market gets a burst window (news shock) with faster arrivals,
  higher volatility, and heavier trading;
- snapshots are emitted periodically so a reader can join mid-tape;
- markets that resolve emit a close status and a Resolution event.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from opentape.adapters._common import sequence_events
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

SOURCE = "synthetic"
TICK = 0.01
DEPTH = 5
SNAPSHOT_EVERY = 60

START = datetime(2026, 3, 2, 14, 30, 0, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class MarketSpec:
    market_id: str
    title: str
    p0: float
    drift: float  # per-step drift in price units
    steps: int
    mean_gap_s: float
    resolves_to: str | None  # "YES", "NO", or None (still open at tape end)
    burst_at: float  # fraction of steps at which the news burst hits


SPECS: tuple[MarketSpec, ...] = (
    MarketSpec(
        market_id="OT-FEDCUT-SEP26",
        title="Will the Fed cut rates at the September 2026 meeting?",
        p0=0.55,
        drift=0.00035,
        steps=900,
        mean_gap_s=6.0,
        resolves_to="YES",
        burst_at=0.62,
    ),
    MarketSpec(
        market_id="OT-RAINSEA-0902",
        title="Will it rain in Seattle on 2026-09-02?",
        p0=0.38,
        drift=-0.00030,
        steps=650,
        mean_gap_s=8.0,
        resolves_to="NO",
        burst_at=0.45,
    ),
    MarketSpec(
        market_id="OT-BTC-150K-Q3",
        title="Will BTC close above $150k on any day in Q3 2026?",
        p0=0.50,
        drift=0.0,
        steps=450,
        mean_gap_s=11.0,
        resolves_to=None,
        burst_at=0.70,
    ),
)


def _ladder(rng: random.Random, mid: float) -> tuple[dict[float, float], dict[float, float]]:
    """Fresh five-deep books around the mid, keyed by price."""
    # Highest whole tick strictly below the mid; clamped so the ladder
    # always has room on both sides.
    best_bid = round(int((mid - 1e-9) / TICK) * TICK, 2)
    best_bid = min(max(best_bid, TICK), round(1.0 - 2 * TICK, 2))
    bids: dict[float, float] = {}
    asks: dict[float, float] = {}
    for i in range(DEPTH):
        bp = round(best_bid - i * TICK, 2)
        ap = round(best_bid + TICK + i * TICK, 2)
        if bp >= TICK:
            bids[bp] = float(rng.randint(5, 40) * 10 * (1 + i))
        if ap <= 1.0 - TICK:
            asks[ap] = float(rng.randint(5, 40) * 10 * (1 + i))
    return bids, asks


def _book_tuple(levels: dict[float, float], *, reverse: bool) -> tuple[BookLevel, ...]:
    return tuple(
        BookLevel(price=p, size=s) for p, s in sorted(levels.items(), reverse=reverse) if s > 0
    )


def _emit_ladder_diff(
    old: dict[float, float],
    new: dict[float, float],
    *,
    side: str,
    ts: datetime,
    spec: MarketSpec,
    events: list[Event],
) -> None:
    for price in sorted(old):
        if price not in new:
            events.append(
                BookDelta(
                    seq=0,
                    ts=ts,
                    market_id=spec.market_id,
                    source=SOURCE,
                    outcome="YES",
                    side=side,
                    price=price,
                    size=0.0,
                )
            )
    for price, size in sorted(new.items()):
        if old.get(price) != size:
            events.append(
                BookDelta(
                    seq=0,
                    ts=ts,
                    market_id=spec.market_id,
                    source=SOURCE,
                    outcome="YES",
                    side=side,
                    price=price,
                    size=size,
                )
            )


def _simulate_market(rng: random.Random, spec: MarketSpec) -> list[Event]:
    events: list[Event] = []
    t = START + timedelta(seconds=rng.uniform(0, 30))
    mid = spec.p0

    events.append(
        Market(
            seq=0,
            ts=t,
            market_id=spec.market_id,
            source=SOURCE,
            title=spec.title,
            outcomes=("YES", "NO"),
        )
    )
    events.append(MarketStatus(seq=0, ts=t, market_id=spec.market_id, source=SOURCE, status="open"))

    bids, asks = _ladder(rng, mid)
    t += timedelta(seconds=rng.expovariate(1.0))
    events.append(
        OrderBookSnapshot(
            seq=0,
            ts=t,
            market_id=spec.market_id,
            source=SOURCE,
            outcome="YES",
            bids=_book_tuple(bids, reverse=True),
            asks=_book_tuple(asks, reverse=False),
        )
    )

    burst_start = int(spec.steps * spec.burst_at)
    burst_len = max(20, spec.steps // 12)
    trade_n = 0

    for step in range(spec.steps):
        in_burst = burst_start <= step < burst_start + burst_len
        gap = spec.mean_gap_s * (0.25 if in_burst else 1.0)
        t += timedelta(seconds=rng.expovariate(1.0 / gap))

        vol = 0.004 if in_burst else 0.0012
        drift = spec.drift * (4.0 if in_burst else 1.0)
        mid = min(0.98, max(0.02, mid + drift + rng.gauss(0.0, vol)))

        new_bids, new_asks = _ladder(rng, mid)
        # Keep unchanged-price levels mostly stable; resize a few.
        for book, new_book in ((bids, new_bids), (asks, new_asks)):
            for price in new_book:
                if price in book and rng.random() > 0.25:
                    new_book[price] = book[price]
        _emit_ladder_diff(bids, new_bids, side="bid", ts=t, spec=spec, events=events)
        _emit_ladder_diff(asks, new_asks, side="ask", ts=t, spec=spec, events=events)
        bids, asks = new_bids, new_asks

        trade_prob = 0.85 if in_burst else 0.35
        n_trades = (1 + (rng.random() < 0.5)) if rng.random() < trade_prob else 0
        for _ in range(n_trades):
            buy_bias = 0.5 + (0.25 if drift > 0 else -0.25 if drift < 0 else 0.0)
            is_buy = rng.random() < buy_bias
            price = min(asks) if is_buy else max(bids)
            size = float(max(1, round(rng.lognormvariate(2.8, 0.9))))
            trade_n += 1
            t += timedelta(milliseconds=rng.randint(1, 400))
            events.append(
                Trade(
                    seq=0,
                    ts=t,
                    market_id=spec.market_id,
                    source=SOURCE,
                    outcome="YES",
                    side="buy" if is_buy else "sell",
                    price=price,
                    size=size,
                    trade_id=f"{spec.market_id}-T{trade_n:05d}",
                )
            )

        if (step + 1) % SNAPSHOT_EVERY == 0:
            events.append(
                OrderBookSnapshot(
                    seq=0,
                    ts=t,
                    market_id=spec.market_id,
                    source=SOURCE,
                    outcome="YES",
                    bids=_book_tuple(bids, reverse=True),
                    asks=_book_tuple(asks, reverse=False),
                )
            )

    if spec.resolves_to is not None:
        t += timedelta(seconds=rng.uniform(30, 120))
        events.append(
            MarketStatus(seq=0, ts=t, market_id=spec.market_id, source=SOURCE, status="closed")
        )
        t += timedelta(seconds=rng.uniform(5, 30))
        events.append(
            Resolution(
                seq=0,
                ts=t,
                market_id=spec.market_id,
                source=SOURCE,
                outcome=spec.resolves_to,
                settlement=1.0 if spec.resolves_to == "YES" else 0.0,
            )
        )
    return events


def generate(seed: int = 42) -> Tape:
    """Generate the bundled synthetic tape. Deterministic per seed."""
    events: list[Event] = []
    for i, spec in enumerate(SPECS):
        # One independent stream per market so specs can change without
        # perturbing each other's randomness.
        events.extend(_simulate_market(random.Random(seed + i * 1000), spec))
    return Tape.from_events(sequence_events(events))
