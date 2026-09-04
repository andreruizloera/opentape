from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

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

FIXTURES = Path(__file__).parent.parent / "examples" / "fixtures"


def ts(second: float) -> datetime:
    return datetime(2026, 3, 2, 14, 30, 0, tzinfo=UTC) + timedelta(seconds=second)


@pytest.fixture
def sample_events() -> list[Event]:
    """A small handcrafted tape covering every event type, with a ts tie."""
    return [
        Market(
            seq=0,
            ts=ts(0),
            market_id="M1",
            source="test",
            title="Test market",
            outcomes=("YES", "NO"),
        ),
        MarketStatus(seq=1, ts=ts(0), market_id="M1", source="test", status="open"),
        OrderBookSnapshot(
            seq=2,
            ts=ts(1),
            market_id="M1",
            source="test",
            outcome="YES",
            bids=(BookLevel(0.58, 500.0), BookLevel(0.57, 900.0)),
            asks=(BookLevel(0.60, 450.0), BookLevel(0.61, 700.0)),
        ),
        # Deliberate timestamp tie at t=2 across three event types: the
        # sequence number must decide replay order.
        Trade(
            seq=3,
            ts=ts(2),
            market_id="M1",
            source="test",
            outcome="YES",
            side="buy",
            price=0.60,
            size=100.0,
            trade_id="t-1",
        ),
        BookDelta(
            seq=4,
            ts=ts(2),
            market_id="M1",
            source="test",
            outcome="YES",
            side="ask",
            price=0.60,
            size=350.0,
        ),
        Trade(
            seq=5,
            ts=ts(2),
            market_id="M1",
            source="test",
            outcome="YES",
            side="buy",
            price=0.61,
            size=50.0,
            trade_id="t-2",
        ),
        Trade(
            seq=6,
            ts=ts(5),
            market_id="M1",
            source="test",
            outcome="YES",
            side="sell",
            price=0.58,
            size=80.0,
            trade_id="t-3",
        ),
        MarketStatus(seq=7, ts=ts(10), market_id="M1", source="test", status="closed"),
        Resolution(
            seq=8,
            ts=ts(11),
            market_id="M1",
            source="test",
            outcome="YES",
            settlement=1.0,
        ),
    ]


@pytest.fixture
def sample_tape(sample_events: list[Event]) -> Tape:
    return Tape.from_events(sample_events)
