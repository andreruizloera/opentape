"""OpenTape: a standard format and replay engine for prediction-market data."""

from __future__ import annotations

__version__ = "0.1.0"

from opentape.errors import AdapterError, OpenTapeError, SchemaError
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
from opentape.schema import SCHEMA_VERSION, TAPE_SCHEMA
from opentape.tape import Tape, TapeSummary

__all__ = [
    "SCHEMA_VERSION",
    "TAPE_SCHEMA",
    "AdapterError",
    "BookDelta",
    "BookLevel",
    "Event",
    "Market",
    "MarketStatus",
    "OpenTapeError",
    "OrderBookSnapshot",
    "Resolution",
    "SchemaError",
    "Tape",
    "TapeSummary",
    "Trade",
    "__version__",
]
